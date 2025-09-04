#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CWBIO LIMS(科研服务) 数据上传服务

该模块提供读取数据文件、处理记录以及使用正确的身份验证和重试机制将记录上传到 LIMS API 的功能。
作者: Linji Li
日期: 2025-05-23
版本: 1.0.1
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import hashlib
import configparser
from pathlib import Path
from enum import Enum
from typing import List, Dict, Any, Optional, Union, Tuple
from urllib.parse import quote
from threading import Lock
from collections import defaultdict
from dataclasses import dataclass, asdict, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('cwbio_lims_push.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)


# --- 实用函数 ---

def md5(text: str) -> str:
    """创建给定文本的 MD5 哈希值。"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def generate_sign(appid: str, appsecret: str) -> str:
    """根据 appid 和 appsecret 生成签名。"""
    text = f"appid={appid}&appsecret={appsecret}"
    return md5(text)


def calculate_backoff_delay(attempt: int, initial_delay_ms: int, multiplier: float, max_delay_ms: int) -> float:
    """
    计算带抖动的退避延迟。
    
    参数:
        attempt: 当前尝试次数 (从 0 开始)
        initial_delay_ms: 初始延迟 (毫秒)
        multiplier: 退避乘数
        max_delay_ms: 最大延迟 (毫秒)
    
    返回:
        float: 计算出的延迟 (毫秒)
    """
    delay = initial_delay_ms * (multiplier ** attempt)
    # 添加抖动以避免惊群效应
    jitter = random.uniform(0, delay / 2)
    delay += jitter
    # 确保不超过最大延迟
    return min(delay, max_delay_ms)


def perform_retry_delay(delay_ms: float, attempt: int, max_retries: int, error_context: Any = None) -> None:
    """
    在重试期间休眠指定的延迟时间。
    
    参数:
        delay_ms: 延迟时间 (毫秒)
        attempt: 当前尝试次数 (从 0 开始)
        max_retries: 最大重试次数
        error_context: 可选的错误上下文，用于日志记录
    """
    if error_context:
        context_str = str(error_context)
        if len(context_str) > 100:  # 截断过长的错误消息
            context_str = f"{context_str[:100]}..."
        logger.warning(
            f"请求失败 (尝试 {attempt + 1}/{max_retries}), "
            f"将在 {delay_ms:.0f} 毫秒后重试。上下文: {context_str}"
        )
    else:
        logger.warning(
            f"请求失败 (尝试 {attempt + 1}/{max_retries}), "
            f"将在 {delay_ms:.0f} 毫秒后重试。"
        )
    
    try:
        time.sleep(delay_ms / 1000)  # 将毫秒转换为秒
    except (InterruptedError, KeyboardInterrupt):
        # 处理休眠期间的潜在中断
        logger.warning("重试延迟期间休眠被中断")
        raise


# --- 异常类 ---

class LimsException(Exception):
    """LIMS 相关错误的基类异常。"""
    pass


class RetryableException(LimsException):
    """可重试的异常。"""
    pass


class ResponseValidationException(LimsException):
    """响应验证错误的异常。"""
    pass


class LimsProcessingException(LimsException):
    """处理错误的异常。"""
    pass


class LimsFileException(LimsException):
    """文件相关错误的异常。"""
    pass


class HttpException(LimsException):
    """HTTP 相关错误的异常。"""
    def __init__(self, message, error_code=None, cause=None):
        super().__init__(message)
        self.error_code = error_code
        self.cause = cause
    
    @property
    def status_code(self):
        return self.error_code.code if self.error_code else None


# --- 模型类 ---

class ErrorCode(Enum):
    """错误码枚举。"""
    SUCCESS = (200, "成功", False)
    INVALID_AUTH = (201, "appid或appsecret不合法", False)
    NO_DATA = (202, "查无数据", False)
    UPLOAD_FAILED = (203, "上传失败", True)
    TOO_MANY_REQUESTS = (429, "请求过多", True)
    INTERNAL_ERROR = (500, "服务器内部错误", True)
    BAD_GATEWAY = (502, "网关错误", True)
    SERVICE_UNAVAILABLE = (503, "服务不可用", True)
    GATEWAY_TIMEOUT = (504, "网关超时", True)
    
    def __init__(self, code, message, retryable):
        self.code = code
        self.message = message
        self.retryable = retryable
    
    @staticmethod
    def from_code(code):
        """从数字代码获取 ErrorCode。"""
        for error_code in ErrorCode:
            if error_code.code == code:
                return error_code
        return None


@dataclass
class RetryConfig:
    """重试行为配置。"""
    max_retries: int = 3
    initial_delay_ms: int = 1000
    backoff_multiplier: float = 2.0
    max_delay_ms: int = 10000


@dataclass
class DataRecord:
    """表示要发送到 LIMS API 的数据记录。"""
    detect_no: str
    status: str
    report_path: str
    report_reason: Optional[str] = None
    report_message: Optional[Dict[str, str]] = None

    def validate(self):
        """验证数据记录字段和业务规则。"""
        # 验证必填字段
        self._validate_required_fields()
        # 验证业务规则
        self._validate_business_rules()

    def _validate_required_fields(self):
        """验证必填字段是否存在且不为空。"""
        if not self.detect_no or not self.detect_no.strip():
            raise ValueError("detect_no 不能为空")
        if not self.status or not self.status.strip():
            raise ValueError("status 不能为空")
        if not self.report_path or not self.report_path.strip():
            raise ValueError("report_path 不能为空")

    def _validate_business_rules(self):
        """验证字段的业务规则。"""
        self._validate_status()
        self._validate_report_path()

    def _validate_status(self):
        """验证 status 字段的值。"""
        valid_statuses = {"seqcancel", "seqconfirm", "seqabnormal"}
        if self.status.lower() not in valid_statuses:
            raise ValueError(f"无效的状态: {self.status}。必须是 {valid_statuses} 中的一个")

    def _validate_report_path(self):
        """验证报告路径格式。"""
        if not self.report_path or not self.report_path.strip():
            raise ValueError("report_path 不能为空")
        
        if ".." in self.report_path or "//" in self.report_path:
            raise ValueError("report_path 包含无效字符")
        
        if not self._is_valid_path_format(self.report_path):
            raise ValueError("无效的路径格式。必须是有效的相对路径、绝对路径或 Windows 路径")

    @staticmethod
    def _is_valid_path_format(path):
        """检查路径格式是否有效。"""
        # 检查相对路径、绝对 Unix 路径或 Windows 路径
        import re
        return bool(re.match(r'^[\w/.\\-]+$', path) or  # 相对路径
                    path.startswith('/') or              # Unix 绝对路径
                    re.match(r'^[a-zA-Z]:\\.*', path))   # Windows 路径


@dataclass
class LimsResponse:
    """表示来自 LIMS API 的响应。"""
    code: int
    message: str
    data: Any = None
    request_id: Optional[str] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    extra: Optional[Dict[str, Any]] = None
    
    def is_success(self):
        """检查响应是否表示成功。"""
        return self.code == 200 or self.code == 0
    
    @classmethod
    def from_json(cls, json_data, request_id=None):
        """从 JSON 数据创建 LimsResponse。"""
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data
        
        return cls(
            code=data.get('code', -1),
            message=data.get('msg', ''),
            data=data.get('data'),
            request_id=request_id,
            timestamp=int(time.time() * 1000)
        )


class MetricsCollector:
    """收集有关执行的指标。"""
    
    def __init__(self):
        self._counters = defaultdict(int)
        self._lock = Lock()
    
    def increment_counter(self, name):
        """递增命名计数器。"""
        with self._lock:
            self._counters[name] += 1
    
    def get_counter(self, name):
        """获取命名计数器的值。"""
        with self._lock:
            return self._counters.get(name, 0)
    
    def get_all_metrics(self):
        """将所有指标作为字典获取。"""
        with self._lock:
            return dict(self._counters)


class CwbioPutDataLims:
    """
    用于生物信息学数据的 LIMS 数据上传服务。
    
    此类提供读取数据文件、处理记录以及使用正确的身份验证和重试机制将记录上传到 LIMS API 的功能。
    """
    
    # 常量
    DEFAULT_TIMEOUT = 30  # 秒
    DEFAULT_BATCH_SIZE = 100
    CONTENT_TYPE = "application/json"
    
    def __init__(self, config):
        """
        初始化 LIMS 数据上传服务。
        
        参数:
            config: 配置字典或类似对象
        """
        self.config = config
        self.metrics = MetricsCollector()
        self.session = self._create_session()
    
    @classmethod
    def create(cls, config):
        """
        使用给定配置创建 CwbioPutDataLims 实例。
        
        参数:
            config: 配置字典
        
        返回:
            CwbioPutDataLims: 一个已初始化的实例
        """
        cls._validate_config(config)
        return cls(config)
    
    @staticmethod
    def _validate_config(config):
        """
        验证配置。
        
        参数:
            config: 配置字典
        
        引发:
            ValueError: 如果配置无效
        """
        required_fields = ['appid', 'appsecret', 'responseurl']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"配置中缺少 {field}")
        
        # 验证 URL
        try:
            requests.utils.urlparse(config['responseurl'])
        except Exception as e:
            raise ValueError(f"无效的 responseurl: {str(e)}")
    
    def _create_session(self):
        """
        创建具有重试配置的 HTTP 会话。
        
        返回:
            requests.Session: 配置好的会话
        """
        session = requests.Session()
        
        retry_config = RetryConfig(
            max_retries=int(self.config.get('maxRetries', 3)),
            initial_delay_ms=int(self.config.get('initialDelayMs', 1000)),
            backoff_multiplier=float(self.config.get('backoffMultiplier', 2.0)),
            max_delay_ms=int(self.config.get('maxDelayMs', 10000))
        )
        
        # 为 5xx 状态码配置重试
        retry = Retry(
            total=retry_config.max_retries,
            backoff_factor=retry_config.backoff_multiplier / 1000,  # Requests 使用秒
            status_forcelist=[502, 503, 504, 500],
            allowed_methods=['POST'],
        )
        
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def process_data(self, args):
        """
        根据命令行参数处理数据。
        
        参数:
            args: 命令行参数
        """
        start_time = time.time()
        try:
            cmd_args = self._parse_args(args)
            if not cmd_args:
                return
                
            records = self._read_data_file(cmd_args.file_path)
            
            if not records:
                logger.warning("在文件中未找到有效记录")
                return
                
            logger.info(f"正在处理 {len(records)} 条记录")
            self._process_and_send_data(records)
            self.metrics.increment_counter("process.success")
            logger.info("成功处理所有记录")
        
        except Exception as e:
            self.metrics.increment_counter("process.error")
            logger.error("处理失败", exc_info=True)
            raise LimsProcessingException("未能处理数据") from e
        
        finally:
            elapsed = time.time() - start_time
            logger.info(f"总处理时间: {elapsed*1000:.0f} 毫秒")
    
    @staticmethod
    def _parse_args(args):
        """
        解析命令行参数。
        
        参数:
            args: 命令行参数
        
        返回:
            Namespace: 解析后的参数
        """
        parser = argparse.ArgumentParser(
            description='LIMS 数据上传服务'
        )
        parser.add_argument(
            '--path', dest='file_path', required=True,
            help='输入数据文件的路径'
        )
        parser.add_argument(
            '--config', dest='config_path', required=True,
            help='配置文件的路径'
        )
        
        try:
            return parser.parse_args(args)
        except SystemExit:
            # 处理 argparse 因 --help 或错误而退出的情况
            return None
    
    def _read_data_file(self, file_path):
        """
        从文件读取数据记录。
        
        参数:
            file_path: 数据文件的路径
        
        返回:
            List[DataRecord]: 解析后的数据记录列表
        
        引发:
            LimsFileException: 如果无法读取文件
        """
        logger.info(f"正在读取文件: {file_path}")
        records = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, start=1):
                    if not line.strip():
                        continue
                    
                    try:
                        record = self._parse_line(line.strip())
                        if record:
                            records.append(record)
                    except Exception as e:
                        logger.warning(
                            f"未能解析第 {line_num} 行: {line.strip()[:50]}... - {str(e)}"
                        )
                        self.metrics.increment_counter("parse.error")
            
            return records
        
        except (IOError, OSError) as e:
            raise LimsFileException(f"未能读取文件: {file_path}") from e
    
    def _parse_line(self, line):
        """
        解析数据文件中的一行。
        
        参数:
            line: 数据文件中的一行
        
        返回:
            Optional[DataRecord]: 解析后的数据记录，如果无效则为 None
        """
        MAX_RETRIES = 3
        BASE_RETRY_DELAY_MS = 100
        
        for attempt in range(MAX_RETRIES):
            try:
                parts = line.split()
                
                # 验证基本格式
                if len(parts) < 3:
                    logger.warning(
                        f"无效的行格式: 至少需要 3 个字段，实际得到 {len(parts)}: {line}"
                    )
                    raise ValueError("行中字段不足")
                
                # 提取字段
                detect_no = self._validate_field(parts[0], "detectNo")
                status = self._validate_field(parts[1], "status")
                report_path = self._validate_field(parts[2], "reportPath")
                report_reason = parts[3] if len(parts) > 3 else ""
                
                # 提取扩展信息
                ext = self._extract_extended_info(parts)
                
                # 创建并验证记录
                record = DataRecord(
                    detect_no=detect_no,
                    status=status,
                    report_path=report_path,
                    report_reason=report_reason,
                    report_message=ext
                )
                record.validate()
                
                return record
            
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"在 {MAX_RETRIES} 次重试后未能解析行: {line}", exc_info=True)
                    self.metrics.increment_counter("parse.error")
                else:
                    logger.debug(f"解析尝试 {attempt + 1} 失败: {str(e)}")
                
                if attempt < MAX_RETRIES - 1:
                    delay_ms = BASE_RETRY_DELAY_MS * (2 ** attempt)
                    time.sleep(delay_ms / 1000)
        
        return None
    
    def _validate_field(self, value, field_name):
        """
        验证字段值不为空。
        
        参数:
            value: 字段值
            field_name: 字段名称
        
        返回:
            str: 验证后的字段值
        
        引发:
            ValueError: 如果字段无效
        """
        value = value.strip()
        if not value:
            raise ValueError(f"字段 {field_name} 不能为空")
        return value
    
    def _extract_extended_info(self, parts):
        """
        从各部分提取扩展信息。
        
        参数:
            parts: 分割后的行各部分
        
        返回:
            Dict[str, str]: 扩展信息
        """
        ext = {}
        
        # 检查 plasmid_length 和 sample_length
        PLASMID_LENGTH_INDEX = 4
        SAMPLE_LENGTH_INDEX = 5
        EMPTY_FIELD = "-"
        
        if len(parts) > PLASMID_LENGTH_INDEX:
            self._handle_length_info(
                parts[PLASMID_LENGTH_INDEX].strip(), 
                "plasmid_length", 
                ext,
                EMPTY_FIELD
            )
            
            if len(parts) > SAMPLE_LENGTH_INDEX:
                self._handle_length_info(
                    parts[SAMPLE_LENGTH_INDEX].strip(),
                    "sample_length",
                    ext,
                    EMPTY_FIELD
                )
        
        return ext
    
    def _handle_length_info(self, length_info, key, ext, empty_field):
        """
        处理长度信息，如果有效则将其添加到 ext 字典中。
        
        参数:
            length_info: 长度信息字符串
            key: ext 字典的键
            ext: 要添加信息的字典
            empty_field: 表示空字段的值
        """
        if length_info and length_info != empty_field:
            try:
                self._validate_length_format(length_info)
                ext[key] = length_info
            except Exception as e:
                logger.warning(f"无效的 {key} 格式: {length_info}")
    
    def _validate_length_format(self, length_info):
        """
        验证长度信息的格式。
        
        参数:
            length_info: 长度信息字符串
        
        引发:
            ValueError: 如果格式无效
        """
        if not length_info.isdigit():
            raise ValueError("长度必须是正数")
    
    def _process_and_send_data(self, records):
        """
        分批处理并发送数据记录。
        
        参数:
            records: 数据记录列表
        """
        batch_size = int(self.config.get('batchSize', self.DEFAULT_BATCH_SIZE))
        
        # 将记录分成批次
        batches = [
            records[i:i + batch_size]
            for i in range(0, len(records), batch_size)
        ]
        
        for batch_num, batch in enumerate(batches, start=1):
            logger.info(f"正在处理批次 {batch_num}/{len(batches)}，包含 {len(batch)} 条记录")
            self._process_batch(batch)
    
    def _process_batch(self, batch):
        """
        处理单个批次的记录。
        
        参数:
            batch: 记录批次
            
        引发:
            LimsProcessingException: 如果批处理失败
        """
        try:
            request_data = self._create_request_body(batch)
            self._send_with_retry(request_data)
            self.metrics.increment_counter("batch.success")
            logger.info(f"成功处理了包含 {len(batch)} 条记录的批次")
        
        except Exception as e:
            self.metrics.increment_counter("batch.error")
            logger.error(f"未能处理包含 {len(batch)} 条记录的批次", exc_info=True)
            raise LimsProcessingException("批处理失败") from e
    
    def _create_request_body(self, batch):
        """
        为一批记录创建请求体。
        
        参数:
            batch: 数据记录批次
        
        返回:
            dict: 请求体数据
        """
        appid = self.config['appid']
        appsecret = self.config['appsecret']
        
        # 对 appid 和 appsecret 进行 URL 编码
        appid_encoded = quote(appid)
        appsecret_encoded = quote(appsecret)
        
        root_node = {
            'appid': appid_encoded,
            'sign': generate_sign(appid, appsecret)
        }
        
        data_array = []
        for record in batch:
            data_node = {
                'detect_no': record.detect_no,
                'status': record.status,
                'report_path': record.report_path
            }
            
            # 如果存在，则添加可选字段
            if record.report_reason:
                data_node['report_reason'] = record.report_reason
            
            # 如果存在，则添加 ext 字段
            if record.report_message:
                data_node['ext'] = record.report_message
            
            data_array.append(data_node)
        
        root_node['data'] = data_array
        return root_node
    
    def _send_with_retry(self, request_data):
        """
        使用重试逻辑发送请求。
        
        参数:
            request_data: 要发送的请求数据
            
        引发:
            LimsProcessingException: 如果所有重试都失败
        """
        retry_config = RetryConfig(
            max_retries=int(self.config.get('maxRetries', 3)),
            initial_delay_ms=int(self.config.get('initialDelayMs', 1000)),
            backoff_multiplier=float(self.config.get('backoffMultiplier', 2.0)),
            max_delay_ms=int(self.config.get('maxDelayMs', 10000))
        )
        
        last_exception = None
        last_response = None
        request_id = f"req-{int(time.time())}-{random.randint(1000, 9999)}"
        
        for attempt in range(retry_config.max_retries):
            try:
                # 执行请求并获取响应
                last_response = self._execute_request(request_data, request_id, attempt)
                
                # 处理成功
                if last_response.is_success():
                    self._log_success_and_update_metrics(last_response, attempt)
                    return
                
                # 处理可重试的响应
                self._handle_retryable_response(
                    last_response, attempt, retry_config
                )
                
            except Exception as e:
                last_exception = e
                
                # 处理异常
                self._handle_exception(
                    e, last_response, attempt, retry_config, request_id
                )
        
        # 处理最终失败
        self._handle_final_failure(
            last_response, last_exception, retry_config, request_id
        )
    
    def _execute_request(self, request_data, request_id, attempt):
        """
        执行单个 HTTP 请求。
        
        参数:
            request_data: 要发送的数据
            request_id: 请求 ID
            attempt: 当前尝试次数
        
        返回:
            LimsResponse: 响应对象
        """
        logger.debug(f"正在执行请求 [RequestId: {request_id}, Attempt: {attempt + 1}]")
        return self._send_http_request(request_data, request_id)
    
    def _log_success_and_update_metrics(self, response, attempt):
        """
        记录成功并更新指标。
        
        参数:
            response: 响应对象
            attempt: 尝试次数
        """
        logger.info(f"请求在尝试 {attempt + 1} 次后成功: {response.message}")
        self.metrics.increment_counter("request.success")
        if attempt > 0:
            self.metrics.increment_counter("request.retry.success")
    
    def _handle_retryable_response(self, response, attempt, config):
        """
        处理可重试的错误响应。
        
        参数:
            response: 响应对象
            attempt: 当前尝试次数
            config: 重试配置
        """
        if not self._is_retryable_code(response.code):
            raise LimsProcessingException(
                f"不可重试的错误: code={response.code}, message={response.message}"
            )
        
        if attempt < config.max_retries - 1:
            delay_ms = calculate_backoff_delay(
                attempt, 
                config.initial_delay_ms,
                config.backoff_multiplier,
                config.max_delay_ms
            )
            
            perform_retry_delay(delay_ms, attempt, config.max_retries, response)
        
        raise RetryableException(
            f"可重试的错误: code={response.code}, message={response.message}"
        )
    
    def _handle_exception(self, e, last_response, attempt, config, request_id):
        """
        处理请求期间的异常。
        
        参数:
            e: 发生的异常
            last_response: 收到的最后一个响应 (如果有)
            attempt: 当前尝试次数
            config: 重试配置
            request_id: 请求 ID
        """
        if last_response:
            logger.debug(f"收到的最后一个响应 [RequestId: {request_id}]: {last_response}")
        
        if not self._is_retryable(e):
            logger.error(f"发生不可重试的错误 [RequestId: {request_id}]", exc_info=True)
            raise LimsProcessingException("发生不可重试的错误") from e
        
        if attempt < config.max_retries - 1:
            delay_ms = calculate_backoff_delay(
                attempt,
                config.initial_delay_ms,
                config.backoff_multiplier,
                config.max_delay_ms
            )
            
            perform_retry_delay(delay_ms, attempt, config.max_retries, e)
    
    def _handle_final_failure(self, last_response, last_exception, config, request_id):
        """
        处理所有重试后的最终失败。
        
        参数:
            last_response: 收到的最后一个响应 (如果有)
            last_exception: 发生的最后一个异常 (如果有)
            config: 重试配置
            request_id: 请求 ID
        """
        self.metrics.increment_counter("request.failure")
        
        error_message = (
            f"在 {config.max_retries} 次尝试后失败 [RequestId: {request_id}]。 "
            f"最后一个响应: {last_response}"
        )
        
        logger.error(error_message)
        if last_exception:
            raise LimsProcessingException(error_message) from last_exception
        else:
            raise LimsProcessingException(error_message)
    
    def _is_retryable(self, e):
        """
        检查异常是否可重试。
        
        参数:
            e: 要检查的异常
        
        返回:
            bool: 如果异常可重试，则为 True
        """
        # 网络相关的错误是可重试的
        if isinstance(e, (IOError, TimeoutError, RetryableException)):
            return True
        
        # HTTP 5xx 错误是可重试的
        if isinstance(e, HttpException) and e.status_code >= 500:
            return True
        
        # 响应验证错误 - 检查特定情况
        if isinstance(e, ResponseValidationException):
            msg = str(e).lower()
            return not any(x in msg for x in ["invalid token", "unauthorized", "forbidden"])
        
        return False
    
    def _is_retryable_code(self, code):
        """
        检查错误代码是否可重试。
        
        参数:
            code: 要检查的错误代码
        
        返回:
            bool: 如果代码可重试，则为 True
        """
        error_code = ErrorCode.from_code(code)
        return error_code is not None and error_code.retryable
    
    def _send_http_request(self, request_data, request_id):
        """
        向 LIMS API 发送 HTTP 请求。
        
        参数:
            request_data: 要发送的数据
            request_id: 请求 ID
        
        返回:
            LimsResponse: 响应对象
        """
        json_body = json.dumps(request_data)
        
        headers = {
            'Content-Type': self.CONTENT_TYPE,
            'X-Request-ID': request_id
        }
        
        try:
            logger.debug(f"正在发送请求 [RequestId: {request_id}] 到 {self.config['responseurl']}")
            
            response = self.session.post(
                self.config['responseurl'],
                data=json_body,
                headers=headers,
                timeout=self.DEFAULT_TIMEOUT
            )
            
            # 处理响应
            lims_response = self._process_response(response.text, request_id)
            
            if response.status_code >= 400:
                error_code = self._map_http_status_to_error_code(response.status_code)
                error_message = (
                    f"HTTP 请求失败 [RequestId: {request_id}]，状态码 "
                    f"{response.status_code}: {lims_response.message}"
                )
                
                logger.error(error_message)
                raise HttpException(error_message, error_code)
            
            # 记录响应
            if lims_response.is_success():
                logger.info(f"请求成功 [RequestId: {request_id}]: {lims_response.message}")
                self.metrics.increment_counter("response.success")
            else:
                logger.warning(
                    f"请求完成，但返回非成功代码 [RequestId: {request_id}]: "
                    f"code={lims_response.code}, message={lims_response.message}"
                )
                self.metrics.increment_counter("response.warning")
            
            return lims_response
        
        except json.JSONDecodeError as e:
            logger.error(f"未能处理请求/响应 [RequestId: {request_id}]", exc_info=True)
            raise HttpException(
                "未能处理请求/响应",
                ErrorCode.INTERNAL_ERROR
            ) from e
    
    def _process_response(self, response_body, request_id):
        """
        处理响应体。
        
        参数:
            response_body: 响应体文本
            request_id: 请求 ID
        
        返回:
            LimsResponse: 处理后的响应
        """
        try:
            response_data = json.loads(response_body)
            
            return LimsResponse(
                code=response_data.get('code', -1),
                message=response_data.get('msg', ''),
                data=response_data.get('data'),
                request_id=request_id,
                timestamp=int(time.time() * 1000)
            )
        except json.JSONDecodeError as e:
            logger.error(f"未能解析响应 [RequestId: {request_id}]: {response_body}", exc_info=True)
            raise ResponseValidationException("无效的响应格式") from e
    
    def _map_http_status_to_error_code(self, status_code):
        """
        将 HTTP 状态码映射到错误代码。
        
        参数:
            status_code: HTTP 状态码
        
        返回:
            ErrorCode: 映射的错误代码
        """
        if status_code in (400, 401, 403):
            return ErrorCode.INVALID_AUTH
        elif status_code == 429:
            return ErrorCode.TOO_MANY_REQUESTS
        elif status_code == 502:
            return ErrorCode.BAD_GATEWAY
        elif status_code == 503:
            return ErrorCode.SERVICE_UNAVAILABLE
        elif status_code == 504:
            return ErrorCode.GATEWAY_TIMEOUT
        else:
            return ErrorCode.INTERNAL_ERROR


def read_config(config_path):
    """
    读取配置文件。
    """
    config = configparser.ConfigParser()
    config.optionxform = lambda option: option  # 保留原始大小写
    
    try:
        print(f"正在读取配置文件: {config_path}")
        config.read(config_path)
        print(f"配置节: {config.sections()}")
        
        if 'LIMS' not in config:
            raise ValueError("配置文件中缺少 [LIMS] 表头")
        
        # 调试：打印所有配置项
        print("配置项:")
        for key, value in config['LIMS'].items():
            print(f"  {key} = {value}")
            
        return dict(config['LIMS'])
    except Exception as e:
        logger.error(f"未能读取配置文件: {str(e)}")
        raise


def parse_args():
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description='LIMS 数据上传服务'
    )
    parser.add_argument(
        '--path', dest='file_path', required=True,
        help='输入数据文件的路径'
    )
    parser.add_argument(
        '--config', dest='config_path', required=True,
        help='配置文件的路径'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='启用详细输出'
    )
    
    return parser.parse_args()


def main():
    """主入口点。"""
    try:
        args = parse_args()
        
        # 根据详细程度设置日志级别
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # 读取配置
        config = read_config(args.config_path)
        
        # 创建并运行处理器
        processor = CwbioPutDataLims.create(config)
        processor.process_data(['--path', args.file_path, '--config', args.config_path])
        
        return 0
    
    except LimsException as e:
        logger.error(f"LIMS 错误: {str(e)}")
        return 1
    
    except Exception as e:
        logger.error(f"意外错误: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 