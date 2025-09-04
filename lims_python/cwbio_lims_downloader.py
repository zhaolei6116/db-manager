#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CWBIO LIMS(科研服务) 数据下载服务

此模块提供从LIMS API获取数据报告并下载的功能，
集成了原始Java程序CwbioRequestDataLims的所有功能。
支持从配置文件读取参数、命令行参数覆盖、
HTTP请求发送和重试、文件下载等功能。

作者: Linji Li
日期: 2024-05-23
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
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from urllib.parse import quote, urlparse
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import requests

## 配置日志
#logging.basicConfig(
#    level=logging.INFO,
#    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#    handlers=[
#        logging.StreamHandler(),
#        logging.FileHandler(Path(__file__).resolve().parent/'cwbio_lims_download.log', mode='a')
#    ]
#)

logger = logging.getLogger(__name__)

#############################
# 异常类
#############################

class DownloadException(Exception):
    """下载过程中发生的异常"""
    def __init__(self, message, cause=None):
        super().__init__(message)
        self.cause = cause


class LimsException(Exception):
    """LIMS API相关异常的基类"""
    pass


class RetryableException(LimsException):
    """可以重试的异常"""
    pass


class ResponseValidationException(LimsException):
    """响应验证失败的异常"""
    pass


#############################
# 模型类
#############################

class DownloadStatus(str, Enum):
    """下载状态枚举"""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ErrorCode(Enum):
    """错误代码枚举"""
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
        """根据数字代码获取ErrorCode"""
        for error_code in ErrorCode:
            if error_code.code == code:
                return error_code
        return None


class DownloadRequest:
    """下载请求"""
    def __init__(self, 
                 url: str, 
                 target_directory: Path,
                 expected_checksum: Optional[str] = None,
                 headers: Dict[str, str] = None,
                 progress_callback: Optional[Callable[[int], None]] = None,
                 retry_attempts: int = 3,
                 retry_delay_ms: int = 1000):
        self.url = url
        self.target_directory = target_directory
        self.expected_checksum = expected_checksum
        self.headers = headers or {}
        self.progress_callback = progress_callback
        self.retry_attempts = retry_attempts
        self.retry_delay_ms = retry_delay_ms
        
        # 验证参数
        self.validate()
        
        # 确保目标目录存在
        self.target_directory.mkdir(parents=True, exist_ok=True)
    
    def validate(self):
        """验证下载请求参数"""
        if not self.url:
            raise ValueError("URL参数不能为空")
        if not self.target_directory:
            raise ValueError("目标目录参数不能为空")
        if self.retry_attempts < 0:
            raise ValueError("重试次数必须为非负数")
        if self.retry_delay_ms < 0:
            raise ValueError("重试延迟必须为非负数")
        
        # 验证URL格式
        try:
            urlparse(self.url)
        except Exception as e:
            raise ValueError(f"URL格式无效: {e}")


class DownloadResult:
    """下载结果"""
    def __init__(self,
                 file_path: Optional[Path] = None,
                 checksum: Optional[str] = None,
                 status: DownloadStatus = DownloadStatus.FAILED,
                 error_message: Optional[str] = None):
        self.file_path = file_path
        self.checksum = checksum
        self.status = status
        self.error_message = error_message
    
    def is_successful(self) -> bool:
        """检查下载是否成功"""
        return self.status == DownloadStatus.SUCCESS
    
    def __str__(self) -> str:
        return (f"DownloadResult(file_path={self.file_path}, "
                f"checksum={self.checksum}, "
                f"status={self.status}, "
                f"error_message={self.error_message})")


class RetryConfig:
    """重试配置"""
    def __init__(self,
                 max_retries: int = 3,
                 initial_delay_ms: int = 1000,
                 backoff_multiplier: float = 2.0,
                 max_delay_ms: int = 10000):
        self.max_retries = max_retries
        self.initial_delay_ms = initial_delay_ms
        self.backoff_multiplier = backoff_multiplier
        self.max_delay_ms = max_delay_ms


class LimsResponse:
    """LIMS API响应"""
    def __init__(self,
                 code: int,
                 message: str,
                 data: Any = None,
                 request_id: Optional[str] = None,
                 timestamp: int = None):
        self.code = code
        self.message = message
        self.data = data
        self.request_id = request_id
        self.timestamp = timestamp or int(time.time() * 1000)
    
    def is_success(self) -> bool:
        """检查响应是否成功"""
        return self.code == 200 or self.code == 0
    
    @classmethod
    def from_json(cls, json_data: Union[str, Dict], request_id: Optional[str] = None):
        """从JSON数据创建LimsResponse"""
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


#############################
# 工具函数
#############################

def md5(text: str) -> str:
    """创建文本的MD5哈希值"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def build_sign(appid: str, appsecret: str) -> str:
    """生成签名字符串"""
    return f"appid={appid}&appsecret={appsecret}"


def calculate_backoff_delay(attempt: int, initial_delay_ms: int, multiplier: float, max_delay_ms: int) -> float:
    """
    计算带抖动的指数退避延迟
    
    Args:
        attempt: 当前尝试次数（从0开始）
        initial_delay_ms: 初始延迟（毫秒）
        multiplier: 退避乘数
        max_delay_ms: 最大延迟（毫秒）
    
    Returns:
        float: 计算的延迟（毫秒）
    """
    delay = initial_delay_ms * (multiplier ** attempt)
    # 添加抖动以避免惊群效应
    jitter = random.uniform(0, delay / 2)
    delay += jitter
    # 确保不超过最大延迟
    return min(delay, max_delay_ms)


def perform_retry_delay(delay_ms: float, attempt: int, max_retries: int, error_context: Any = None) -> None:
    """
    在重试时休眠指定的延迟时间
    
    Args:
        delay_ms: 延迟（毫秒）
        attempt: 当前尝试次数（从0开始）
        max_retries: 最大重试次数
        error_context: 可选的错误上下文，用于日志记录
    """
    if error_context:
        context_str = str(error_context)
        if len(context_str) > 100:  # 截断长错误消息
            context_str = f"{context_str[:100]}..."
        logger.warning(
            f"请求失败 (尝试 {attempt + 1}/{max_retries}), "
            f"{delay_ms:.0f} 毫秒后重试. 上下文: {context_str}"
        )
    else:
        logger.warning(
            f"请求失败 (尝试 {attempt + 1}/{max_retries}), "
            f"{delay_ms:.0f} 毫秒后重试."
        )
    
    try:
        time.sleep(delay_ms / 1000)  # 将毫秒转换为秒
    except (InterruptedError, KeyboardInterrupt):
        # 处理睡眠期间可能的中断
        logger.warning("重试延迟期间休眠被中断")
        raise


#############################
# 文件下载器类
#############################

class FileDownloader:
    """文件下载器，处理文件下载任务"""
    
    def __init__(self, max_workers: int = None):
        """
        初始化文件下载器
        
        Args:
            max_workers: 最大工作线程数，默认为CPU核心数的2倍
        """
        self.max_workers = max_workers or (os.cpu_count() or 1) * 2
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.session = requests.Session()
        self.active_downloads: Dict[str, Future] = {}
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
    
    def shutdown(self):
        """关闭线程池和会话"""
        self.executor.shutdown(wait=True)
        self.session.close()
    
    def download_file(self, download_request: DownloadRequest) -> Future:
        """
        提交下载任务并返回Future对象
        
        Args:
            download_request: 下载请求
        
        Returns:
            Future对象，可用于获取下载结果
        """
        future = self.executor.submit(self._download_with_retry, download_request)
        url_key = download_request.url
        self.active_downloads[url_key] = future
        
        # 添加回调以便在完成时移除活动下载
        def _cleanup_callback(f):
            if url_key in self.active_downloads:
                del self.active_downloads[url_key]
        
        future.add_done_callback(_cleanup_callback)
        return future
    
    def _download_with_retry(self, request: DownloadRequest) -> DownloadResult:
        """
        带有重试逻辑的文件下载
        
        Args:
            request: 下载请求
        
        Returns:
            下载结果
        """
        attempt = 0
        last_error = None
        
        while attempt <= request.retry_attempts:
            try:
                return self._perform_download(request)
            except Exception as e:
                attempt += 1
                last_error = e
                
                if attempt <= request.retry_attempts:
                    logger.warning(f"下载失败，重试第{attempt}次，URL: {request.url}")
                    time.sleep(request.retry_delay_ms / 1000.0)
                else:
                    logger.error(f"下载失败，已达最大重试次数: {request.url}")
                    break
        
        # 如果所有尝试都失败，则返回失败结果
        error_message = f"下载失败，原因: {str(last_error) if last_error else '未知错误'}"
        return DownloadResult(None, None, DownloadStatus.FAILED, error_message)
    
    def _perform_download(self, request: DownloadRequest) -> DownloadResult:
        """
        执行实际的下载操作
        
        Args:
            request: 下载请求
        
        Returns:
            下载结果
        """
        url = request.url
        target_dir = request.target_directory
        
        # 从URL中提取文件名
        filename = os.path.basename(urlparse(url).path)
        if not filename:
            filename = f"download_{int(time.time())}"
        
        file_path = target_dir / filename
        temp_path = file_path.with_suffix(f"{file_path.suffix}.part")
        
        # 设置headers
        headers = {**request.headers} if request.headers else {}
        
        # 发送HEAD请求检查文件大小
        try:
            head_response = self.session.head(url, headers=headers, timeout=30)
            head_response.raise_for_status()
            total_size = int(head_response.headers.get('content-length', 0))
        except Exception as e:
            logger.debug(f"HEAD请求失败，无法获取文件大小: {url}, 错误: {str(e)}")
            total_size = 0
        
        # 发送GET请求下载文件
        try:
            with self.session.get(url, headers=headers, stream=True, timeout=30) as response:
                response.raise_for_status()
                
                # 如果HEAD请求未返回大小，尝试从GET响应获取
                if total_size == 0:
                    total_size = int(response.headers.get('content-length', 0))
                
                # 确保父目录存在
                os.makedirs(os.path.dirname(temp_path), exist_ok=True)
                
                # 下载文件
                downloaded_size = 0
                checksum = hashlib.md5()
                
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            checksum.update(chunk)
                            downloaded_size += len(chunk)
                            
                            # 更新进度
                            if total_size > 0 and request.progress_callback:
                                progress = int((downloaded_size / total_size) * 100)
                                request.progress_callback(progress)
                
                # 下载完成后，重命名临时文件
                if os.path.exists(file_path):
                    os.remove(file_path)
                os.rename(temp_path, file_path)
                
                # 检查校验和
                calculated_checksum = checksum.hexdigest()
                if request.expected_checksum and calculated_checksum != request.expected_checksum:
                    raise DownloadException(f"校验和不匹配: 预期 {request.expected_checksum}，实际 {calculated_checksum}")
                
                return DownloadResult(
                    file_path=file_path,
                    checksum=calculated_checksum,
                    status=DownloadStatus.SUCCESS
                )
                
        except Exception as e:
            # 清理临时文件
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            
            # 如果是已知的下载异常，直接抛出
            if isinstance(e, DownloadException):
                raise
            
            # 否则包装为下载异常并抛出
            raise DownloadException(f"下载失败: {str(e)}") from e


#############################
# LIMS下载器类
#############################

class CwbioLimsDownloader:
    """LIMS数据下载器"""
    
    DEFAULT_CONFIG_FILE_PATH = "config.ini"
    
    def __init__(self, config_path: str = Path(__file__).resolve().parent/"config.ini"):
        """
        初始化LIMS下载器
        
        Args:
            config_path: 配置文件路径，如果不提供则使用默认值
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_FILE_PATH
        self.config = {}
    
    def load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置项的字典
        """
        config = configparser.ConfigParser()
        
        # 保留键名的大小写，这对于后面检索responseUrl等配置很重要
        config.optionxform = lambda option: option
        
        try:
            logger.info(f"读取配置文件: {self.config_path}")
            config.read(self.config_path, encoding='utf-8')
            
            if 'LIMS' not in config:
                raise ValueError("配置文件中缺少[LIMS]部分")
            
            self.config = dict(config['LIMS'])
            
            logger.debug("配置项:")
            for key, value in self.config.items():
                logger.debug(f"  {key} = {value}")
            
            return self.config
            
        except Exception as e:
            logger.error(f"读取配置文件失败: {str(e)}")
            raise
    
    def send_api_request(self, args) -> Dict[str, Any]:
        """
        发送API请求获取报告信息
        
        Args:
            args: 命令行参数
        
        Returns:
            API响应数据
        """
        # 获取必要的参数
        url = self.config.get('url')
        appid = self.config.get('appid')
        #sign_value = self.config.get('sign')
        sign_value = self.config.get(args.lab if args.lab else "B")
        
        if not url:
            raise ValueError("配置文件中缺少'url'参数")
        
        if not appid:
            raise ValueError("配置文件中缺少'appid'参数")
        
        # 生成签名
        appid_md5 = md5(build_sign(appid, sign_value))
        
        # 获取查询参数
        start_time = args.startTime if args.startTime else self.config.get('startTime', '')
        end_time = args.endTime if args.endTime else self.config.get('endTime', '')
        
        # 构造请求体
        request_body = {
            "appid": appid,
            "sign": appid_md5,
            "startTime": start_time,
            "endTime": end_time
        }
        
        # 设置超时
        timeout_seconds = int(self.config.get('timeoutSeconds', '30'))
        
        # 设置重试参数
        max_retries = int(self.config.get('maxRetries', '3'))
        retry_delay_seconds = int(self.config.get('retryDelaySeconds', '5'))
        
        # 发送请求
        logger.info(f"发送请求到: {url}")
        logger.info(f"{request_body}")
        logger.debug(f"请求体: {json.dumps(request_body, ensure_ascii=False)}")
        
        # 重试逻辑
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json=request_body,
                    timeout=timeout_seconds
                )
                response.raise_for_status()
                
                # 解析响应
                response_data = response.json()
                logger.debug(f"API响应: {json.dumps(response_data, ensure_ascii=False)}")
                
                # 处理不同的响应代码
                if response_data.get('code') == 200:
                    logger.info("API请求成功")
                    return response_data
                elif response_data.get('code') == 201:
                    # 处理无数据的情况，这不是错误，只是没有数据
                    logger.info(f"API响应: {response_data.get('msg', '无数据')}")
                    return response_data
                else:
                    error_msg = f"API返回错误: 代码={response_data.get('code')}, 消息={response_data.get('msg', '')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
            except Exception as e:
                logger.error(f"API请求失败 (尝试 {attempt+1}/{max_retries}): {str(e)}")
                
                if attempt < max_retries - 1:
                    logger.info(f"等待 {retry_delay_seconds} 秒后重试...")
                    time.sleep(retry_delay_seconds)
                else:
                    logger.error("已达到最大重试次数，放弃请求")
                    raise
        
        raise Exception("API请求失败，未知错误")
    
    def download_reports(self, response_data: Dict[str, Any], args) -> None:
        """
        下载报告文件
        
        Args:
            response_data: API响应数据
            args: 命令行参数
        """
        # 获取数据列表
        if 'data' not in response_data or not isinstance(response_data['data'], list):
            logger.warning("响应中没有数据或data不是列表")
            return
        
        data_list = response_data['data']
        if not data_list:
            logger.info("没有报告数据需要下载")
            return
        
        # 获取下载路径
        download_path = args.path if args.path else self.config.get('downloadPath', 'downloads')
        download_dir = Path(download_path)
        download_dir.mkdir(parents=True, exist_ok=True)
        
        # 获取缓冲区大小
        buffer_size = int(self.config.get('bufferSize', '8192'))
        
        logger.info(f"找到 {len(data_list)} 个报告，将下载到: {download_dir}")
        
        # 配置下载参数
        retry_attempts = int(self.config.get('maxRetries', '3'))
        retry_delay_ms = int(self.config.get('retryDelaySeconds', '5')) * 1000
        
        # 创建下载器
        with FileDownloader(max_workers=5) as downloader:
            # 提交所有下载任务
            download_futures = []
            
            for item in data_list:
                # 获取报告路径
                board_no = item.get("board_no")
                board_download_dir = download_dir / board_no
                board_download_dir.mkdir(parents=True, exist_ok=True)
                report_path = item.get('report_path')
                if not report_path:
                    logger.warning(f"跳过无效的报告路径: {item}")
                    continue
                
                # 确保URL格式正确
                if not report_path.lower().startswith(('http://', 'https://')):
                    report_path = f"https://{report_path}"
                
                try:
                    # 创建下载请求
                    request = DownloadRequest(
                        url=report_path,
                        target_directory=board_download_dir,
                        retry_attempts=retry_attempts,
                        retry_delay_ms=retry_delay_ms,
                        progress_callback=lambda progress, url=report_path: logger.info(f"下载进度 {url}: {progress}%")
                    )
                    
                    # 提交下载任务
                    future = downloader.download_file(request)
                    download_futures.append(future)
                    logger.info(f"已提交下载任务: {report_path}")
                
                except Exception as e:
                    logger.error(f"提交下载任务失败: {str(e)}")
            
            # 处理所有下载结果
            success_count = 0
            failure_count = 0
            
            for future in as_completed(download_futures):
                try:
                    result = future.result()
                    if result.is_successful():
                        success_count += 1
                        logger.info(f"下载成功: {result.file_path}")
                    else:
                        failure_count += 1
                        logger.error(f"下载失败: {result.error_message}")
                except Exception as e:
                    failure_count += 1
                    logger.error(f"获取下载结果失败: {str(e)}")
            
            logger.info(f"下载完成: 成功 {success_count}, 失败 {failure_count}")
    
    def run(self, args) -> int:
        """
        运行下载器
        
        Args:
            args: 命令行参数
        
        Returns:
            int: 退出代码
        """
        try:
            # 读取配置文件
            self.load_config()
            
            # 发送API请求获取报告信息
            response_data = self.send_api_request(args)
            
            # 下载报告文件
            self.download_reports(response_data, args)
            
            return 0
        
        except Exception as e:
            logger.error(f"程序执行失败: {str(e)}", exc_info=True)
            return 1


#############################
# 命令行解析
#############################

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='CWBIO LIMS 数据下载服务',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--config', default=Path(__file__).resolve().parent/"config.ini", help='配置文件路径 (默认: config.ini)')
    parser.add_argument('--lab', help='实验室名称 (覆盖配置文件)', choices=["B", "S", "T", "G", "W"])
    parser.add_argument('--startTime', help='开始时间 (覆盖配置文件)')
    parser.add_argument('--endTime', help='结束时间 (覆盖配置文件)')
    parser.add_argument('--path', help='下载路径 (覆盖配置文件)')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    parser.add_argument('--help-full', action='store_true', help='显示详细帮助信息')

    args = parser.parse_args()

    if args.help_full:
        print_detailed_help()
        sys.exit(0)
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    return args


def print_detailed_help():
    """打印详细的帮助信息"""
    help_text = """
CWBIO LIMS 数据下载服务 - 详细帮助

说明:
  此程序从LIMS API获取数据报告信息并下载报告文件。

配置文件格式:
  [LIMS]
  url = https://www.cwbio.com/gwapi/get_report    # API请求URL
  responseUrl = https://www.cwbio.com/gwapi/push_report  # 响应URL
  appid = your_appid                              # 应用ID
  # 查询时间范围
  startTime = 2024-01-01 00:00:00                 # 开始时间
  endTime = 2024-01-31 23:59:59                   # 结束时间
  
  
  # 下载配置
  downloadPath = downloads                        # 下载路径
  bufferSize = 8192                               # 缓冲区大小
  
  # 重试配置
  maxRetries = 3                                  # 最大重试次数
  retryDelaySeconds = 5                           # 重试延迟（秒）
  timeoutSeconds = 30                             # 超时时间（秒）
  initialDelayMs = 1000                           # 初始延迟（毫秒）
  backoffMultiplier = 2.0                         # 退避乘数
  maxDelayMs = 30000                              # 最大延迟（毫秒）

示例:
  # 使用默认配置文件
  python cwbio_lims_downloader.py
  
  # 指定配置文件
  python cwbio_lims_downloader.py --config my_config.ini
  
  # 覆盖时间范围
  python cwbio_lims_downloader.py --startTime "2024-02-01 00:00:00" --endTime "2024-02-29 23:59:59"
  
  # 覆盖下载路径
  python cwbio_lims_downloader.py --path ./my_downloads
  
  # 详细日志输出
  python cwbio_lims_downloader.py --verbose
"""
    print(help_text)


#############################
# 主程序
#############################

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    # 创建并运行下载器
    downloader = CwbioLimsDownloader(args.config)
    return downloader.run(args)


if __name__ == "__main__":
    sys.exit(main()) 
