"""统一日志配置模块

实现功能：
- 同时输出日志到文件和控制台
- 支持日志文件自动滚动（防止过大）
- 从配置文件读取日志路径和级别
- 提供专用日志函数（如新字段检测）
"""
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

from src.utils.yaml_config import get_yaml_config


def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """
    配置并返回指定名称的日志器，确保只有一个处理器
    
    Args:
        name: 日志器名称，用于区分不同模块的日志
    
    Returns:
        配置好的日志器实例
    """
    # 获取日志配置
    config = get_yaml_config()
    log_config = config.get_log_config()
    
    # 确保日志目录存在
    log_dir = Path(log_config["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建日志器并设置级别
    logger_name = name or "app"
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_config["log_level"].upper())
    
    # 配置根日志器，确保所有子模块的日志都能被捕获
    root_logger = logging.getLogger()
    root_logger.setLevel(log_config["log_level"].upper())
    
    # 清除所有已有处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 清除根日志器的处理器（除了第一个运行时添加的）
    if len(root_logger.handlers) > 0:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    # 只添加一个复合处理器，避免重复输出
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
    )
    
    # 文件处理器（支持日志滚动）
    log_file = log_dir / f"{logger_name}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=log_config["max_bytes"],
        backupCount=log_config["backup_count"],
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 防止通过父记录器传播（避免重复日志）
    logger.propagate = False
    
    return logger


# 专用日志函数（hooks）
def log_unknown_field(table_name: str, field_name: str, logger: logging.Logger) -> None:
    """记录未知字段日志（WARNING级别）"""
    logger.warning(
        f"新字段检测 - 表: {table_name}, 字段: {field_name} "
        f"（未在fields_mapping中配置）"
    )


def log_field_addition(table_name: str, field_name: str, logger: logging.Logger) -> None:
    """记录字段添加日志（INFO级别）"""
    logger.info(f"字段添加 - 表: {table_name}, 字段: {field_name}")


def log_ingestion_result(file_name: str, success: bool, message: str, logger: logging.Logger) -> None:
    """记录数据摄入结果日志"""
    if success:
        logger.info(f"数据摄入成功 - 文件: {file_name}, 信息: {message}")
    else:
        logger.error(f"数据摄入失败 - 文件: {file_name}, 原因: {message}")


# 模块专用日志器快捷获取
def get_field_handler_logger() -> logging.Logger:
    """获取字段处理器专用日志器"""
    return setup_logger("field_handler")


def get_ingestion_logger() -> logging.Logger:
    """获取数据摄入专用日志器"""
    return setup_logger("ingestion")


def get_lims_puller_logger() -> logging.Logger:
    """获取LIMS拉取器专用日志器"""
    return setup_logger("lims_puller")


# 测试代码
if __name__ == "__main__":
    # 测试日志系统
    test_logger = setup_logger("test")
    test_logger.debug("这是调试信息（默认不显示）")
    test_logger.info("这是普通信息")
    test_logger.warning("这是警告信息")
    test_logger.error("这是错误信息")
    
    # 测试专用日志函数
    log_unknown_field("sample", "new_quality", test_logger)
    log_field_addition("sample", "new_quality", test_logger)
    log_ingestion_result("test.json", True, "导入10条记录", test_logger)
    log_ingestion_result("error.json", False, "格式错误", test_logger)
