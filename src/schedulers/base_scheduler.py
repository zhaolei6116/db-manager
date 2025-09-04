"""调度器基类模块

定义所有调度器的通用接口和基础功能
"""
import signal
import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.job import Job

from src.utils.yaml_config import get_yaml_config
from src.utils.logging_config import setup_logger


class BaseScheduler(ABC):
    """调度器基类，所有具体调度器需继承此类"""
    
    def __init__(self, scheduler_name: str, config_section: str):
        """
        初始化基础调度器
        
        Args:
            scheduler_name: 调度器名称（用于日志和标识）
            config_section: 配置文件中的配置节点名称
        """
        self.scheduler_name = scheduler_name
        self.config_section = config_section
        
        # 初始化日志
        self.logger = setup_logger(f"scheduler.{scheduler_name}")
        
        # 加载配置
        self.config = get_yaml_config()
        self.scheduler_config = self._load_scheduler_config()
        
        # 初始化调度器
        self.scheduler = BackgroundScheduler()
        self._setup_signal_handlers()
        
        # 任务列表
        self.jobs: List[Job] = []
        
    def _load_scheduler_config(self) -> Dict:
        """加载当前调度器的配置"""
        config = self.config.get(self.config_section, {})
        # 设置默认调度间隔（30分钟）
        if "interval_minutes" not in config:
            self.logger.warning(
                f"配置中未指定'{self.config_section}.interval_minutes'，"
                f"使用默认值30分钟"
            )
            config["interval_minutes"] = 30
        return config
    
    def _setup_signal_handlers(self):
        """设置信号处理器，支持优雅退出"""
        def handle_shutdown(signum, frame):
            self.logger.info(f"接收到退出信号 {signum}，正在停止{self.scheduler_name}调度器...")
            self.stop()
            
        # 处理常见终止信号
        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)
    
    @abstractmethod
    def _register_jobs(self):
        """注册所有任务（子类必须实现）"""
        pass
    
    def start(self):
        """启动调度器"""
        # 注册任务
        self._register_jobs()
        
        # 启动调度器
        self.scheduler.start()
        self.logger.info(
            f"{self.scheduler_name}调度器已启动，"
            f"配置节点: {self.config_section}, "
            f"任务数量: {len(self.jobs)}"
        )
    
    def stop(self):
        """停止调度器"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            self.logger.info(f"{self.scheduler_name}调度器已停止")
    
    def add_job(self, func, trigger, **kwargs):
        """添加任务到调度器"""
        job = self.scheduler.add_job(func, trigger,** kwargs)
        self.jobs.append(job)
        self.logger.info(
            f"已添加任务: {func.__name__}, "
            f"触发器: {trigger}, "
            f"任务ID: {job.id}"
        )
        return job
