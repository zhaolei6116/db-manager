"""调度器管理主脚本

负责启动和管理所有独立的调度器
"""
import signal
import sys
import logging
from typing import List

from src.utils.logging_config import setup_logger
from src.schedulers.base_scheduler import BaseScheduler
from src.schedulers.lims_scheduler import LIMSScheduler
from src.schedulers.sequencing_scheduler import SequencingScheduler
from src.schedulers.analysis_scheduler import AnalysisScheduler


class SchedulerManager:
    """调度器管理器，负责启动和管理所有调度器"""
    
    def __init__(self):
        self.logger = setup_logger("scheduler_manager")
        self.schedulers: List[BaseScheduler] = []
        self._register_all_schedulers()
        self._setup_global_signal_handler()
    
    def _register_all_schedulers(self):
        """注册所有可用的调度器"""
        # 可以根据配置决定是否启用某个调度器
        self.schedulers = [
            LIMSScheduler(),
            SequencingScheduler(),
            AnalysisScheduler()
            # 未来添加新的调度器只需在这里实例化并添加
        ]
        self.logger.info(f"已注册{len(self.schedulers)}个调度器")
    
    def _setup_global_signal_handler(self):
        """设置全局信号处理器，用于优雅关闭所有调度器"""
        def handle_global_shutdown(signum, frame):
            self.logger.info(f"接收到全局退出信号 {signum}，正在停止所有调度器...")
            self.stop_all()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, handle_global_shutdown)
        signal.signal(signal.SIGTERM, handle_global_shutdown)
    
    def start_all(self):
        """启动所有调度器"""
        self.logger.info("开始启动所有调度器...")
        for scheduler in self.schedulers:
            try:
                scheduler.start()
            except Exception as e:
                self.logger.error(
                    f"启动{scheduler.scheduler_name}调度器失败: {str(e)}",
                    exc_info=True
                )
        
        self.logger.info("所有调度器启动完成，进入运行状态")
        
        # 保持主进程运行
        try:
            while True:
                signal.pause()
        except KeyboardInterrupt:
            self.stop_all()
    
    def stop_all(self):
        """停止所有调度器"""
        self.logger.info("开始停止所有调度器...")
        for scheduler in self.schedulers:
            try:
                scheduler.stop()
            except Exception as e:
                self.logger.error(
                    f"停止{scheduler.scheduler_name}调度器失败: {str(e)}",
                    exc_info=True
                )
        self.logger.info("所有调度器已停止")


if __name__ == "__main__":
    try:
        manager = SchedulerManager()
        manager.start_all()
    except Exception as e:
        logging.critical(f"调度器管理器启动失败: {str(e)}", exc_info=True)
        sys.exit(1)
