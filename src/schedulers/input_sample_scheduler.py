"""信息单录入调度器
负责定时将信息单的信息存到数据库中
"""
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
from src.services.ingestion_service import run_ingestion_process


class InputSampleScheduler(BaseScheduler):
    """信息单录入调度器"""
    
    def __init__(self):
        super().__init__(
            scheduler_name="input_sample_processor",
            config_section="scheduler.input_sample"
        )
        
    def _register_jobs(self):
        """注册信息单录入任务"""
        self.add_job(
            func=self.input_sample_process_job,
            trigger=CronTrigger(minute=f"*/{self.scheduler_config['interval_minutes']}"),
            name="input_sample_process",
            misfire_grace_time=180  # 允许3分钟的执行延迟
        )
    
    def input_sample_process_job(self):
        """信息单录入任务"""
        self.logger.info("开始执行信息单录入任务")
        
        try:
            # 1. 执行数据录入流程，将信息单信息存入数据库
            result = run_ingestion_process()
            
            # 2. 记录结果
            if "error" in result:
                self.logger.error(f"信息单录入任务执行失败: {result['error']}")
            else:
                self.logger.info(
                    f"信息单录入任务执行完成，总文件数{result['total']}，" \
                    f"成功{result['success_count']}，失败{result['failure_count']}"
                )
                    
        except Exception as e:
            self.logger.error(f"信息单录入任务执行过程中发生异常", exc_info=True)
