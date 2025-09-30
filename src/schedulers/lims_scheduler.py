"""LIMS数据拉取调度器"""
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
from src.services.ingestion_service import run_ingestion_process


class LIMSScheduler(BaseScheduler):
    """LIMS数据拉取调度器"""
    
    def __init__(self):
        # 调用基类构造函数，指定名称和配置节点
        super().__init__(
            scheduler_name="lims_puller",
            config_section="scheduler.lims"
        )
        
        # 获取LIMS数据目录
        self.lims_dir = self.config.get(
            "ingestion.lims_data_dir", 
            "/nas02/project/zhaolei/pipeline/data_management/LimsData"
        )
    
    def _register_jobs(self):
        """注册LIMS相关任务"""
        # 添加LIMS数据拉取任务
        self.add_job(
            func=self.lims_pull_job,
            trigger=CronTrigger(minute=f"*/{self.scheduler_config['interval_minutes']}"),
            name="lims_data_pull",
            misfire_grace_time=60  # 允许60秒的执行延迟
        )
    
    def lims_pull_job(self):
        """LIMS数据拉取任务（定时执行）"""
        self.logger.info("开始执行LIMS数据拉取任务")
        
        try:
            # 调用完整的数据录入流程
            result = run_ingestion_process()
            
            # 记录处理结果
            if "error" in result:
                self.logger.warning(f"LIMS数据拉取任务执行完成，但包含错误: {result['error']}")
            else:
                self.logger.info(f"LIMS数据拉取任务执行完成: 总文件数{result['total']}，成功{result['success_count']}，失败{result['failure_count']}")
                
        except Exception as e:
            self.logger.error(f"LIMS数据拉取任务执行过程中发生异常", exc_info=True)
