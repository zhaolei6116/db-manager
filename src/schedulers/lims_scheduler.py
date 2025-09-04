"""LIMS数据拉取调度器"""
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
from src.ingestion.lims_puller import run_lims_puller, get_all_json_in_lims_dir


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
            # 1. 扫描 Limes 未分析样本，执行拉取操作
            result = run_lims_puller()
            
            # 3. 记录结果
            if result.success:
                self.logger.info(
                    f"LIMS数据拉取完成，成功处理{result.success_count}个文件，"
                    f"失败{result.failure_count}个"
                )
            else:
                self.logger.error(f"LIMS数据拉取任务执行失败: {result.message}")
                
        except Exception as e:
            self.logger.error(f"LIMS数据拉取任务执行过程中发生异常", exc_info=True)
