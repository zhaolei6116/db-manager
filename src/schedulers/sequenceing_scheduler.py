"""下机路径扫描调度器"""
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
# 假设的下机路径扫描模块，实际使用时替换为真实模块
from src.ingestion.sequencing_scanner import scan_sequencing_paths, process_new_sequencing_data


class SequencingScheduler(BaseScheduler):
    """下机路径扫描调度器"""
    
    def __init__(self):
        super().__init__(
            scheduler_name="sequencing_scanner",
            config_section="scheduler.sequencing"
        )
        
        # 获取下机数据目录配置
        self.sequencing_dirs = self.config.get(
            "sequencing.data_dirs", 
            ["/nas02/sequencing_data"]
        )
    
    def _register_jobs(self):
        """注册下机路径扫描任务"""
        self.add_job(
            func=self.sequencing_scan_job,
            trigger=CronTrigger(minute=f"*/{self.scheduler_config['interval_minutes']}"),
            name="sequencing_path_scan",
            misfire_grace_time=120  # 允许2分钟的执行延迟
        )
    
    def sequencing_scan_job(self):
        """下机路径扫描任务"""
        self.logger.info("开始执行下机路径扫描任务")
        
        try:
            # 1. 扫描下机数据路径
            new_paths = scan_sequencing_paths(self.sequencing_dirs)
            self.logger.info(f"发现{len(new_paths)}个新的下机数据路径")
            
            if not new_paths:
                self.logger.info("没有发现新的下机数据，跳过本次执行")
                return
            
            # 2. 处理新发现的下机数据
            process_result = process_new_sequencing_data(new_paths)
            
            # 3. 记录结果
            self.logger.info(
                f"下机数据处理完成，成功处理{process_result.success_count}个路径，"
                f"失败{process_result.failure_count}个"
            )
                
        except Exception as e:
            self.logger.error(f"下机路径扫描任务执行过程中发生异常", exc_info=True)
