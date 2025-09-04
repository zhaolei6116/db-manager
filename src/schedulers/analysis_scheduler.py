"""分析信息录入调度器"""
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
# 假设的分析信息录入模块，实际使用时替换为真实模块
from src.analysis.analysis_importer import get_pending_analyses, import_analysis_results


class AnalysisScheduler(BaseScheduler):
    """分析信息录入调度器"""
    
    def __init__(self):
        super().__init__(
            scheduler_name="analysis_importer",
            config_section="scheduler.analysis"
        )
        
        # 分析结果目录
        self.analysis_results_dir = self.config.get(
            "analysis.results_dir", 
            "/nas02/analysis_results"
        )
    
    def _register_jobs(self):
        """注册分析信息录入任务"""
        self.add_job(
            func=self.analysis_import_job,
            trigger=CronTrigger(minute=f"*/{self.scheduler_config['interval_minutes']}"),
            name="analysis_info_import",
            misfire_grace_time=180  # 允许3分钟的执行延迟
        )
    
    def analysis_import_job(self):
        """分析信息录入任务"""
        self.logger.info("开始执行分析信息录入任务")
        
        try:
            # 1. 获取待处理的分析结果
            pending_analyses = get_pending_analyses(self.analysis_results_dir)
            self.logger.info(f"发现{len(pending_analyses)}个待录入的分析结果")
            
            if not pending_analyses:
                self.logger.info("没有发现待录入的分析结果，跳过本次执行")
                return
            
            # 2. 导入分析结果
            import_result = import_analysis_results(pending_analyses)
            
            # 3. 记录结果
            self.logger.info(
                f"分析信息录入完成，成功录入{import_result.success_count}个结果，"
                f"失败{import_result.failure_count}个"
            )
                
        except Exception as e:
            self.logger.error(f"分析信息录入任务执行过程中发生异常", exc_info=True)
