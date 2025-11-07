"""分析任务执行调度器
负责定期执行分析任务提交流程，调用analysis_execution_service中的run_analysis_execution_process函数
"""
import logging
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
from src.services.analysis_execution_service import run_analysis_execution_process

logger = logging.getLogger(__name__)


class AnalysisExecutionScheduler(BaseScheduler):
    """分析任务执行调度器"""
    
    def __init__(self):
        super().__init__(
            scheduler_name="analysis_execution",
            config_section="scheduler.analysis_execution"
        )
        
    def _register_jobs(self):
        """注册分析任务提交流度任务"""
        interval_minutes = self.scheduler_config['interval_minutes']
        
        # 根据间隔时间选择合适的CronTrigger配置
        if interval_minutes >= 60:
            # 对于小时级别的间隔，使用小时字段
            hours = interval_minutes // 60
            self.logger.info(f"配置为每{hours}小时执行一次分析任务提交")
            trigger = CronTrigger(hour=f"*/{hours}", minute=0)
        else:
            # 对于分钟级别的间隔，使用分钟字段
            self.logger.info(f"配置为每{interval_minutes}分钟执行一次分析任务提交")
            trigger = CronTrigger(minute=f"*/{interval_minutes}")
        
        self.add_job(
            func=self.run_execution_job,
            trigger=trigger,
            name="analysis_execution_processing",
            misfire_grace_time=120  # 允许2分钟的执行延迟
        )
    
    def run_execution_job(self):
        """执行分析任务提交流程
        调用 analysis_execution_service 中的 run_analysis_execution_process 函数
        """
        logger.info("开始执行分析任务提交流度")
        
        try:
            # 调用分析执行服务的入口函数
            result = run_analysis_execution_process()
            
            # 记录分析结果
            if 'error' in result:
                logger.error(f"分析任务提交流程执行失败: {result['error']}")
            else:
                logger.info(
                    f"分析任务提交流程执行完成: 总待处理任务{result['total_pending_tasks']}, "
                    f"提交成功{result['successfully_submitted']}个, "
                    f"提交失败{result['failed_to_submit']}个, "
                    f"处理时间: {result['process_time']}"
                )
        except Exception as e:
            logger.error(f"分析任务提交流度执行过程中发生异常", exc_info=True)


# 如果直接运行此脚本，则启动调度器进行测试
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建并启动调度器
    scheduler = AnalysisExecutionScheduler()
    print("启动分析任务执行调度器...")
    scheduler.start()
    
    try:
        # 保持脚本运行
        print("调度器已启动，按Ctrl+C停止...")
        while True:
            import time
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        print("正在停止调度器...")
        scheduler.stop()
        print("调度器已停止")