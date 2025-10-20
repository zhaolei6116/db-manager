"""分析任务调度器
负责定期执行分析任务处理流程，调用analysis_service中的run_analysis_process函数
"""
import logging
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
from src.services.analysis_service import run_analysis_process

logger = logging.getLogger(__name__)


class AnalysisScheduler(BaseScheduler):
    """分析任务调度器"""
    
    def __init__(self):
        super().__init__(
            scheduler_name="analysis_task",
            config_section="scheduler.analysis"
        )
        
    def _register_jobs(self):
        """注册分析任务处理调度任务"""
        interval_minutes = self.scheduler_config['interval_minutes']
        
        # 根据间隔时间选择合适的CronTrigger配置
        if interval_minutes >= 60:
            # 对于小时级别的间隔，使用小时字段
            hours = interval_minutes // 60
            self.logger.info(f"配置为每{hours}小时执行一次分析任务")
            trigger = CronTrigger(hour=f"*/{hours}", minute=0)
        else:
            # 对于分钟级别的间隔，使用分钟字段
            self.logger.info(f"配置为每{interval_minutes}分钟执行一次分析任务")
            trigger = CronTrigger(minute=f"*/{interval_minutes}")
        
        self.add_job(
            func=self.run_analysis_job,
            trigger=trigger,
            name="analysis_task_processing",
            misfire_grace_time=120  # 允许2分钟的执行延迟
        )
    
    def run_analysis_job(self):
        """执行分析任务处理流程
        调用 analysis_service 中的 run_analysis_process 函数
        """
        logger.info("开始执行分析任务处理调度")
        
        try:
            # 调用分析服务的入口函数
            result = run_analysis_process()
            
            # 记录分析结果
            if 'error' in result:
                logger.error(f"分析任务处理执行失败: {result['error']}")
            else:
                logger.info(
                    f"分析任务处理执行完成: 总项目组{result['total_project_groups']}, "
                    f"任务处理成功{result['success_task_processing']}个, "
                    f"任务处理失败{result['failed_task_processing']}个, "
                    f"文件生成成功{result['success_file_generation']}个, "
                    f"文件生成失败{result['failed_file_generation']}个"
                )
        except Exception as e:
            logger.error(f"分析任务处理调度执行过程中发生异常", exc_info=True)


# 如果直接运行此脚本，则启动调度器进行测试
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建并启动调度器
    scheduler = AnalysisScheduler()
    print("启动分析任务调度器...")
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