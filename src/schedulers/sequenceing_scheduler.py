"""下机路径扫描和数据验证调度器
负责定期扫描下机数据路径，并调用数据验证服务验证序列数据
"""
import logging
from apscheduler.triggers.cron import CronTrigger

from src.schedulers.base_scheduler import BaseScheduler
from src.services.validation_service import run_validation_process

logger = logging.getLogger(__name__)


class SequencingScheduler(BaseScheduler):
    """下机路径扫描和数据验证调度器"""
    
    def __init__(self):
        super().__init__(
            scheduler_name="sequencing_validation",
            config_section="scheduler.sequencing"
        )
        
    def _register_jobs(self):
        """注册下机路径扫描和数据验证任务"""
        self.add_job(
            func=self.run_validation_job,
            trigger=CronTrigger(minute=f"*/{self.scheduler_config['interval_minutes']}"),
            name="sequence_data_validation",
            misfire_grace_time=120  # 允许2分钟的执行延迟
        )
    
    def run_validation_job(self):
        """执行数据验证任务
        调用 validation_service 中的 run_validation_process 函数
        """
        logger.info("开始执行序列数据验证任务")
        
        try:
            # 调用数据验证服务的入口函数
            result = run_validation_process()
            
            # 记录验证结果
            if 'error' in result:
                logger.error(f"数据验证任务执行失败: {result['error']}")
            else:
                logger.info(
                    f"数据验证任务执行完成: 总记录数{result['total']}, "
                    f"验证通过{result['valid']}条, "
                    f"更新成功{result['update_success']}条, "
                    f"更新失败{result['update_failure']}条"
                )
        except Exception as e:
            logger.error(f"数据验证任务执行过程中发生异常", exc_info=True)


# 如果直接运行此脚本，则启动调度器进行测试
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建并启动调度器
    scheduler = SequencingScheduler()
    print("启动下机路径扫描和数据验证调度器...")
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
