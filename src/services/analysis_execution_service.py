# -*- coding: utf-8 -*-
"""
分析执行服务模块，负责启动分析任务

该模块实现了以下核心功能：
1. 处理analysis_tasks表中analysis_status为pending的记录
2. 进入分析目录，执行qsub命令提交任务
3. 任务提交成功后，修改analysis_status为running
4. 通过配置获取qsub路径，若不存在则使用模拟路径
"""

import os
import subprocess
from typing import Dict, Any
import logging

from src.models.database import get_session
from src.repositories.analysis_task_repository import AnalysisTaskRepository
from src.utils.yaml_config import get_yaml_config
from src.utils.logging_config import setup_logger

# 设置日志
logger = setup_logger("analysis_execution_service")


class AnalysisExecutionService:
    """分析执行服务类，负责启动分析任务"""

    def __init__(self, test_mode: bool = False):
        """初始化分析执行服务
        
        Args:
            test_mode: 是否为测试模式，测试模式下使用模拟的qsub脚本
        """
        logger.info("初始化分析执行服务")
        self.config = get_yaml_config()
        self.test_mode = test_mode
        self.qsub_path = self._get_qsub_path()

    def _get_qsub_path(self) -> str:
        """
        从配置中获取qsub命令路径，若不存在则返回模拟路径
        
        Returns:
            str: qsub命令的完整路径
        """
        try:
            # 测试模式下直接使用项目中的模拟qsub脚本
            if self.test_mode:
                simulated_qsub_path = "/home/zhaolei/project/data_management/tests/simulated_qsub.sh"
                logger.info(f"测试模式: 使用项目中的模拟qsub脚本: {simulated_qsub_path}")
                return simulated_qsub_path
                
            # 尝试从配置中获取qsub路径
            qsub_path = self.config.get("job_submission.qsub_path", required=False)
            if qsub_path and os.path.exists(qsub_path):
                logger.info(f"成功获取qsub命令路径: {qsub_path}")
                return qsub_path
            else:
                # 如果配置中不存在或路径不存在，返回模拟路径
                simulated_path = "/usr/local/bin/qsub"
                logger.warning(f"配置中qsub路径不存在或无效，使用模拟路径: {simulated_path}")
                return simulated_path
        except Exception as e:
            logger.error(f"获取qsub路径失败: {str(e)}")
            # 出现异常时也返回模拟路径
            return "/usr/local/bin/qsub"

    def process_pending_tasks(self) -> Dict[str, Any]:
        """
        处理所有待执行的分析任务
        
        Returns:
            Dict[str, Any]: 处理结果统计信息
        """
        logger.info("开始处理待执行的分析任务")
        
        # 结果统计
        stats = {
            "total_pending_tasks": 0,
            "successfully_submitted": 0,
            "failed_to_submit": 0
        }
        
        try:
            # 步骤1: 获取所有待处理的任务（使用字典格式以支持会话外使用）
            with get_session() as db_session:
                task_repo = AnalysisTaskRepository(db_session)
                # 使用新方法获取字典格式的任务数据
                pending_tasks = task_repo.get_pending_tasks_as_dicts()
            
            stats["total_pending_tasks"] = len(pending_tasks)
            logger.info(f"获取到 {stats['total_pending_tasks']} 个待执行的分析任务")
            
            if not pending_tasks:
                logger.warning("没有待执行的分析任务，处理结束")
                return stats
            
            # 步骤2: 处理每个待执行的任务
            for task_dict in pending_tasks:
                try:
                    # 在处理任务时保持会话打开
                    success = self._execute_analysis_task(task_dict)
                    if success:
                        stats["successfully_submitted"] += 1
                        logger.info(f"成功提交任务 {task_dict['task_id']}: {task_dict['project_id']}")
                    else:
                        stats["failed_to_submit"] += 1
                        logger.error(f"提交任务 {task_dict['task_id']}: {task_dict['project_id']} 失败")
                except Exception as e:
                    stats["failed_to_submit"] += 1
                    logger.error(f"处理任务 {task_dict['task_id']}: {task_dict['project_id']} 时发生错误: {str(e)}", exc_info=True)
            
        except Exception as e:
            logger.error(f"处理待执行任务时发生错误: {str(e)}", exc_info=True)
            stats["failed_to_submit"] += 1
        
        logger.info(f"分析任务处理完成: {stats}")
        return stats

    def _execute_analysis_task(self, task_dict) -> bool:
        """
        执行单个分析任务，提交到qsub
        
        Args:
            task_dict: 包含任务信息的字典
            
        Returns:
            bool: 任务提交是否成功
        """
        logger.info(f"开始执行任务 {task_dict['task_id']}: {task_dict['project_id']}")
        
        # 检查分析路径是否存在
        if not os.path.exists(task_dict['analysis_path']):
            logger.error(f"分析路径不存在: {task_dict['analysis_path']}")
            return False
        
        # 检查run.sh文件是否存在
        run_script_path = os.path.join(task_dict['analysis_path'], "run.sh")
        if not os.path.exists(run_script_path):
            logger.error(f"run.sh文件不存在: {run_script_path}")
            return False
        
        try:
            # 进入分析目录
            original_dir = os.getcwd()
            os.chdir(task_dict['analysis_path'])
            
            try:
                # 提交任务到qsub
                # 注意：实际使用时可能需要根据系统环境调整qsub命令的参数
                result = subprocess.run(
                    [self.qsub_path, "run.sh"],
                    capture_output=True,
                    text=True,
                    check=False  # 不抛出异常，手动检查返回码
                )
                
                if result.returncode == 0:
                    logger.info(f"任务提交成功，qsub输出: {result.stdout.strip()}")
                    # 更新任务状态为running
                    self._update_task_status(task_dict['task_id'], "running")
                    return True
                else:
                    logger.error(f"任务提交失败，qsub错误输出: {result.stderr.strip()}")
                    return False
            finally:
                # 恢复原来的工作目录
                os.chdir(original_dir)
        except Exception as e:
            logger.error(f"执行任务时发生异常: {str(e)}", exc_info=True)
            # 确保恢复工作目录
            try:
                os.chdir(original_dir)
            except:
                pass
            return False

    def _update_task_status(self, task_id: str, new_status: str) -> None:
        """
        更新任务状态
        
        Args:
            task_id: 任务ID
            new_status: 新的状态
        """
        try:
            with get_session() as db_session:
                task_repo = AnalysisTaskRepository(db_session)
                # 使用update_field方法来更新数据库中的状态字段
                success, _ = task_repo.update_field(
                    pk_value=task_id, 
                    field_name="analysis_status", 
                    new_value=new_status, 
                    operator="AnalysisExecutionService"
                )
                if success:
                    logger.info(f"已更新任务 {task_id} 的状态为: {new_status}")
                else:
                    logger.error(f"未找到任务 {task_id}，无法更新状态")
        except Exception as e:
            logger.error(f"更新任务 {task_id} 状态时发生错误: {str(e)}", exc_info=True)


def run_analysis_execution_process(test_mode: bool = False) -> Dict[str, Any]:
    """
    分析执行流程入口函数，供调度器调用
    
    Args:
        test_mode: 是否为测试模式，测试模式下使用模拟的qsub脚本
    
    Returns:
        Dict[str, Any]: 分析执行结果统计信息和处理时间
    """
    logger.info("开始执行分析任务提交流程")
    try:
        from datetime import datetime
        service = AnalysisExecutionService(test_mode=test_mode)
        result_stats = service.process_pending_tasks()
        
        # 添加处理时间
        result = {
            **result_stats,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return result
    except Exception as e:
        logger.error(f"分析任务提交流程执行失败: {str(e)}", exc_info=True)
        return {
            "error": str(e),
            "total_pending_tasks": 0,
            "successfully_submitted": 0,
            "failed_to_submit": 0,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


if __name__ == "__main__":
    """主函数，用于独立运行分析执行服务"""
    import argparse
    
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='运行分析执行服务')
    parser.add_argument('--test-mode', action='store_true', help='启用测试模式，使用模拟的qsub脚本')
    args = parser.parse_args()
    
    try:
        # 根据命令行参数决定是否启用测试模式
        test_mode = args.test_mode
        result = run_analysis_execution_process(test_mode=test_mode)
        print(f"分析执行服务执行结果: {result}")
    except Exception as e:
        logger.error(f"分析执行服务运行失败: {str(e)}", exc_info=True)
        raise