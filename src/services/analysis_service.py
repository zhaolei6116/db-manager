#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析服务模块，负责协调分析任务的生成和管理

该模块实现了以下核心功能：
1. 调用 SequenceAnalysisQueryGenerator 获取待处理的序列数据
2. 使用 AnalysisTaskProcessor 处理数据并存入数据库
3. 通过 ProjectTypeManager 生成分析目录和相关文件
"""

import os
from typing import Dict, List, Tuple, Any

from sqlalchemy.orm import Session

from src.models.database import get_session
from src.query.sequence_analysis_query import SequenceAnalysisQueryGenerator, AnalysisTaskProcessor
from src.services.project_type_manager import ProjectTypeManager

# 在模块级别配置日志
from src.utils.logging_config import setup_logger
logger = setup_logger("analysis_service")


class AnalysisService:
    """分析服务类，负责协调分析任务的生成和管理"""

    def __init__(self):
        """初始化分析服务"""
        logger.info("初始化分析服务")

    def process_analysis_tasks(self) -> Dict[str, Any]:
        """
        处理分析任务的主入口
        
        Returns:
            Dict[str, Any]: 处理结果统计信息
        """
        logger.info("开始处理分析任务")
        
        # 结果统计
        stats = {
            "total_project_groups": 0,
            "success_task_processing": 0,
            "failed_task_processing": 0,
            "success_file_generation": 0,
            "failed_file_generation": 0
        }
        
        try:
            # 步骤1: 获取待处理的序列数据字典
            dict1, dict2 = self._get_sequence_data()
            stats["total_project_groups"] = len(dict2)
            logger.info(f"获取到 {stats['total_project_groups']} 个项目组的待处理序列数据")
            
            if not dict2:
                logger.warning("没有待处理的序列数据，分析任务处理结束")
                return stats
            
            # 步骤2: 处理每个项目组的数据并保存到数据库
            task_processing_result = self._process_project_groups(dict2)
            stats.update(task_processing_result)
            
            # 步骤3: 生成分析目录和文件
            file_generation_result = self._generate_analysis_files(dict2)
            stats.update(file_generation_result)
            
        except Exception as e:
            logger.error(f"处理分析任务时发生错误: {str(e)}", exc_info=True)
            stats["failed_task_processing"] += 1
        
        logger.info(f"分析任务处理完成: {stats}")
        return stats

    def _get_sequence_data(self) -> Tuple[Dict, Dict]:
        """
        调用 SequenceAnalysisQueryGenerator 获取待处理的序列数据字典
        
        Returns:
            Tuple[Dict, Dict]: 返回两个字典，dict1和dict2
        """
        logger.info("正在获取待处理的序列数据")
        
        with get_session() as db_session:
            query_generator = SequenceAnalysisQueryGenerator(db_session)
            dict1, dict2 = query_generator.execute_query()
            
        logger.info("成功获取待处理的序列数据")
        return dict1, dict2

    def _process_project_groups(self, dict2: Dict) -> Dict[str, int]:
        """
        遍历dict2，处理每个项目组的数据并存入数据库
        
        Args:
            dict2: 包含项目组数据的字典
            
        Returns:
            Dict[str, int]: 任务处理结果统计
        """
        logger.info("开始处理每个项目组的数据并存入数据库")
        
        success_count = 0
        failed_count = 0
        
        for project_key, sequences in dict2.items():
            # project_key 是元组类型 (project_id, project_type)
            project_id, project_type = project_key
            
            try:
                # 获取分析路径
                project_manager = ProjectTypeManager(project_type)
                analysis_path = project_manager.generate_project_analysis_path(project_id)
                
                with get_session() as db_session:
                    task_processor = AnalysisTaskProcessor(db_session)
                    # 正确传递参数：project_key, sequence_data, analysis_path
                    result = task_processor.process_single_project_group(
                        project_key=project_key,
                        sequence_data=sequences,
                        analysis_path=analysis_path
                    )
                    
                if result:
                    success_count += 1
                    logger.info(f"成功处理项目组: {project_key}")
                else:
                    failed_count += 1
                    logger.warning(f"处理项目组 {project_key} 结果为失败")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"处理项目组 {project_key} 时发生错误: {str(e)}", exc_info=True)
        
        result = {
            "success_task_processing": success_count,
            "failed_task_processing": failed_count
        }
        
        logger.info(f"项目组数据处理完成: 成功 {success_count} 个，失败 {failed_count} 个")
        return result

    def _generate_analysis_files(self, dict2: Dict) -> Dict[str, int]:
        """
        遍历dict2，为每个项目组生成分析目录和相关文件
        
        Args:
            dict2: 包含项目组数据的字典
            
        Returns:
            Dict[str, int]: 文件生成结果统计
        """
        logger.info("开始为每个项目组生成分析目录和相关文件")
        
        success_count = 0
        failed_count = 0
        
        for project_key, sequences in dict2.items():
            # project_key 是元组类型 (project_id, project_type)
            project_id, project_type = project_key
            
            try:
                # 创建项目类型管理器实例
                project_manager = ProjectTypeManager(project_type)
                
                # 生成分析目录
                analysis_path = project_manager.generate_project_analysis_path(project_id)
                
                # 生成input.tsv文件
                if project_manager.generate_input_tsv(analysis_path, sequences):
                    # 生成run.sh文件
                    project_manager.generate_run_sh(analysis_path, project_id)
                    success_count += 1
                    logger.info(f"成功为项目组 {project_key} 生成分析文件")
                else:
                    raise Exception("生成input.tsv文件失败")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"为项目组 {project_key} 生成分析文件时发生错误: {str(e)}", exc_info=True)
        
        result = {
            "success_file_generation": success_count,
            "failed_file_generation": failed_count
        }
        
        logger.info(f"分析文件生成完成: 成功 {success_count} 个，失败 {failed_count} 个")
        return result


def run_analysis_process() -> Dict[str, Any]:
    """
    分析任务处理流程入口函数，供调度器调用
    
    Returns:
        Dict[str, Any]: 分析处理结果统计信息和处理时间
    """
    logger.info("开始执行分析任务处理流程")
    try:
        from datetime import datetime
        service = AnalysisService()
        result_stats = service.process_analysis_tasks()
        
        # 添加处理时间
        result = {
            **result_stats,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return result
    except Exception as e:
        logger.error(f"分析任务处理流程执行失败: {str(e)}", exc_info=True)
        return {
            "error": str(e),
            "total_project_groups": 0,
            "success_task_processing": 0,
            "failed_task_processing": 0,
            "success_file_generation": 0,
            "failed_file_generation": 0,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


if __name__ == "__main__":
    """主函数，用于独立运行分析服务"""
    try:
        result = run_analysis_process()
        print(f"分析服务执行结果: {result}")
    except Exception as e:
        logger.error(f"分析服务运行失败: {str(e)}", exc_info=True)
        raise