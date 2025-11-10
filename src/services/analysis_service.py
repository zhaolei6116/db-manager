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
from src.repositories.sequence_repository import SequenceRepository

# 在模块级别配置日志
from src.utils.logging_config import setup_logger
# 导入通知管理器
from src.utils.notification_manager import notification_manager
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
        
        # 存储每个项目组的处理状态
        project_group_status = {}
        
        try:
            # 步骤1: 获取待处理的序列数据字典
            dict1, dict2 = self._get_sequence_data()
            stats["total_project_groups"] = len(dict2)
            logger.info(f"获取到 {stats['total_project_groups']} 个项目组的待处理序列数据")
            
            if not dict2:
                logger.warning("没有待处理的序列数据，分析任务处理结束")
                return stats
            
            # 步骤2: 处理每个项目组的数据并保存到数据库
            task_processing_result, task_status = self._process_project_groups(dict2)
            stats.update(task_processing_result)
            project_group_status.update(task_status)
            
            # 步骤3: 生成分析目录和文件
            file_generation_result, file_status = self._generate_analysis_files(dict2)
            stats.update(file_generation_result)
            
            # 更新项目组状态信息
            for project_key in project_group_status:
                if project_key in file_status:
                    project_group_status[project_key].update({
                        'file_generation': file_status[project_key]
                    })
            
            # 步骤4: 更新已处理序列的process_status - 只更新文件生成成功的项目组
            update_success = True
            try:
                # 收集文件生成成功的项目组
                successful_project_groups = {}
                for project_key in file_status:
                    if file_status[project_key]['success'] and project_key in dict1:
                        successful_project_groups[project_key] = dict1[project_key]
                
                # 只更新文件生成成功的项目组对应的序列状态
                if successful_project_groups:
                    logger.info(f"准备更新 {len(successful_project_groups)} 个成功项目组的序列状态")
                    self.update_sequence_process_status(successful_project_groups)
                else:
                    logger.info("没有文件生成成功的项目组，无需更新序列状态")
            except Exception as e:
                update_success = False
                logger.error(f"更新序列处理状态失败: {str(e)}")
            
            # 步骤5: 发送通知提醒
            self._send_analysis_notifications(dict2, project_group_status, update_success)
            
        except Exception as e:
            logger.error(f"处理分析任务时发生错误: {str(e)}", exc_info=True)
            stats["failed_task_processing"] += 1
        
        logger.info(f"分析任务处理完成: {stats}")
        return stats
    
    def update_sequence_process_status(self, dict1: Dict) -> None:
        """
        遍历dict1，更新涉及的序列的process_status为yes
        
        Args:
            dict1: 包含序列数据的字典，键为(project_id, project_type)元组，值为sequence_id列表
        """
        logger.info("开始更新序列的处理状态")
        
        try:
            # 从dict1中提取所有的sequence_id（dict1的值是序列ID列表）
            sequence_ids = []
            for seq_id_list in dict1.values():
                sequence_ids.extend(seq_id_list)
                
            logger.info(f"需要更新的序列数量: {len(sequence_ids)}")
            
            if sequence_ids:
                with get_session() as db_session:
                    sequence_repo = SequenceRepository(db_session)
                    sequence_repo.update_sequence_process_status(sequence_ids, status='yes')
                
                logger.info(f"成功将 {len(sequence_ids)} 个序列的处理状态更新为'yes'")
            else:
                logger.warning("没有需要更新处理状态的序列")
                
        except Exception as e:
            logger.error(f"更新序列处理状态时发生错误: {str(e)}", exc_info=True)
            raise

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

    def _process_project_groups(self, dict2: Dict) -> Tuple[Dict[str, int], Dict]:
        """
        遍历dict2，处理每个项目组的数据并存入数据库
        
        Args:
            dict2: 包含项目组数据的字典
            
        Returns:
            Tuple[Dict[str, int], Dict]: 任务处理结果统计和每个项目组的状态信息
        """
        logger.info("开始处理每个项目组的数据并存入数据库")
        
        success_count = 0
        failed_count = 0
        project_status = {}
        
        for project_key, sequences in dict2.items():
            # project_key 是元组类型 (project_id, project_type)
            project_id, project_type = project_key
            project_status[project_key] = {'success': False, 'error': None}
            
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
                    project_status[project_key] = {'success': True, 'error': None}
                    logger.info(f"成功处理项目组: {project_key}")
                else:
                    failed_count += 1
                    project_status[project_key] = {'success': False, 'error': '处理结果为失败'}
                    logger.warning(f"处理项目组 {project_key} 结果为失败")
                
            except Exception as e:
                failed_count += 1
                project_status[project_key] = {'success': False, 'error': str(e)}
                logger.error(f"处理项目组 {project_key} 时发生错误: {str(e)}", exc_info=True)
        
        result = {
            "success_task_processing": success_count,
            "failed_task_processing": failed_count
        }
        
        logger.info(f"项目组数据处理完成: 成功 {success_count} 个，失败 {failed_count} 个")
        return result, project_status

    def _generate_analysis_files(self, dict2: Dict) -> Tuple[Dict[str, int], Dict]:
        """
        遍历dict2，为每个项目组生成分析目录和相关文件
        
        Args:
            dict2: 包含项目组数据的字典
            
        Returns:
            Tuple[Dict[str, int], Dict]: 文件生成结果统计和每个项目组的状态信息
        """
        logger.info("开始为每个项目组生成分析目录和相关文件")
        
        success_count = 0
        failed_count = 0
        file_status = {}
        
        for project_key, sequences in dict2.items():
            # project_key 是元组类型 (project_id, project_type)
            project_id, project_type = project_key
            file_status[project_key] = {'success': False, 'error': None}
            
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
                    file_status[project_key] = {'success': True, 'error': None}
                    logger.info(f"成功为项目组 {project_key} 生成分析文件")
                else:
                    raise Exception("生成input.tsv文件失败")
                    
            except Exception as e:
                failed_count += 1
                file_status[project_key] = {'success': False, 'error': str(e)}
                logger.error(f"为项目组 {project_key} 生成分析文件时发生错误: {str(e)}", exc_info=True)
        
        result = {
            "success_file_generation": success_count,
            "failed_file_generation": failed_count
        }
        
        logger.info(f"分析文件生成完成: 成功 {success_count} 个，失败 {failed_count} 个")
        return result, file_status


    def _send_analysis_notifications(self, dict2: Dict, project_group_status: Dict, update_success: bool) -> None:
        """
        发送分析结果通知提醒
        
        Args:
            dict2: 包含项目组数据的字典
            project_group_status: 每个项目组的处理状态信息
            update_success: 序列处理状态更新是否成功
        """
        logger.info("开始发送分析结果通知提醒")
        
        for project_key in dict2:
            project_id, project_type = project_key
            status_info = project_group_status.get(project_key, {})
            
            # 检查步骤2、3、4是否都处理通过
            task_success = status_info.get('success', False)
            file_success = status_info.get('file_generation', {}).get('success', False)
            
            if task_success and file_success and update_success:
                # 所有步骤都成功，发送成功提醒
                message = f"项目 {project_id} 的分析文件已准备好，可以开始分析任务"
                module = "Analysis Service"
                status = "success"
                
                try:
                    notification_manager.send_yunzhijia_alert(
                        message=message,
                        module=module,
                        status=status,
                        project_type=project_type
                    )
                    logger.info(f"已发送项目 {project_id} 的分析文件准备成功提醒")
                except Exception as e:
                    logger.error(f"发送项目 {project_id} 的分析文件准备成功提醒失败: {str(e)}")
            else:
                # 有步骤失败，发送失败提醒
                error_reasons = []
                
                if not task_success:
                    task_error = status_info.get('error', '任务处理失败')
                    error_reasons.append(f"数据处理失败: {task_error}")
                
                if not file_success:
                    file_error = status_info.get('file_generation', {}).get('error', '文件生成失败')
                    error_reasons.append(f"文件生成失败: {file_error}")
                
                if not update_success:
                    error_reasons.append("序列状态更新失败")
                
                message = f"项目 {project_id} 的分析处理失败，原因：" + ", ".join(error_reasons)
                module = "Analysis Service"
                status = "error"
                
                try:
                    notification_manager.send_yunzhijia_alert(
                        message=message,
                        module=module,
                        status=status,
                        project_type=project_type
                    )
                    logger.info(f"已发送项目 {project_id} 的分析失败提醒")
                except Exception as e:
                    logger.error(f"发送项目 {project_id} 的分析失败提醒失败: {str(e)}")


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