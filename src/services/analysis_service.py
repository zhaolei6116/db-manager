"""分析服务
负责管理分析任务，包括查询符合条件的序列数据、生成分析目录和输入文件、更新数据库状态等
"""

import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from src.models.database import get_session
from src.utils.yaml_config import get_yaml_config
from src.repositories.sequence_repository import SequenceRepository
from src.repositories.analysis_task_repository import AnalysisTaskRepository
from src.processing.analysis_processor import AnalysisTaskProcessor

# 在模块级别配置日志
from src.utils.logging_config import setup_logger
logger = setup_logger("analysis_service")


class AnalysisService:
    """分析服务，负责管理分析任务的完整流程"""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化分析服务
        
        Args:
            config_file: 配置文件路径
        """
        self.config = get_yaml_config(config_file)
    
    def get_pending_sequences(self) -> Dict[tuple, List[str]]:
        """
        查询 sequence 表中 data_status 为 valid 且 process_status 为 no 的记录
        并按(project_id, project_type)元组分组
        
        Returns:
            Dict[tuple, List[str]]: 以(project_id, project_type)元组为键，包含符合条件的序列ID列表的字典
        """
        logger.info("开始查询待分析的序列数据")
        
        sequences_by_project = {}
        valid_sequences = []
        try:
            with get_session() as db_session:
                sequence_repo = SequenceRepository(db_session)
                # 调用 Repository 中已有的方法获取数据
                valid_sequences = sequence_repo.get_valid_unprocessed_sequences()
            
            # 遍历查询结果，直接获取(project_id, project_type)元组为key的字典
            for seq in valid_sequences:
                # 创建(project_id, project_type)元组作为键
                key = (seq.project_id, seq.project_type)
                
                # 如果键不存在，初始化一个空列表
                if key not in sequences_by_project:
                    sequences_by_project[key] = []
                
                # 将序列信息添加到对应分组中
                # 直接将sequence_id添加到列表中
                sequences_by_project[key].append(seq.sequence_id)
            
            total_count = sum(len(seqs) for seqs in sequences_by_project.values())
            logger.info(f"查询到 {total_count} 条待分析的序列数据，分布在 {len(sequences_by_project)} 个项目组合中")
        except Exception as e:
            logger.error(f"查询待分析序列数据时发生异常: {str(e)}", exc_info=True)
        
        return sequences_by_project
    
    def group_sequences_by_project(self, grouped_sequences: Dict[tuple, List[str]]) -> Dict[tuple, Dict[str, Any]]:
        """
        处理已经按(project_id, project_type)元组分组的序列数据，并为每个组合获取数据库中所有记录
        
        Args:
            grouped_sequences: 按(project_id, project_type)元组分组的序列数据字典
                               （值为data_status=valid, process_status=no的记录列表）
            
        Returns:
            Dict[tuple, Dict[str, Any]]: 处理后的数据字典，key为(project_id, project_type)元组，
                                      包含两个字段：待处理记录（pending_sequences）和所有记录（all_sequences）
        """
        grouped_data = {}
        
        # 为每个分组获取数据库中所有记录
        try:
            with get_session() as db_session:
                sequence_repo = SequenceRepository(db_session)
                
                for key, pending_sequences in grouped_sequences.items():
                    project_id, project_type = key
                    
                    # 调用Repository获取该project_id和project_type的所有记录
                    all_records = sequence_repo.get_by_project_id_and_type(project_id, project_type)
                    
                    # 转换为字典列表
                    all_sequences = []
                    for record in all_records:
                        all_sequences.append({
                            'sequence_id': record.sequence_id,
                            'project_id': record.project_id,
                            'project_type': record.project_type,
                            'sample_id': record.sample_id,
                            'parameters': record.parameters
                        })
                    
                    # 存储结果
                    grouped_data[key] = {
                        'pending_sequences': pending_sequences,  # 用于后续状态修改的待处理记录
                        'all_sequences': all_sequences  # 用于分析文件生成的所有记录
                    }
        except Exception as e:
            logger.error(f"获取项目所有序列记录时发生异常: {str(e)}", exc_info=True)
            # 如果发生异常，至少返回待处理记录的分组
            for key, pending_sequences in grouped_sequences.items():
                grouped_data[key] = {
                    'pending_sequences': pending_sequences,
                    'all_sequences': []
                }
        
        logger.info(f"序列数据按项目分组完成，共 {len(grouped_data)} 个项目组")
        return grouped_data
    
    def generate_analysis_path(self, project_id: str, project_type: str) -> str:
        """
        根据项目ID和类型生成分析路径
        
        Args:
            project_id: 项目ID
            project_type: 项目类型
            
        Returns:
            str: 分析路径
            
        Raises:
            ValueError: 当项目类型未配置对应的分析路径时抛出
        """
        try:
            # 1. 从配置中获取项目类型与模板目录的映射关系
            project_type_mapping = self.config.get('project_type_to_template', {})
            
            # 2. 根据中文项目类型获取对应的英文字符串
            if project_type not in project_type_mapping:
                logger.error(f"项目类型 '{project_type}' 未在 project_type_to_template 中配置对应的英文字符串")
                raise ValueError(f"项目类型 '{project_type}' 未配置对应的英文字符串，请管理员在 config.yaml 中添加")
            
            # 获取对应的英文字符串
            english_project_type = project_type_mapping[project_type]
            logger.info(f"项目类型 '{project_type}' 对应的英文字符串为: {english_project_type}")
            
            # 3. 根据英文字符串获取对应的分析路径
            project_type_paths = self.config.get('project_type', {})
            if english_project_type not in project_type_paths:
                logger.error(f"英文字符串 '{english_project_type}' 未在 project_type 中配置对应的分析路径")
                raise ValueError(f"英文字符串 '{english_project_type}' 未配置对应的分析路径，请管理员在 config.yaml 中添加")
            
            # 获取基本分析路径
            base_path = project_type_paths[english_project_type]
            
            # 4. 组合完整的分析路径（基础路径 + project_id）
            analysis_path = os.path.join(base_path, project_id)
            logger.info(f"为项目 '{project_id}' 生成的分析路径: {analysis_path}")
            
        except Exception as e:
            # 如果不是ValueError，则转换为ValueError
            if not isinstance(e, ValueError):
                logger.error(f"生成分析路径时发生错误: {str(e)}")
                raise ValueError(f"生成分析路径失败: {str(e)}")
            # 已经是ValueError，直接抛出
            raise
        
        return analysis_path
    
    def prepare_analysis_directory(self, analysis_path: str) -> bool:
        """
        准备分析目录，创建目录并处理已存在的情况
        
        Args:
            analysis_path: 分析目录路径
            
        Returns:
            bool: 准备是否成功
        """
        path_obj = Path(analysis_path)
        
        try:
            if path_obj.exists():
                logger.info(f"分析目录已存在: {analysis_path}")
                # 检查目录是否为空
                if not any(path_obj.iterdir()):
                    logger.info(f"分析目录为空，继续使用: {analysis_path}")
                else:
                    logger.info(f"分析目录不为空，准备保留现有文件并添加新内容: {analysis_path}")
            else:
                # 创建目录及父目录
                path_obj.mkdir(parents=True, exist_ok=True)
                logger.info(f"创建分析目录成功: {analysis_path}")
            
            return True
        except Exception as e:
            logger.error(f"准备分析目录失败: {str(e)}", exc_info=True)
            return False
    
    def backup_existing_file(self, file_path: str) -> bool:
        """
        备份已存在的文件
        
        Args:
            file_path: 要备份的文件路径
            
        Returns:
            bool: 备份是否成功
        """
        path_obj = Path(file_path)
        
        if not path_obj.exists():
            return True
        
        try:
            # 生成带时间戳的备份文件名
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_path = f"{file_path}.bak.{timestamp}"
            
            # 复制文件进行备份
            shutil.copy2(file_path, backup_path)
            logger.info(f"备份文件成功: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"备份文件失败: {str(e)}", exc_info=True)
            return False
    
    def generate_input_tsv(self, analysis_path: str, sequences: List[Dict[str, Any]]) -> bool:
        """
        生成 input.tsv 文件
        
        Args:
            analysis_path: 分析目录路径
            sequences: 属于该分析任务的序列数据列表
            
        Returns:
            bool: 生成是否成功
        """
        input_file_path = os.path.join(analysis_path, "input.tsv")
        
        # 备份已存在的文件
        if not self.backup_existing_file(input_file_path):
            logger.error(f"备份现有 input.tsv 文件失败: {input_file_path}")
            return False
        
        try:
            with open(input_file_path, 'w', encoding='utf-8') as f:
                # 写入表头
                f.write("sample_id\tversion\tproject_type\traw_data_path\tparameters_json\n")
                
                # 写入数据
                for seq in sequences:
                    sample_id = seq['sample_id']
                    version = 1  # 假设版本默认为1，后续可以根据实际情况调整
                    project_type = seq['project_type']
                    raw_data_path = seq['raw_data_path']
                    parameters = str(seq.get('parameters', {})).replace('\t', ' ').replace('\n', ' ')
                    
                    f.write(f"{sample_id}\t{version}\t{project_type}\t{raw_data_path}\t{parameters}\n")
            
            logger.info(f"生成 input.tsv 文件成功: {input_file_path}")
            return True
        except Exception as e:
            logger.error(f"生成 input.tsv 文件失败: {str(e)}", exc_info=True)
            return False
    
    def generate_run_sh(self, analysis_path: str, project_type: str) -> bool:
        """
        生成 run.sh 执行脚本
        
        Args:
            analysis_path: 分析目录路径
            project_type: 项目类型
            
        Returns:
            bool: 生成是否成功
        """
        run_file_path = os.path.join(analysis_path, "run.sh")
        
        # 备份已存在的文件
        if not self.backup_existing_file(run_file_path):
            logger.error(f"备份现有 run.sh 文件失败: {run_file_path}")
            return False
        
        try:
            # 从配置中获取模板路径
            template_dir = self.config.get('project_type_to_template', {}).get(project_type, '')
            if not template_dir:
                logger.warning(f"未找到项目类型 {project_type} 对应的模板目录，使用默认内容")
                
                # 生成默认的 run.sh 内容
                run_content = "#!/bin/bash\n\n"
                run_content += "# 分析执行脚本\n"
                run_content += "set -e\n\n"
                run_content += "echo \"开始分析任务...\"\n"
                run_content += "make -f run.mk all\n"
                run_content += "echo \"分析任务完成\"\n"
            else:
                # 这里可以根据模板目录生成更复杂的 run.sh 内容
                run_content = "#!/bin/bash\n\n"
                run_content += f"# 分析执行脚本 - 项目类型: {project_type}\n"
                run_content += "set -e\n\n"
                run_content += "echo \"开始分析任务...\"\n"
                run_content += "make -f run.mk all\n"
                run_content += "echo \"分析任务完成\"\n"
            
            with open(run_file_path, 'w', encoding='utf-8') as f:
                f.write(run_content)
            
            # 添加执行权限
            os.chmod(run_file_path, 0o755)
            
            logger.info(f"生成 run.sh 文件成功: {run_file_path}")
            return True
        except Exception as e:
            logger.error(f"生成 run.sh 文件失败: {str(e)}", exc_info=True)
            return False
    
    def create_analysis_task(self, project_id: str, project_type: str, 
                            analysis_path: str, sample_ids: List[str]) -> bool:
        """
        创建分析任务记录
        
        Args:
            project_id: 项目ID
            project_type: 项目类型
            analysis_path: 分析路径
            sample_ids: 样本ID列表
            
        Returns:
            bool: 创建是否成功
        """
        try:
            with get_session() as db_session:
                processor = AnalysisTaskProcessor(db_session)
                
                # 准备任务数据
                task_data = {
                    'project_id': project_id,
                    'project_type': project_type,
                    'analysis_path': analysis_path,
                    'sample_ids': sample_ids
                }
                
                # 补全任务数据
                completed_data = processor.complete_task_dict(task_data)
                
                # 处理任务
                result = processor.process(completed_data, source="analysis_service")
                
                if result:
                    logger.info(f"创建分析任务成功: project_id={project_id}, project_type={project_type}")
                else:
                    logger.error(f"创建分析任务失败: project_id={project_id}, project_type={project_type}")
                
                db_session.commit()
                return result
        except Exception as e:
            logger.error(f"创建分析任务时发生异常: {str(e)}", exc_info=True)
            return False
    
    def update_sequence_process_status(self, sequence_ids: List[str]) -> bool:
        """
        更新序列的处理状态为 'yes'
        
        Args:
            sequence_ids: 序列ID列表
            
        Returns:
            bool: 更新是否成功
        """
        try:
            with get_session() as db_session:
                sequence_repo = SequenceRepository(db_session)
                
                for seq_id in sequence_ids:
                    try:
                        sequence = sequence_repo.get_by_pk(seq_id)
                        if sequence:
                            sequence.process_status = 'yes'
                            logger.info(f"更新序列处理状态成功: sequence_id={seq_id}")
                    except Exception as e:
                        logger.error(f"更新序列处理状态失败: sequence_id={seq_id}, 错误: {str(e)}")
                
                db_session.commit()
                return True
        except Exception as e:
            logger.error(f"批量更新序列处理状态时发生异常: {str(e)}", exc_info=True)
            return False
    
    def process_analysis_groups(self, grouped_data: Dict[tuple, Dict[str, Any]]) -> Dict[str, List]:
        """
        处理按项目分组的序列数据，检查analysis_tasks表中是否存在记录并进行相应处理
        
        Args:
            grouped_data: 按(project_id, project_type)元组分组的数据字典
                        包含pending_sequences和all_sequences字段
                        
        Returns:
            Dict[str, List]: 处理结果，包含成功和失败的项目组
        """
        success_groups = []
        failure_groups = []
        processed_groups = {
            'success': success_groups,
            'failure': failure_groups
        }
        
        try:
            for project_key, data in grouped_data.items():
                project_id, project_type = project_key
                pending_sequences = data.get('pending_sequences', [])
                all_sequences = data.get('all_sequences', [])
                
                logger.info(f"开始处理项目组: project_id={project_id}, project_type={project_type}")
                
                try:
                    # 1. 在analysis_tasks表中查询是否存在记录
                    with get_session() as db_session:
                        analysis_repo = AnalysisTaskRepository(db_session)
                        existing_tasks = analysis_repo.get_by_project_and_type(project_id, project_type)
                    
                    # 2. 根据查询结果进行处理
                    if not existing_tasks:
                        # 2.1 记录不存在，说明之前这个项目没有分析过
                        logger.info(f"项目组 {project_id}:{project_type} 在analysis_tasks表中不存在，创建新分析任务")
                        
                        # a. 生成分析路径
                        analysis_path = self.generate_analysis_path(project_id, project_type)
                        
                        # b. 准备分析目录
                        if not self.prepare_analysis_directory(analysis_path):
                            raise Exception("准备分析目录失败")
                        
                        # c. 提取样本ID列表
                        sample_ids = [seq['sample_id'] for seq in all_sequences]
                        
                        # d. 创建分析任务记录
                        with get_session() as db_session:
                            processor = AnalysisTaskProcessor(db_session)
                            
                            # 合并所有序列的parameters
                            merged_parameters = {}
                            for seq in all_sequences:
                                if 'parameters' in seq and seq['parameters']:
                                    # 将每个序列的parameters合并到一个字典中
                                    merged_parameters.update(seq['parameters'])

                            # 准备任务数据
                            task_data = {
                                'project_id': project_id,
                                'project_type': project_type,
                                'analysis_path': analysis_path,
                                'sample_ids': sample_ids,
                                'parameters': merged_parameters
                            }
                            
                            # 调用create_task_with_validation方法创建分析任务
                            if not processor.create_task_with_validation(task_data, source="analysis_service"):
                                raise Exception("创建分析任务记录失败")
                            
                            logger.info(f"创建分析任务成功: project_id={project_id}, project_type={project_type}")
                    else:
                        # 2.2 记录存在，检查记录数量
                        if len(existing_tasks) != 1:
                            error_msg = f"项目组 {project_id}:{project_type} 在analysis_tasks表中存在{len(existing_tasks)}条记录，应仅存在1条记录"
                            logger.error(error_msg)
                            raise Exception(error_msg)
                        
                        # 获取现有分析目录
                        analysis_path = existing_tasks[0].analysis_path
                        logger.info(f"项目组 {project_id}:{project_type} 在analysis_tasks表中存在，使用现有分析目录: {analysis_path}")
                        
                        # 分析次数
                        retry_count = existing_tasks[0].retry_count + 1

                        # 2.3 更新现有任务记录
                        with get_session() as db_session:
                            processor = AnalysisTaskProcessor(db_session)
                            analysis_repo = AnalysisTaskRepository(db_session)
                            
                            # 提取样本ID列表
                            sample_ids = [seq['sample_id'] for seq in all_sequences]
                            
                            # 合并所有序列的parameters
                            merged_parameters = {}
                            for seq in all_sequences:
                                if 'parameters' in seq and seq['parameters']:
                                    # 将每个序列的parameters合并到一个字典中
                                    merged_parameters.update(seq['parameters'])
                            

                            # 准备更新数据
                            existing_task = existing_tasks[0]
                            update_data = {
                                'project_id': project_id,
                                'project_type': project_type,
                                'analysis_path': analysis_path,
                                'sample_ids': sample_ids,
                                'parameters': merged_parameters,
                                'retry_count': retry_count
                            }
                            
                            # 获取主键字段并添加到更新数据中
                            pk_field = analysis_repo.get_pk_field()
                            update_data[pk_field] = getattr(existing_task, pk_field)
                            
                            logger.info(f"准备更新分析任务记录: project_id={project_id}, project_type={project_type}")
                            logger.debug(f"更新样本数量: {len(sample_ids)}, 更新parameters字段")
                            
                            # 调用process方法更新记录
                            if processor.process(update_data, source="analysis_service_update"):
                                logger.info(f"分析任务记录更新成功: project_id={project_id}, project_type={project_type}")
                            else:
                                logger.error(f"分析任务记录更新失败: project_id={project_id}, project_type={project_type}")
                    
                    # 3. 在分析目录中生成分析文件
                    if not self.generate_input_tsv(analysis_path, all_sequences):
                        raise Exception("生成 input.tsv 文件失败")
                    
                    if not self.generate_run_sh(analysis_path, project_type):
                        raise Exception("生成 run.sh 文件失败")
                    
                    # 记录成功处理的项目组
                    success_groups.append({
                        'project_key': project_key,
                        'analysis_path': analysis_path,
                        'pending_sequence_count': len(pending_sequences)
                    })
                    
                except Exception as e:
                    logger.error(f"处理项目组失败: project_id={project_id}, project_type={project_type}, 错误: {str(e)}")
                    failure_groups.append({
                        'project_key': project_key,
                        'error': str(e)
                    })
                    continue
            
            logger.info(f"分析项目组处理完成: 成功{len(success_groups)}个，失败{len(failure_groups)}个")
            return processed_groups
            
        except Exception as e:
            logger.error(f"处理分析项目组时发生异常: {str(e)}")
            return processed_groups
    
    def process_analysis_tasks(self) -> Dict[str, int]:
        """
        处理所有分析任务的主流程
        
        Returns:
            Dict[str, int]: 处理结果统计
        """
        logger.info("开始处理分析任务")
        
        # 初始化统计信息
        stats = {
            'total_sequences': 0,
            'total_projects': 0,
            'success_projects': 0,
            'failure_projects': 0,
            'update_success_sequences': 0
        }
        
        try:
            # 1. 获取待分析的序列数据
            pending_sequences = self.get_pending_sequences()
            stats['total_sequences'] = sum(len(seqs) for seqs in pending_sequences.values())
            
            if not pending_sequences:
                logger.info("没有待分析的序列数据，任务结束")
                return stats
            
            # 2. 按项目分组
            grouped_data = self.group_sequences_by_project(pending_sequences)
            stats['total_projects'] = len(grouped_data)
            
            # 3. 处理每个项目组
            processed_groups = self.process_analysis_groups(grouped_data)
            
            # 更新统计信息
            stats['success_projects'] = len(processed_groups['success'])
            stats['failure_projects'] = len(processed_groups['failure'])
            
            # 收集所有待处理的序列ID
            updated_sequence_ids = []
            for project_key, data in grouped_data.items():
                updated_sequence_ids.extend(data.get('pending_sequences', []))
            
            # 4. 更新已处理序列的状态
            if updated_sequence_ids:
                if self.update_sequence_process_status(updated_sequence_ids):
                    stats['update_success_sequences'] = len(updated_sequence_ids)
            
            logger.info(f"分析任务处理完成，统计结果: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"处理分析任务时发生异常: {str(e)}", exc_info=True)
            return stats


if __name__ == "__main__":
    """测试分析服务"""
    service = AnalysisService()
    result = service.process_analysis_tasks()
    print(f"测试结果: {result}")