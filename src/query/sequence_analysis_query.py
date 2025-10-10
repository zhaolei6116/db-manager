"""序列分析查询工具
负责查询符合条件的序列数据，并按项目和类型分组，为后续分析做准备
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session
from uuid import uuid4

from src.models.database import get_session
from src.repositories.sequence_repository import SequenceRepository
from src.repositories.analysis_task_repository import AnalysisTaskRepository
from src.processing.analysis_processor import AnalysisTaskProcessor

# 自定义异常类
class DuplicateTaskError(Exception):
    """发现重复的分析任务记录时抛出的异常"""

logger = logging.getLogger(__name__)


class SequenceAnalysisQueryGenerator:
    """序列分析查询生成器，负责生成两个字典：
    1. dict1: 按(project_id, project_type)分组的序列ID字典
    2. dict2: 按(project_id, project_type)分组的完整序列信息字典
    """
    
    def __init__(self, db_session: Optional[Session] = None):
        """
        初始化SequenceAnalysisQueryGenerator
        
        Args:
            db_session: 数据库会话对象，如果不提供则自动创建
        """
        self.db_session = db_session if db_session else get_session()
        self.sequence_repo = SequenceRepository(self.db_session)
    
    def get_pending_sequences(self) -> Dict[Tuple[str, str], List[str]]:
        """
        查询data_status为valid，process_status为no的记录
        生成字典：key是(project_id, project_type)元组，value是sequence_id列表
        
        Returns:
            Dict[Tuple[str, str], List[str]]: 按项目ID和类型分组的序列ID字典
        """
        logger.info("开始查询待分析的序列数据")
        
        try:
            # 获取数据有效且未处理的序列
            valid_sequences = self.sequence_repo.get_valid_unprocessed_sequences()
            
            # 按(project_id, project_type)分组
            grouped_sequences = {}
            
            for sequence in valid_sequences:
                if not sequence.project_id or not sequence.project_type:
                    logger.warning(f"序列缺少必要的项目信息: sequence_id={sequence.sequence_id}")
                    continue
                
                key = (sequence.project_id, sequence.project_type)
                if key not in grouped_sequences:
                    grouped_sequences[key] = []
                grouped_sequences[key].append(sequence.sequence_id)
            
            logger.info(f"成功获取待分析序列数据，共 {len(grouped_sequences)} 个项目组")
            return grouped_sequences
        except Exception as e:
            logger.error(f"获取待分析序列数据失败: {str(e)}", exc_info=True)
            return {}
    
    def get_project_sequences(self, project_id: str, project_type: str) -> List[Dict[str, Any]]:
        """
        获取指定项目和类型的所有有效序列信息
        
        Args:
            project_id: 项目ID
            project_type: 项目类型
        
        Returns:
            List[Dict[str, Any]]: 序列信息字典列表
        """
        try:
            sequences = self.sequence_repo.get_by_project_id_and_type(project_id, project_type)
            
            # 转换为字典列表
            result = []
            for seq in sequences:
                seq_dict = {
                    'sequence_id': seq.sequence_id,
                    'sample_id': seq.sample_id,
                    'project_id': seq.project_id,
                    'project_type': seq.project_type,
                    'parameters': seq.parameters or {}
                }
                result.append(seq_dict)
            
            return result
        except Exception as e:
            logger.error(f"获取项目序列信息失败: project_id={project_id}, project_type={project_type}, 错误: {str(e)}")
            return []
    
    def get_all_project_sequences(self, grouped_sequence_ids: Dict[Tuple[str, str], List[str]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        """
        根据分组的序列ID，获取每个项目的每种类型的所有样本信息
        
        Args:
            grouped_sequence_ids: 按(project_id, project_type)分组的序列ID字典
        
        Returns:
            Dict[Tuple[str, str], List[Dict[str, Any]]]: 按项目ID和类型分组的完整序列信息字典
        """
        logger.info("开始获取所有项目的序列信息")
        
        result = {}
        
        for (project_id, project_type), _ in grouped_sequence_ids.items():
            # 获取该项目类型的所有有效序列信息
            sequences = self.get_project_sequences(project_id, project_type)
            if sequences:
                result[(project_id, project_type)] = sequences
                logger.info(f"成功获取项目序列信息: project_id={project_id}, project_type={project_type}, 共{len(sequences)}条记录")
            else:
                logger.warning(f"未找到项目相关序列: project_id={project_id}, project_type={project_type}")
        
        return result
    
    def execute_query(self) -> Tuple[Dict[Tuple[str, str], List[str]], Dict[Tuple[str, str], List[Dict[str, Any]]]]:
        """
        执行完整的查询流程
        
        Returns:
            Tuple[Dict[Tuple[str, str], List[str]], Dict[Tuple[str, str], List[Dict[str, Any]]]]: 
                dict1和dict2，分别是按项目ID和类型分组的序列ID字典和完整序列信息字典
        """
        logger.info("开始执行序列分析查询流程")
        
        try:
            # 1. 获取待分析的序列数据（dict1）
            dict1 = self.get_pending_sequences()
            
            if not dict1:
                logger.info("没有找到待分析的序列数据")
                return {}, {}
            
            # 2. 获取每个项目的每种类型的所有样本信息（dict2）
            dict2 = self.get_all_project_sequences(dict1)
            
            logger.info(f"序列分析查询流程执行完成: dict1={len(dict1)}个项目组, dict2={len(dict2)}个项目组")
            return dict1, dict2
        except Exception as e:
            logger.error(f"执行序列分析查询流程时发生异常: {str(e)}", exc_info=True)
            return {}, {}


class AnalysisTaskProcessor:
    """分析任务处理器，负责处理单个项目组并与analysis task表交互"""
    
    def __init__(self, db_session: Optional[Session] = None):
        """
        初始化AnalysisTaskProcessor
        
        Args:
            db_session: 数据库会话对象，如果不提供则自动创建
        """
        self.db_session = db_session if db_session else get_session()
        # 这里应该使用从src.processing.analysis_processor导入的AnalysisTaskProcessor类，而不是当前文件中的类
        # 为避免命名冲突，我们使用导入的类的实际名称
        from src.processing.analysis_processor import AnalysisTaskProcessor as ProcessingAnalysisTaskProcessor
        self.processor = ProcessingAnalysisTaskProcessor(self.db_session)
    
    def check_task_exists(self, project_id: str, project_type: str) -> Optional[Any]:
        """检查指定项目和类型的分析任务是否存在
        
        Args:
            project_id: 项目ID
            project_type: 项目类型
        
        Returns:
            Optional[Any]: 存在返回任务对象，不存在返回None
        
        Raises:
            DuplicateTaskError: 当发现重复的分析任务记录时抛出
            Exception: 当查询过程中发生错误时抛出，用于区分任务不存在和查询错误的情况
        """
        try:
            tasks = self.processor.get_by_project_and_type(project_id, project_type)
            
            # 检查返回结果
            if not tasks:
                logger.debug(f"任务不存在: project_id={project_id}, project_type={project_type}")
                return None
            elif len(tasks) > 1:
                error_msg = f"发现重复的分析任务记录: project_id={project_id}, project_type={project_type}, 任务数量={len(tasks)}"
                logger.error(error_msg)
                raise DuplicateTaskError(error_msg)
            else:
                logger.debug(f"找到任务: project_id={project_id}, project_type={project_type}, task_id={tasks[0].task_id}")
                return tasks[0]
        except DuplicateTaskError:
            raise  # 重新抛出DuplicateTaskError异常
        except Exception as e:
            logger.error(f"检查任务是否存在失败: project_id={project_id}, project_type={project_type}, 错误: {str(e)}")
            # 抛出异常而不是返回None，以便与任务不存在的情况区分开
            raise Exception(f"检查任务是否存在时发生错误: {str(e)}")
    
    def create_or_update_analysis_task(self, project_id: str, project_type: str, 
                                      sequence_data: List[Dict[str, Any]], 
                                      analysis_path: str, 
                                      existing_task: Optional[Any] = None) -> Dict[str, Any]:
        """
        创建或更新分析任务记录
        
        Args:
            project_id: 项目ID
            project_type: 项目类型
            sequence_data: 序列数据列表
            analysis_path: 分析路径
            existing_task: 已存在的任务对象，如果已在外部检查过则传入，避免重复检查
            
        
        Returns:
            Dict[str, Any]: 任务数据字典
        
        Raises:
            DuplicateTaskError: 当发现重复的分析任务记录时抛出
            Exception: 当查询过程中发生错误时抛出
        """
        # 提取样本ID列表
        sample_ids = [seq['sample_id'] for seq in sequence_data]
        
        # 合并所有序列的parameters
        merged_parameters = {}  
        for seq in sequence_data:
            if 'parameters' in seq and seq['parameters']:
                merged_parameters.update(seq['parameters'])
        
        
        # 基础任务数据
        task_data = {
            'project_id': project_id,
            'project_type': project_type,
            'analysis_path': analysis_path,
            'sample_ids': sample_ids,
            'parameters': merged_parameters,
            'analysis_status': 'pending'
        }
        
        # 如果任务已存在
        if existing_task:
            task_data['task_id'] = existing_task.task_id
            task_data['retry_count'] = existing_task.retry_count + 1 if existing_task.retry_count else 1
            task_data['created_at'] = existing_task.created_at
        
        return task_data
    
    def process_single_project_group(self, project_key: Tuple[str, str], 
                                    sequence_data: List[Dict[str, Any]], 
                                    analysis_path: str) -> bool:
        """
        处理单个项目组的分析任务
        
        Args:
            project_key: (project_id, project_type)元组
            sequence_data: 序列数据列表
            analysis_path: 分析路径
        
        Returns:
            bool: 处理是否成功
        """
        project_id, project_type = project_key
        logger.info(f"开始处理项目组: project_id={project_id}, project_type={project_type}")
        
        try:
            # 先检查任务是否存在（可能会抛出DuplicateTaskError异常或其他异常）
            existing_task = self.check_task_exists(project_id, project_type)
            
            # 创建或更新分析任务，传入已检查的existing_task避免重复检查
            # 注意：由于已经在外部调用了check_task_exists并传入了结果，内部不会再次调用
            task_data = self.create_or_update_analysis_task(project_id, project_type, sequence_data, analysis_path, existing_task)
            
            # 根据existing_task判断是否需要处理
            if existing_task:
                # 已存在任务记录，更新记录
                if self.processor.process(task_data, source="analysis_service_update"):
                    logger.info(f"分析任务记录更新成功: project_id={project_id}, project_type={project_type}")
                else:
                    logger.error(f"分析任务记录更新失败: project_id={project_id}, project_type={project_type}")
                    return False

            else:
                # 创建新任务记录
                if self.processor.create_task_with_validation(task_data, source="analysis_service_new"):
                    logger.info(f"分析任务记录创建成功: project_id={project_id}, project_type={project_type}")
                else:
                    logger.error(f"分析任务记录创建失败: project_id={project_id}, project_type={project_type}")
                    return False
            
            return True
        except DuplicateTaskError as e:
            # 发现重复任务，跳过当前项目组处理，但返回True表示该项目组已处理（跳过）
            logger.warning(f"发现重复任务记录，跳过当前项目组处理: {str(e)}")
            return True
        except Exception as e:
            logger.error(f"处理项目组失败: project_id={project_id}, project_type={project_type}, 错误: {str(e)}")
            return False
    



if __name__ == "__main__":
    """测试序列分析查询工具和分析任务处理器"""
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    try:
        # 使用上下文管理器获取数据库会话
        with get_session() as db_session:
            print("===== 测试 SequenceAnalysisQueryGenerator =====")
            # 创建查询实例并传入会话
            query_tool = SequenceAnalysisQueryGenerator(db_session)
            
            # 执行查询
            dict1, dict2 = query_tool.execute_query()
            
            # 打印结果统计信息
            print(f"查询结果统计:")
            print(f"dict1 (按(project_id, project_type)分组的序列ID): {len(dict1)}个项目组")
            print(f"dict2 (按(project_id, project_type)分组的完整序列信息): {len(dict2)}个项目组")
            
            # 打印部分详细信息（如果有数据）
            if dict1:
                for idx, ((project_id, project_type), sequence_ids) in enumerate(list(dict1.items())[:3]):
                    print(f"\n项目组 {idx+1}:")
                    print(f"  project_id: {project_id}")
                    print(f"  project_type: {project_type}")
                    print(f"  序列数量: {len(sequence_ids)}")
                    print(f"  示例序列ID: {sequence_ids[:2] if len(sequence_ids) >= 2 else sequence_ids}")
                    
                    # 查看dict2中对应的数据
                    if (project_id, project_type) in dict2:
                        print(f"  dict2中该项目组的序列信息数量: {len(dict2[(project_id, project_type)])}")
                        print(f"  示例序列信息: {dict2[(project_id, project_type)][:2] if len(dict2[(project_id, project_type)]) >= 2 else dict2[(project_id, project_type)]}")
            
            print("\n===== 测试 AnalysisTaskProcessor =====")
            # 创建分析任务处理器实例
            task_processor = AnalysisTaskProcessor(db_session)
            
            # 用于测试的模拟分析路径
            mock_analysis_path = "/path/to/analysis" 
            
            # 测试计数器
            success_count = 0
            failed_count = 0
            skipped_count = 0
            
            # 遍历dict2中的项目组，测试AnalysisTaskProcessor的功能
            if dict2:
                print(f"开始测试分析任务处理器，共处理{len(dict2)}个项目组")
                
                for idx, (project_key, sequence_data) in enumerate(list(dict2.items())[:5]):  # 限制只测试前5个项目组
                    project_id, project_type = project_key
                    print(f"\n处理项目组 {idx+1}/{min(5, len(dict2))}:")
                    print(f"  project_id: {project_id}")
                    print(f"  project_type: {project_type}")
                    print(f"  序列数量: {len(sequence_data)}")
                    
                    try:
                        # 测试check_task_exists方法
                        print(f"  测试check_task_exists方法")
                        existing_task = task_processor.check_task_exists(project_id, project_type)
                        if existing_task:
                            print(f"  ✓ 发现现有任务: task_id={existing_task.task_id}")
                        else:
                            print(f"  ✓ 未发现现有任务")
                        
                        # 测试create_or_update_analysis_task方法
                        print(f"  测试create_or_update_analysis_task方法")
                        task_data = task_processor.create_or_update_analysis_task(project_id, project_type, sequence_data, mock_analysis_path, existing_task)
                        print(f"  ✓ 成功创建/更新任务数据: task_data={'更新' if existing_task else '新建'}")
                        print(f"  ✓ 任务数据包含: project_id={task_data['project_id']}, project_type={task_data['project_type']}, sample_ids_count={len(task_data['sample_ids'])}")
                        
                        # 测试process_single_project_group方法
                        print(f"  测试process_single_project_group方法")
                        result = task_processor.process_single_project_group(project_key, sequence_data, mock_analysis_path)
                        if result:
                            print(f"  ✓ 项目组处理{'成功' if not isinstance(result, str) else result}")
                            success_count += 1
                        else:
                            print(f"  ✗ 项目组处理失败")
                            failed_count += 1
                    except DuplicateTaskError as e:
                        print(f"  ✓ 发现重复任务，已跳过: {str(e)}")
                        skipped_count += 1
                    except Exception as e:
                        print(f"  ✗ 处理出错: {str(e)}")
                        failed_count += 1
                
                # 打印测试结果统计
                print(f"\n分析任务处理器测试结果统计:")
                print(f"  成功处理: {success_count}")
                print(f"  失败处理: {failed_count}")
                print(f"  跳过处理: {skipped_count}")
            else:
                print("警告: dict2为空，无法测试AnalysisTaskProcessor")
    except Exception as e:
        print(f"测试失败: {str(e)}")