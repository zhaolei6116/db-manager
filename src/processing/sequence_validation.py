from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
import os
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.sequence_repository import SequenceRepository
from src.repositories.sample_repository import SampleRepository
from src.repositories.project_repository import ProjectRepository
from src.utils.yaml_config import YAMLConfig
from src.utils.notification_manager import notification_manager
from src.query.sequence_parameter_generator import SequenceParameterGenerator

logger = logging.getLogger(__name__)


class SequenceValidation:
    """序列数据验证类，负责验证sequence数据状态和路径的有效性"""
    
    def __init__(self, db_session: Session):
        """
        初始化SequenceValidation
        
        Args:
            db_session: 数据库会话对象
        """
        if db_session is None:
            raise ValueError("数据库会话对象必须外部输入")
        self.db_session = db_session
        self.sequence_repo = SequenceRepository(db_session)
        self.sample_repo = SampleRepository(db_session)
        self.project_repo = ProjectRepository(db_session)
        # 加载配置
        self.config = YAMLConfig()
    
    def validate_sequence_data_status(self) -> tuple:
        """
        仅验证sequence表中data_status为'pending'的记录的raw_data_path有效性，不进行修改
        
        逻辑：
        1. 查询data_status='pending'的记录
        2. 获取记录中的raw_data_path、batch_id、barcode等字段
        3. 根据config.yaml中的sequence_info配置验证路径
        4. 记录验证结果，但不更新数据状态
        5. 对超过2小时未满足条件的记录报出原因
        
        Returns:
            tuple: (验证通过的记录主键列表, 总验证记录数)
        """
        valid_sequence_ids = []
        total_count = 0
        
        try:
            # 1. 查询data_status='pending'的记录
            pending_sequences = self.sequence_repo.query_filter(data_status='pending')
            total_count = len(pending_sequences)
            logger.info(f"开始验证{total_count}条data_status为'pending'的sequence记录")
            
            # 2. 获取sequence_info配置
            sequence_info = self.config.get_sequence_info_config()
            dir1 = sequence_info.get('dir1', 'no_sample_id')
            dir2 = sequence_info.get('dir2', 'fastq_pass')
            key_file_ext = sequence_info.get('key_file', 'html')
            
            # 当前时间，用于判断超时
            current_time = datetime.now()
            two_hours_ago = current_time - timedelta(hours=2)
            
            # 3. 遍历验证每条记录
            for sequence in pending_sequences:
                sequence_id = sequence.sequence_id
                raw_data_path = sequence.raw_data_path
                batch_id = sequence.batch_id
                barcode = sequence.barcode
                created_at = sequence.created_at
                project_type = sequence.project_type
                
                # 验证路径和文件完整性
                is_valid, result = self._validate_sequence_path(
                    raw_data_path=raw_data_path,
                    barcode=barcode,
                    dir2=dir2
                )
                
                # 如果验证通过，result是最终路径；如果不通过，result是原因说明
                reason = result if not is_valid else f"验证通过，最终路径：{result}"
                
                # 检查是否超时
                is_timeout = created_at < two_hours_ago
                
                # 记录结果，验证通过时更新raw_data_path
                if is_valid:
                    valid_sequence_ids.append(sequence_id)
                    logger.info(f"sequence_id={sequence_id}数据验证通过")
                    
                    # 更新sequence表的raw_data_path字段为最终路径
                    try:
                        self.sequence_repo.update_sequence_fields(
                            sequence_id=sequence_id,
                            raw_data_path=str(result)
                        )
                        logger.info(f"已更新sequence_id={sequence_id}的raw_data_path字段为: {result}")
                    except Exception as update_err:
                        logger.error(f"更新sequence_id={sequence_id}的raw_data_path失败: {str(update_err)}")
                else:
                    # 如果未通过验证且超时，记录日志并发送云之家提醒
                    if is_timeout:
                        error_msg = f"sequence_id={sequence_id}验证失败且已超时(>2小时)：{reason}\nraw_data_path: {raw_data_path}\nbarcode: {barcode}\n项目类型: {project_type}"
                        logger.error(error_msg)
                        
                        # 发送云之家提醒
                        try:
                            notification_manager.send_yunzhijia_alert(
                                message=error_msg,
                                module="Sequence Validation",
                                status="timeout",
                                project_type=project_type
                            )
                            logger.info(f"已发送云之家提醒：{sequence_id}超时验证失败，项目类型：{project_type}")
                        except Exception as notify_err:
                            logger.error(f"发送云之家提醒失败：{str(notify_err)}")
                    else:
                        logger.info(f"sequence_id={sequence_id}验证未通过，等待后续重试：{reason}")
                    
            logger.info(f"sequence数据状态验证完成：共{total_count}条，通过{len(valid_sequence_ids)}条")
            return valid_sequence_ids, total_count
            
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：验证sequence数据状态失败", exc_info=True)
            return valid_sequence_ids, total_count
        except Exception as e:
            logger.error(f"验证sequence数据状态失败", exc_info=True)
            return valid_sequence_ids, total_count
    
    def update_validated_sequence(self, sequence_id: str) -> bool:
        """
        根据验证通过的记录主键，更新data_status为'valid'
        
        Args:
            sequence_id: 验证通过的记录主键
        
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info(f"开始更新验证通过的sequence记录状态，sequence_id={sequence_id}")
            
            # 初始化参数生成器，直接传入已有的Repository实例以避免重复创建
            parameter_generator = SequenceParameterGenerator(
                db_session=self.db_session,
                sequence_repo=self.sequence_repo,
                sample_repo=self.sample_repo,
                project_repo=self.project_repo
            )
            
            # 更新parameter字段json
            param_success = parameter_generator.generate_and_update_parameter(sequence_id)
            
            if param_success:
                # parameter更新成功后，再更新数据状态为'valid'
                update_success = self._update_sequence_data_status(sequence_id, 'valid')
                if update_success:
                    logger.info(f"sequence_id={sequence_id}状态更新成功")
                    return True
                else:
                    logger.error(f"sequence_id={sequence_id} parameter更新成功但状态更新失败")
                    return False
            else:
                logger.error(f"sequence_id={sequence_id} parameter更新失败")
                return False
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：更新sequence数据状态失败，sequence_id={sequence_id}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"更新sequence数据状态失败，sequence_id={sequence_id}", exc_info=True)
            return False
    
    def _get_latest_subdirectory(self, parent_dir: Path) -> Tuple[Optional[Path], str]:
        """
        获取指定目录下最新的子目录
        
        Args:
            parent_dir: 父目录路径
        
        Returns:
            Tuple[Optional[Path], str]: (最新子目录路径，如果没有子目录则为None, 错误信息或空字符串)
        """
        try:
            subdirs = [d for d in parent_dir.iterdir() if d.is_dir()]
            if not subdirs:
                return None, f"目录{parent_dir}下没有子目录"
            
            # 按修改时间排序，最新的在前面
            subdirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            return subdirs[0], ""
        except Exception as e:
            return None, f"获取子目录过程中发生错误: {str(e)}"
    
    def _validate_sequence_path(self, raw_data_path: str, barcode: str, 
                              dir2: str) -> Tuple[bool, str]:
        """
        验证sequence的raw_data_path路径有效性
        
        Args:
            raw_data_path: sequence记录中的raw_data_path字段值（格式为/batch_id_path/）
            barcode: 条形码
            dir2: 配置中的dir2值
        
        Returns:
            Tuple[bool, str]: (验证是否通过, 最终路径或原因说明)
        """
        # 检查raw_data_path是否存在
        if not raw_data_path:
            return False, "raw_data_path字段为空"
        
        # 1. 检查raw_data_path是否存在且为目录
        scan_dir = Path(raw_data_path)
        if not scan_dir.exists() or not scan_dir.is_dir():
            return False, f"目录{scan_dir}不存在或不是目录"
        
        try:
            # 获取第一层子目录并按修改时间排序，取最新的
            latest_first_level_dir, first_level_error = self._get_latest_subdirectory(scan_dir)
            if not latest_first_level_dir:
                return False, first_level_error
            
            # 获取第二层子目录并按修改时间排序，取最新的
            latest_second_level_dir, second_level_error = self._get_latest_subdirectory(latest_first_level_dir)
            if not latest_second_level_dir:
                return False, second_level_error
            
            # 2. 优先检查updated.done文件是否存在
            updated_done_file = latest_second_level_dir / "updated.done"
            if not updated_done_file.exists() or not updated_done_file.is_file():
                return False, f"文件{updated_done_file}不存在，数据可能传输不完整"
            
            # 3. 检查dir2/barcode是否存在且不为空
            full_barcode_path = latest_second_level_dir / dir2 / barcode
            if not full_barcode_path.exists() or not full_barcode_path.is_dir():
                return False, f"路径{full_barcode_path}不存在或不是目录"
            
            # 检查barcode文件夹是否为空
            if not any(full_barcode_path.iterdir()):
                return False, f"路径{full_barcode_path}存在但为空文件夹，下机数据不存在"
            
            # 全部验证通过，返回最终路径
            return True, str(full_barcode_path)
            
        except Exception as e:
            return False, f"路径验证过程中发生错误: {str(e)}"
    
    def _update_sequence_data_status(self, sequence_id: str, new_status: str) -> bool:
        """
        更新sequence记录的data_status字段
        
        Args:
            sequence_id: sequence的主键
            new_status: 新的状态值（'valid'或'invalid'）
        
        Returns:
            bool: 更新是否成功
        """
        try:
            # 使用update_sequence_fields方法更新字段
            success = self.sequence_repo.update_sequence_fields(
                sequence_id=sequence_id,
                update_data={'data_status': new_status},
                operator='system'
            )
            
            if success:
                logger.info(f"已更新sequence_id={sequence_id}的data_status为{new_status}")
                return True
            else:
                logger.error(f"更新sequence_id={sequence_id}的data_status失败")
                return False
                
        except Exception as e:
            logger.error(f"更新sequence_id={sequence_id}的data_status时发生错误", exc_info=True)
            return False
    
    def update_sequence_parameters(self, sequence_id: str, parameters: dict) -> bool:
        """
        更新sequence记录的parameters字段
        
        Args:
            sequence_id: sequence的主键
            parameters: 新的参数字典
        
        Returns:
            bool: 更新是否成功
        """
        try:
            # 验证输入参数
            if not isinstance(parameters, dict):
                raise ValueError("parameters必须是字典类型")
            
            # 使用update_sequence_fields方法更新字段
            success = self.sequence_repo.update_sequence_fields(
                sequence_id=sequence_id,
                update_data={'parameters': parameters},
                operator='system'
            )
            
            if success:
                logger.info(f"已更新sequence_id={sequence_id}的parameters字段")
                return True
            else:
                logger.error(f"更新sequence_id={sequence_id}的parameters字段失败")
                return False
                
        except Exception as e:
            logger.error(f"更新sequence_id={sequence_id}的parameters字段时发生错误", exc_info=True)
            return False
    
    