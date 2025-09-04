from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from src.processing.base_processor import BaseProcessor


class SequenceProcessor(BaseProcessor):
    """Sequencing表数据处理器"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(db_session, "sequence")


class SequenceRunProcessor(BaseProcessor):
    """SequenceRun表数据处理器"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(db_session, "sequence_run")
    
    def process(self, json_data: Dict[str, Any], file_name: str) -> bool:
        """处理SequenceRun表数据"""
        try:
            # 获取ORM实例
            orm_instance = self.get_orm_instance(json_data)
            if not orm_instance:
                self.logger.error(f"文件[{file_name}]的sequence_run表数据生成ORM实例失败")
                return False
            
            # 获取主键值和已有记录
            pk_value = self.get_pk_value(orm_instance)
            existing_record = self.get_existing_record(pk_value)
            
            if existing_record is None:
                # 插入新记录
                inserted = self.repo.insert_if_not_exists(orm_instance)
                if inserted:
                    self.logger.info(f"文件[{file_name}]的sequence_run表数据插入成功")
                return inserted
            else:
                # 处理更新
                changed_fields = self.get_changed_fields(existing_record, orm_instance)
                if changed_fields:
                    # 执行更新
                    result = self.repo.update_sequence_run_fields(pk_value, changed_fields)
                    
                    if result:
                        self.logger.info(f"文件[{file_name}]的sequence_run表数据更新成功，字段: {list(changed_fields.keys())}")
                        
                        # 处理更新触发操作
                        old_data = {field: getattr(existing_record, field) for field in changed_fields}
                        self.process_field_updates(pk_value, old_data, changed_fields)
                    return result
                else:
                    self.logger.info(f"文件[{file_name}]的sequence_run表数据无变化，跳过更新")
                    return True
                    
        except Exception as e:
            self.logger.error(f"处理[{file_name}]的sequence_run表数据失败", exc_info=True)
            self.db_session.rollback()
            return False


class CombinedSequenceProcessor:
    """组合处理器，同时处理Sequencing和SequenceRun表"""
    
    def __init__(self, db_session: Optional[Session] = None):
        self.db_session = db_session or Session()
        self.sequence_processor = SequenceProcessor(db_session)
        self.sequence_run_processor = SequenceRunProcessor(db_session)
    
    def process(self, json_data: Dict[str, Any], file_name: str) -> bool:
        """先处理Sequencing表，再处理SequenceRun表"""
        # 处理Sequencing表
        sequence_success = self._process_sequencing(json_data, file_name)
        if not sequence_success:
            return False
            
        # 处理SequenceRun表
        return self.sequence_run_processor.process(json_data, file_name)
    
    def _process_sequencing(self, json_data: Dict[str, Any], file_name: str) -> bool:
        """处理Sequencing表，包含run_type逻辑"""
        try:
            processor = self.sequence_processor
            repo = processor.repo
            
            # 获取ORM实例
            orm_instance = processor.get_orm_instance(json_data)
            if not orm_instance:
                processor.logger.error(f"文件[{file_name}]的sequence表数据生成ORM实例失败")
                return False
            
            # 获取主键值和已有记录
            pk_value = processor.get_pk_value(orm_instance)
            existing_record = processor.get_existing_record(pk_value)
            
            # 检查样本和批次组合是否存在
            sample_id = getattr(orm_instance, "sample_id")
            batch_id = getattr(orm_instance, "batch_id")
            if existing_record is None and repo.exists_by_sample_and_batch(sample_id, batch_id):
                processor.logger.info(f"文件[{file_name}]的sequence表数据已存在（样本和批次组合），跳过")
                return True
            
            if existing_record is None:
                # 插入新记录，使用带run_type逻辑的方法
                inserted = repo.insert_with_run_type(orm_instance)
                if inserted:
                    processor.logger.info(f"文件[{file_name}]的sequence表数据插入成功（自动设置run_type）")
                return inserted
            else:
                # 处理更新
                changed_fields = processor.get_changed_fields(existing_record, orm_instance)
                if changed_fields:
                    # 执行更新
                    result = repo.update_sequence_fields(pk_value, changed_fields)
                    
                    if result:
                        processor.logger.info(f"文件[{file_name}]的sequence表数据更新成功，字段: {list(changed_fields.keys())}")
                        
                        # 处理更新触发操作
                        old_data = {field: getattr(existing_record, field) for field in changed_fields}
                        processor.process_field_updates(pk_value, old_data, changed_fields)
                    return result
                else:
                    processor.logger.info(f"文件[{file_name}]的sequence表数据无变化，跳过更新")
                    return True
                    
        except Exception as e:
            self.sequence_processor.logger.error(f"处理[{file_name}]的sequence表数据失败", exc_info=True)
            self.db_session.rollback()
            return False