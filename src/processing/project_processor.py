from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from src.processing.base_processor import BaseProcessor


class ProjectProcessor(BaseProcessor):
    """Project表数据处理器"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(db_session, "project")
    
    def process(self, json_data: Dict[str, Any], file_name: str) -> bool:
        """处理Project表数据"""
        try:
            # 获取ORM实例
            orm_instance = self.get_orm_instance(json_data)
            if not orm_instance:
                self.logger.error(f"文件[{file_name}]的project表数据生成ORM实例失败")
                return False
            
            # 获取主键值和已有记录
            pk_value = self.get_pk_value(orm_instance)
            existing_record = self.get_existing_record(pk_value)
            
            if existing_record is None:
                # 插入新记录
                inserted = self.repo.insert_if_not_exists(orm_instance)
                if inserted:
                    self.logger.info(f"文件[{file_name}]的project表数据插入成功")
                return inserted
            else:
                # 处理更新
                changed_fields = self.get_changed_fields(existing_record, orm_instance)
                if changed_fields:
                    # 执行更新
                    result = self.repo.update_project_fields(pk_value, changed_fields)
                    
                    if result:
                        self.logger.info(f"文件[{file_name}]的project表数据更新成功，字段: {list(changed_fields.keys())}")
                        
                        # 处理更新触发操作
                        old_data = {field: getattr(existing_record, field) for field in changed_fields}
                        self.process_field_updates(pk_value, old_data, changed_fields)
                    return result
                else:
                    self.logger.info(f"文件[{file_name}]的project表数据无变化，跳过更新")
                    return True
                    
        except Exception as e:
            self.logger.error(f"处理[{file_name}]的project表数据失败", exc_info=True)
            self.db_session.rollback()
            return False