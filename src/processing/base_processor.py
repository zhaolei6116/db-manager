import logging
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import inspect

from src.utils.yaml_config import get_yaml_config
from src.utils.logging_config import get_lims_puller_logger
from src.models.database import get_session
from src.utils.field_mapping_handler import FieldMappingHandler
from src.utils.field_update_handler import FieldUpdateHandler


class BaseProcessor:
    """基础处理器类，提供公共方法"""
    
    def __init__(self, db_session: Optional[Session] = None, table_name: str = ""):
        self.db_session = db_session or get_session()
        self.config = get_yaml_config()
        self.logger = get_lims_puller_logger()
        self.mapping_handler = FieldMappingHandler()
        self.update_handler = FieldUpdateHandler(self.db_session)
        self.table_name = table_name
        self.repo = self._initialize_repo()
        
    def _initialize_repo(self):
        """初始化对应的Repository"""
        if not self.table_name:
            return None
            
        try:
            repo_module = __import__(
                f"src.repositories.{self.table_name}_repository",
                fromlist=[f"{self.table_name.capitalize()}Repository"]
            )
            repo_class = getattr(repo_module, f"{self.table_name.capitalize()}Repository")
            return repo_class(self.db_session)
        except Exception as e:
            self.logger.error(f"初始化{self.table_name}的Repository失败", exc_info=True)
            return None
    
    def get_orm_instance(self, json_data: Dict[str, Any]) -> Any:
        """获取ORM实例"""
        if not self.table_name:
            self.logger.error("未指定表名，无法创建ORM实例")
            return None
            
        return self.mapping_handler.json_to_orm_instance(
            table_name=self.table_name,
            json_data=json_data,
            ignore_unknown=True
        )
    
    def get_pk_value(self, orm_instance: Any) -> Any:
        """获取主键值"""
        if not self.repo:
            return None
            
        pk_field = self.repo.get_pk_field()
        return getattr(orm_instance, pk_field, None)
    
    def get_existing_record(self, pk_value: Any) -> Any:
        """获取已存在的记录"""
        if not self.repo or pk_value is None:
            return None
            
        return self.repo.get_by_pk(pk_value)
    
    def get_changed_fields(self, existing_record: Any, new_instance: Any) -> Dict[str, Any]:
        """获取变更的字段"""
        if not existing_record or not new_instance:
            return {}
            
        inspector = inspect(new_instance.__class__)
        pk_field = self.repo.get_pk_field()
        changed_fields = {}
        
        for column in inspector.columns:
            field = column.name
            if field == pk_field:
                continue
                
            old_value = getattr(existing_record, field)
            new_value = getattr(new_instance, field)
            
            if old_value != new_value:
                changed_fields[field] = new_value
                
        return changed_fields
    
    def process_field_updates(self, record_id: str, old_data: Dict[str, Any], new_data: Dict[str, Any]) -> bool:
        """处理字段更新"""
        return self.update_handler.process_table_updates(
            table_name=self.table_name,
            record_id=record_id,
            old_data=old_data,
            new_data=new_data
        )
    
    def close(self):
        """关闭资源"""
        if hasattr(self, "db_session") and self.db_session.is_active:
            self.db_session.close()