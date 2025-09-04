from .base_repository import BaseRepository, ModelType
from src.models.models import ProcessData


class ProcessDataRepository(BaseRepository[ProcessData]):
    """ProcessData表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return ProcessData
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "process_id"
