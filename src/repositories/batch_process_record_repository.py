from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import BatchProcessRecord

class BatchProcessRecordRepository(BaseRepository[BatchProcessRecord]):
    """BatchProcessRecord表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return BatchProcessRecord
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "id"
