from .base_repository import BaseRepository, ModelType
from src.models.models import FieldCorrections


class FieldCorrectionsRepository(BaseRepository[FieldCorrections]):
    """FieldCorrections表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return FieldCorrections
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "correction_id"
