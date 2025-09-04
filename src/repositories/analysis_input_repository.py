from .base_repository import BaseRepository, ModelType
from src.models.models import AnalysisInput


class AnalysisInputRepository(BaseRepository[AnalysisInput]):
    """AnalysisInput表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return AnalysisInput
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "input_id"
