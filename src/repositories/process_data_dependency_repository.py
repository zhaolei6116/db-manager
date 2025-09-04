from .base_repository import BaseRepository, ModelType
from src.models.models import ProcessedDataDependency


class ProcessedDataDependencyRepository(BaseRepository[ProcessedDataDependency]):
    """ProcessedDataDependency表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return ProcessedDataDependency
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "id"
