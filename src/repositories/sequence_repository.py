# src/repositories/sequence_repository.py
from typing import Optional, List, Dict, Any
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging

from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import Sequence

logger = logging.getLogger(__name__)

class SequenceRepository(BaseRepository[Sequence]):
    """Sequence表专用Repository"""
    
    def _get_model(self) -> ModelType:
        return Sequence
    
    def get_pk_field(self) -> str:
        return "sequence_id"

    
    def get_sequences_by_batch(self, batch_id: str) -> List[Sequence]:
        """
        获取指定批次的所有测序记录
        """
        return self.query_filter(batch_id=batch_id)
    
    def update_sequence_fields(self, sequence_id: str, update_data: Dict[str, Any], operator: str = "system") -> bool:
        """
        更新Sequencing表的非主键字段
        """
        try:
            if self.get_pk_field() in update_data:
                del update_data[self.get_pk_field()]
            for field, value in update_data.items():
                success, _ = self.update_field(sequence_id, field, value, operator)
                if not success:
                    return False
            return True
        except SQLAlchemyError as e:
            logger.error(f"更新Sequencing字段失败: {str(e)}", exc_info=True)
            raise

    