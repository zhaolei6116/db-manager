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
    
    def get_valid_unprocessed_sequences(self):
        """获取数据有效且未处理的序列"""
        return self.db_session.query(self._get_model()).filter(
            self._get_model().data_status == 'valid',
            self._get_model().process_status == 'no'
        ).all()
    
    def get_by_project_id_and_type(self, project_id: str, project_type: str):
        """根据项目ID和类型获取有效的序列数据
        
        仅返回data_status为valid的序列记录
        
        Args:
            project_id: 项目ID
            project_type: 项目类型
            
        Returns:
            List[Sequence]: 符合条件的序列记录列表
        """
        return self.db_session.query(self._get_model()).filter(
            self._get_model().project_id == project_id,
            self._get_model().project_type == project_type,
            self._get_model().data_status == 'valid'
        ).all()
    
    def update_sequence_process_status(self, sequence_ids: List[str], status: str = 'yes') -> bool:
        """批量更新序列的处理状态
        
        Args:
            sequence_ids (List[str]): 序列ID列表
            status (str): 处理状态，默认为'yes'
        
        Returns:
            bool: 更新是否成功
        """
        if not sequence_ids:
            return True
            
        try:
            # 更新序列的处理状态
            self.db_session.query(self._get_model()).filter(
                self._get_model().sequence_id.in_(sequence_ids)
            ).update({
                'process_status': status
            }, synchronize_session=False)
            
            return True
        except SQLAlchemyError as e:
            logger.error(f"批量更新序列处理状态失败: {str(e)}", exc_info=True)
            raise

    