from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import Sequencing
from sqlalchemy.orm import Session


class SequenceRepository(BaseRepository[Sequencing]):
    """Sequencing表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return Sequencing
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "sequence_id"
    
    def exists_by_sample_and_batch(self, sample_id: str, batch_id: str) -> bool:
        """
        检查样本和批次的组合是否已存在
        
        Args:
            sample_id: 样本ID
            batch_id: 批次ID
        
        Returns:
            是否存在
        """
        try:
            return self.db_session.query(self.model).filter(
                self.model.sample_id == sample_id,
                self.model.batch_id == batch_id
            ).first() is not None
        except Exception as e:
            self.logger.error(f"检查样本和批次组合失败: {str(e)}", exc_info=True)
            return False
    
    def get_sequences_by_batch(self, batch_id: str) -> list[Sequencing]:
        """
        获取指定批次的所有测序记录
        
        Args:
            batch_id: 批次ID
        
        Returns:
            测序记录列表
        """
        return self.query_filter({"batch_id": batch_id})
    
    def update_sequence_fields(self, sequence_id: str, update_data: dict, operator: str = "system") -> bool:
        """
        更新Sequencing表的非主键字段
        
        Args:
            sequence_id: 测序ID
            update_data: 待更新的字段字典
            operator: 操作人
        
        Returns:
            是否更新成功
        """
        try:
            # 过滤掉主键字段
            if self.get_pk_field() in update_data:
                del update_data[self.get_pk_field()]
            
            # 逐个更新字段
            for field, value in update_data.items():
                result = self.update_field(
                    pk_value=sequence_id,
                    field_name=field,
                    new_value=value,
                    operator=operator
                )
                if not result:
                    return False
            return True
        except Exception as e:
            self.logger.error(f"更新Sequencing字段失败: {str(e)}", exc_info=True)
            return False
