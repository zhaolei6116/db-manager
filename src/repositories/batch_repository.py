from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import Batch
from sqlalchemy.orm import Session


class BatchRepository(BaseRepository[Batch]):
    """Batch表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return Batch
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "batch_id"
    
    def get_batches_by_sequencer(self, sequencer_id: str) -> list[Batch]:
        """
        获取指定测序仪的所有批次
        
        Args:
            sequencer_id: 测序仪ID
        
        Returns:
            批次列表
        """
        return self.query_filter({"sequencer_id": sequencer_id})
    
    def update_batch_fields(self, batch_id: str, update_data: dict, operator: str = "system") -> bool:
        """
        更新Batch表的非主键字段
        
        Args:
            batch_id: 批次ID
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
                    pk_value=batch_id,
                    field_name=field,
                    new_value=value,
                    operator=operator
                )
                if not result:
                    return False
            return True
        except Exception as e:
            self.logger.error(f"更新Batch字段失败: {str(e)}", exc_info=True)
            return False
