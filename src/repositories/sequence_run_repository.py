from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import SequenceRun
from sqlalchemy.orm import Session


class SequenceRunRepository(BaseRepository[SequenceRun]):
    """SequenceRun表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return SequenceRun
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "sequence_id"
    
    def set_reanalysis_flag(self, sequence_id: str, operator: str = "system") -> bool:
        """
        设置重新分析标记（将process_status设为'no'）
        
        Args:
            sequence_id: 测序ID
            operator: 操作人
        
        Returns:
            是否设置成功
        """
        return self.update_field(
            pk_value=sequence_id,
            field_name="process_status",
            new_value="no",
            operator=operator
        )
    
    def update_data_status(self, sequence_id: str, status: str, operator: str = "system") -> bool:
        """
        更新数据状态
        
        Args:
            sequence_id: 测序ID
            status: 状态值（valid/invalid/pending）
            operator: 操作人
        
        Returns:
            是否更新成功
        """
        return self.update_field(
            pk_value=sequence_id,
            field_name="data_status",
            new_value=status,
            operator=operator
        )
    
    def update_sequence_run_fields(self, sequence_id: str, update_data: dict, operator: str = "system") -> bool:
        """
        更新SequenceRun表的非主键字段
        
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
            self.logger.error(f"更新SequenceRun字段失败: {str(e)}", exc_info=True)
            return False
