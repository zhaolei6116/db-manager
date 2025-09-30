from .base_repository import BaseRepository, ModelType
from typing import Dict, Any
from src.models.models import Batch
from sqlalchemy.orm import Session
from typing import Generic, TypeVar

# 泛型类型定义
ModelType = TypeVar('ModelType')


class BatchRepository(BaseRepository[Batch]):
    """Batch表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return Batch
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "batch_id"
        
    def dict_to_orm(self, json_data: Dict[str, Any]) -> Batch:
        """
        将JSON数据字典转换为Batch ORM实例
        
        Args:
            json_data: 包含Batch字段的JSON数据字典
        
        Returns:
            Batch: Batch ORM实例
        
        Notes:
            从JSON中提取需要的字段，创建Batch实例
            如果JSON中缺少某些字段，将使用None
        """
        # 映射JSON字段到Batch模型字段
        # 根据models.py中的字段注释，Batch_id对应batch_id
        batch_id = json_data.get('Batch_id') or json_data.get('batch_id')
        
        # 创建Batch实例
        batch = Batch(
            batch_id=batch_id,
            sequencer_id=json_data.get('Sequencer_id') or json_data.get('sequencer_id'),
            laboratory=json_data.get('Laboratory') or json_data.get('laboratory')
        )
        
        return batch
    
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
