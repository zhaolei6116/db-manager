from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import Sample
from sqlalchemy.orm import Session


class SampleRepository(BaseRepository[Sample]):
    """Sample表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return Sample
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "sample_id"
    
    def get_samples_by_project(self, project_id: str) -> list[Sample]:
        """
        获取指定项目下的所有样本
        
        Args:
            project_id: 项目ID
        
        Returns:
            样本列表
        """
        return self.query_filter({"project_id": project_id})
    
    def update_sample_fields(self, sample_id: str, update_data: dict, operator: str = "system") -> bool:
        """
        更新Sample表的非主键字段
        
        Args:
            sample_id: 样本ID
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
                    pk_value=sample_id,
                    field_name=field,
                    new_value=value,
                    operator=operator
                )
                if not result:
                    return False
            return True
        except Exception as e:
            self.logger.error(f"更新Sample字段失败: {str(e)}", exc_info=True)
            return False
