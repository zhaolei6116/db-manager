from src.repositories.base_repository import BaseRepository, ModelType
from src.models.models import Project
from sqlalchemy.orm import Session


class ProjectRepository(BaseRepository[Project]):
    """Project表专用Repository"""
    
    def _get_model(self) -> Project:
        """返回关联的ORM模型"""
        return Project
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "project_id"
    
    def update_project_fields(self, project_id: str, update_data: dict, operator: str = "system") -> bool:
        """
        更新Project表的非主键字段
        
        Args:
            project_id: 项目ID
            update_data: 待更新的字段字典
            operator: 操作人
        
        Returns:
            是否更新成功
        """
        try:
            # 过滤掉主键字段，防止意外修改
            if self.get_pk_field() in update_data:
                del update_data[self.get_pk_field()]
            
            # 逐个更新字段
            for field, value in update_data.items():
                result = self.update_field(
                    pk_value=project_id,
                    field_name=field,
                    new_value=value,
                    operator=operator
                )
                if not result:
                    return False
            return True
        except Exception as e:
            self.logger.error(f"更新Project字段失败: {str(e)}", exc_info=True)
            return False
    
    
