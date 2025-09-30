from src.repositories.base_repository import BaseRepository, ModelType
from typing import Dict, Any
from src.models.models import Sample
from sqlalchemy.orm import Session
from typing import Generic, TypeVar

# 泛型类型定义
ModelType = TypeVar('ModelType')


class SampleRepository(BaseRepository[Sample]):
    """Sample表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return Sample
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "sample_id"
        
    def dict_to_orm(self, json_data: Dict[str, Any]) -> Sample:
        """
        将JSON数据字典转换为Sample ORM实例
        
        Args:
            json_data: 包含Sample字段的JSON数据字典
        
        Returns:
            Sample: Sample ORM实例
        
        Notes:
            从JSON中提取需要的字段，创建Sample实例
            如果JSON中缺少某些字段，将使用None
        """
        # 映射JSON字段到Sample模型字段
        # 注意：根据models.py中的字段注释，Detect_no对应sample_id
        sample_id = json_data.get('Detect_no') or json_data.get('sample_id')
        
        # 创建Sample实例
        sample = Sample(
            sample_id=sample_id,
            project_id=json_data.get('project_id'),
            sample_name=json_data.get('Sample_name'),
            sample_type=json_data.get('Sample_type'),
            sample_type_raw=json_data.get('Sample_type_raw'),
            resistance_type=json_data.get('Resistance_type'),
            species_name=json_data.get('Species_name'),
            genome_size=json_data.get('Genome_size'),
            data_volume=json_data.get('Data_volume'),
            ref=json_data.get('Ref'),
            plasmid_length=json_data.get('PLASMID_LENGTH'),
            length=json_data.get('Length')
        )
        
        return sample
    
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
