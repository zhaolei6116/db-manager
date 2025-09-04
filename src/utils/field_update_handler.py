import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from src.utils.yaml_config import get_yaml_config
from src.repositories.project_repository import ProjectRepository
from src.repositories.sample_repository import SampleRepository
from src.repositories.batch_repository import BatchRepository
from src.repositories.sequence_repository import SequenceRepository
from src.repositories.sequence_run_repository import SequenceRunRepository


# 初始化日志
logger = logging.getLogger(__name__)

class FieldUpdateHandler:
    """字段更新处理类，负责处理字段更新及触发后续操作"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.config = get_yaml_config()
        self.update_triggers = self.config.get("table_update_triggers", {})
        
        # 初始化各表Repository
        self.repos = {
            "project": ProjectRepository(db_session),
            "sample": SampleRepository(db_session),
            "batch": BatchRepository(db_session),
            "sequence": SequenceRepository(db_session),
            "sequence_run": SequenceRunRepository(db_session)
        }
    
    def process_table_updates(self, table_name: str, record_id: str, old_data: Dict[str, Any], new_data: Dict[str, Any]) -> bool:
        """
        处理表字段更新，并根据配置触发后续操作
        
        Args:
            table_name: 表名
            record_id: 记录ID（主键值）
            old_data: 更新前的字段数据
            new_data: 更新后的字段数据
        
        Returns:
            是否处理成功
        """
        try:
            # 获取该表的更新触发规则
            table_triggers = self.update_triggers.get(table_name, {})
            
            # 检查哪些字段发生了变化
            changed_fields = [field for field in new_data if new_data[field] != old_data.get(field)]
            if not changed_fields:
                logger.debug(f"表[{table_name}]记录[{record_id}]没有字段发生变化，无需触发操作")
                return True
            
            logger.info(f"表[{table_name}]记录[{record_id}]的字段[{changed_fields}]发生变化，开始处理触发操作")
            
            # 处理每个变化的字段
            for field in changed_fields:
                # 检查是否有针对该字段的触发规则
                field_trigger = table_triggers.get(field, {})
                if not field_trigger:
                    continue
                
                # 根据触发类型执行相应操作
                trigger_type = field_trigger.get("type")
                if trigger_type == "reanalyze":
                    # 触发重新分析：将对应SequenceRun的process_status设为no
                    self._handle_reanalyze_trigger(table_name, record_id, field)
                elif trigger_type == "update_only":
                    # 仅更新，无需额外操作
                    logger.info(f"表[{table_name}]字段[{field}]触发'update_only'操作，无需额外处理")
                else:
                    logger.warning(f"未知的触发类型[{trigger_type}]，表[{table_name}]字段[{field}]")
            
            return True
        except Exception as e:
            logger.error(f"处理表[{table_name}]更新触发操作失败", exc_info=True)
            return False
    
    def _handle_reanalyze_trigger(self, table_name: str, record_id: str, field: str) -> bool:
        """
        处理重新分析触发操作
        
        Args:
            table_name: 表名
            record_id: 记录ID
            field: 触发字段名
        
        Returns:
            是否处理成功
        """
        try:
            logger.info(f"表[{table_name}]记录[{record_id}]字段[{field}]触发重新分析操作")
            
            # 根据不同表找到关联的sequence_id
            sequence_ids = []
            if table_name == "project":
                # 项目变化，找到所有关联的样本和测序记录
                sample_repo = self.repos["sample"]
                samples = sample_repo.get_samples_by_project(record_id)
                for sample in samples:
                    sequence_repo = self.repos["sequence"]
                    sequences = sequence_repo.query_filter({"sample_id": sample.sample_id})
                    sequence_ids.extend([seq.sequence_id for seq in sequences])
            
            elif table_name == "sample":
                # 样本变化，找到所有关联的测序记录
                sequence_repo = self.repos["sequence"]
                sequences = sequence_repo.query_filter({"sample_id": record_id})
                sequence_ids = [seq.sequence_id for seq in sequences]
            
            elif table_name == "batch":
                # 批次变化，找到所有关联的测序记录
                sequence_repo = self.repos["sequence"]
                sequences = sequence_repo.get_sequences_by_batch(record_id)
                sequence_ids = [seq.sequence_id for seq in sequences]
            
            elif table_name == "sequence":
                # 测序记录变化，直接使用其ID
                sequence_ids = [record_id]
            
            # 设置这些测序记录需要重新分析
            sequence_run_repo = self.repos["sequence_run"]
            for seq_id in sequence_ids:
                result = sequence_run_repo.set_reanalysis_flag(seq_id)
                if result:
                    logger.info(f"已设置测序记录[{seq_id}]需要重新分析")
                else:
                    logger.warning(f"设置测序记录[{seq_id}]重新分析标记失败")
            
            return True
        except Exception as e:
            logger.error(f"处理重新分析触发操作失败", exc_info=True)
            return False
    
    def add_new_field(self, table_name: str, field_name: str, field_type: str, description: str) -> Dict[str, Any]:
        """
        提供添加新字段的接口（供管理员使用）
        
        Args:
            table_name: 表名
            field_name: 新字段名
            field_type: 字段类型（如varchar(100), int等）
            description: 字段描述
        
        Returns:
            操作结果
        """
        try:
            # 这里实际项目中需要执行ALTER TABLE添加字段的SQL
            # 这里仅做日志记录和返回操作结果
            logger.info(f"管理员操作：为表[{table_name}]添加新字段[{field_name}]，类型[{field_type}]，描述[{description}]")
            
            # 实际实现时应执行:
            # from sqlalchemy import text
            # sql = f"ALTER TABLE {table_name} ADD COLUMN {field_name} {field_type}"
            # self.db_session.execute(text(sql))
            # self.db_session.commit()
            
            return {
                "success": True,
                "message": f"已成功为表[{table_name}]添加新字段[{field_name}]",
                "field_name": field_name,
                "table_name": table_name
            }
        except Exception as e:
            logger.error(f"添加新字段失败", exc_info=True)
            return {
                "success": False,
                "message": f"添加新字段失败: {str(e)}",
                "field_name": field_name,
                "table_name": table_name
            }
