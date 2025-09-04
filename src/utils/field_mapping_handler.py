# src/utils/field_mapping_handler.py

```

```

import logging
from typing import Dict, Any, Optional, Type, List
from sqlalchemy.ext.declarative import DeclarativeMeta
from src.models.database import get_session
from src.utils.yaml_config import get_yaml_config 
from src.models.models import (
    Project, Sample, Batch, Sequencing, SequenceRun,
    ProcessData, ProcessedDataDependency, AnalysisInput, AnalysisTask
)

# 初始化日志（与已有日志风格保持一致）
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 表名与ORM模型的映射字典（关联config.yaml的表名和实际模型）
TABLE_MODEL_MAPPING: Dict[str, Type[DeclarativeMeta]] = {
    "project": Project,
    "sample": Sample,
    "batch": Batch,
    "sequence": Sequencing,
    "sequence_run": SequenceRun,
    "process_data": ProcessData,
    "processed_data_dependency": ProcessedDataDependency,
    "analysis_inputs": AnalysisInput,
    "analysis_tasks": AnalysisTask
}

class FieldMappingHandler:
    """
    基于config.yaml的字段映射处理类
    功能：加载映射配置、JSON转ORM实例、未知字段检测、新字段提示
    """
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化：加载配置文件，解析字段映射规则
        :param config_file: 配置文件路径（默认使用database.py的默认路径）
        """
        # 1. 加载config.yaml配置（复用database.py的load_config方法，保证配置源一致）
        self.config = get_yaml_config(config_file)
        # 2. 解析核心配置节点（若配置缺失，抛出明确异常）
        self.fields_mapping = self._parse_required_config("fields_mapping", "字段映射规则")
        self.new_field_rules = self.config.get("new_field_rules", [])  # 新字段规则（可选）
        self.table_update_triggers = self._parse_required_config("table_update_triggers", "表更新触发规则")
        
        # 3. 初始化数据库会话（用于后续新字段检测时的表结构校验，可选）
        self.db_session = get_session(config_file)

    def _parse_required_config(self, config_key: str, config_desc: str) -> Dict[str, Any]:
        """
        解析必填的配置节点，若缺失则抛出异常
        :param config_key: 配置节点键（如fields_mapping）
        :param config_desc: 配置节点描述（用于异常提示）
        :return: 配置节点的字典数据
        """
        config_data = self.config.get(config_key)
        if not config_data or not isinstance(config_data, dict):
            raise ValueError(
                f"config.yaml中缺失有效的'{config_key}'配置（{config_desc}），"
                f"请检查配置文件是否包含该节点且格式为字典"
            )
        return config_data

    def get_table_mapping(self, table_name: str) -> Dict[str, Any]:
        """
        获取指定表的完整映射规则（从fields_mapping中）
        :param table_name: 表名（如project、sample，需与config.yaml的fields_mapping键一致）
        :return: 表的映射规则（包含key主键字段、fields字段映射）
        """
        table_mapping = self.fields_mapping.get(table_name)
        if not table_mapping:
            raise KeyError(
                f"fields_mapping中未配置表'{table_name}'的映射规则，"
                f"请在config.yaml的fields_mapping下添加'{table_name}'节点"
            )
        # 校验表映射的必填子节点（key：主键字段，fields：字段映射）
        required_subkeys = ["key", "fields"]
        for subkey in required_subkeys:
            if subkey not in table_mapping:
                raise ValueError(
                    f"表'{table_name}'的映射规则缺失必填子节点'{subkey}'，"
                    f"请补充配置（例：{table_name}: {{key: project_id, fields: ...}}）"
                )
        return table_mapping

    def get_orm_model(self, table_name: str) -> Type[DeclarativeMeta]:
        """
        根据表名获取对应的ORM模型（从TABLE_MODEL_MAPPING映射）
        :param table_name: 表名（如project、sample）
        :return: ORM模型类（如Project、Sample）
        """
        model = TABLE_MODEL_MAPPING.get(table_name)
        if not model:
            raise KeyError(
                f"表'{table_name}'未绑定ORM模型，"
                f"请在TABLE_MODEL_MAPPING中添加'{table_name}: 模型类'的映射（例：'sample': Sample）"
            )
        return model

    def detect_unknown_fields(self, table_name: str, json_data: Dict[str, Any]) -> List[str]:
        """
        检测JSON数据中的未知字段（未在fields_mapping配置的字段）
        :param table_name: 表名（用于获取配置的字段列表）
        :param json_data: LIMS拉取的JSON数据
        :return: 未知字段列表（空列表表示无未知字段）
        """
        # 1. 获取表配置的字段列表（含主键字段）
        table_mapping = self.get_table_mapping(table_name)
        config_fields = list(table_mapping["fields"].keys())
        # 2. 提取JSON中的所有字段
        json_fields = list(json_data.keys())
        # 3. 找出JSON中存在但配置中没有的字段（未知字段）
        unknown_fields = [f for f in json_fields if f not in config_fields]
        
        # 4. 日志记录未知字段（后续可触发new_field_rules处理）
        if unknown_fields:
            logger.warning(
                f"表'{table_name}'检测到{len(unknown_fields)}个未知字段（未在fields_mapping配置）："
                f"{unknown_fields}，请检查是否需要添加到config.yaml的fields_mapping或new_field_rules"
            )
        return unknown_fields

    def json_to_orm_instance(
        self, table_name: str, json_data: Dict[str, Any], ignore_unknown: bool = True
    ) -> DeclarativeMeta:
        """
        将LIMS JSON数据按配置映射为ORM模型实例（核心方法）
        :param table_name: 表名（用于获取映射规则和ORM模型）
        :param json_data: LIMS拉取的JSON数据（键为配置中的原始字段，值为数据）
        :param ignore_unknown: 是否忽略未知字段（True：忽略，False：未知字段时报错）
        :return: ORM模型实例（已填充数据，未提交到数据库）
        """
        # 1. 检测未知字段（根据ignore_unknown决定是否报错）
        unknown_fields = self.detect_unknown_fields(table_name, json_data)
        if unknown_fields and not ignore_unknown:
            raise ValueError(
                f"表'{table_name}'的JSON数据中存在未知字段，且ignore_unknown=False："
                f"{unknown_fields}，请补充配置或设置ignore_unknown=True"
            )

        # 2. 获取表的映射规则和ORM模型
        table_mapping = self.get_table_mapping(table_name)
        model = self.get_orm_model(table_name)
        field_mapping = table_mapping["fields"]  # 配置：ORM字段 -> JSON原始字段（例：project_id: Client）

        # 3. 按映射规则填充ORM实例数据
        orm_instance = model()
        for orm_field, json_field in field_mapping.items():
            # 检查JSON中是否包含当前映射的字段（不强制要求，允许为None）
            if json_field not in json_data:
                logger.debug(
                    f"表'{table_name}'的JSON数据中缺失映射字段'{json_field}'（对应ORM字段'{orm_field}'），"
                    f"ORM字段将设为默认值（如None）"
                )
                continue
            
            # 获取JSON字段值，并赋值给ORM实例的对应字段
            json_value = json_data[json_field]
            # 校验ORM字段是否存在（避免配置错误导致的属性不存在）
            if not hasattr(orm_instance, orm_field):
                raise AttributeError(
                    f"ORM模型'{model.__name__}'不存在字段'{orm_field}'，"
                    f"请检查config.yaml的fields_mapping配置是否正确"
                )
            setattr(orm_instance, orm_field, json_value)
            logger.debug(
                f"表'{table_name}'：ORM字段'{orm_field}' <- JSON字段'{json_field}'，值：{json_value}"
            )

        # 4. 特殊处理：SequenceRun的路径模板（基于config.yaml的sequence_run节点）
        if table_name == "sequence_run":
            self._fill_sequence_run_templates(orm_instance)

        return orm_instance

    def _fill_sequence_run_templates(self, sequence_run_instance: SequenceRun) -> None:
        """
        补充SequenceRun实例的路径模板字段（如lab_sequencer_id、raw_data_path，基于config.yaml的sequence_run节点）
        :param sequence_run_instance: 未填充模板的SequenceRun实例
        """
        # 1. 获取sequence_run的模板配置
        sequence_run_config = self.config.get("sequence_run")
        if not sequence_run_config:
            logger.warning("config.yaml中缺失'sequence_run'节点，无法填充SequenceRun的路径模板字段")
            return

        # 2. 准备模板所需的上下文数据（从实例或配置中获取）
        template_context = {
            "laboratory": sequence_run_instance.sequencing.batch.laboratory,  # 从关联的Batch获取
            "sequence_name": self.config.get("sequence_data", {}).get("sequence_name", "UnknownSequencer"),
            "sequencer_id": sequence_run_instance.sequencing.batch.sequencer_id,  # 从关联的Batch获取
            "barcode_prefix": sequence_run_instance.sequencing.barcode_prefix,  # 从关联的Sequencing获取
            "barcode_number": sequence_run_instance.sequencing.barcode_number,  # 从关联的Sequencing获取
            "sequence_data_path": self.config.get("sequence_data", {}).get("sequence_data_path", "/path/to/sequence"),
            "batch_id": sequence_run_instance.sequencing.batch.batch_id,  # 从关联的Batch获取
            "dir1": self.config.get("sequence_data", {}).get("dir1", "no_sample_id"),
            "dir2": self.config.get("sequence_data", {}).get("dir2", "fastq_pass"),
            "run_name": sequence_run_instance.sequencing.batch.batch_id,  # 暂用batch_id作为run_name，可按需调整
            "barcode": f"{sequence_run_instance.sequencing.barcode_prefix}{sequence_run_instance.sequencing.barcode_number}"
        }

        # 3. 填充模板字段（lab_sequencer_id、batch_id_path、raw_data_path）
        try:
            # 实验室测序仪ID（例：LabAUnknownSequencerSeq001）
            if "lab_sequencer_id_template" in sequence_run_config:
                sequence_run_instance.lab_sequencer_id = sequence_run_config["lab_sequencer_id_template"].format(**template_context)
            # 批次路径（例：/path/to/sequence/LabAUnknownSequencerSeq001/Batch001）
            if "batch_id_path_template" in sequence_run_config:
                sequence_run_instance.batch_id_path = sequence_run_config["batch_id_path_template"].format(**template_context)
            # 原始数据路径（初始设为模板值，后续任务8验证有效性）
            if "raw_data_path_template" in sequence_run_config:
                sequence_run_instance.raw_data_path = sequence_run_config["raw_data_path_template"].format(**template_context)
                # 初始数据状态设为pending（后续任务8验证后更新为valid/invalid）
                sequence_run_instance.data_status = "pending"
                sequence_run_instance.process_status = "no"

            logger.debug(
                f"SequenceRun模板字段填充完成：lab_sequencer_id={sequence_run_instance.lab_sequencer_id}, "
                f"batch_id_path={sequence_run_instance.batch_id_path}, raw_data_path={sequence_run_instance.raw_data_path}"
            )
        except KeyError as e:
            raise ValueError(
                f"SequenceRun路径模板填充失败，缺失上下文数据：{str(e)}，"
                f"请检查config.yaml的sequence_data或sequence_run节点配置"
            )

    def close(self) -> None:
        """关闭数据库会话（避免连接泄漏）"""
        if hasattr(self, "db_session") and self.db_session.is_active:
            self.db_session.close()
            logger.info("FieldMappingHandler的数据库会话已关闭")

    def __del__(self) -> None:
        """析构函数：自动关闭数据库会话"""
        self.close()