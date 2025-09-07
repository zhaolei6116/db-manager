# src/utils/data_processor.py
"""
数据处理器：解析JSON文件并生成各表字典，为后续各表处理自己数据生成ORM实例提供基础
"""

import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional
from src.utils.yaml_config import get_yaml_config

logger = logging.getLogger(__name__)

class DataProcessor:
    """数据处理器：解析JSON文件并生成project/sample/batch/sequence/sequence_run表的字段字典"""

    def __init__(self, config_file: Optional[str] = None):
        """
        初始化配置和字段映射关系
        
        Args:
            config_file: YAML配置文件路径，默认使用yaml_config.py的默认路径
        """
        self.config = get_yaml_config(config_file)
        self.fields_mapping = self.config.get_fields_mapping()
        self.sequence_info_config = self.config.get_sequence_info_config()
        self.sequence_run_config = self.config.get_sequence_run_config()

    def parse_json_file(self, json_path: Path) -> Optional[Dict[str, Any]]:
        """
        解析JSON文件并生成各表字段字典
        
        Args:
            json_path: JSON文件路径
        
        Returns:
            字典，键为表名（project/sample/batch/sequence/sequence_run），值为对应表的字段字典；
            如果解析失败，返回None
        """
        try:
            # 检查文件是否存在
            if not json_path.exists():
                logger.error(f"JSON文件不存在：{json_path}")
                return None
            if not json_path.is_file():
                logger.error(f"路径不是文件：{json_path}")
                return None

            # 读取JSON文件
            with open(json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # 生成各表字段字典
            result = {
                'project': self.get_project_dict(json_data),
                'sample': self.get_sample_dict(json_data),
                'batch': self.get_batch_dict(json_data),
                'sequence': self.get_sequence_dict(json_data),
                'sequence_run': self.get_sequence_run_dict(json_data)
            }
            
            logger.info(f"成功解析JSON文件：{json_path}")
            return result
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON文件解析错误：{json_path}，错误：{str(e)}")
            return None
        except Exception as e:
            logger.error(f"处理JSON文件失败：{json_path}，错误：{str(e)}")
            return None

    def get_table_field_dict(self, table_name: str, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        通用方法：根据表名和JSON数据生成字段字典
        
        Args:
            table_name: 表名（project/sample/batch/sequence）
            json_data: 输入的JSON数据
        
        Returns:
            字段字典，键为ORM字段名，值为JSON数据或None
        """
        try:
            table_mapping = self.fields_mapping.get(table_name)
            if not table_mapping:
                logger.error(f"表'{table_name}'的字段映射未在config.yaml中配置")
                raise KeyError(f"表'{table_name}'的字段映射未配置")

            field_dict = {}           

            # 映射字段
            for orm_field, json_field in table_mapping.items():
                if json_field in json_data:
                    field_dict[orm_field] = json_data[json_field]
                else:
                    field_dict[orm_field] = None
                    logger.debug(f"表'{table_name}'的JSON字段'{json_field}'缺失，ORM字段'{orm_field}'设为None")

            return field_dict
        
        except Exception as e:
            logger.error(f"生成表'{table_name}'字段字典失败：{str(e)}")
            raise

    def get_project_dict(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成project表的字段字典
        
        Args:
            json_data: 输入的JSON数据
        
        Returns:
            project表的字段字典
        """
        return self.get_table_field_dict('project', json_data)

    def get_sample_dict(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成sample表的字段字典
        
        Args:
            json_data: 输入的JSON数据
        
        Returns:
            sample表的字段字典
        """
        return self.get_table_field_dict('sample', json_data)

    def get_batch_dict(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成batch表的字段字典
        
        Args:
            json_data: 输入的JSON数据
        
        Returns:
            batch表的字段字典
        """
        return self.get_table_field_dict('batch', json_data)

    def get_sequence_dict(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成sequence表的字段字典
        
        Args:
            json_data: 输入的JSON数据
        
        Returns:
            sequence表的字段字典
        """
        return self.get_table_field_dict('sequence', json_data)

    def get_sequence_run_dict(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成sequence_run表的字段字典，基于sequence_run模板和JSON数据
        
        Args:
            json_data: 输入的JSON数据
        
        Returns:
            sequence_run表的字段字典
        """
        try:
            # 获取batch和sequence表的字段字典，用于模板渲染
            batch_dict = self.get_batch_dict(json_data)
            sequence_dict = self.get_sequence_dict(json_data)
            
            sample_id = sequence_dict.get('sample_id')
            batch_id = batch_dict.get('batch_id')
            laboratory = batch_dict.get("laboratory")
            sequencer_id = batch_dict.get("sequencer_id")

            lab_sequencer_id = f"{laboratory}{self.sequence_info_config.get('sequence_name')}{sequencer_id}"
            barcode = f"{sequence_dict.get('barcode_prefix')}{sequence_dict.get('barcode_number')}"
            batch_id_path = f"{self.sequence_info_config.get('sequence_data_path')}/{lab_sequencer_id}/{batch_id}"
            raw_data_path = f"{batch_id_path}/{self.sequence_info_config.get('dir1')}/"


            # 生成sequence_run字段
            field_dict = {
                'sample_id': sample_id,
                'batch_id': batch_id,
                'lab_sequencer_id': lab_sequencer_id,
                'barcode': barcode,
                'batch_id_path': batch_id_path,
                'raw_data_path': raw_data_path,
                'data_status': 'pending',  # 初始状态
                'process_status': 'no'     # 初始状态
            }

            # 移除None值（避免插入空字段）
            # field_dict = {k: v for k, v in field_dict.items() if v is not None}
            
            logger.debug(f"生成sequence_run字段字典：{field_dict}")
            return field_dict
        
        except Exception as e:
            logger.error(f"生成sequence_run字段字典失败：{str(e)}")
            raise

# 添加可执行测试部分
if __name__ == "__main__":
    # 配置日志以显示详细信息
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_logger = logging.getLogger("JsonDataProcessorTest")
    
    # 创建数据处理器实例
    processor = DataProcessor()
    
    # 测试JSON文件路径（请替换为实际的测试文件路径）
    test_json_path = Path("/nas02/project/zhaolei/pipeline/data_management/LimsData/25083005/T22508295523_R1.json")
    
    # 解析JSON文件并打印结果
    test_logger.info(f"开始测试解析JSON文件: {test_json_path}")
    result = processor.parse_json_file(test_json_path)
    print(result)
    if result:
        test_logger.info("JSON文件解析成功，结果如下:")
        for table_name, table_data in result.items():
            test_logger.info(f"\n===== {table_name} 表数据 =====, 数据条数: {len(table_data.items())}")
            
            for field, value in table_data.items():
                test_logger.info(f"{field}: {value}")
    else:
        test_logger.error("JSON文件解析失败，请检查日志获取详细错误信息")