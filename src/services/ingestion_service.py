"""数据录入服务
负责组合其他脚本功能，实现完整的数据录入业务逻辑
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from datetime import datetime

from src.models.database import get_session
from src.utils.yaml_config import get_yaml_config
from src.processing.file_management import FileManager
from src.processing.json_data_processor import DataProcessor
from src.processing.lims_data_processor import LIMSDataProcessor

logger = logging.getLogger(__name__)


class IngestionService:
    """数据录入服务，组合其他脚本功能实现业务逻辑"""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化数据录入服务
        
        Args:
            config_file: 配置文件路径
        """
        self.config = get_yaml_config(config_file)
        self.file_manager = None
        self.data_processor = DataProcessor(config_file)
        
    def get_new_json_files(self) -> List[Path]:
        """
        调用run_lims_puller获取所有新的JSON文件
        
        Returns:
            JSON文件路径列表
        """
        logger.info("从LIMS系统拉取新的JSON文件")
        with get_session() as db_session:
            self.file_manager = FileManager(db_session)
            new_files = self.file_manager.get_new_files_from_run_lims_puller()
        
        logger.info(f"从LIMS系统拉取到{len(new_files)}个新的JSON文件")
        return new_files
    
    def process_single_json_file(self, file_path: Union[Path, str]) -> bool:
        """
        处理单个JSON文件
        
        Args:
            file_path: JSON文件路径（可以是Path对象或字符串）
            
        Returns:
            处理是否成功
        """
        # 确保file_path是Path对象
        if isinstance(file_path, str):
            file_path = Path(file_path)
        
        file_name = file_path.name
        logger.info(f"开始处理文件: {file_path}")
        
        try:
            # 1. 解析JSON文件获取结果字典
            json_data = self.data_processor.parse_json_file(file_path)
            if not json_data:
                logger.error(f"文件[{file_name}]解析失败")
                return False
            
            # 2. 为每个文件创建单独的session，调用process_parsed_json_dict处理解析后的字典
            with get_session() as db_session:
                lims_processor = LIMSDataProcessor()
                result = lims_processor.process_parsed_json_dict(
                    parsed_data=json_data,
                    db_session=db_session,
                    source_name=file_name
                )
                
                success = result["success"]
                
                # 提交事务
                db_session.commit()
                
            if success:
                logger.info(f"文件[{file_name}]处理成功")
            else:
                logger.error(f"文件[{file_name}]处理失败")
                
            return success
            
        except Exception as e:
            logger.error(f"文件[{file_name}]处理过程中发生异常: {str(e)}", exc_info=True)
            return False
    
    def process_all_new_files(self) -> Dict[str, Any]:
        """
        循环处理所有新的JSON文件
        
        Returns:
            处理结果统计信息
        """
        logger.info("开始处理所有新的JSON文件")
        
        # 1. 获取所有新的JSON文件
        new_files = self.get_new_json_files()
        total = len(new_files)
        success_count = 0
        failure_count = 0
        
        # 2. 循环处理每个文件
        for file_path in new_files:
            success = self.process_single_json_file(file_path)
            if success:
                success_count += 1
            else:
                failure_count += 1
        
        # 3. 返回处理结果统计
        result = {
            "total": total,
            "success_count": success_count,
            "failure_count": failure_count,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.info(
            f"所有新文件处理完成：总文件数{total}，成功{success_count}，失败{failure_count}"
        )
        return result


def run_ingestion_process() -> Dict[str, Any]:
    """
    数据录入流程入口函数，供调度器调用
    
    Returns:
        处理结果统计信息
    """
    logger.info("开始执行数据录入流程")
    try:
        service = IngestionService()
        result = service.process_all_new_files()
        return result
    except Exception as e:
        logger.error(f"数据录入流程执行失败: {str(e)}", exc_info=True)
        return {
            "error": str(e),
            "total": 0,
            "success_count": 0,
            "failure_count": 0,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 测试数据录入流程
    result = run_ingestion_process()
    print(f"数据录入流程执行结果: {result}")