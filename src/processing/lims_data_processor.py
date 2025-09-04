"""LIMS数据处理主入口
协调各处理器完成数据处理流程
"""
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from src.utils.yaml_config import get_yaml_config
from src.utils.logging_config import get_lims_puller_logger
from src.models.database import get_session

from src.processing.file_management import FileManager
from src.processing.project_processor import ProjectProcessor
from src.processing.sample_processor import SampleProcessor
from src.processing.batch_processor import BatchProcessor
from src.processing.sequence_processor import CombinedSequenceProcessor


class LIMSDataProcessor:
    """LIMS数据处理主类，协调各组件工作"""
    
    def __init__(self, db_session: Optional[Session] = None):
        self.db_session = db_session or get_session()
        self.config = get_yaml_config()
        self.logger = get_lims_puller_logger()
        
        # 初始化各处理器
        self.file_manager = FileManager(self.db_session)
        self.processors = {
            "project": ProjectProcessor(self.db_session),
            "sample": SampleProcessor(self.db_session),
            "batch": BatchProcessor(self.db_session),
            "sequence": CombinedSequenceProcessor(self.db_session)
        }
        
        # 处理顺序从配置获取
        self.process_order = self.config.get("processing.business_tables", 
                                           ["project", "sample", "batch", "sequence"])
    
    def process_single_file(self, json_path: Path) -> bool:
        """处理单个JSON文件"""
        file_name = json_path.name
        self.logger.info(f"开始处理文件：{json_path}")
        
        try:
            # 1. 检查文件是否已处理
            if self.file_manager.check_file_existence(file_name):
                self.logger.info(f"文件[{file_name}]已处理过，跳过")
                return True
            
            # 2. 新文件入库
            self.file_manager.insert_new_file(file_name, str(json_path))
            
            # 3. 解析JSON数据
            json_data = self.file_manager.parse_json_file(json_path)
            if not json_data:
                raise ValueError(f"文件[{file_name}]解析失败，无法继续处理")
            
            # 4. 按顺序处理所有业务表
            all_success = True
            for table_name in self.process_order:
                processor = self.processors.get(table_name)
                if not processor:
                    self.logger.warning(f"未找到[{table_name}]表的处理器，跳过")
                    continue
                
                # 处理单个表
                success = processor.process(json_data, file_name)
                all_success = all_success and success
                
                # 关键表处理失败则终止后续处理
                if not success and table_name in ["project", "sample", "batch"]:
                    self.logger.warning(f"关键表[{table_name}]处理失败，终止后续表处理")
                    all_success = False
                    break
            
            # 5. 更新文件处理状态
            if all_success:
                self.file_manager.update_file_status(file_name, "success")
                self.logger.info(f"文件[{file_name}]处理完成（所有业务表处理成功）")
                return True
            else:
                self.file_manager.update_file_status(file_name, "failed", "部分业务表处理失败")
                self.logger.warning(f"文件[{file_name}]处理完成（部分业务表处理失败）")
                return False
        
        except Exception as e:
            self.file_manager.update_file_status(file_name, "failed", str(e)[:500])
            self.logger.error(f"文件[{file_name}]处理异常", exc_info=True)
            return False
    
    def process_all_new_files(self) -> Dict[str, Any]:
        """批量处理所有新文件"""
        self.logger.info("开始批量处理LIMS新文件")
        
        new_files = self.file_manager.get_new_file_list()
        total = len(new_files)
        success_count = 0
        failure_count = 0
        
        for file_path in new_files:
            processed = self.process_single_file(file_path)
            if processed:
                success_count += 1
            else:
                failure_count += 1
        
        result = {
            "total": total,
            "success_count": success_count,
            "failure_count": failure_count,
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        self.logger.info(
            f"批量处理完成：总文件数{total}，成功{success_count}，失败{failure_count}"
        )
        return result
    
    def close(self) -> None:
        """关闭所有资源"""
        self.file_manager.close()
        for processor in self.processors.values():
            if hasattr(processor, "close"):
                processor.close()


def run_lims_data_process() -> Dict[str, Any]:
    """LIMS数据处理入口接口"""
    processor = None
    try:
        processor = LIMSDataProcessor()
        return processor.process_all_new_files()
    except Exception as e:
        logger = get_lims_puller_logger()
        logger.error("LIMS数据处理入口接口异常", exc_info=True)
        return {
            "total": 0,
            "success_count": 0,
            "failure_count": 0,
            "error": str(e),
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    finally:
        if processor:
            processor.close()


# 测试入口
if __name__ == "__main__":
    # 单文件测试
    test_file = Path("/nas02/project/zhaolei/pipeline/data_management/LimsData/25083011/S22508292761.json")
    processor = LIMSDataProcessor()
    processor.process_single_file(test_file)
    
    # 批量测试
    # result = run_lims_data_process()
    # print(f"批量处理结果：{result}")