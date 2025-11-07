"""LIMS数据处理主入口
协调各处理器完成数据处理流程
"""
import logging
from pathlib import Path
from typing import Dict, Any, Optional
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
        """
        初始化LIMS数据处理器
        
        Args:
            db_session: SQLAlchemy会话对象，如果未提供，会创建新的会话
        """
        self.db_session = db_session  # 不再使用get_session()作为默认值
        self.config = get_yaml_config()
        self.logger = get_lims_puller_logger()
        
        # 处理顺序从配置获取
        self.process_order = self.config.get("processing.business_tables", 
                                           ["project", "sample", "batch", "sequence"])
        
        # 初始化各处理器（延迟到需要时）
        self.file_manager = None
        self.processors = None
    
    def process_parsed_json_dict(self, parsed_data: Dict[str, Dict[str, Any]], source_name: str = "parsed_json") -> Dict[str, Any]:
        """
        处理从json_data_processor生成的字典数据，并在处理完成后更新input_file_metadata表中的process_status
        
        Args:
            parsed_data: 从json_data_processor处理得到的字典，包含project、sample、batch、sequence四个表的数据
            source_name: 数据源名称（文件名，也是input_file_metadata表的主键），用于日志和状态跟踪
        
        Returns:
            处理结果字典，包含各表处理状态
        """
        self.logger.info(f"开始处理解析后的JSON字典数据，源名称：{source_name}")
        
        # 初始化结果
        result = {
            "source": source_name,
            "tables": {},
            "success": True
        }
        
        # 确保有有效的数据库会话
        if self.db_session is None:
            with get_session() as session:
                self.db_session = session
                return self._process_with_session(parsed_data, source_name, result)
        else:
            return self._process_with_session(parsed_data, source_name, result)
            
    def _process_with_session(self, parsed_data: Dict[str, Dict[str, Any]], source_name: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        在有效的会话中处理数据的内部方法
        
        Args:
            parsed_data: 从json_data_processor处理得到的数据字典
            source_name: 数据源名称
            result: 结果字典对象
        
        Returns:
            更新后的结果字典
        """
        try:
            # 初始化处理器和文件管理器
            if self.processors is None:
                self.processors = {
                    "project": ProjectProcessor(self.db_session),
                    "sample": SampleProcessor(self.db_session),
                    "batch": BatchProcessor(self.db_session),
                    "sequence": CombinedSequenceProcessor(self.db_session)
                }
                
            if self.file_manager is None:
                self.file_manager = FileManager(self.db_session)
            
            # 按顺序处理各业务表
            for table_name in self.process_order:
                if table_name not in parsed_data:
                    self.logger.warning(f"解析数据中缺少[{table_name}]表的数据，跳过")
                    result["tables"][table_name] = {"status": "skipped", "reason": "data_not_found"}
                    continue
                
                processor = self.processors.get(table_name)
                if not processor:
                    self.logger.warning(f"未找到[{table_name}]表的处理器，跳过")
                    result["tables"][table_name] = {"status": "skipped", "reason": "processor_not_found"}
                    continue
                
                # 获取表数据并交给对应处理器处理
                table_data = parsed_data[table_name]
                success = processor.process(table_data, source_name)
                result["tables"][table_name] = {"status": "success" if success else "failed"}
                
                # 更新整体处理成功状态
                result["success"] = result["success"] and success
                
                # 关键表处理失败则终止后续处理
                if not success and table_name in ["project", "sample", "batch"]:
                    self.logger.warning(f"关键表[{table_name}]处理失败，终止后续表处理")
                    break
            
            # 在所有表处理完成后，根据整体处理状态更新input_file_metadata表的process_status
            if result["success"]:
                update_status = "success"
            else:
                update_status = "failed"
            
            # 调用file_manager的update_file_process_status方法更新处理状态
            update_success = self.file_manager.update_file_process_status(
                file_name=source_name,
                status=update_status
            )
            
            if update_success:
                self.logger.info(f"文件[{source_name}]处理状态已更新为[{update_status}]")
            else:
                self.logger.warning(f"文件[{source_name}]处理状态更新失败，但数据处理本身已完成")
            
            self.logger.info(f"解析后的JSON字典数据处理完成，源名称：{source_name}，整体状态：{'成功' if result['success'] else '失败'}")
            return result
            
        except Exception as e:
            error_msg = str(e)[:500]
            self.logger.error(f"处理解析后的JSON字典数据时发生异常，源名称：{source_name}", exc_info=True)
            
            # 发生异常时也尝试更新处理状态为failed
            try:
                if self.file_manager:
                    self.file_manager.update_file_process_status(
                        file_name=source_name,
                        status="failed"
                    )
            except Exception as inner_e:
                self.logger.warning(f"尝试更新文件[{source_name}]处理状态为failed时发生异常: {str(inner_e)}")
            
            result["success"] = False
            result["error"] = error_msg
            return result

    


# 测试入口
if __name__ == "__main__":
    # 测试process_parsed_json_dict方法
    try:
        from src.models.database import get_session
        from src.processing.json_data_processor import DataProcessor
        from contextlib import contextmanager
        
    
        # 示例：使用process_parsed_json_dict方法处理数据
        with get_session() as session:
            try:
                # 1. 创建DataProcessor实例并解析JSON文件
                json_processor = DataProcessor()
                test_json_path = Path("/home/zhaolei/project/LimsData/25092402/S22509231629_R1.json")
                parsed_data = json_processor.parse_json_file(test_json_path)
                
                if parsed_data:
                    # 2. 创建LIMSDataProcessor实例
                    lims_processor = LIMSDataProcessor()
                    
                    # 3. 调用process_parsed_json_dict方法处理解析后的数据
                    result = lims_processor.process_parsed_json_dict(
                        parsed_data=parsed_data,
                        db_session=session,
                        source_name="test_example"
                    )
                    
                    # 4. 打印处理结果
                    print(f"处理结果：{result}")
                    # 在实际应用中，上层代码会根据业务需求决定是否commit或rollback
                    # session.commit()  # 上层控制事务
                else:
                    print("JSON文件解析失败")
            except Exception as inner_e:
                print(f"处理数据过程中发生异常：{str(inner_e)}")
    except Exception as e:
        print(f"测试过程中发生异常：{str(e)}")