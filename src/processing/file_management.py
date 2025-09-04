import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.utils.yaml_config import get_yaml_config
from src.utils.logging_config import get_lims_puller_logger
from src.models.database import get_session
from src.models.models import InputFileMetadata
from src.repositories.input_file_repository import InputFileRepository
from src.ingestion.lims_puller import get_all_json_in_lims_dir


class FileManager:
    """文件管理处理器，负责InputFileMetadata表操作和文件发现"""
    
    def __init__(self, db_session: Optional[Session] = None):
        self.db_session = db_session or get_session()
        self.config = get_yaml_config()
        self.logger = get_lims_puller_logger()
        self.input_file_repo = InputFileRepository(self.db_session)
        self.lims_dir = self.config.get("lims.directory", "/default/lims/dir")
    
    def check_file_existence(self, file_name: str) -> bool:
        """检查文件是否已存在于input_file_metadata表"""
        try:
            exists = self.input_file_repo.exists_by_pk(file_name)
            self.logger.debug(f"文件[{file_name}]存在性检查结果: {'已存在' if exists else '不存在'}")
            return exists
        except SQLAlchemyError as e:
            self.logger.error(f"检查文件[{file_name}]存在性失败", exc_info=True)
            self.db_session.rollback()
            raise
    
    def insert_new_file(self, file_name: str, file_path: str) -> bool:
        """将新文件信息插入input_file_metadata表"""
        try:
            file_metadata = InputFileMetadata(
                file_name=file_name,
                file_path=str(file_path),
                process_status="pending",
                process_time=None,
                error_msg=None
            )
            
            inserted = self.input_file_repo.insert_if_not_exists(file_metadata)
            if inserted:
                self.logger.info(f"文件[{file_name}]已添加到input_file_metadata表（路径：{file_path}）")
                self.db_session.commit()
            return inserted
        except SQLAlchemyError as e:
            self.logger.error(f"插入文件[{file_name}]到input_file_metadata表失败", exc_info=True)
            self.db_session.rollback()
            raise
        except Exception as e:
            self.logger.error(f"处理文件[{file_name}]元数据时异常", exc_info=True)
            return False
    
    def update_file_status(self, file_name: str, status: str, error_msg: Optional[str] = None) -> None:
        """更新文件处理状态"""
        try:
            self.input_file_repo.update_field(
                pk_value=file_name,
                field_name="process_status",
                new_value=status,
                operator="system"
            )
            
            if status == "failed" and error_msg:
                self.input_file_repo.update_field(
                    pk_value=file_name,
                    field_name="error_msg",
                    new_value=error_msg[:500],
                    operator="system"
                )
            
            self.input_file_repo.update_field(
                pk_value=file_name,
                field_name="process_time",
                new_value=datetime.now(),
                operator="system"
            )
            
            self.db_session.commit()
            self.logger.debug(f"文件[{file_name}]状态更新为: {status}")
        except Exception as e:
            self.logger.error(f"更新文件[{file_name}]状态失败", exc_info=True)
            self.db_session.rollback()
    
    def get_new_file_list(self) -> List[Path]:
        """获取所有未处理的新文件列表"""
        try:
            all_json_paths = get_all_json_in_lims_dir()
            if not all_json_paths:
                self.logger.info("未获取到任何JSON文件")
                return []
            
            new_files = []
            for json_path in all_json_paths:
                file_name = Path(json_path).name
                if not self.check_file_existence(file_name):
                    new_files.append(Path(json_path))
            
            self.logger.info(f"发现{len(new_files)}个未处理的新文件")
            return new_files
        except Exception as e:
            self.logger.error("获取新文件列表失败", exc_info=True)
            return []
    
    def parse_json_file(self, json_path: Path) -> Optional[Dict[str, Any]]:
        """解析JSON文件"""
        try:
            import yaml  # 延迟导入，仅在需要时加载
            with open(json_path, "r", encoding="utf-8") as f:
                json_data = yaml.safe_load(f)  # 兼容JSON语法
            
            if not isinstance(json_data, dict):
                self.logger.error(f"文件[{json_path.name}]解析结果非字典（实际类型：{type(json_data)}）")
                return None
            
            self.logger.debug(f"文件[{json_path.name}]解析成功，含{len(json_data)}个字段")
            return json_data
        except Exception as e:
            self.logger.error(f"读取文件[{json_path.name}]时异常", exc_info=True)
            return None
    
    def close(self):
        """关闭数据库会话"""
        if hasattr(self, "db_session") and self.db_session.is_active:
            self.db_session.close()
            self.logger.info("FileManager数据库会话已关闭")