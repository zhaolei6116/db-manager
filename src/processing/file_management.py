# src/processing/file_management.py
"""
文件管理模块：负责 InputFileMetadata 表的操作和文件发现。
"""


import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError


from src.utils.logging_config import get_lims_puller_logger
from src.models.models import InputFileMetadata
from src.repositories import InputFileRepository
from src.ingestion.lims_puller import get_all_json_in_lims_dir


class FileManager:
    """
    文件管理处理器，负责 InputFileMetadata 表的操作和文件发现。
    
    ✅ 设计原则：
    - 不管理事务（不调用 commit / rollback）
    - session 可由外部传入或内部使用上下文管理器
    - 所有数据库操作异常向上抛出，由调用方（如 Service 或 main）处理
    """

    def __init__(self, db_session: Optional[Session] = None):
        """
        初始化 FileManager
        :param db_session: 可选，用于传入已存在的 session（如在事务中共享）
                           若为 None，则后续操作需配合 `with get_session()` 使用
        """
        self.db_session = db_session
        self.config = get_yaml_config()
        self.logger = get_lims_puller_logger()
        self.lims_dir = Path(self.config.get("lims.directory", "/default/lims/dir"))

        # 注意：只有当 session 已传入时才能初始化 repo
        if self.db_session is not None:
            self.input_file_repo = InputFileRepository(self.db_session)
        else:
            self.input_file_repo = None  # 延迟初始化

    def _ensure_repo(self):
        """确保 Repository 已初始化（仅在有 session 时可用）"""
        if self.input_file_repo is None:
            raise RuntimeError("db_session 未设置，无法执行数据库操作。请传入 session 或使用上下文管理。")

    def check_file_existence(self, file_name: str) -> bool:
        """
        检查文件是否已存在于 input_file_metadata 表中
        :param file_name: 文件名（主键）
        :return: 是否存在
        """
        self._ensure_repo()
        try:
            exists = self.input_file_repo.exists_by_pk(file_name)
            self.logger.debug(f"文件[{file_name}]存在性检查结果: {'已存在' if exists else '不存在'}")
            return exists
        except SQLAlchemyError as e:
            self.logger.error(f"检查文件[{file_name}]存在性失败", exc_info=True)
            raise  # 向上抛出，由外层处理

    def insert_new_file(self, file_name: str) -> bool:
        """
        将新文件信息插入 input_file_metadata 表
        :param file_name: 文件名
        :return: 是否为新插入（True = 新插入，False = 已存在）
        """
        self._ensure_repo()
        try:
            file_metadata = InputFileMetadata(
                file_name=file_name,
                process_status="pending"
            )

            inserted = self.input_file_repo.insert_if_not_exists(file_metadata)
            if inserted:
                self.logger.info(f"文件[{file_name}]已添加到 input_file_metadata 表（路径：{file_path}）")
            else:
                self.logger.debug(f"文件[{file_name}]已存在，跳过插入。")
            return inserted
        except SQLAlchemyError as e:
            self.logger.error(f"插入文件[{file_name}]元数据失败", exc_info=True)
            raise  # 不处理异常，交由上层回滚
        except Exception as e:
            self.logger.error(f"处理文件[{file_name}]元数据时发生未预期异常", exc_info=True)
            raise
    
    def get_new_file_list(self) -> List[Path]:
        """获取所有未处理的新文件列表并添加到库中"""
        try:
            all_json_paths = get_all_json_in_lims_dir()
            if not all_json_paths:
                self.logger.info("未获取到任何JSON文件")
                return []
            
            new_files = []
            for json_path in all_json_paths:
                file_name = Path(json_path).name
                if not self.check_file_existence(file_name):
                    # 插入新文件到数据库
                    if self.insert_new_file(file_name):
                        new_files.append(json_path)
            
            self.logger.info(f"发现并添加了{len(new_files)}个未处理的新文件到库中")
            return new_files
        except Exception as e:
            self.logger.error("获取新文件列表失败", exc_info=True)
            return []
    
