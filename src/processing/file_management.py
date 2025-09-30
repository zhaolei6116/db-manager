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


# from src.utils.logging_config import get_lims_puller_logger
from src.models.models import InputFileMetadata
from src.repositories import InputFileRepository
from src.ingestion.lims_puller import get_all_json_in_lims_dir, run_lims_puller, PullResult
from src.utils.yaml_config import get_yaml_config


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
        self.logger = logging.getLogger('file_management')
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
                self.logger.info(f"文件[{file_name}]已添加到 input_file_metadata 表")
            else:
                self.logger.debug(f"文件[{file_name}]已存在，跳过插入。")
            return inserted
        except SQLAlchemyError as e:
            self.logger.error(f"插入文件[{file_name}]元数据失败", exc_info=True)
            raise  # 不处理异常，交由上层回滚
        except Exception as e:
            self.logger.error(f"处理文件[{file_name}]元数据时发生未预期异常", exc_info=True)
            raise
    
    def get_new_file_list(self, temp_path: Optional[str] = None) -> List[Path]:
        """
        获取所有未处理的新文件列表并添加到库中
        temp_path 仅测试使用
        """
        try:
            all_json_paths = get_all_json_in_lims_dir(temp_path)
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
    
    def get_new_files_from_run_lims_puller(self, config_file: Optional[str] = None) -> List[Path]:
        """
        调用 run_lims_puller 获取所有实验室的拉取结果，并提取未处理的新文件列表添加到库中
        
        :param config_file: 配置文件路径（默认使用 config/config.yaml）
        :return: 未处理的新文件路径列表
        """
        try:
            # 调用 run_lims_puller 获取所有实验室的拉取结果
            pull_results = run_lims_puller(config_file)
            if not pull_results:
                self.logger.info("run_lims_puller 未返回任何拉取结果")
                return []
            
            # 收集所有实验室的 JSON 路径
            all_json_paths = []
            for lab, result in pull_results.items():
                if result.success and result.new_json_paths:
                    self.logger.info(f"实验室[{lab}]拉取成功，新增{result.new_json_count}个JSON文件")
                    all_json_paths.extend(result.new_json_paths)
                elif not result.success:
                    self.logger.warning(f"实验室[{lab}]拉取失败：{result.error_msg}")
            
            if not all_json_paths:
                self.logger.info("所有实验室拉取结果中未包含任何JSON文件路径")
                return []
            
            # 检查并添加新文件到数据库
            new_files = []
            for json_path in all_json_paths:
                file_name = Path(json_path).name
                if not self.check_file_existence(file_name):
                    # 插入新文件到数据库
                    if self.insert_new_file(file_name):
                        new_files.append(json_path)
            
            self.logger.info(f"从所有实验室拉取结果中发现并添加了{len(new_files)}个未处理的新文件到库中")
            return new_files
        except Exception as e:
            self.logger.error("从run_lims_puller获取新文件列表失败", exc_info=True)
            return []
    


# 添加测试代码块

# src/processing/file_management.py 中的 if __name__ == "__main__": 部分

if __name__ == "__main__":
    """测试 FileManager 的 get_new_files_from_run_lims_puller 方法"""
    from src.models.database import get_session
    with get_session() as session:
        file_manager = FileManager(db_session=session)
        new_files = file_manager.get_new_files_from_run_lims_puller()
        print(f"从run_lims_puller获取到的新文件列表: {new_files}")
   
   