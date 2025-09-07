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
from src.ingestion.lims_puller import get_all_json_in_lims_dir
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
    


# 添加测试代码块

# src/processing/file_management.py 中的 if __name__ == "__main__": 部分

if __name__ == "__main__":
    """测试 FileManager 核心功能：文件存在性检查、插入新文件、获取新文件列表"""
    import sys
    from unittest.mock import patch
    from src.models.database import get_session
    from src.repositories import InputFileRepository
    import json

    # 配置测试日志
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    test_logger = logging.getLogger("FileManagerTest")

    def run_tests():
        # 测试用临时文件名和目录
        TEST_FILE_NAME = "T22508295523_R1.json"
        TEST_LIMS_DIR = Path("/nas02/project/zhaolei/pipeline/data_management/LimsData/25083005")
        TEST_JSON_PATH = TEST_LIMS_DIR / TEST_FILE_NAME

        try:
            test_logger.info("=== 开始测试 FileManager 功能 ===")

            # 创建测试目录和文件
            TEST_LIMS_DIR.mkdir(parents=True, exist_ok=True)
            with open(TEST_JSON_PATH, 'w', encoding='utf-8') as f:
                json.dump({"test": "data"}, f)
            test_logger.debug(f"创建测试文件：{TEST_JSON_PATH}")

            # 模拟 get_all_json_in_lims_dir 返回测试文件路径
            def mock_get_all_json_in_lims_dir(*args, **kwargs):
                return [str(TEST_JSON_PATH.absolute())]

            # --- 阶段 1：插入数据（会自动 commit）---
            test_logger.info("\n1. 测试插入新文件...")
            with get_session() as session:
                file_manager = FileManager(db_session=session)
                inserted = file_manager.insert_new_file(TEST_FILE_NAME)
                if not inserted:
                    test_logger.warning(f"文件 [{TEST_FILE_NAME}] 可能已存在，跳过插入")
                else:
                    test_logger.info(f"文件 [{TEST_FILE_NAME}] 已插入并将在会话结束时提交")

            # 注意：session 已退出，事务已提交！数据应持久化到数据库

            # --- 阶段 2：使用新会话验证数据是否真正存在 ---
            test_logger.info("\n2. 验证数据是否真正持久化到数据库...")
            with get_session() as verify_session:
                # 直接使用 Repository 查询（或自定义 SQL）
                file_manager = FileManager(db_session=verify_session)
                
                exists_in_db = file_manager.check_file_existence(TEST_FILE_NAME)

                if exists_in_db:
                    test_logger.info(f"✅ 验证通过：文件 [{TEST_FILE_NAME}] 在数据库中存在")
                else:
                    test_logger.error(f"❌ 验证失败：文件 [{TEST_FILE_NAME}] 在数据库中不存在")
                    assert False, "插入的文件未在数据库中找到"

            # --- 阶段 3：测试获取新文件列表（可选）---
            test_logger.info("\n3. 测试获取新文件列表...")
            with get_session() as session_for_list:
                file_manager_for_list = FileManager(db_session=session_for_list)
                new_files = file_manager_for_list.get_new_file_list(temp_path=TEST_LIMS_DIR)
                test_logger.info(f"发现 {len(new_files)} 个新文件")
                assert len(new_files) == 0, "文件已录入，不应再被识别为新文件"
                test_logger.info("✅ 获取新文件列表测试通过（已录入文件不再返回）")

            # --- 阶段 4：清理测试数据 ---
            test_logger.info("\n4. 清理测试数据...")
            with get_session() as cleanup_session:
                repo = InputFileRepository(cleanup_session)
                deleted = repo.delete_by_pk(TEST_FILE_NAME)
                if deleted:
                    test_logger.info(f"已清理测试文件元数据：{TEST_FILE_NAME}")
                else:
                    test_logger.warning(f"测试文件元数据 [{TEST_FILE_NAME}] 未找到，无需清理")

        except AssertionError as ae:
            test_logger.error(f"测试断言失败：{str(ae)}", exc_info=True)
            return False
        except Exception as e:
            test_logger.error(f"测试过程发生错误：{str(e)}", exc_info=True)
            return False
        finally:
            # 清理测试文件和目录
            if TEST_JSON_PATH.exists():
                TEST_JSON_PATH.unlink()
                test_logger.debug(f"已删除测试文件：{TEST_JSON_PATH}")
            if TEST_LIMS_DIR.exists() and not any(TEST_LIMS_DIR.iterdir()):
                TEST_LIMS_DIR.rmdir()
                test_logger.debug(f"已删除测试目录：{TEST_LIMS_DIR}")

        test_logger.info("\n=== 所有测试完成，全部通过！ ===")
        return True

    # 执行测试
    test_result = run_tests()
    sys.exit(0 if test_result else 1)