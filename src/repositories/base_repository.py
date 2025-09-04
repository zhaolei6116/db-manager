# src/repositories/base_repository.py
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from src.models.models import FieldCorrections  # 任务2的字段修正表模型FieldCorrection
# from src.utils.db_utils import get_db_session
from src.models.database import get_session as get_db_session
import logging
from datetime import datetime

# 泛型变量：绑定SQLAlchemy ORM模型
ModelType = TypeVar("ModelType")

# 初始化日志
logger = logging.getLogger(__name__)

class BaseRepository(ABC, Generic[ModelType]):
    """
    抽象基础Repository类，封装通用CRUD操作
    所有具体表的Repository需继承此类并指定ORM模型
    """
    def __init__(self, db_session: Optional[Session] = None):
        """
        初始化Repository
        :param db_session: 数据库会话（外部传入，默认使用get_db_session获取）
        """
        self.db_session = db_session or next(get_db_session())
        # 抽象属性：子类必须指定对应的ORM模型（如Project、Sample）
        self.model: ModelType = self._get_model()

    @abstractmethod
    def _get_model(self) -> ModelType:
        """
        抽象方法：获取当前Repository绑定的ORM模型
        子类实现示例：return Project
        """
        raise NotImplementedError("Subclasses must implement _get_model()")

    @abstractmethod
    def get_pk_field(self) -> str:
        """
        抽象方法：获取当前模型的主键字段名（如project_id、sample_id）
        子类实现示例：return "project_id"
        """
        raise NotImplementedError("Subclasses must implement get_pk_field()")

    def exists_by_pk(self, pk_value: Any) -> bool:
        """
        按主键判断记录是否存在（核心去重方法）
        :param pk_value: 主键值（如project_id=123）
        :return: 存在返回True，否则False
        """
        try:
            # 动态拼接主键查询条件（如filter_by(project_id=123)）
            filter_condition = {self.get_pk_field(): pk_value}
            return self.db_session.query(self.model).filter_by(**filter_condition).first() is not None
        except SQLAlchemyError as e:
            logger.error(f"Failed to check existence by {self.get_pk_field()}={pk_value}: {str(e)}", exc_info=True)
            self.db_session.rollback()
            raise

    def insert_if_not_exists(self, record: ModelType) -> bool:
        """
        插入记录（去重：主键不存在才插入）
        :param record: ORM模型实例（如Project(project_id=123, ...)）
        :return: 插入成功返回True，已存在返回False
        """
        try:
            # 获取记录的主键值（如record.project_id）
            pk_value = getattr(record, self.get_pk_field())
            if self.exists_by_pk(pk_value):
                logger.info(f"Record {self.model.__name__}.{self.get_pk_field()}={pk_value} already exists, skip insertion")
                return False

            # 插入新记录
            self.db_session.add(record)
            self.db_session.commit()
            logger.info(f"Inserted {self.model.__name__}.{self.get_pk_field()}={pk_value} successfully")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to insert {self.model.__name__}: {str(e)}", exc_info=True)
            self.db_session.rollback()
            raise

    def get_by_pk(self, pk_value: Any) -> Optional[ModelType]:
        """
        按主键查询单条记录
        :param pk_value: 主键值
        :return: ORM模型实例（不存在返回None）
        """
        try:
            filter_condition = {self.get_pk_field(): pk_value}
            return self.db_session.query(self.model).filter_by(**filter_condition).first()
        except SQLAlchemyError as e:
            logger.error(f"Failed to get {self.model.__name__} by {self.get_pk_field()}={pk_value}: {str(e)}", exc_info=True)
            self.db_session.rollback()
            raise

    def update_field(self, pk_value: Any, field_name: str, new_value: Any, operator: str = "system") -> bool:
        """
        更新指定字段（自动记录字段修正日志到FieldCorrections表）
        :param pk_value: 主键值（定位要更新的记录）
        :param field_name: 要更新的字段名（如data_status、analysis_status）
        :param new_value: 新字段值（如"valid"、"no"）
        :param operator: 操作人（默认"system"，手动操作时传用户名）
        :return: 更新成功返回True
        """
        try:
            # 1. 查询要更新的记录
            record = self.get_by_pk(pk_value)
            if not record:
                logger.warning(f"{self.model.__name__}.{self.get_pk_field()}={pk_value} not found, skip update")
                return False

            # 2. 获取旧字段值（用于记录修正日志）
            old_value = getattr(record, field_name, None)
            if old_value == new_value:
                logger.info(f"{self.model.__name__}.{field_name} is already {new_value}, skip update")
                return False

            # 3. 更新字段值
            setattr(record, field_name, new_value)
            self.db_session.commit()
            logger.info(f"Updated {self.model.__name__}.{self.get_pk_field()}={pk_value}.{field_name}: {old_value} -> {new_value}")

            # 4. 插入字段修正记录到FieldCorrections表（自动日志）
            correction = FieldCorrections(
                table_name=self.model.__tablename__,  # 表名（如"project"）
                record_id=str(pk_value),              # 记录ID（主键值，转字符串统一格式）
                field_name=field_name,
                old_value=str(old_value) if old_value is not None else "",
                new_value=str(new_value) if new_value is not None else "",
                operator=operator,
                correction_time=datetime.now()       # 修正时间
            )
            self.db_session.add(correction)
            self.db_session.commit()
            logger.info(f"Logged field correction for {self.model.__tablename__}.{record_id}.{field_name}")

            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to update {self.model.__name__}.{field_name}: {str(e)}", exc_info=True)
            self.db_session.rollback()
            raise

    def bulk_insert(self, records: List[ModelType]) -> int:
        """
        批量插入记录（去重：跳过主键已存在的记录）
        :param records: ORM模型实例列表
        :return: 实际插入的记录数
        """
        inserted_count = 0
        try:
            for record in records:
                if self.insert_if_not_exists(record):
                    inserted_count += 1
            logger.info(f"Bulk insert completed: total {len(records)}, inserted {inserted_count}")
            return inserted_count
        except SQLAlchemyError as e:
            logger.error(f"Failed to bulk insert {self.model.__name__}: {str(e)}", exc_info=True)
            self.db_session.rollback()
            raise

    def query_filter(self, filter_conditions: Dict[str, Any]) -> List[ModelType]:
        """
        按条件查询多条记录（通用查询方法）
        :param filter_conditions: 查询条件字典（如{"data_status": "valid", "process_status": False}）
        :return: 符合条件的ORM模型实例列表
        """
        try:
            return self.db_session.query(self.model).filter_by(**filter_conditions).all()
        except SQLAlchemyError as e:
            logger.error(f"Failed to query {self.model.__name__} with conditions {filter_conditions}: {str(e)}", exc_info=True)
            self.db_session.rollback()
            raise