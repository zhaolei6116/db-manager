# src/repositories/base_repository.py
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, List, Dict, Any, Type
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect, or_, and_
from src.models.models import FieldCorrections  # 字段修正日志表
import logging
from datetime import datetime

# 泛型：绑定具体的 ORM 模型类
ModelType = TypeVar("ModelType")

# 初始化日志
logger = logging.getLogger(__name__)


class BaseRepository(ABC, Generic[ModelType]):
    """
    抽象基础 Repository 类
    封装通用的 CRUD 操作，所有具体表的 Repository 需继承此类

    ✅ 设计原则：
    - 不创建 session，由外部传入
    - 不调用 commit / rollback，事务由上层（Service 或 contextmanager）控制
    - 提供通用方法：去重插入、字段更新（带审计日志）、批量操作等
    """

    def __init__(self, db_session: Session):
        """
        初始化 Repository
        :param db_session: 数据库会话（必须由上层传入）
        :raises ValueError: 如果 session 为 None
        """
        if db_session is None:
            raise ValueError("db_session cannot be None. Must be provided by caller.")
        self.db_session = db_session
        self.model: Type[ModelType] = self._get_model()

    @abstractmethod
    def _get_model(self) -> Type[ModelType]:
        """
        子类必须实现：返回对应的 ORM 模型类
        示例：return Project
        """
        raise NotImplementedError("Subclasses must implement _get_model()")

    @abstractmethod
    def get_pk_field(self) -> str:
        """
        子类必须实现：返回主键字段名（如 'project_id', 'sample_id'）
        示例：return "project_id"
        """
        raise NotImplementedError("Subclasses must implement get_pk_field()")

    # ========================================================================
    # ✅ 1. 存在性检查（去重核心）
    # ========================================================================

    def exists_by_pk(self, pk_value: Any) -> bool:
        """
        根据主键判断记录是否存在
        :param pk_value: 主键值
        :return: 存在返回 True，否则 False
        """
        try:
            filter_condition = {self.get_pk_field(): pk_value}
            return self.db_session.query(self.model).filter_by(**filter_condition).first() is not None
        except SQLAlchemyError as e:
            logger.error(
                f"Failed to check existence of {self.model.__name__} by {self.get_pk_field()}={pk_value}: {str(e)}",
                exc_info=True
            )
            raise

    def exists_by_fields(self, **filter_by) -> bool:
        """
        根据任意字段组合判断是否存在（用于复合唯一键场景）
        :param filter_by: 查询字段，如 sample_id=123, batch_id=456
        :return: 存在返回 True
        """
        try:
            return self.db_session.query(self.model).filter_by(**filter_by).first() is not None
        except SQLAlchemyError as e:
            logger.error(
                f"Failed to check existence of {self.model.__name__} with {filter_by}: {str(e)}",
                exc_info=True
            )
            raise

    # ========================================================================
    # ✅ 2. 查询操作
    # ========================================================================

    def get_by_pk(self, pk_value: Any) -> Optional[ModelType]:
        """
        根据主键获取单条记录
        :param pk_value: 主键值
        :return: ORM 实例或 None
        """
        try:
            filter_condition = {self.get_pk_field(): pk_value}
            return self.db_session.query(self.model).filter_by(**filter_condition).first()
        except SQLAlchemyError as e:
            logger.error(
                f"Failed to query {self.model.__name__} by {self.get_pk_field()}={pk_value}: {str(e)}",
                exc_info=True
            )
            raise

    def get_all(self) -> List[ModelType]:
        """
        获取所有记录（慎用，大数据量时建议分页）
        :return: 模型实例列表
        """
        try:
            return self.db_session.query(self.model).all()
        except SQLAlchemyError as e:
            logger.error(f"Failed to query all {self.model.__name__}: {str(e)}", exc_info=True)
            raise

    def query_filter(self, **filter_conditions) -> List[ModelType]:
        """
        通用条件查询（AND 条件）
        :param filter_conditions: 字段=值，如 status="valid", project_id="P001"
        :return: 匹配的记录列表
        """
        try:
            return self.db_session.query(self.model).filter_by(**filter_conditions).all()
        except SQLAlchemyError as e:
            logger.error(
                f"Failed to query {self.model.__name__} with {filter_conditions}: {str(e)}",
                exc_info=True
            )
            raise

    def query_filter_or(self, **filter_conditions) -> List[ModelType]:
        """
        OR 条件查询（任意一个条件匹配即可）
        :param filter_conditions: 如 name="test", status="error"
        :return: 匹配的记录列表
        """
        try:
            filters = [getattr(self.model, k) == v for k, v in filter_conditions.items()]
            return self.db_session.query(self.model).filter(or_(*filters)).all()
        except SQLAlchemyError as e:
            logger.error(
                f"Failed to query {self.model.__name__} with OR {filter_conditions}: {str(e)}",
                exc_info=True
            )
            raise

    def query_filter_advanced(self, *criteria) -> List[ModelType]:
        """
        高级查询：支持复杂条件，如 and_(...), or_(...), 比较操作等
        :param criteria: SQLAlchemy 表达式，如 Project.status != 'deleted'
        :return: 匹配的记录列表
        """
        try:
            return self.db_session.query(self.model).filter(*criteria).all()
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute advanced query on {self.model.__name__}: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # ✅ 3. 插入操作
    # ========================================================================

    def insert_if_not_exists(self, record: ModelType, conflict_fields: Optional[List[str]] = None) -> bool:
        """
        插入记录，若主键或指定字段已存在则跳过
        :param record: 要插入的 ORM 实例
        :param conflict_fields: 可选，用于判断冲突的字段列表（如 ["sample_id"]）
        :return: 是否为新插入（True=新插入，False=已存在）
        """
        try:
            pk_field = self.get_pk_field()
            pk_value = getattr(record, pk_field)

            # 使用主键或指定字段判断是否存在
            if conflict_fields:
                conditions = {f: getattr(record, f) for f in conflict_fields if hasattr(record, f)}
                if self.exists_by_fields(**conditions):
                    logger.info(f"{self.model.__name__} with {conditions} already exists, skipped.")
                    return False
            else:
                if self.exists_by_pk(pk_value):
                    logger.info(f"{self.model.__name__}.{pk_field}={pk_value} already exists, skipped.")
                    return False

            self.db_session.add(record)
            logger.info(f"Inserted {self.model.__name__}.{pk_field}={pk_value}")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to insert {self.model.__name__}: {str(e)}", exc_info=True)
            raise

    def bulk_insert_if_not_exists(self, records: List[ModelType], conflict_fields: Optional[List[str]] = None) -> int:
        """
        批量插入，跳过已存在的记录
        :param records: 要插入的实例列表
        :param conflict_fields: 判断冲突的字段列表
        :return: 实际插入的数量
        """
        inserted = 0
        for record in records:
            if self.insert_if_not_exists(record, conflict_fields):
                inserted += 1
        logger.info(f"Bulk insert: {len(records)} total, {inserted} inserted.")
        return inserted

    # ========================================================================
    # ✅ 4. 更新操作（带字段变更日志）
    # ========================================================================

    def update_field(
        self,
        pk_value: Any,
        field_name: str,
        new_value: Any,
        operator: str = "system"
    ) -> bool:
        """
        更新单个字段，并记录变更日志到 FieldCorrections 表
        :param pk_value: 主键值，用于定位记录
        :param field_name: 要更新的字段名
        :param new_value: 新值
        :param operator: 操作人（用户名或 'system'）
        :return: 是否成功更新（False 表示未变更或记录不存在）
        """
        try:
            record = self.get_by_pk(pk_value)
            if not record:
                logger.warning(f"{self.model.__name__}.{self.get_pk_field()}={pk_value} not found.")
                return False

            old_value = getattr(record, field_name, None)
            if old_value == new_value:
                logger.info(f"{field_name} already set to {new_value}, no change.")
                return False

            setattr(record, field_name, new_value)
            logger.info(f"Updated {self.model.__name__}.{pk_value}.{field_name}: {old_value} -> {new_value}")

            # 记录字段变更日志
            self._log_field_correction(
                table_name=self.model.__tablename__,
                record_id=str(pk_value),
                field_name=field_name,
                old_value=old_value,
                new_value=new_value,
                operator=operator
            )
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to update field {field_name}: {str(e)}", exc_info=True)
            raise

    def _log_field_correction(
        self,
        table_name: str,
        record_id: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        operator: str
    ):
        """内部方法：记录字段修正日志"""
        correction = FieldCorrections(
            table_name=table_name,
            record_id=record_id,
            field_name=field_name,
            old_value=str(old_value) if old_value is not None else "",
            new_value=str(new_value) if new_value is not None else "",
            operator=operator,
            correction_time=datetime.now()
        )
        self.db_session.add(correction)
        logger.info(f"Logged correction: {table_name}.{record_id}.{field_name}")

    # ========================================================================
    # ✅ 5. 删除操作
    # ========================================================================

    def delete_by_pk(self, pk_value: Any) -> bool:
        """
        根据主键删除记录
        :param pk_value: 主键值
        :return: 是否删除成功（False 表示记录不存在）
        """
        try:
            record = self.get_by_pk(pk_value)
            if not record:
                logger.warning(f"{self.model.__name__}.{self.get_pk_field()}={pk_value} not found.")
                return False
            self.db_session.delete(record)
            logger.info(f"Deleted {self.model.__name__}.{self.get_pk_field()}={pk_value}")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to delete {self.model.__name__}.{pk_value}: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # ✅ 6. 其他通用方法
    # ========================================================================

    def count(self, **filter_by) -> int:
        """
        统计记录数
        :param filter_by: 可选过滤条件
        :return: 数量
        """
        try:
            query = self.db_session.query(self.model)
            if filter_by:
                query = query.filter_by(**filter_by)
            return query.count()
        except SQLAlchemyError as e:
            logger.error(f"Failed to count {self.model.__name__}: {str(e)}", exc_info=True)
            raise

    def upsert(self, record: ModelType, update_on_duplicate: List[str] = None) -> ModelType:
        """
        Upsert：存在则更新，不存在则插入
        :param record: 要操作的实例
        :param update_on_duplicate: 若存在，要更新的字段列表（如 ["status", "updated_at"]），None 表示更新所有字段
        :return: 操作后的实例
        """
        try:
            pk_field = self.get_pk_field()
            pk_value = getattr(record, pk_field)
            existing = self.get_by_pk(pk_value)

            if existing:
                if update_on_duplicate:
                    for field in update_on_duplicate:
                        if hasattr(record, field):
                            setattr(existing, field, getattr(record, field))
                else:
                    # 更新所有字段
                    for key, value in record.__dict__.items():
                        if not key.startswith("_") and hasattr(existing, key):
                            setattr(existing, key, value)
                logger.info(f"Upsert: updated {self.model.__name__}.{pk_field}={pk_value}")
                return existing
            else:
                self.db_session.add(record)
                logger.info(f"Upsert: inserted {self.model.__name__}.{pk_field}={pk_value}")
                return record
        except SQLAlchemyError as e:
            logger.error(f"Failed to upsert {self.model.__name__}: {str(e)}", exc_info=True)
            raise