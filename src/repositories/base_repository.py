# src/repositories/base_repository.py
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, List, Dict, Any, Type
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect, or_, and_, text
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
    ) -> tuple[bool, Optional[Dict[str, Any]]]:
        """
        更新单个字段，并返回变更日志字典（不再直接操作FieldCorrections表）
        
        Args:
            pk_value: 主键值，用于定位记录
            field_name: 要更新的字段名
            new_value: 新值
            operator: 操作人（用户名或 'system'）
        
        Returns:
            tuple: (是否成功更新, 变更日志字典或None)
            - 变更日志字典格式: {
                "table_name": str,
                "record_id": str,
                "field_name": str,
                "old_value": Any,
                "new_value": Any,
                "operator": str,
                "correction_time": datetime
            }
        """
        try:
            record = self.get_by_pk(pk_value)
            if not record:
                logger.warning(f"{self.model.__name__}.{self.get_pk_field()}={pk_value} not found.")
                return False, None

            # 直接访问字段检查是否存在，不存在则立即返回
            try:
                old_value = getattr(record, field_name)
            except AttributeError:
                logger.error(f"Field {field_name} does not exist in model {self.model.__name__}")
                return False, None

            if old_value == new_value:
                logger.info(f"{field_name} already set to {new_value}, no change.")
                return False, None

            # 执行字段更新
            setattr(record, field_name, new_value)
            logger.info(f"Updated {self.model.__name__}.{pk_value}.{field_name}: {old_value} -> {new_value}")

            # 生成变更日志字典（原_log_field_correction逻辑迁移至此）
            correction_dict = {
                "table_name": self.model.__tablename__,
                "record_id": str(pk_value),
                "field_name": field_name,
                "old_value": str(old_value) if old_value is not None else "",
                "new_value": str(new_value) if new_value is not None else "",
                "operator": operator,
                "note": "",
                "correction_time": datetime.now()
            }

            return True, correction_dict  # 返回更新状态和变更字典
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
    

    # ========================================================================
    # ✅ 7. 表结构操作（新增）
    # ========================================================================

    def add_table_field(self, field_name: str, field_type: str, description: str = "") -> Dict[str, Any]:
        """
        添加表字段（列）的接口
        
        Args:
            field_name: 字段名（需符合数据库命名规范）
            field_type: 字段类型（如 'VARCHAR(100)', 'INT', 'DATETIME', 'DECIMAL(10,2)'）
            description: 字段描述（可选，用于文档记录）
        
        Returns:
            操作结果字典，包含:
            - success: bool 操作是否成功
            - message: str 操作结果描述
            - table_name: str 表名
            - field_name: str 字段名
        """
        try:
            table_name = self.model.__tablename__
            logger.info(f"开始处理表 [{table_name}] 添加字段: {field_name}（类型: {field_type}，描述: {description}）")

            # 1. 检查字段是否已存在
            inspector = inspect(self.db_session.bind)
            existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
            if field_name in existing_columns:
                logger.warning(f"表 [{table_name}] 已存在字段 [{field_name}]，无需重复添加")
                return {
                    "success": False,
                    "message": f"字段 [{field_name}] 已存在于表 [{table_name}]",
                    "table_name": table_name,
                    "field_name": field_name
                }

            # 2. 执行添加字段 SQL
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {field_name} {field_type}"
            self.db_session.execute(text(alter_sql))
            logger.info(f"表 [{table_name}] 字段 [{field_name}] 添加成功")

            return {
                "success": True,
                "message": f"成功为表 [{table_name}] 添加字段 [{field_name}]（类型: {field_type}）",
                "table_name": table_name,
                "field_name": field_name
            }

        except SQLAlchemyError as e:
            logger.error(f"表 [{table_name}] 添加字段 [{field_name}] 失败（SQL错误）", exc_info=True)
            return {
                "success": False,
                "message": f"数据库操作失败: {str(e)}",
                "table_name": table_name,
                "field_name": field_name
            }
        except Exception as e:
            logger.error(f"表 [{table_name}] 添加字段 [{field_name}] 失败（系统错误）", exc_info=True)
            return {
                "success": False,
                "message": f"系统错误: {str(e)}",
                "table_name": table_name,
                "field_name": field_name
            }

    def dict_to_orm_with_validation(self, data_dict: Dict[str, Any], required_fields: Optional[List[str]] = None) -> ModelType:
        """
        将字典转换为表对应的ORM对象，并进行字段检查
        
        Args:
            data_dict: 包含字段数据的字典
            required_fields: 必需的字段列表，如果为None，则至少检查主键字段
        
        Returns:
            转换后的ORM实例
        
        Raises:
            ValueError: 如果缺失必要的字段
        """
        try:
            # 1. 确定必要的字段列表
            if required_fields is None:
                # 默认至少检查主键字段
                required_fields = [self.get_pk_field()]
            else:
                # 确保主键字段总是被检查
                pk_field = self.get_pk_field()
                if pk_field not in required_fields:
                    required_fields.append(pk_field)
                    logger.debug(f"自动添加主键字段 '{pk_field}' 到必要字段列表")
            
            # 2. 检查必要字段是否存在
            missing_fields = []
            for field in required_fields:
                if field not in data_dict or data_dict[field] is None:
                    missing_fields.append(field)
            
            # 3. 如果缺失必要字段，抛出异常
            if missing_fields:
                error_msg = f"Missing required fields for {self.model.__name__}: {', '.join(missing_fields)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # 4. 创建ORM实例
            orm_instance = self.model()
            
            # 5. 设置字段值（只设置模型中存在的字段）
            model_fields = [column.name for column in inspect(self.model).columns]
            for field, value in data_dict.items():
                if field in model_fields:
                    setattr(orm_instance, field, value)
                    logger.debug(f"Set {self.model.__name__}.{field} = {value}")
                else:
                    logger.warning(f"Field '{field}' not found in {self.model.__name__} model, skipped")
            
            logger.info(f"Successfully converted dict to {self.model.__name__} instance with ID: {getattr(orm_instance, self.get_pk_field())}")
            return orm_instance
        except ValueError as e:
            # 直接传递已记录的ValueError异常
            raise
        except Exception as e:
            logger.error(f"Failed to convert dict to {self.model.__name__} instance: {str(e)}", exc_info=True)
            raise ValueError(f"Error converting dict to ORM instance: {str(e)}") from e

"""
BaseRepository父类功能说明（子类继承参考）
=========================================
【核心定位】抽象基础仓库类，封装通用CRUD操作，子类需实现_get_model()和get_pk_field()

【功能模块速查】
1. 存在性检查（去重核心）
-------------------------
- exists_by_pk(pk_value): 根据主键判断记录是否存在
  * 参数: pk_value - 主键值
  * 返回: bool（True=存在，False=不存在）

- exists_by_fields(**filter_by): 根据字段组合判断是否存在（复合唯一键场景）
  * 参数: 关键字参数形式的字段条件（如sample_id=123, batch_id=456）
  * 返回: bool（True=存在）


2. 查询操作
-------------------------
- get_by_pk(pk_value): 根据主键获取单条记录
  * 返回: ORM实例或None

- get_all(): 获取所有记录（慎用大数据量场景）
  * 返回: ORM实例列表

- query_filter(**filter_conditions): 多字段AND条件查询
  * 参数: 关键字参数形式的查询条件（如status="valid"）
  * 返回: 匹配的ORM实例列表

- query_filter_or(**filter_conditions): 多字段OR条件查询
  * 参数: 关键字参数形式的查询条件
  * 返回: 匹配的ORM实例列表

- query_filter_advanced(*criteria): 高级查询（支持复杂条件组合）
  * 参数: SQLAlchemy表达式（如Model.status != 'deleted'）
  * 返回: 匹配的ORM实例列表


3. 插入操作
-------------------------
- insert_if_not_exists(record, conflict_fields=None): 去重插入
  * 参数: 
    - record: 要插入的ORM实例
    - conflict_fields: 可选，用于判断冲突的字段列表（默认使用主键）
  * 返回: bool（True=新插入，False=已存在）

- bulk_insert_if_not_exists(records, conflict_fields=None): 批量去重插入
  * 参数: 
    - records: ORM实例列表
    - conflict_fields: 冲突判断字段列表
  * 返回: int（实际插入数量）


4. 更新操作（带审计日志）
-------------------------
- update_field(pk_value, field_name, new_value, operator="system"): 单字段更新+日志
  * 参数:
    - pk_value: 主键值
    - field_name: 要更新的字段名
    - new_value: 新值
    - operator: 操作人（默认"system"）
  * 返回: bool（True=更新成功，False=未变更/记录不存在）


5. 删除操作
-------------------------
- delete_by_pk(pk_value): 根据主键删除记录
  * 返回: bool（True=删除成功，False=记录不存在）


6. 其他通用方法
-------------------------
- count(**filter_by): 统计记录数（支持条件过滤）
  * 返回: int（符合条件的记录总数）

- upsert(record, update_on_duplicate=None): 存在则更新，不存在则插入
  * 参数:
    - record: ORM实例
    - update_on_duplicate: 存在时要更新的字段列表（None=更新所有字段）
  * 返回: 操作后的ORM实例


7. 表结构操作
-------------------------
- add_table_field(field_name, field_type, description=""): 动态添加表字段
  * 参数:
    - field_name: 字段名
    - field_type: 字段类型（如'VARCHAR(100)', 'INT'）
    - description: 字段描述（可选）
  * 返回: 操作结果字典（含success状态和message）


【子类实现要求】
1. 必须实现:
   - _get_model(): 返回对应的ORM模型类（如return Project）
   - get_pk_field(): 返回主键字段名（如return "project_id"）

2. 使用建议:
   - 优先使用父类提供的通用方法，避免重复开发
   - 复杂查询使用query_filter_advanced()组合条件
   - 批量操作使用bulk_insert_if_not_exists提高效率
   - 字段更新必须通过update_field()确保审计日志记录
"""
