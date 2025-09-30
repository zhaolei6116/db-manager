from typing import Dict, Any
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.sample_repository import SampleRepository

logger = logging.getLogger(__name__)

class SampleProcessor:
    """Sample表数据处理器"""
    
    def __init__(self, db_session: Session):
        """
        初始化SampleProcessor
        
        Args:
            db_session: 数据库会话对象（必须外部输入）
        """
        if db_session is None:
            raise ValueError("数据库会话对象必须外部输入")
        self.db_session = db_session
        self.repo = SampleRepository(db_session)
    
    def process(self, data_dict: Dict[str, Any], file_name: str) -> bool:
        """
        处理Sample表数据
        
        Args:
            data_dict: 包含sample数据的字典
            file_name: 文件名，用于日志记录
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 获取主键字段名
            pk_field = self.repo.get_pk_field()
            
            # 检查字典中是否包含主键字段
            if pk_field not in data_dict or data_dict[pk_field] is None:
                logger.error(f"文件[{file_name}]的sample表数据缺少主键字段 '{pk_field}'")
                return False
            
            # 获取主键值
            pk_value = data_dict[pk_field]
            
            # 判断记录是否已存在
            if self.repo.exists_by_pk(pk_value):
                logger.info(f"文件[{file_name}]的sample表数据主键 '{pk_value}' 已存在，跳过处理")
                return True
            
            # 字典转换为ORM实例（带字段验证）
            orm_instance = self.repo.dict_to_orm_with_validation(data_dict)
            
            # 插入记录（如果不存在）
            inserted = self.repo.insert_if_not_exists(orm_instance)
            
            if inserted:
                logger.info(f"文件[{file_name}]的sample表数据插入成功，主键: {pk_value}")
                return True
            else:
                logger.warning(f"文件[{file_name}]的sample表数据插入失败，可能是并发插入导致")
                return False
                
        except ValueError as e:
            logger.error(f"处理[{file_name}]的sample表数据失败：{str(e)}")
            return False
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：处理[{file_name}]的sample表数据失败", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"处理[{file_name}]的sample表数据失败", exc_info=True)
            return False