# src/repositories/field_corrections_repository.py
from typing import List, Dict, Any
from uuid import uuid4
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging

from .base_repository import BaseRepository
from src.models.models import FieldCorrections

logger = logging.getLogger(__name__)

class FieldCorrectionsRepository(BaseRepository[FieldCorrections]):
    """
    FieldCorrections 模型的专用 Repository
    处理字段修正日志的批量插入等操作。
    """

    def _get_model(self) -> FieldCorrections:
        return FieldCorrections

    def get_pk_field(self) -> str:
        return "correction_id"

    def bulk_insert_corrections(self, correction_dicts: List[Dict[str, Any]]) -> int:
        """
        批量插入字段修正日志
        :param correction_dicts: 变更日志字典列表（从update_field生成）
        :return: 实际插入数量
        """
        try:
            correction_list = []
            for corr_dict in correction_dicts:
                # 生成唯一PK（uuid4，确保不冲突）
                corr_dict["correction_id"] = str(uuid4())
                # 确保notes为空字符串（如果未提供）
                corr_dict.setdefault("notes", "")
                # 转换dict到ORM实例
                correction = FieldCorrections(**corr_dict)
                correction_list.append(correction)
            inserted_count = self.bulk_insert_if_not_exists(correction_list)
            logger.info(f"Bulk inserted {inserted_count} field corrections")
            return inserted_count
        except SQLAlchemyError as e:
            logger.error(f"Failed to bulk insert field corrections: {str(e)}", exc_info=True)
            raise