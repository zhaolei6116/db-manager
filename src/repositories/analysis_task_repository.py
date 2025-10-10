# src/repositories/analysis_task_repository.py
from typing import List, Dict, Any, Optional
from .base_repository import BaseRepository, ModelType
from src.models.database import get_session
from src.models.models import AnalysisTask, Sequence


class AnalysisTaskRepository(BaseRepository[AnalysisTask]):
    """AnalysisTask表专用Repository"""
    
    def _get_model(self) -> ModelType:
        """返回关联的ORM模型"""
        return AnalysisTask
    
    def get_pk_field(self) -> str:
        """返回主键字段名"""
        return "task_id"
    
    def get_by_project_and_type(self, project_id: str, project_type: str):
        """根据项目ID和类型获取任务"""
        return self.db_session.query(self._get_model()).filter(
            self._get_model().project_id == project_id,
            self._get_model().project_type == project_type
        ).all()
    
    def get_pending_tasks(self):
        """获取所有待处理的任务"""
        return self.db_session.query(self._get_model()).filter(
            self._get_model().analysis_status == 'pending'
        ).all()