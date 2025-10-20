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
    
    def get_pending_tasks_as_dicts(self):
        """获取所有待处理的任务，返回字典列表格式（可脱离会话使用）"""
        # 使用query.options(lazyload('*'))确保不会自动加载关联对象
        # 然后将结果转换为字典列表
        tasks = self.db_session.query(
            self._get_model().task_id,
            self._get_model().project_id,
            self._get_model().project_type,
            self._get_model().analysis_path
        ).filter(
            self._get_model().analysis_status == 'pending'
        ).all()
        
        # 转换为字典列表
        return [
            {
                'task_id': task[0],
                'project_id': task[1],
                'project_type': task[2],
                'analysis_path': task[3]
            } for task in tasks
        ]