from .base_repository import BaseRepository
from src.models.models import InputFileMetadata


class InputFileRepository(BaseRepository[InputFileMetadata]):
    """
    InputFileMetadata 模型的专用 Repository
    实现 BaseRepository 的抽象方法，绑定 InputFileMetadata 模型
    """
    def __init__(self, db_session: Session):
        """初始化，传入数据库会话"""
        super().__init__(db_session)

    def _get_model(self) -> InputFileMetadata:
        """返回绑定的ORM模型"""
        return InputFileMetadata

    def get_pk_field(self) -> str:
        """返回主键字段名（根据 InputFileMetadata 模型定义）"""
        # 假设 InputFileMetadata 的主键字段是 'file_name'
        # 如果实际主键不同，请修改为正确的字段名
        return "file_name"
