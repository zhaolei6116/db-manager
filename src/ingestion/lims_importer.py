# src/ingestion/lims_importer.py（示例）
from src.ingestion.lims_puller import get_all_json_in_lims_dir
from src.models.models import InputFileMetadata
from src.repositories.base_repository import BaseRepository
from src.utils.field_mapping_handler import FieldMappingHandler



def check_file_in_db(file_name: str, db_session) -> bool:
    """检查文件是否已存在于input_file_metadata表"""
    repo = BaseRepository(InputFileMetadata, db_session)
    # 按file_name（主键）查询
    existing = repo.get_by_id(file_name)
    return existing is not None

def insert_file_to_db(file_name: str, db_session) -> None:
    """将新文件插入input_file_metadata表"""
    file_metadata = InputFileMetadata(file_name=file_name)
    repo = BaseRepository(InputFileMetadata, db_session)
    repo.insert(file_metadata)
    db_session.commit()
    logger.info(f"文件[{file_name}]已插入input_file_metadata表")

def import_lims_data():
    """12分钟一次的录入主逻辑"""
    # 1. 获取拉取根目录下所有JSON文件（含手动创建的）
    all_json_paths = get_all_json_in_lims_dir(config_file="config/config.yaml")
    if not all_json_paths:
        logger.info("未找到任何JSON文件，跳过录入")
        return

    # 2. 初始化数据库会话和字段映射处理器
    db_session = get_session()
    mapping_handler = FieldMappingHandler()

    # 3. 循环处理每个JSON文件
    for json_path in all_json_paths:
        file_name = Path(json_path).name  # 获取文件名（如 T22508290838.json）
        
        # 4. 检查文件是否已在数据库中
        if check_file_in_db(file_name, db_session):
            logger.info(f"文件[{file_name}]已存在于input_file_metadata，舍弃")
            continue
        
        # 5. 新文件：插入数据库 + 解析JSON录入业务表
        logger.info(f"处理新文件[{json_path}]")
        # 5.1 插入input_file_metadata
        insert_file_to_db(file_name, db_session)
        # 5.2 解析JSON并录入业务表（使用FieldMappingHandler）
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = yaml.safe_load(f)
            # 示例：录入Project表
            project_instance = mapping_handler.json_to_orm_instance("project", json_data)
            # 后续调用ProjectRepository插入数据库...

    # 关闭资源
    db_session.close()
    mapping_handler.close()

if __name__ == "__main__":
    import_lims_data()