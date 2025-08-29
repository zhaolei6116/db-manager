import os
import json
import logging
from sqlalchemy.orm import sessionmaker
from core.database import get_engine
from core.utils import load_config
from ingestion.metadata_handler import check_and_record_file
from ingestion.field_mapper import map_fields
from ingestion.table_managers import (
    project_manager,
    sample_manager,
    batch_manager,
    sequencing_manager
)

config = load_config()
logger = logging.getLogger(__name__)

def process_json_file(file_path, session):
    """处理单个JSON文件"""
    file_name = os.path.basename(file_path)
    
    # 检查文件是否已处理
    if check_and_record_file(session, file_name):
        logger.info(f"File already processed: {file_name}")
        return
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_name}: {e}")
        return
    
    # 映射字段到各表
    mapped_data = map_fields(data, config['field_mapping'])
    
    # 处理project表
    project_data = mapped_data.get('project', {})
    project_manager.upsert_project(session, project_data)
    
    # 处理sample表
    sample_data = mapped_data.get('sample', {})
    sample_manager.upsert_sample(session, sample_data)
    
    # 处理batch表
    batch_data = mapped_data.get('batch', {})
    batch_manager.upsert_batch(session, batch_data)
    
    # 处理sequencing表
    sequencing_data = mapped_data.get('sequencing', {})
    sequencing_manager.upsert_sequencing(session, sequencing_data)
    
    # 记录文件处理完成
    session.commit()
    logger.info(f"Processed file: {file_name}")

def ingest_new_files():
    """扫描并处理新JSON文件"""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    json_dir = config['ingestion']['json_dir']
    if not os.path.exists(json_dir):
        logger.error(f"JSON directory not found: {json_dir}")
        return
    
    for root, _, files in os.walk(json_dir):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                process_json_file(file_path, session)
    
    session.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_new_files()