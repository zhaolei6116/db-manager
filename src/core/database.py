'''
数据库连接和工具函数 
'''

import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_engine():
    """创建数据库引擎"""
    config = load_config()
    db_config = config['database']
    connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(connection_string, pool_pre_ping=True)

def get_session():
    """创建数据库会话"""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

Base = declarative_base()