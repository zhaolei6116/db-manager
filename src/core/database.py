# src/core/database.py
"""Database connection and utility functions."""
import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import logging

Base = declarative_base()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file: str = None) -> dict:
    """Load configuration from a YAML file.

    Args:
        config_file (str, optional): Path to the YAML config file. 
            Defaults to 'config/mysql_config.yaml' relative to this file, 
            or uses DB_CONFIG_PATH environment variable if set.

    Returns:
        dict: Configuration dictionary.

    Raises:
        FileNotFoundError: If the config file is not found.
        yaml.YAMLError: If the config file is invalid.
    """
    config_path = os.getenv("DB_CONFIG_PATH")
    if not config_path:
        config_path = config_file or os.path.join(os.path.dirname(__file__), '../../config/mysql_config.yaml')
    
    config_path = os.path.abspath(config_path)
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing config file {config_path}: {e}")

def get_engine(config_file: str = None) -> 'Engine':
    """Create and return a SQLAlchemy engine.

    Args:
        config_file (str, optional): Path to the config file for DB connection.

    Returns:
        Engine: SQLAlchemy engine instance.
    """
    config = load_config(config_file)
    db_config = config['database']
    connection_string = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"
    )
    logger.info(f"Connecting to database: {connection_string}")
    return create_engine(connection_string, pool_pre_ping=True)

def get_session(config_file: str = None) -> 'Session':
    """Create and return a SQLAlchemy session.

    Args:
        config_file (str, optional): Path to the config file for DB connection.

    Returns:
        Session: SQLAlchemy session instance.
    """
    engine = get_engine(config_file)
    Session = sessionmaker(bind=engine)
    logger.info("Creating new SQLAlchemy session")
    return Session()