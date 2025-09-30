# tests/test_connection.py
from sqlalchemy.sql import text
from src.models.database import get_session
import os

try:
    # 获取当前脚本所在目录的绝对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建配置文件的绝对路径
    config_path = os.path.join(current_dir, '..', 'config', 'database.ini')
    
    with get_session(config_path) as session:
        print("Connection successful")
        session.execute(text("SELECT 1"))
        print("Query successful")
except Exception as e:
    print(f"Connection failed: {e}")
    print(f"Error type: {type(e).__name__}")