# tests/test_connection.py
from sqlalchemy.sql import text
from src.core.database  import get_session

try:
    session = get_session(config_file='config/mysql_config.yaml')
    print("Connection successful")
    session.execute(text("SELECT 1"))
    print("Query successful")
    session.close()
except Exception as e:
    print(f"Connection failed: {e}")