# src/models/database.py
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from typing import Optional, Generator, Dict
from contextlib import contextmanager

# 导入YAML配置工具
from src.utils.yaml_config import get_yaml_config

# 数据库连接池配置（优化性能）
POOL_SIZE = 10
MAX_OVERFLOW = 20
POOL_RECYCLE = 3600  # 1小时回收连接，避免超时


def get_db_config(config_file: Optional[str] = None, user_role: Optional[str] = None) -> dict:
    """
    读取数据库配置（从 config/config.yaml 读取）
    支持多用户配置，可以根据角色选择不同的数据库用户
    
    :param config_file: 配置文件路径（可选，用于向后兼容）
    :param user_role: 用户角色（reader/writer/admin/backup），默认为reader
    :return: 数据库配置字典（含host、port、user、password、database）
    """
    # 使用YAML配置工具获取配置
    config = get_yaml_config(config_file)
    db_config = config.get_database_config()
    
    # 默认使用reader角色
    if not user_role:
        user_role = "admin"
    
    # 提取公共配置
    result_config = {
        "host": db_config.get("host", "localhost"),
        "port": db_config.get("port", 3306),
        "database": db_config.get("db_name", "bio_db"),
        "charset": db_config.get("charset", "utf8mb4")
    }
    
    # 提取用户特定配置
    users_config = db_config.get("users", {})
    if user_role not in users_config:
        raise ValueError(f"配置文件中缺少用户角色 '{user_role}' 的配置")
    
    user_specific = users_config[user_role]
    result_config["user"] = user_specific.get("user", "")
    result_config["password"] = user_specific.get("password", "")
    
    # 校验必填配置项
    required_keys = ["host", "port", "user", "password", "database"]
    missing_keys = [key for key in required_keys if key not in result_config or not result_config[key]]
    if missing_keys:
        raise ValueError(f"数据库配置缺失必填项：{missing_keys}")
    
    # 转换port为整数
    result_config["port"] = int(result_config["port"])
    
    return result_config


def get_engine(config_file: Optional[str] = None, user_role: Optional[str] = None) -> create_engine:
    """
    创建SQLAlchemy引擎（单例模式，避免重复创建连接）
    :param config_file: 数据库配置文件路径（可选）
    :param user_role: 用户角色（reader/writer/admin/backup），默认为reader
    :return: SQLAlchemy引擎
    """
    # 读取数据库配置
    db_config = get_db_config(config_file, user_role)
    
    # 拼接MySQL连接字符串（格式：mysql+pymysql://user:password@host:port/database?charset=utf8mb4）
    connect_str = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config.get('charset', 'utf8mb4')}"
    )
    
    # 创建引擎（配置连接池）
    engine = create_engine(
        connect_str,                  # 连接池大小：最多保持 10 个连接
        pool_size=POOL_SIZE,          # 最多可超出 20 个“临时”连接
        max_overflow=MAX_OVERFLOW,    # 获取连接的超时时间（秒）
        pool_recycle=POOL_RECYCLE,   # 每 3600 秒（60分钟）重建连接，防止过期
        echo=False  # 生产环境设为False，避免打印SQL日志；调试时可设为True；打印 SQL 语句，调试用
    )
    
    return engine



@contextmanager
def get_session(config_file: Optional[str] = None, user_role: Optional[str] = None) -> Generator[Session, None, None]:
    """
    获取数据库会话的上下文管理器
    使用方式：
        with get_session() as db_session:  # 默认使用reader角色
            # 执行查询操作...
        
        with get_session(user_role="writer") as db_session:  # 使用writer角色
            # 执行写入操作...
    
    :param config_file: 配置文件路径（可选）
    :param user_role: 用户角色（reader/writer/admin/backup），默认为reader
    :yield: SQLAlchemy 会话
    """
    engine = get_engine(config_file, user_role)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    
    try:
        yield session  # 👈 会话交给 with 块使用
        session.commit()  # ✅ 成功则提交
    except Exception as e:
        session.rollback()  # ✅ 出错自动回滚
        raise  # 重新抛出异常
    finally:
        session.close()  # ✅ 无论成败都关闭连接


# 测试：验证数据库连接（运行database.py时执行）
if __name__ == "__main__":
    try:
        # 测试引擎创建
        engine = get_engine()
        print(f"数据库引擎创建成功：{engine}")
        
        # 测试会话创建和使用（通过上下文管理器）
        with get_session() as session:
            print(f"数据库会话创建成功")
            # 执行一个简单查询来验证连接
            result = session.execute(text("SELECT VERSION()"))
            db_version = result.scalar_one_or_none()
            print(f"成功连接到数据库，版本: {db_version}")
        
        print("数据库会话已自动关闭（上下文管理器）")
        print("数据库连接测试成功！")
    except Exception as e:
        print(f"数据库连接失败：{str(e)}")