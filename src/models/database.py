# src/models/database.py
import configparser
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Optional, Generator
from contextlib import contextmanager

# 数据库连接池配置（优化性能）
POOL_SIZE = 10
MAX_OVERFLOW = 20
POOL_RECYCLE = 3600  # 1小时回收连接，避免超时


def get_db_config(config_file: Optional[str] = None) -> dict:
    """
    读取数据库配置（从 config/database.ini 读取）
    :param config_file: 配置文件路径（默认使用项目根目录的 config/database.ini）
    :return: 数据库配置字典（含host、port、user、password、database）
    """
    # 1. 确定配置文件路径（默认项目根目录下的 config/database.ini）
    if not config_file:
        # 从脚本路径向上查找项目根目录（适配不同调用场景）
        current_dir = Path(__file__).absolute().parent.parent.parent  # src/models/ → src/ → 项目根目录
        config_file = current_dir / "config" / "database.ini"
    
    # 2. 检查配置文件是否存在
    if not Path(config_file).exists():
        raise FileNotFoundError(f"数据库配置文件不存在：{config_file}")
    
    # 3. 读取配置
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    
    # 4. 提取[database]节点配置（确保所有关键键存在）
    if "database" not in config.sections():
        raise ValueError(f"配置文件 {config_file} 中缺少 [database] 节点")
    
    db_config = dict(config["database"])
    
    # 5. 校验必填配置项（避免KeyError）
    required_keys = ["host", "port", "user", "password", "database"]
    missing_keys = [key for key in required_keys if key not in db_config or not db_config[key]]
    if missing_keys:
        raise ValueError(f"数据库配置缺失必填项：{missing_keys}，请检查 {config_file}")
    
    # 6. 转换port为整数（配置文件中默认是字符串，需转成int）
    db_config["port"] = int(db_config["port"])
    
    return db_config


def get_engine(config_file: Optional[str] = None) -> create_engine:
    """
    创建SQLAlchemy引擎（单例模式，避免重复创建连接）
    :param config_file: 数据库配置文件路径
    :return: SQLAlchemy引擎
    """
    # 读取数据库配置
    db_config = get_db_config(config_file)
    
    # 拼接MySQL连接字符串（格式：mysql+pymysql://user:password@host:port/database?charset=utf8mb4）
    connect_str = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"
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
def get_session(config_file: Optional[str] = None) -> Generator[Session, None, None]:
    """
    获取数据库会话的上下文管理器
    使用方式：
        with get_session() as db_session:
            repo = SomeRepository(db_session)
            repo.insert(...)
    :param config_file: 配置文件路径
    :yield: SQLAlchemy 会话
    """
    engine = get_engine(config_file)
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
        engine = get_engine()
        print(f"数据库引擎创建成功：{engine}")
        session = get_session()
        print(f"数据库会话创建成功：{session}")
        session.close()
        print("数据库会话已关闭")
    except Exception as e:
        print(f"数据库连接失败：{str(e)}")