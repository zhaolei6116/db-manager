#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""临时设置MySQL数据库时区脚本
此脚本用于在不重启容器的情况下，临时设置MySQL数据库的时区，方便测试时间同步。
注意：此设置仅在当前会话和全局配置中生效，容器重启后将恢复默认设置。"""

import os
import sys
import logging

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.utils.yaml_config import YAMLConfig
from sqlalchemy import create_engine, text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """获取数据库连接引擎
    
    Returns:
        engine: SQLAlchemy 数据库引擎
    """
    try:
        # 加载配置文件
        yaml_config = YAMLConfig()
        
        # 从配置中获取数据库连接信息
        db_config = yaml_config.get_database_config()
        
        # 获取管理员用户信息（需要高权限来修改全局时区）
        admin_user = db_config['users']['admin']
        
        # 创建数据库连接 URL
        db_url = f"mysql+pymysql://{admin_user['user']}:{admin_user['password']}@{db_config['host']}:{db_config['port']}/{db_config['db_name']}?charset={db_config.get('charset', 'utf8mb4')}"
        
        # 创建引擎
        engine = create_engine(db_url)
        logger.info(f"成功连接到数据库: {db_config['host']}:{db_config['port']}/{db_config['db_name']}")
        return engine
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        raise

def set_temporary_timezone(timezone='+08:00'):
    """临时设置数据库时区
    
    Args:
        timezone: 要设置的时区，默认为北京时间(东八区)
    """
    try:
        engine = get_db_connection()
        
        with engine.connect() as connection:
            # 开始事务
            transaction = connection.begin()
            
            try:
                # 设置全局时区
                logger.info(f"设置全局时区为: {timezone}")
                connection.execute(text(f"SET GLOBAL time_zone = '{timezone}'"))
                
                # 设置当前会话时区
                logger.info(f"设置当前会话时区为: {timezone}")
                connection.execute(text(f"SET time_zone = '{timezone}'"))
                
                # 查询当前时区设置
                result = connection.execute(text("SELECT @@global.time_zone, @@session.time_zone"))
                global_time_zone, session_time_zone = result.fetchone()
                logger.info(f"数据库全局时区: {global_time_zone}, 会话时区: {session_time_zone}")
                
                # 提交事务
                transaction.commit()
                logger.info("时区设置成功！")
                
                # 验证当前时间
                result = connection.execute(text("SELECT NOW() as db_time"))
                current_time = result.scalar_one()
                logger.info(f"设置后的数据库当前时间: {current_time}")
                
            except Exception as e:
                # 回滚事务
                transaction.rollback()
                logger.error(f"时区设置失败: {str(e)}")
                raise
        
        logger.info("注意：此设置仅在当前容器运行期间有效，容器重启后将恢复默认设置。如需永久生效，请使用修改后的docker-compose.yml和my.cnf文件重新部署。")
        
    except Exception as e:
        logger.error(f"操作失败: {str(e)}")
        raise

def check_time_difference():
    """检查系统时间与数据库时间的差异
    """
    try:
        from datetime import datetime
        
        # 获取系统当前时间
        system_time = datetime.now()
        logger.info(f"系统当前时间: {system_time}")
        
        # 获取数据库当前时间
        engine = get_db_connection()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT NOW() as db_time"))
            db_time = result.scalar_one()
            logger.info(f"数据库当前时间: {db_time}")
        
        # 计算时间差
        time_diff = abs((system_time - db_time).total_seconds())
        logger.info(f"系统时间与数据库时间差: {time_diff:.2f} 秒")
        
        # 判断时间是否同步（误差小于1分钟视为同步）
        if time_diff < 60:
            logger.info("✓ 数据库时间同步正常")
        else:
            logger.warning(f"✗ 数据库时间与系统时间不同步，误差为 {time_diff:.2f} 秒")
            
    except Exception as e:
        logger.error(f"时间差检查失败: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("开始临时设置数据库时区...")
    try:
        # 设置时区为北京时间(东八区)
        set_temporary_timezone('+08:00')
        
        # 检查时间同步情况
        logger.info("\n检查系统时间与数据库时间同步情况...")
        check_time_difference()
        
        logger.info("\n临时时区设置完成！")
        sys.exit(0)
    except Exception:
        logger.error("临时时区设置失败。")
        sys.exit(1)