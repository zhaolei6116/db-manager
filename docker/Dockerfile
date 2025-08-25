# 使用官方 MySQL 8 镜像作为基础
FROM mysql:8

ARG docker_registry=docker.1ms.run

# 设置维护者信息
LABEL maintainer="zhao.ldb@qq.com"

# 环境变量：数据库名、root 密码等（可在运行时通过 -e 指定）
ENV MYSQL_DATABASE=bioinfo_db \
    MYSQL_ROOT_PASSWORD=password \
    MYSQL_USER=bioinfo_user \
    MYSQL_PASSWORD=bioinfo_pass

# 复制 MySQL 配置文件（优化性能）
COPY my.cnf /etc/mysql/conf.d/my.cnf

# 复制初始化 SQL 脚本到 /docker-entrypoint-initdb.d/（自动执行）
COPY init.sql /docker-entrypoint-initdb.d/init.sql

# 暴露 MySQL 默认端口
EXPOSE 3306

# 数据卷：持久化 MySQL 数据
VOLUME /var/lib/mysql

# 健康检查：确保 MySQL 服务正常运行
HEALTHCHECK --interval=30s --timeout=3s \
  CMD mysqladmin ping -h localhost -u root -p$MYSQL_ROOT_PASSWORD || exit 1

# 入口点（继承自官方镜像）
CMD ["mysqld"]
