FROM python:3.10-slim-buster

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    vim \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY src/ /app/src/
COPY config/ /app/config/
COPY lims_python/ /app/lims_python/
COPY pipeline_templates/ /app/pipeline_templates/

# 创建日志目录
RUN mkdir -p /app/logs

# 创建分析目录（与宿主机共享）
RUN mkdir -p /home/zhaolei/project_analysis

# 配置环境变量
ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai

# 设置非root用户运行应用
RUN useradd -m appuser && chown -R appuser:appuser /app && chown -R appuser:appuser /home/zhaolei/project_analysis
USER appuser

# 设置启动命令
CMD ["python", "/app/src/main.py"]