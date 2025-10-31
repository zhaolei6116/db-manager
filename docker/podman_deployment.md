# Podman 部署指南

本文档提供了使用 Podman 部署生物样本测序数据管理系统的详细步骤和配置说明。

## 环境准备

- 已安装 Podman (v3.0+) 和 podman-compose
- 确保服务器有足够的磁盘空间用于存储数据库和分析文件
- 确认 `/home/zhaolei/project_analysis` 和 `/home/zhaolei/project/data_management/logs` 目录存在并有正确的权限

## 目录结构

```
/home/zhaolei/project/data_management/
├── docker/
│   ├── app.Dockerfile         # 应用服务 Dockerfile
│   ├── Dockerfile             # MySQL 服务 Dockerfile
│   ├── docker-compose.yml     # Compose 配置文件
│   └── podman_deployment.md   # 本部署指南
├── config/
│   ├── config.yaml            # 应用配置文件
│   └── database.ini           # 数据库配置文件（支持环境变量）
├── src/                       # 源代码目录
└── logs/                      # 日志目录
```

## 部署步骤

### 1. 修改权限（重要）

确保分析目录和日志目录对容器内的用户有正确的读写权限：

```bash
# 设置分析目录权限
mkdir -p /home/zhaolei/project_analysis
chown -R 1000:1000 /home/zhaolei/project_analysis
chmod -R 775 /home/zhaolei/project_analysis

# 设置日志目录权限
mkdir -p /home/zhaolei/project/data_management/logs
chown -R 1000:1000 /home/zhaolei/project/data_management/logs
chmod -R 775 /home/zhaolei/project/data_management/logs

# 设置原始数据目录权限（只读）
chmod -R 755 /bioinformation/Project/Sequencing
```

### 2. 使用 podman-compose 部署

在 `docker` 目录下执行以下命令：

```bash
cd /home/zhaolei/project/data_management/docker
podman-compose up -d --build
```

此命令将：
- 构建 MySQL 和应用服务的镜像
- 创建并启动两个容器
- 自动配置网络以允许容器间通信
- 挂载必要的卷以实现数据持久化

### 3. 验证部署

检查容器状态：

```bash
podman-compose ps
```

查看应用日志：

```bash
podman-compose logs app
```

查看数据库日志：

```bash
podman-compose logs mysql
```

## 配置说明

### 容器启动顺序

在 `docker-compose.yml` 中，我们配置了应用服务依赖于 MySQL 服务，确保 MySQL 完全启动并健康后才启动应用：

```yaml
depends_on:
  mysql:
    condition: service_healthy
```

### 自动重启配置

两个容器都配置了 `restart: unless-stopped`，确保：
- 容器意外停止时自动重启
- 服务器重启后自动启动容器

### 文件权限管理

应用容器内使用非 root 用户 `appuser`（UID=1000，GID=1000）运行，与宿主机上的权限映射保持一致：

```yaml
volumes:
  # 挂载分析目录（rw权限）
  - /home/zhaolei/project_analysis:/home/zhaolei/project_analysis
  # 挂载日志目录（rw权限）
  - /home/zhaolei/project/data_management/logs:/app/logs
  # 挂载原始数据目录（只读权限）
  - /bioinformation/Project/Sequencing:/bioinformation/Project/Sequencing:ro
```

Dockerfile 中已设置相应的权限：

```dockerfile
RUN useradd -m appuser && chown -R appuser:appuser /app && chown -R appuser:appuser /home/zhaolei/project_analysis
USER appuser
```

### 容器间通信

通过 Docker Compose 的网络配置，两个容器可以通过服务名称互相访问：

- 应用容器可以使用 `mysql` 主机名访问数据库容器，而不是使用固定 IP
- 数据库连接配置通过环境变量注入到应用容器中

## 开机启动配置

要确保系统重启后容器能自动启动，需要配置 Podman 的系统服务：

1. 创建 systemd 服务文件：

```bash
sudo vi /etc/systemd/system/bioinfo-containers.service
```

2. 添加以下内容：

```ini
[Unit]
Description=Bioinfo Containers
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/bin/podman-compose -f /home/zhaolei/project/data_management/docker/docker-compose.yml up -d
ExecStop=/usr/bin/podman-compose -f /home/zhaolei/project/data_management/docker/docker-compose.yml down
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
```

3. 启用并启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable bioinfo-containers.service
sudo systemctl start bioinfo-containers.service
```

## 手动控制容器

### 停止所有容器

```bash
podman-compose down
```

### 重启所有容器

```bash
podman-compose restart
```

### 查看应用容器内的文件

```bash
podman exec -it bioinfo_app ls -la /home/zhaolei/project_analysis
```

### 进入应用容器

```bash
podman exec -it bioinfo_app /bin/bash
```

## 常见问题解决

### 1. 文件权限错误

如果应用无法创建或写入文件，检查宿主机上的目录权限是否正确设置：

```bash
# 重置权限
chown -R 1000:1000 /home/zhaolei/project_analysis
chown -R 1000:1000 /home/zhaolei/project/data_management/logs
chmod -R 775 /home/zhaolei/project_analysis
chmod -R 775 /home/zhaolei/project/data_management/logs
```

### 2. 数据库连接失败

检查环境变量配置和网络连接：

```bash
# 查看应用容器的环境变量
podman exec bioinfo_app env | grep DB_

# 验证数据库连接
podman exec -it bioinfo_app mysql -h mysql -u root -pvklz123 bio_db
```

### 3. 容器启动失败

查看详细日志找出问题：

```bash
podman-compose logs --tail=100
```

## 性能优化建议

1. 对于大型项目，考虑增加 MySQL 的内存限制
2. 根据服务器性能调整应用容器的资源限制
3. 定期清理日志文件以避免磁盘空间不足

## 版本更新

当需要更新应用版本时，只需更新代码后重新构建镜像：

```bash
podman-compose up -d --build
```

## 注意事项

1. 首次启动时，MySQL 容器可能需要几分钟时间初始化数据库
2. 确保防火墙配置允许容器间通信
3. 定期备份数据库和分析结果
4. 不要直接修改容器内的配置文件，而是修改宿主机上的配置文件后重启容器