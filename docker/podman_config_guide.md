# Podman 配置指南

## 问题说明
在运行`podman ps`命令时出现了警告：`WARN[0000] "/" is not a shared mount, this could cause issues or missing mounts with rootless containers`

这个警告表明根目录不是共享挂载，这可能会影响rootless模式下的容器挂载功能。本指南将帮助您配置Podman以解决这个问题。

## 配置步骤

### 1. 创建配置目录
```bash
mkdir -p ~/.config/containers
```

### 2. 创建或编辑containers.conf文件
使用您喜欢的编辑器创建或修改配置文件：
```bash
vim ~/.config/containers/containers.conf
```

添加以下内容：
```ini
[containers]
# 配置容器默认使用的日志驱动
log_driver = "k8s-file"

# 允许在rootless模式下使用共享挂载
# 这将解决"/" is not a shared mount警告
[storage]
# 设置存储驱动为overlay（通常是默认值）
driver = "overlay"

# 为rootless用户配置挂载选项
[storage.options]
# 启用overlay2的挂载选项
additionalimagestores = [
]

# 设置网络配置来解决容器通信问题
[network]
# 使用cni网络后端
network_backend = "cni"
```

### 3. 配置存储选项
编辑storage.conf文件：
```bash
vim ~/.config/containers/storage.conf
```

添加或修改以下内容：
```ini
[storage]
driver = "overlay"

[storage.options.overlay]
# 解决共享挂载警告
mount_program = "/usr/bin/fuse-overlayfs"
# 启用metadata缓存以提高性能
mountopt = "nodev,metacopy=on"
```

### 4. 配置rootless模式
确保您的用户在rootless模式下正确配置：
```bash
# 初始化rootless模式（只需运行一次）
podman system service --time=0
```

### 5. 验证配置
重启Podman服务并验证配置是否生效：
```bash
# 重启Podman服务
systemctl --user restart podman

# 验证配置是否生效
podman info
```

## 常见问题解决

### 1. 权限问题
如果遇到权限错误，请确保您有适当的权限创建和编辑配置文件：
```bash
# 检查并设置正确的权限
chmod -R 755 ~/.config/containers
```

### 2. 共享挂载问题
如果共享挂载问题仍然存在，您可以尝试以下解决方案：
```bash
# 查看当前挂载状态
findmnt -o TARGET,PROPAGATION /

# 如果需要，更改挂载传播属性（需要root权限）
sudo mount --make-rshared /
```

### 3. 容器网络问题
如果容器之间无法通信，请检查网络配置：
```bash
# 列出所有Podman网络
podman network ls

# 检查默认网络配置
podman network inspect podman
```

## 测试部署
配置完成后，您可以按照`podman_deployment.md`中的步骤测试部署：
```bash
cd /home/zhaolei/project/data_management/docker
podman-compose up -d --build
```

## 版本兼容性
本配置指南适用于Podman 4.x版本（当前版本：4.9.3）和podman-compose 1.x版本（当前版本：1.0.6）。