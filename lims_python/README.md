# 01 CWBIO LIMS 数据上传服务 (功能未测试)

一个用于将生物信息学数据上传到 LIMS API 的 Python 服务。本项目是 Java 实现的 Python 移植版本，并进行了改进和优化。

## 功能特性

- 从文件读取数据并处理记录
- 通过正确的身份验证将数据发送到 LIMS API
- 实现带有指数退避的重试逻辑
- 提供全面的错误处理和日志记录
- 收集执行过程中的指标信息

## 安装

### 先决条件

- Python 3.6 或更高版本
- 依赖的 Python 包：
  - requests

### 基本安装

1. 克隆此仓库：
   ```bash
   git clone http://192.168.100.13/kw_bio/lims_pythonapi
   cd lims_pythonapi
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

## 配置不需要变动

创建一个 INI 格式的配置文件，包含 `[LIMS]` 部分：

```ini
[LIMS]
# 必填项
appid = your_app_id
responseUrl = https://www.cwbio.com/gwapi/push_report
S = 5dbc98dcc983a70728bd082d1a47546e
# BJ
B = 9d5ed678fe57bcca610140957afab571
# GZ
G = dfcf28d0734569a6a693bc8194de62bf
# TZ
T = b9ece18c950afbfa6b0fdbfa4ff731d3
# WH
W = 61e9c06ea9a85a5088a499df6458d276

# 可选项及默认值
maxRetries = 3
initialDelayMs = 1000
backoffMultiplier = 2.0
maxDelayMs = 10000
batchSize = 100
```

## 数据格式

输入数据文件应为每行一条记录，字段以空格分隔：

```
detectNo status reportPath [reportReason] [plasmidLength] [sampleLength]
```

示例：
```
B22406270200 seqconfirm /OSS/20240601/B22406270200.zip "Successful sequencing" 100 200
B22406270201 seqcancel /OSS/20240601/B22406270201.zip "Failed due to contamination" - -
```

### 字段说明

- `detectNo`：检测编号/ID（必填）
- `status`：实验状态（必填）
  - 有效值：`seqconfirm`（成功）、`seqcancel`（失败）、`seqabnormal`（异常）
- `reportPath`：报告文件路径（必填）
- `reportReason`：结果原因（可选）
- `plasmidLength`：质粒长度（可选，无则用 `-`）
- `sampleLength`：样本长度（可选，无则用 `-`）

## 使用方法

使用以下命令运行程序：

```bash
python cwbio_lims.py --path /path/to/data/file --config /path/to/config/file
```

### 命令行参数

- `--path`：输入数据文件路径（必填）
- `--config`：配置文件路径（必填）
- `--verbose`，`-v`：启用详细日志输出

## API 文档

服务通过以下端点与 LIMS API 通信：

### 推送实验结果

**接口地址：** `/push_report`

**请求格式：**
```json
{
  "appid": "your_app_id",
  "sign": "generated_signature",
  "data": [
    {
      "detect_no": "B22406270200",
      "status": "seqconfirm",
      "report_path": "/OSS/20240601/B22406270200.zip",
      "report_reason": "Successful sequencing",
      "ext": {
        "plasmid_length": "100",
        "sample_length": "200"
      }
    }
  ]
}
```

**响应格式：**
```json
{
  "code": 200,
  "msg": "成功上传数据成功1条",
  "data": []
}
```

## 错误处理

程序包含全面的错误处理机制，并对可重试错误自动重试：

- 网络相关错误（连接问题、超时）
- 服务器端错误（HTTP 5xx）
- API 特定错误（代码 203、429、500、502、503、504）

不可重试错误包括：
- 认证错误（代码 201）
- 未找到数据错误（代码 202）
- HTTP 客户端错误（400、401、403）

## 日志

程序会将运行信息输出到控制台。使用 `--verbose` 参数可获得更详细的日志。

## 示例

### 基本用法

```bash
python cwbio_lims.py --path data.txt --config config.ini
```

### 启用详细日志

```bash
python cwbio_lims.py --path data.txt --config config.ini --verbose
```

## 错误码说明

| 代码 | 信息 | 可重试 |
|------|------|--------|
| 200 | 成功 | N/A |
| 201 | appid或appsecret不合法 | 否 |
| 202 | 查无数据 | 否 |
| 203 | 上传失败 | 是 |
| 429 | 请求过多 | 是 |
| 500 | 服务器内部错误 | 是 |
| 502 | 网关错误 | 是 |
| 503 | 服务不可用 | 是 |
| 504 | 网关超时 | 是 |

# 02 CWBIO LIMS 数据下载服务

这个程序提供从LIMS API获取数据报告并下载的功能，是Java程序CwbioRequestDataLims的Python实现版本。

## 功能

- 从配置文件读取API参数
- 向LIMS系统发送请求获取报告信息
- 下载报告文件
- 支持重试机制
- 支持进度显示
- 详细日志记录

## 安装

### 依赖项

- Python 3.6或更高版本
- requests库

可以通过pip安装依赖项：

```bash
python3 -m pip install requests
```

## 配置文件不需要变动

程序使用.ini格式的配置文件，示例配置如下：

```ini
[LIMS]
# API URLs
url = https://www.cwbio.com/gwapi/get_report
responseUrl = https://www.cwbio.com/gwapi/push_report

# Authentication
appid = cwbioprimecx
S = 5dbc98dcc983a70728bd082d1a47546e
# BJ
B = 9d5ed678fe57bcca610140957afab571
# GZ
G = dfcf28d0734569a6a693bc8194de62bf
# TZ
T = b9ece18c950afbfa6b0fdbfa4ff731d3
# WH
W = 61e9c06ea9a85a5088a499df6458d276

# Query parameters
startTime = 2024-01-01 00:00:00
endTime = 2024-01-31 23:59:59
page = 1
limit = 100

# Download settings
downloadPath = downloads
bufferSize = 8192

# Retry settings
maxRetries = 3
retryDelaySeconds = 5
timeoutSeconds = 30
initialDelayMs = 1000
backoffMultiplier = 2.0
maxDelayMs = 30000
```

## 使用方法

### 基本使用

```bash
python cwbio_lims_downloader.py --config config.ini
# 上海实验室
python3 cwbio_lims_downloader.py  --startTime "$(date +"%Y-%m-%d %H:%M:%S")" --endTime "$(date -d "24 hours ago" +"%Y-%m-%d %H:%M:%S")" --lab S --path ./LimsData
```

### 命令行参数

可以通过命令行参数覆盖配置文件中的设置：

- `--config`: 配置文件路径 (默认: config.ini)
- `--startTime`: 开始时间 (覆盖配置文件)
- `--endTime`: 结束时间 (覆盖配置文件)
- `--lab`: 实验室 (覆盖配置文件)
- `--path`: 下载路径 (覆盖配置文件)
- `--verbose` 或 `-v`: 详细输出
- `--help-full`: 显示详细帮助信息

### 示例

1. 使用默认配置文件
   ```bash
   python cwbio_lims_downloader.py
   ```

2. 指定配置文件
   ```bash
   python cwbio_lims_downloader.py --config my_config.ini
   ```

3. 覆盖时间范围
   ```bash
   python cwbio_lims_downloader.py --startTime "2024-02-01 00:00:00" --endTime "2024-02-29 23:59:59"
   ```

4. 覆盖下载路径
   ```bash
   python cwbio_lims_downloader.py --path ./my_downloads
   ```

## 错误代码说明

- 200: 成功
- 201: 无满足条件的数据（这不是错误，只是没有符合条件的数据）
- 其他代码：各种错误情况
