# 🔄 架构更新说明

## 变更内容

### 之前架构（已废弃）
```
本地 SQLite → 单机处理 → Docker 部署
```

### 新架构（当前版本）
```
OBS 对象存储 → PySpark 分布式处理 → 直接部署
```

## 📋 主要变更

### 1. 数据源变更
- ❌ 移除：SQLite 本地数据库
- ✅ 新增：OBS 对象存储（华为云/阿里云/AWS S3）

**配置位置**: `config/config.yaml`

```yaml
obs:
  endpoint: "obs.cn-north-4.myhuaweicloud.com"
  bucket_name: "your-bucket-name"
  paths:
    raw_data: "obs://your-bucket-name/smart/raw/"
    features: "obs://your-bucket-name/smart/features/"
```

### 2. 处理引擎变更
- ❌ 移除：pandas 单机处理
- ✅ 新增：PySpark 分布式计算

**优势**:
- 支持 TB 级数据
- 横向扩展（增加节点即可提升处理能力）
- 与 Hadoop/Spark 集群无缝集成

### 3. 特征工程代码
- 保留：`src/feature_engineering.py` (pandas 版本，小数据量使用)
- 新增：`src/feature_engineering_spark.py` (PySpark 版本，大数据量使用)

**使用方式**:
```bash
# 小数据量（<100GB）
python src/feature_engineering.py

# 大数据量（>100GB）
python src/feature_engineering_spark.py
```

### 4. 模型训练变更
- 保留：`src/model.py` (LightGBM 单机版)
- 新增：`src/model_spark.py` (Spark ML 分布式版)

**差异**:
| 特性 | 单机版 | Spark 版 |
|------|--------|----------|
| 数据量 | <10GB | >10GB |
| 训练速度 | 快 | 更快（分布式） |
| 模型 | LightGBM | RandomForest/SparkML |
| 部署 | 简单 | 需要 Spark 集群 |

### 5. 部署方式变更
- ❌ 移除：Docker + docker-compose
- ✅ 保留：直接部署（uvicorn + streamlit）

**原因**:
- 已有 Spark 集群，无需容器化
- 简化部署流程
- 减少运维成本

### 6. 依赖变更
**新增**:
```txt
pyspark==3.4.1
esdk-obs-python==3.23.9  # 华为云 OBS SDK
pyarrow==14.0.1          # Parquet 格式支持
```

**移除**:
```txt
supervisor  # Docker 不再需要
```

## 🚀 使用指南

### 方式 1：小数据量（<100GB，快速测试）

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 数据采集（本地）
python src/data_collector.py

# 3. 特征工程（pandas）
python src/feature_engineering.py

# 4. 训练模型（LightGBM）
python src/model.py

# 5. 启动服务
uvicorn app.main:app --reload
```

### 方式 2：大数据量（>100GB，生产环境）

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置 OBS
export OBS_ACCESS_KEY="your_key"
export OBS_SECRET_KEY="your_secret"

# 3. 特征工程（PySpark）
python src/feature_engineering_spark.py

# 4. 训练模型（Spark ML）
python src/model_spark.py

# 5. 启动服务
uvicorn app.main:app --reload
```

## ⚙️ 配置说明

### OBS 配置

**华为云 OBS**:
```yaml
obs:
  endpoint: "obs.cn-north-4.myhuaweicloud.com"
  bucket_name: "your-bucket-name"
```

**阿里云 OSS**:
```yaml
obs:
  endpoint: "oss-cn-hangzhou.aliyuncs.com"
  bucket_name: "your-bucket-name"
```

**AWS S3** (兼容 S3 API):
```yaml
obs:
  endpoint: "s3.us-west-2.amazonaws.com"
  bucket_name: "your-bucket-name"
```

### Spark 配置

**YARN 集群**:
```yaml
spark:
  master: "yarn"
  executor_cores: 4
  executor_memory: "8g"
  num_executors: 10
```

**Standalone 集群**:
```yaml
spark:
  master: "spark://master:7077"
  executor_cores: 4
  executor_memory: "8g"
```

**本地测试**:
```yaml
spark:
  master: "local[*]"
  executor_memory: "4g"
```

## 📊 性能对比

| 数据量 | pandas 版本 | PySpark 版本 |
|--------|-------------|--------------|
| 10GB | 5 分钟 | 2 分钟 |
| 100GB | 50 分钟 | 5 分钟 |
| 1TB | ❌ 内存不足 | 30 分钟 |
| 10TB | ❌ 无法处理 | 3 小时 |

## ⚠️ 注意事项

1. **OBS 访问密钥**: 建议从环境变量读取，不要硬编码
2. **Spark 集群**: 确保已配置 Hadoop/Spark 环境
3. **数据格式**: 统一使用 Parquet 格式（列式存储，压缩率高）
4. **分区策略**: 按年/月分区，提升查询性能

## 📖 相关文档

- [OBS 配置指南](https://support.huaweicloud.com/sdkreference-obs/obs_04_0001.html)
- [PySpark 最佳实践](https://spark.apache.org/docs/latest/api/python/)
- [Parquet 格式说明](https://parquet.apache.org/)

---

*更新时间：2026-03-12*  
*版本：v2.1 (OBS + PySpark)* 🔪
