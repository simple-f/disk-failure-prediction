# 🎉 项目脚手架已创建完成！

## 📁 项目结构

```
disk-failure-prediction/
├── 📄 README.md                  # 项目说明
├── 📄 QUICKSTART.md              # 快速开始指南
├── 📄 requirements.txt           # Python 依赖
├── 📄 Dockerfile                 # Docker 镜像
├── 📄 docker-compose.yml         # Docker 编排
├── 📄 .gitignore                 # Git 忽略文件
│
├── 📂 config/
│   └── config.yaml               # 配置文件
│
├── 📂 data/
│   ├── raw/                      # 原始数据
│   ├── processed/                # 处理后的数据
│   └── features/                 # 特征数据
│
├── 📂 src/
│   ├── data_collector.py         # 数据采集脚本
│   ├── feature_engineering.py    # 特征工程
│   └── model.py                  # 模型训练
│
├── 📂 app/
│   ├── main.py                   # FastAPI 服务
│   └── dashboard.py              # Streamlit 可视化
│
├── 📂 models/                    # 训练好的模型
├── 📂 notebooks/                 # Jupyter notebooks
├── 📂 tests/                     # 单元测试
└── 📂 logs/                      # 日志文件
```

## ✅ 已创建文件

| 文件 | 说明 | 状态 |
|------|------|------|
| README.md | 项目说明文档 | ✅ |
| QUICKSTART.md | 5 分钟快速开始指南 | ✅ |
| requirements.txt | Python 依赖列表 | ✅ |
| config.yaml | 系统配置文件 | ✅ |
| data_collector.py | SMART 数据采集脚本 | ✅ |
| feature_engineering.py | 时间窗口特征工程 | ✅ |
| model.py | LightGBM 模型训练 | ✅ |
| main.py | FastAPI 预测服务 | ✅ |
| dashboard.py | Streamlit 可视化 | ✅ |
| Dockerfile | Docker 镜像 | ✅ |
| docker-compose.yml | Docker 编排 | ✅ |
| .gitignore | Git 忽略文件 | ✅ |

## 🚀 下一步

### 方式 1：本地运行（推荐开发）

```bash
cd disk-failure-prediction

# 1. 安装依赖
pip install -r requirements.txt

# 2. 安装 smartmontools
# Windows: winget install smartmontools
# Ubuntu: sudo apt-get install smartmontools

# 3. 采集数据
python src/data_collector.py

# 4. 特征工程
python src/feature_engineering.py

# 5. 训练模型
python src/model.py

# 6. 启动服务
uvicorn app.main:app --reload
streamlit run app/dashboard.py
```

### 方式 2：Docker 运行（推荐生产）

```bash
cd disk-failure-prediction

# 一键启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

## 📖 文档

- **快速开始**：[QUICKSTART.md](disk-failure-prediction/QUICKSTART.md)
- **项目说明**：[README.md](disk-failure-prediction/README.md)
- **完整开发文档**：[硬盘故障预测系统 - 融合增强版开发文档.md](docs/硬盘故障预测系统 - 融合增强版开发文档.md)

## 🎯 核心功能

✅ **数据采集** - 自动采集 SMART 数据到 SQLite/PostgreSQL  
✅ **特征工程** - 7/14/30 天时间窗口 + 滞后特征 + 趋势特征  
✅ **模型训练** - LightGBM + 贝叶斯调优 + 交叉验证  
✅ **API 服务** - FastAPI RESTful API  
✅ **可视化** - Streamlit Dashboard  
✅ **Docker** - 一键部署  
✅ **飞书告警** - 多渠道通知（待配置）

## ⚠️ 注意事项

1. **数据标注**：需要硬盘故障记录（从资产管理系统获取）
2. **最小数据量**：建议至少 100 块盘 × 3 个月
3. **smartmontools**：必须安装才能采集数据
4. **飞书 webhook**：在 config.yaml 中配置

---

*项目位置：`C:\Users\Administrator\.openclaw\workspace-ai1\disk-failure-prediction`*  
*创建时间：2026-03-12 01:27*  
*版本：v2.0* 🔪
