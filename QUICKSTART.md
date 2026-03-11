# 硬盘故障预测系统 - 快速启动指南

## 🚀 快速开始（5 分钟）

### 1. 安装依赖

```bash
cd disk-failure-prediction

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt
```

### 2. 安装 smartmontools

**Windows:**
```powershell
# 下载安装：https://www.smartmontools.org/wiki/Download
# 或 winget
winget install smartmontools
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install smartmontools
```

**macOS:**
```bash
brew install smartmontools
```

### 3. 采集数据

```bash
# 运行采集脚本
python src/data_collector.py

# 查看采集结果
ls data/raw/
```

### 4. 特征工程

```bash
python src/feature_engineering.py
```

### 5. 训练模型

```bash
python src/model.py
```

### 6. 启动服务

```bash
# 终端 1: 启动 API 服务
uvicorn app.main:app --reload

# 终端 2: 启动 Dashboard
streamlit run app/dashboard.py
```

访问：
- API 文档：http://localhost:8000/docs
- Dashboard: http://localhost:8501

---

## 🐳 Docker 启动（推荐）

```bash
# 一键启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

访问：
- API: http://localhost:8000
- Dashboard: http://localhost:8501

---

## 📝 使用说明

### 单盘预测

1. 打开 Dashboard (http://localhost:8501)
2. 选择"单盘预测"
3. 输入硬盘序列号和 SMART 数据
4. 点击"开始预测"

### 批量预测

1. 准备 CSV 文件（包含 SMART 数据）
2. 上传到 Dashboard
3. 下载预测结果

### 查看趋势

1. 选择"趋势分析"
2. 选择硬盘
3. 查看 SMART 趋势图

---

## 🔧 常见问题

### Q: smartctl 命令找不到？
A: 请安装 smartmontools（见步骤 2）

### Q: 模型训练失败？
A: 确保有足够的样本（至少 100 条记录）

### Q: API 服务无法启动？
A: 检查端口 8000 是否被占用

### Q: Docker 启动失败？
A: 确保 Docker Desktop 已启动

---

## 📖 更多文档

- [完整开发文档](../docs/硬盘故障预测系统 - 融合增强版开发文档.md)
- [API 文档](http://localhost:8000/docs)
- [使用指南](./docs/USAGE.md)

---

*版本：v2.0 | 最后更新：2026-03-12*
