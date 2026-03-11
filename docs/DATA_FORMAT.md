# 📊 数据输入输出格式说明

## 📥 输入数据格式

### 1. 原始数据（OBS 存储）

**路径：** `obs://your-bucket-name/smart/raw/`

**格式：** Parquet（分区存储）

**Schema：**

```python
root
 |-- serial_number: string      # 硬盘序列号（主键）
 |-- timestamp: timestamp       # 采集时间戳
 |-- failure: int               # 故障标签（0=健康，1=故障）
 |-- smart_5_raw: double        # SMART 属性 5（重映射扇区计数）
 |-- smart_187_raw: double      # SMART 属性 187（报告不可纠正错误）
 |-- smart_188_raw: double      # SMART 属性 188（命令超时）
 |-- smart_197_raw: double      # SMART 属性 197（当前待处理扇区）
 |-- smart_198_raw: double      # SMART 属性 198（无法校正扇区）
 |-- smart_1_raw: double        # SMART 属性 1（读取错误率）
 |-- smart_3_raw: double        # SMART 属性 3（自旋启动时间）
 |-- smart_4_raw: double        # SMART 属性 4（启动/停止循环计数）
 |-- smart_7_raw: double        # SMART 属性 7（寻道错误率）
 |-- smart_9_raw: double        # SMART 属性 9（通电时间）
 |-- smart_10_raw: double       # SMART 属性 10（自旋重试计数）
 |-- smart_12_raw: double       # SMART 属性 12（电源循环计数）
 |-- smart_194_raw: double      # SMART 属性 194（硬盘温度）
 |-- smart_199_raw: double      # SMART 属性 199（UDMA CRC 错误）
 |-- smart_201_raw: double      # SMART 属性 201（软读取错误）
 |-- smart_240_raw: double      # SMART 属性 240（磁头移动时间）
 |-- smart_241_raw: double      # SMART 属性 241（总写入量）
 |-- smart_242_raw: double      # SMART 属性 242（总读取量）
 |-- smart_184_raw: double      # SMART 属性 184（端点 - 端点 CRC 错误）
 |-- year: int                  # 年份（分区列）
 |-- month: int                 # 月份（分区列）
```

**数据示例：**

```python
+------------------+-------------------+-------+-------------+---------------+
| serial_number    | timestamp         |failure| smart_5_raw | smart_187_raw |
+------------------+-------------------+-------+-------------+---------------+
| ZD12345678       | 2026-01-01 00:00  |   0   |    0.0      |     0.0       |
| ZD12345678       | 2026-01-02 00:00  |   0   |    2.0      |     1.0       |
| ZD12345678       | 2026-01-03 00:00  |   0   |    5.0      |     3.0       |
| ...              | ...               |  ...  |   ...       |    ...        |
| ZD12345678       | 2026-01-20 00:00  |   1   |   150.0     |    50.0       |
+------------------+-------------------+-------+-------------+---------------+
```

**数据特点：**
- **时间序列数据**：每块硬盘每天一条记录
- **不平衡数据**：故障样本约占 5-10%
- **缺失值**：部分 SMART 属性可能为空

---

### 2. 特征数据（特征工程后）

**路径：** `obs://your-bucket-name/smart/features/`

**特征类型：**

#### 2.1 时间窗口特征（滚动统计）

对每个 SMART 属性计算以下统计量：

| 特征名 | 说明 | 窗口大小 |
|--------|------|----------|
| `{smart}_mean_7d` | 7 天滚动平均值 | 7 天 |
| `{smart}_mean_14d` | 14 天滚动平均值 | 14 天 |
| `{smart}_mean_30d` | 30 天滚动平均值 | 30 天 |
| `{smart}_max_7d` | 7 天滚动最大值 | 7 天 |
| `{smart}_min_7d` | 7 天滚动最小值 | 7 天 |
| `{smart}_std_7d` | 7 天滚动标准差 | 7 天 |
| `{smart}_delta_7d` | 7 天变化量 | 7 天 |

**示例：**
```python
# smart_5_raw 的窗口特征
smart_5_raw_mean_7d:  3.5    # 过去 7 天 smart_5 的平均值
smart_5_raw_max_7d:   10.0   # 过去 7 天 smart_5 的最大值
smart_5_raw_delta_7d: 5.0    # smart_5 当前值 - 7 天前的值
```

#### 2.2 滞后特征

| 特征名 | 说明 |
|--------|------|
| `{smart}_lag_1d` | 1 天前的值 |
| `{smart}_lag_3d` | 3 天前的值 |
| `{smart}_lag_7d` | 7 天前的值 |

**示例：**
```python
smart_5_raw_lag_1d:  2.0   # 昨天的 smart_5 值
smart_5_raw_lag_7d:  0.0   # 7 天前的 smart_5 值
```

#### 2.3 趋势特征

| 特征名 | 说明 |
|--------|------|
| `{smart}_trend_7d` | 7 天变化率（斜率） |

**示例：**
```python
smart_5_raw_trend_7d: 0.71  # (当前值 - 7 天前值) / 7
```

**特征数据 Schema（部分）：**

```python
root
 |-- serial_number: string
 |-- timestamp: timestamp
 |-- failure: int
 |-- smart_5_raw: double
 |-- smart_5_raw_mean_7d: double
 |-- smart_5_raw_mean_14d: double
 |-- smart_5_raw_mean_30d: double
 |-- smart_5_raw_max_7d: double
 |-- smart_5_raw_min_7d: double
 |-- smart_5_raw_std_7d: double
 |-- smart_5_raw_delta_7d: double
 |-- smart_5_raw_lag_1d: double
 |-- smart_5_raw_lag_3d: double
 |-- smart_5_raw_lag_7d: double
 |-- smart_5_raw_trend_7d: double
 |-- ... (其他 SMART 属性同理)
 |-- year: int
 |-- month: int
```

**特征数量：**
- 原始 SMART 属性：19 个
- 时间窗口特征：19 × 5 统计量 × 3 窗口 = 285 个
- 滞后特征：19 × 3 滞后 = 57 个
- 趋势特征：19 个
- **总计：约 380 个特征**

---

## 📤 输出数据格式

### 1. 模型预测结果

**格式：** DataFrame / JSON

**Schema：**

```python
root
 |-- serial_number: string       # 硬盘序列号
 |-- timestamp: timestamp        # 预测时间
 |-- prediction: double          # 预测概率（0-1）
 |-- prediction_label: int       # 预测标签（0=健康，1=故障）
 |-- actual_label: int           # 实际标签（用于评估）
 |-- risk_level: string          # 风险等级（low/medium/high）
```

**预测结果示例：**

```python
+------------------+-------------------+------------+----------------+--------------+------------+
| serial_number    | timestamp         |prediction|prediction_label|actual_label|risk_level  |
+------------------+-------------------+------------+----------------+--------------+------------+
| ZD12345678       | 2026-02-01 00:00  |   0.92     |       1        |      1       | high       |
| ZD12345679       | 2026-02-01 00:00  |   0.15     |       0        |      0       | low        |
| ZD12345680       | 2026-02-01 00:00  |   0.55     |       1        |      0       | medium     |
+------------------+-------------------+------------+----------------+--------------+------------+
```

**风险等级划分：**
- `low`: 预测概率 < 0.4
- `medium`: 0.4 ≤ 预测概率 < 0.7
- `high`: 预测概率 ≥ 0.7

---

### 2. API 响应格式

**端点：** `POST /api/v1/predict`

**请求体：**

```json
{
  "serial_number": "ZD12345678",
  "smart_data": {
    "smart_5_raw": 10.0,
    "smart_187_raw": 5.0,
    "smart_188_raw": 0.0,
    "...": "..."
  },
  "historical_data": [
    {
      "timestamp": "2026-01-01T00:00:00",
      "smart_5_raw": 8.0,
      "smart_187_raw": 3.0
    },
    {
      "timestamp": "2026-01-02T00:00:00",
      "smart_5_raw": 9.0,
      "smart_187_raw": 4.0
    }
  ]
}
```

**响应体：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "serial_number": "ZD12345678",
    "prediction": 0.92,
    "prediction_label": 1,
    "risk_level": "high",
    "failure_probability": "92%",
    "predicted_failure_date": "2026-02-15",
    "days_to_failure": 14,
    "top_risk_factors": [
      {"feature": "smart_5_raw_mean_7d", "importance": 0.25},
      {"feature": "smart_187_raw_delta_7d", "importance": 0.18},
      {"feature": "smart_197_raw_max_14d", "importance": 0.15}
    ],
    "recommendation": "建议立即更换硬盘，数据迁移优先级：高"
  }
}
```

---

### 3. Dashboard 展示数据

**端点：** `GET /api/v1/dashboard`

**响应格式：**

```json
{
  "summary": {
    "total_disks": 10000,
    "healthy_disks": 9200,
    "warning_disks": 600,
    "critical_disks": 200,
    "failure_rate": 0.08
  },
  "predictions": [
    {
      "date": "2026-02-01",
      "failure_count": 15,
      "warning_count": 50
    },
    {
      "date": "2026-02-02",
      "failure_count": 12,
      "warning_count": 45
    }
  ],
  "risk_distribution": {
    "low": 9200,
    "medium": 600,
    "high": 200
  },
  "top_risk_disks": [
    {
      "serial_number": "ZD12345678",
      "prediction": 0.92,
      "risk_level": "high",
      "days_to_failure": 14
    },
    {
      "serial_number": "ZD12345679",
      "prediction": 0.85,
      "risk_level": "high",
      "days_to_failure": 20
    }
  ]
}
```

---

### 4. 告警通知（飞书）

**格式：** JSON（Webhook）

**示例：**

```json
{
  "msg_type": "interactive",
  "card": {
    "header": {
      "title": {
        "tag": "plain_text",
        "content": "🚨 硬盘故障预警"
      },
      "template": "red"
    },
    "elements": [
      {
        "tag": "div",
        "text": {
          "tag": "markdown",
          "content": "**硬盘序列号**: ZD12345678\n**故障概率**: 92%\n**预计故障时间**: 2026-02-15 (14 天后)\n**风险等级**: 🔴 高危"
        }
      },
      {
        "tag": "action",
        "actions": [
          {
            "tag": "button",
            "text": {
              "tag": "plain_text",
              "content": "查看详情"
            },
            "url": "http://dashboard.local/disk/ZD12345678"
          }
        ]
      }
    ]
  }
}
```

---

## 🔄 数据流转图

```
┌─────────────────┐
│   OBS 对象存储   │
│  (原始 SMART 数据) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  特征工程 (Spark)│
│ - 时间窗口特征   │
│ - 滞后特征      │
│ - 趋势特征      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  模型训练 (Spark)│
│ - LightGBM      │
│ - 贝叶斯调优    │
│ - 交叉验证      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   模型文件      │
│  (保存至 OBS)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   预测服务      │
│  (FastAPI)      │
└────────┬────────┘
         │
         ├──────►┌──────────────┐
         │       │  Dashboard   │
         │       │ (Streamlit)  │
         │       └──────────────┘
         │
         ├──────►┌──────────────┐
         │       │  飞书告警    │
         │       │  (Webhook)   │
         │       └──────────────┘
         │
         └──────►┌──────────────┐
                 │  数据导出    │
                 │  (CSV/Excel) │
                 └──────────────┘
```

---

## 📏 数据质量标准

### 完整性要求

- **数据缺失率** < 5%
- **时间连续性**：每块硬盘每天至少有 1 条记录
- **字段完整性**：关键 SMART 属性缺失率 < 1%

### 准确性要求

- **标签准确率**：故障标签准确率 ≥ 95%
- **时间戳精度**：精确到天
- **数值范围**：SMART 值在合理范围内

### 时效性要求

- **数据延迟**：T+1（当天数据次日可用）
- **预测延迟**：< 1 小时
- **告警延迟**：< 5 分钟

---

## 📝 注意事项

1. **数据隐私**：硬盘序列号需要脱敏处理
2. **数据备份**：原始数据保留至少 6 个月
3. **模型更新**：每月重新训练一次
4. **监控指标**：
   - 预测准确率（AUC ≥ 0.96）
   - 告警准确率（Precision ≥ 85%）
   - 召回率（Recall ≥ 90%）

---

*最后更新：2026-03-12*  
*版本：v1.0*
