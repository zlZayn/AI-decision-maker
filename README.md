# SignalChain

基于 AI 信号链决策的数据清洗框架，专注于 ETL 流程中的数据标准化环节。

**核心思路**：用 AI 识别"这是什么字段"，用本地脚本执行"怎么清洗"。

---

## 核心特点

- **AI 语义识别**：AI 根据字段名和样本值，自动识别字段类型（性别、年龄、金额、邮箱等）
- **本地高效执行**：识别结果通过本地脚本执行，无 Token 浪费
- **智能缓存**：相同数据结构的文件再次处理时，秒级完成（跳过 AI 调用）
- **场景感知**：内置医疗、财务、用户、日志、地理 5 种数据场景
- **可扩展架构**：通过注册表机制，可自由扩展新的字段类型和处理逻辑

---

## 快速开始

### 1. 安装依赖

```bash
pip install pandas openai
```

### 2. 配置 API Key

编辑 `run_clean.py`，修改以下配置：

```python
API_KEY = "your-api-key"           # DeepSeek/OpenAI API Key
API_URL = "https://api.deepseek.com/v1"
MODEL = "deepseek-chat"
```

### 3. 准备数据

将脏数据 CSV 文件放入 `data/dirty/` 目录。

### 4. 运行清洗

```bash
# 清洗所有文件
python run_clean.py

# 清洗指定文件
python run_clean.py medical
```

清洗结果保存在 `data/clean/` 目录。

---

## 工作流程

数据从"脏 CSV"到"干净 CSV"，经过以下步骤：

| 步骤 | 谁做的 | 做什么 | 输入 | 输出 |
|------|--------|--------|------|------|
| 1. 提取元信息 | [脚本] | 读取 CSV，统计每个字段的名字、类型、样本值 | 脏数据 | DataProfile（字段画像） |
| 2. 生成指纹 | [脚本] | 根据字段信息生成唯一指纹 | DataProfile | fingerprint（哈希值） |
| 3. 查询缓存 | [脚本] | 查缓存文件，看是否处理过 | fingerprint | 命中/未命中 |
| 4. 识别场景 | [AI 判断] | AI 根据数据结构判断这是什么场景 | DataProfile | SceneCode（S0-S5） |
| 5. 组装提示词 | [脚本] | 根据场景组装字段识别提示词 | SceneCode, DataProfile | Prompt（提示词） |
| 6. 识别字段类型 | [AI 判断] | AI 根据字段信息判断每个字段的类型 | Prompt | signal_sequence（如 "IIGADDNT"） |
| 7. 组装操作链 | [脚本] | 把信号序列翻译成操作命令 | signal_sequence | 操作链 |
| 8. 执行清洗 | [脚本] | 按操作链逐列处理数据 | 脏数据, 操作链 | 干净数据 |

**简化理解**：

```
脏数据 CSV
    │
    ▼
┌──────────────────────────────────────────────────────────────┐
│ 第1-3步 [脚本]: 读取数据，生成指纹，查缓存                      │
└──────────────────────────────────────────────────────────────┘
    │
    ▼ (未命中缓存时)
┌──────────────────────────────────────────────────────────────┐
│ 第4步 [AI 判断]: 判断这是什么场景 → S1/S2/S3...              │
│ 第5步 [脚本]: 根据场景组装提示词                               │
│ 第6步 [AI 判断]: 判断每个字段的类型 → IIGADDNT...           │
└──────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────────┐
│ 第7-8步 [脚本]: 把 AI 说的翻译成操作，执行，产出干净数据        │
└──────────────────────────────────────────────────────────────┘
    │
    ▼
干净数据 CSV
```

**关键概念**：
- **DataProfile**：数据的"体检报告"，包含每个字段的名字、类型、样本值、缺失率
- **fingerprint**：数据的"指纹"，相同结构的数据有相同的指纹
- **SceneCode**：场景码，告诉系统这是哪种数据（S1=医疗，S2=财务，S3=用户...）
- **signal_sequence**：信号序列，每个字符代表一个字段的类型（如 "G"=性别，"A"=年龄）

---

## 文件结构

```
AI-decision-maker/
|
|-- run_clean.py                 # [脚本] 入口脚本，批量清洗数据
|
|-- signal_cache.json            # [脚本] 缓存文件，存储指纹与识别结果
|
|-- signalchain/                 # 核心框架
|   |
|   |-- pipeline.py              # [脚本] 主流程编排，协调5个Stage执行
|   |-- models.py                # [脚本] 数据结构定义（DataProfile、SceneConfig等）
|   |-- ai_client.py             # [AI] AI客户端封装（OpenAI/DeepSeek）
|   |-- cache.py                 # [脚本] 缓存系统（指纹生成、读写、失效）
|   |-- knowledge.py              # [脚本] 语义知识库（映射规则、验证模式）
|   |
|   |-- stage0_profile.py        # [脚本] 元信息提取（字段名、类型、样本）
|   |-- stage1_scene.py          # [AI] 场景识别（判断数据属于哪种场景）
|   |-- stage2_router.py         # [脚本] 路由与Prompt组装（查表+组装）
|   |-- stage3_semantic.py       # [AI] 字段语义识别（识别每个字段的类型）
|   |-- stage4_assemble.py        # [脚本] 执行计划组装（生成操作链）
|   |-- stage5_execute.py         # [脚本] 本地执行引擎（执行操作链）
|   |
|   |-- operations/              # 数据处理操作
|   |   |-- base.py              # [脚本] Operation基类定义
|   |   |-- registry.py          # [脚本] 操作注册表
|   |   |
|   |   |-- pass_through.py      # [脚本] 透传（原样保留）
|   |   |-- gender.py            # [脚本] 性别标准化
|   |   |-- age.py               # [脚本] 年龄提取
|   |   |-- department.py        # [脚本] 科室标准化
|   |   |-- drug_name.py         # [脚本] 药品名标准化
|   |   |-- icd10.py             # [脚本] ICD10诊断码校验
|   |   |-- datetime_parser.py    # [脚本] 日期时间解析
|   |   |-- currency.py          # [脚本] 金额拆分（分列操作）
|   |   |-- email.py             # [脚本] 邮箱验证
|   |   |-- phone.py             # [脚本] 手机号验证
|   |   |-- log_level.py         # [脚本] 日志级别标准化
|   |   |-- coordinates.py       # [脚本] 经纬度校验
|   |
|   |-- __init__.py
|
|-- data/
    |-- dirty/                    # 脏数据目录
    |   |-- medical.csv           # 医疗数据示例
    |   |-- user.csv              # 用户数据示例
    |   |-- finance.csv           # 财务数据示例
    |
    |-- clean/                    # 清洗后数据目录
        |-- medical_clean.csv
        |-- user_clean.csv
        |-- finance_clean.csv
```

---

## Python API 使用

```python
import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import OpenAIClient

# 初始化Pipeline
pipeline = SignalChainPipeline(
    ai_client=OpenAIClient(
        model="deepseek-chat",
        api_key="your-api-key",
        base_url="https://api.deepseek.com/v1"
    )
)

# 读取脏数据
dirty = pd.read_csv("data/dirty/medical.csv")

# 执行清洗
clean, report = pipeline.run(dirty)

# 查看质量报告
print(report.summary())

# 保存结果
clean.to_csv("data/clean/medical_clean.csv", index=False)
```

### 本地模式（跳过AI调用）

如果已有识别结果，可直接指定场景码和信号序列：

```python
from signalchain.pipeline import SignalChainPipeline

pipeline = SignalChainPipeline()
clean, report = SignalChainPipeline.run_local(
    df=dirty,
    scene_code="S1",           # 医疗数据场景
    signal_sequence="IIGADDNT" # 每位对应一个字段的类型
)
```

---

## 信号码速查表

| 信号码 | 含义 | 标准列名 | 处理操作 |
|--------|------|----------|----------|
| I | 编号/ID | id | pass_through |
| G | 性别 | gender | normalize_gender |
| A | 年龄 | age | extract_age |
| D | 科室 | department | normalize_department |
| N | 药品名 | drug_name | normalize_drug_name |
| C | 诊断码 | diagnosis_code | validate_icd10 |
| T | 时间日期 | date | parse_datetime |
| M | 金额 | amount_value/amount_currency | split_currency |
| E | 邮箱 | email | validate_email |
| P | 手机号 | phone | validate_phone |
| L | 日志级别 | log_level | normalize_log_level |
| R | 经纬度 | coordinate | validate_coordinates |
| X | 其他 | other | pass_through |

---

## 场景支持

| 场景码 | 场景名 | 支持的信号码 |
|--------|--------|--------------|
| S0 | 未知数据 | I, X |
| S1 | 医疗数据 | I, G, A, D, N, C, T, X |
| S2 | 财务数据 | I, M, T, X |
| S3 | 用户数据 | I, G, A, E, P, X |
| S4 | 日志数据 | I, T, L, X |
| S5 | 地理数据 | I, R, X |

---

## 缓存机制

首次清洗会调用 AI 并缓存结果。相同数据结构的文件再次处理时，直接从缓存读取，跳过 AI 调用。

缓存文件：`signal_cache.json`

**缓存失效条件**：
- 代码配置变更（路由表、操作注册表、标准列名等）
- 字段数量或字段名变更
- 样本值发生显著变化

清除缓存：删除 `signal_cache.json` 文件。
