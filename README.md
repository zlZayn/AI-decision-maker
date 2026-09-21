# SignalChain

AI 驱动的数据分析框架，包含两条链路：数据清洗（认字段）和分类变量分析（认分类变量）。

**核心思路**：AI 负责决策，程序负责执行。

---

## 核心特点

- **AI 语义识别**：AI 根据字段名和样本值，自动识别字段类型（性别、年龄、金额、邮箱等）
- **本地高效执行**：识别结果通过本地脚本执行，无 Token 浪费
- **智能缓存**：相同数据结构的文件再次处理时，秒级完成（跳过 AI 调用）
- **场景感知**：内置医疗、财务、用户、日志、地理 5 种数据场景
- **分类变量分析**：AI 筛选分类变量并判断有序性，自动选择统计方法
- **可扩展架构**：通过注册表机制，可自由扩展新的字段类型和处理逻辑

---

## 快速开始

### 环境要求

- Python >= 3.10
- R 环境（仅分类变量分析需要）

### 1. 安装依赖

```bash
pip install pandas openai openpyxl
```

分类变量分析还需要 R 环境，以及 R 包：`tidyverse`、`jsonlite`、`openxlsx`。

### 2. 配置 API Key

复制 `config.example.py` 为 `config.py`（已 gitignore，密钥不会进版本库），按两套系统分别填写：

```python
# 系统二：通用大语言模型（慢、审慎、可生成文本）
SYSTEM2_API_KEY = "your-deepseek-api-key"
SYSTEM2_BASE_URL = "https://api.deepseek.com"
SYSTEM2_MODEL = "deepseek-v4-flash"

# 系统一：Jev / TypeSafe System One（快、只出概率分布）—— 可选
SYSTEM1_API_KEY = ""                # 留空则自动降级为纯系统二
SYSTEM1_BASE_URL = "https://api.typesafe.ai"
SYSTEM1_MODEL = "jev-latest"
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

### 5. 分类变量分析（可选）

```bash
# 分析分类变量 + 统计检验
python run_categorical.py

# 强制重新分类（忽略缓存）
python run_categorical.py --no-cache
```

分析结果保存在 `data/categorical/output/` 目录（JSON + Excel）。

---

## 工作流程

数据从"脏 CSV"到"干净 CSV"，经过以下步骤：

| 步骤 | 谁做的 | 做什么 | 输入 | 输出 |
| --- | --- | --- | --- | --- |
| 1. 提取元信息 | [脚本] | 读取 CSV，统计每个字段的名字、类型、样本值 | 脏数据 | DataProfile（字段画像） |
| 2. 生成指纹 | [脚本] | 根据字段信息生成唯一指纹 | DataProfile | fingerprint（哈希值） |
| 3. 查询缓存 | [脚本] | 查缓存文件，看是否处理过 | fingerprint | 命中/未命中 |
| 4. 识别场景 | [AI 判断] | AI 根据数据结构判断这是什么场景 | DataProfile | SceneCode（S0-S5） |
| 5. 组装提示词 | [脚本] | 根据场景组装字段识别提示词 | SceneCode, DataProfile | Prompt（提示词） |
| 6. 识别字段类型 | [AI 判断] | AI 根据字段信息判断每个字段的类型 | Prompt | signal_sequence（如 "IIGADDNT"） |
| 7. 组装操作链 | [脚本] | 把信号序列翻译成操作命令 | signal_sequence | 操作链 |
| 8. 执行清洗 | [脚本] | 按操作链逐列处理数据 | 脏数据, 操作链 | 干净数据 |

**简化理解**：

脏数据 CSV 进入后，分三个阶段处理：

1. **第1-3步 [脚本]**：读取数据，生成指纹，查缓存
2. **第4-6步 [AI + 脚本]**：未命中缓存时，AI 判断场景和字段类型
3. **第7-8步 [脚本]**：把 AI 识别结果翻译成操作，执行清洗，产出干净数据

**关键概念**：

- **DataProfile**：数据的"体检报告"，包含每个字段的名字、类型、样本值、缺失率
- **fingerprint**：数据的"指纹"，相同结构的数据有相同的指纹
- **SceneCode**：场景码，告诉系统这是哪种数据（S1=医疗，S2=财务，S3=用户...）
- **signal_sequence**：信号序列，每个字符代表一个字段的类型（如 "G"=性别，"A"=年龄）

### 分类变量分析流程

CSV 输入后，经过三层处理：

1. **第一层 AI**：筛选分类变量，输出字段名列表（如 gender, education, satisfaction）
2. **脚本 + 第二层 AI**：提取唯一值，判断每个变量有序还是无序（如 education: 小学 > 本科 > 硕士）
3. **R 脚本**：根据变量类型自动选择统计方法并执行检验，产出 report.json 和 report.xlsx

---

## 文件结构

```text
AI-decision-maker/
├── data/
│   ├── categorical/
│   │   ├── input/
│   │   │   ├── data_A.csv
│   │   │   ├── data_B.csv
│   │   │   ├── data_C.csv
│   │   │   └── data_D.csv
│   │   └── output/
│   │       ├── data_A_type.json
│   │       ├── data_B_type.json
│   │       ├── data_C_type.json
│   │       ├── data_D_type.json
│   │       ├── report.json
│   │       └── report.xlsx
│   ├── clean/
│   │   ├── finance_clean.csv
│   │   ├── medical_clean.csv
│   │   └── user_clean.csv
│   └── dirty/
│       ├── finance.csv
│       ├── medical.csv
│       └── user.csv
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SYSTEM1_JEV.md
│   ├── unified_framework_design.md
│   └── 分类变量有序判断.md
├── examples/
│   └── demo.py
├── signalchain/
│   ├── operations/
│   │   ├── __init__.py
│   │   ├── age.py
│   │   ├── base.py
│   │   ├── coordinates.py
│   │   ├── currency.py
│   │   ├── datetime.py
│   │   ├── department.py
│   │   ├── drug_name.py
│   │   ├── email.py
│   │   ├── gender.py
│   │   ├── icd10.py
│   │   ├── log_level.py
│   │   ├── pass_through.py
│   │   ├── phone.py
│   │   └── registry.py
│   ├── __init__.py
│   ├── ai_client.py
│   ├── cache.py
│   ├── categorical.py
│   ├── categorical_system1.py
│   ├── fastpath.py
│   ├── knowledge.py
│   ├── models.py
│   ├── pipeline.py
│   ├── system1.py
│   ├── run_categorical_analysis.R
│   ├── stage0_profile.py
│   ├── stage1_scene.py
│   ├── stage2_router.py
│   ├── stage3_semantic.py
│   ├── stage4_assemble.py
│   ├── stage5_execute.py
│   └── tokenizer.py
├── tests/
│   ├── __init__.py
│   ├── deepseek_tokenizer/
│   ├── run_all.py
│   ├── run_e2e_categorical.py
│   ├── run_e2e_pipeline.py
│   ├── run_token_benchmark.py
│   ├── run_unit.py
│   ├── test_cache.py
│   ├── test_categorical.py
│   ├── test_categorical_system1.py
│   ├── test_fastpath.py
│   ├── test_operations.py
│   ├── test_pipeline.py
│   ├── test_stage0.py
│   ├── test_system1.py
│   ├── test_stage1.py
│   ├── test_stage2.py
│   ├── test_stage3.py
│   ├── test_stage4.py
│   └── test_stage5.py
├── .gitignore
├── LICENSE
├── PROJECT_INTRO.md
├── README.md
├── .python-version
├── config.example.py
├── config.py
├── pyproject.toml
├── run_categorical.py
├── run_clean.py
├── run_smoke_jev.py
├── signal_cache.json
└── uv.lock
```

---

## Python API 使用

```python
import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import DeepSeekV4Client

# 初始化Pipeline（默认关闭思考模式，字段识别不需要深度推理）
pipeline = SignalChainPipeline(
    ai_client=DeepSeekV4Client(
        model="deepseek-v4-flash",
        api_key="your-api-key",
        base_url="https://api.deepseek.com",
        thinking=False  # 关闭可省约50%费用，输出结果一致
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
| --- | --- | --- | --- |
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
| --- | --- | --- |
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

---

## 系统一（可选）：Jev 快通道

默认走**系统二**（通用大模型）。额外接上 **系统一** Jev 后，清洗链路的两次 AI 往返
会压成一次，并且每个字段都带概率与置信度：

```bash
uv sync --extra system1          # 装官方 SDK（不装也能跑，只是没系统一）
uv run python run_clean.py medical --system1
```

两套系统默认互不依赖，三种跑法：

```bash
uv run python run_clean.py medical                      # 纯系统二（默认）
uv run python run_clean.py medical --system1            # 纯系统一，不需要 DeepSeek Key
uv run python run_clean.py medical --system1 --escalate # 串联：系统一拿不准时交系统二
```

纯系统一时，低置信字段落保守默认值（`X` = pass_through，即不猜、不改）；
串联是显式选择，不传 `--escalate` 时系统二完全不参与。
实测数据（中文脏数据准确率、延迟、token 成本、有序性判定）见
[docs/SYSTEM1_JEV.md](docs/SYSTEM1_JEV.md) §13。

---

## 开发者文档

- 维护索引（规则与仪表盘）→ [AGENTS.md](AGENTS.md)
- 架构设计 → [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 项目简介 → [PROJECT_INTRO.md](PROJECT_INTRO.md)
- 核心模块手册 → [signalchain/README.md](signalchain/README.md)
- 测试手册 → [tests/README.md](tests/README.md)
