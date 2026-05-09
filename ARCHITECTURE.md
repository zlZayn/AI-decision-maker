# SignalChain 架构设计

本文档详细描述 SignalChain 框架的架构设计、数据流、以及如何进行定制开发。

---

## 设计哲学

SignalChain 的核心设计思路：**AI 负责"认知"，本地负责"执行"**。

| 处理方 | 职责 | 特点 |
|--------|------|------|
| AI (LLM) | 语义识别 | 理解意图、适配模糊场景 |
| 本地脚本 | 格式标准化 | 确定性操作、性能敏感 |

**为什么这样设计**：
1. **成本**：一次 AI 调用（约 30-60 Token）解决语义识别，后续大量操作本地执行
2. **速度**：本地执行毫秒级，无需等待网络
3. **可控**：清洗规则完全在代码中，可审计、可调试
4. **可扩展**：新增字段类型只需添加 Operation，无需改 AI Prompt

---

## AI 交互原理

SignalChain 的每个 AI 调用都遵循同一套设计原则：**极简输入、约束输出、严格校验**。

### 输入设计：只传 AI 需要的信息

AI 不接收原始数据，只接收结构化摘要：

| 信息 | 来源 | 用途 |
|------|------|------|
| 字段名 | DataFrame.columns | AI 判断语义的主要依据 |
| 数据类型 | dtype 检测 | 区分数值/字符串/日期 |
| 唯一值数量 | nunique() | 区分分类变量和连续变量 |
| 样本值（前 5 个） | dropna().unique()[:5] | 确认 AI 判断，防误判 |

**不传**：全量数据行、统计量（均值/方差）、数据轮廓（行数/列数统计）。这些信息对 AI 的语义判断没有帮助，只会浪费 Token。

### 输出设计：强制格式，禁止自由文本

每个 Prompt 都通过末尾的"输出："指令约束 AI 输出格式：

| 阶段 | Prompt 末尾 | 期望输出 | Token 消耗 |
|------|------------|----------|-----------|
| Stage 1 场景识别 | "只输出1个代码" | `S1` | ~30 in + 1 out |
| Stage 3 字段识别 | "只输出代码，无空格无解释" | `IIGADDNT` | ~60 in + N out |
| 分类变量筛选 | "只输出字段名，逗号分隔" | `gender,education` | ~80 in + 5 out |
| 有序判断 | "格式：字段名:值1>值2>值3" | `education:小学>本科` | ~100 in + 10 out |

**关键**：Prompt 末尾的"输出："后面直接拼接 AI 的回答，减少 AI 输出无关内容的概率。

### 校验层：不信任 AI 输出

每个 AI 输出都经过严格校验，非法值被替换为安全默认值：

```python
# Stage 1：场景码校验
cleaned = raw_output.strip()[:2]        # 截取前2字符
if cleaned not in VALID_SCENE_CODES:
    cleaned = "S0"                       # 回退到"未知"

# Stage 3：信号序列校验
for ch in cleaned:
    if ch not in scene_valid_codes:      # 场景白名单，不是全局白名单
        ch = "X"                         # 非法码替换为 X

# 分类变量：字段名校验
candidates = raw_output.split(",")
valid = [c for c in candidates if c in actual_field_names]  # 只保留真实存在的字段

# 有序判断：值校验
order = [v for v in order if v in unique_values[name]]     # 只保留数据中实际存在的值
```

**设计原则**：AI 的输出是"建议"，校验层决定最终值。

### 调用次数

| 流程 | AI 调用次数 | 说明 |
|------|------------|------|
| 数据清洗 Pipeline | 2 次 | Stage 1（场景）+ Stage 3（字段类型） |
| 分类变量分析 | 2 次 | 第一层（筛选分类变量）+ 第二层（有序/无序判断） |
| 缓存命中 | 0 次 | 跳过所有 AI 调用 |

### 模型配置

```python
DeepSeekV4Client(
    model="deepseek-v4-flash",
    thinking=False,      # 关闭思考模式（字段识别不需要深度推理）
    max_tokens=256,      # 输出上限
    temperature=0,       # 确定性输出，保证可复现
)
```

**思考模式关闭的理由**：字段识别和分类变量判断都是简单分类任务，关闭后费用降低约 50%，速度提升约 4 倍，输出结果与开启时一致。

---

## 分类变量分析

独立于数据清洗 Pipeline 的第二条分析链路，用于识别分类变量并判断有序性。

### 两层 AI 设计

```
CSV 输入
    │
    ▼
┌──────────────────────────────────────────────┐
│ 第一层 AI：从所有字段中筛选分类变量              │
│ 输入：字段名(类型, N种): 值1, 值2, ...         │
│ 输出：gender,education,satisfaction 或 "无"    │
└──────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────┐
│ 脚本：提取每个分类变量的唯一值（不修改原数据）    │
└──────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────┐
│ 第二层 AI：判断有序/无序，输出顺序              │
│ 输入：变量名 + 唯一值列表                       │
│ 输出：education:小学>本科>硕士 或 "无"          │
└──────────────────────────────────────────────┘
    │
    ▼
ClassificationResult(ordinal={...}, nominal=[...])
```

### 决策逻辑：根据变量类型选择统计方法

分类变量分析完成后，R 脚本根据两个变量的类型组合，自动选择最合适的统计方法：

| 变量组合 | 选择的方法 | 决策理由 |
|----------|-----------|---------|
| 有序 vs 有序 | Spearman rho | 有顺序信息，秩相关充分利用排序 |
| 无序 vs 无序 | Cramér's V | 无顺序可利用，衡量关联强度 |
| 有序 vs 无序 | Kruskal-Wallis H | 按无序分组，检验有序值的分布差异 |
| 通用兜底 | 卡方检验 | 不依赖类型假设，所有组合均适用 |

**设计意图**：不是无脑跑所有方法，而是根据数据特征做决策——这正是"AI Decision Maker"的核心理念。

### 分析缓存

分类变量分析使用文件级缓存（`*_type.json`），基于文件修改时间判断：

```
如果 output/data_A_type.json 存在 且 比 input/data_A.csv 更新：
    读取缓存，跳过 AI → [cache]
否则：
    调用 AI 分类，写入 JSON
```

### R 统计输出

R 脚本输出 2 个文件：

- `report.json` — 全部结果（JSON 格式）
- `report.xlsx` — 4 个 Sheet，按方法分类（Spearman / Cramer V / Kruskal-Wallis / Chi-square）

此外，每个 CSV 的 AI 分类结果保存为 `*_type.json`（含 ordinal 和 nominal）。

---

## 完整数据流

### 链路一：数据清洗

数据从脏数据到清洗后数据，经过以下阶段：

| 阶段 | 处理方 | 输入 | 输出 | 关键文件 |
|------|--------|------|------|----------|
| Stage 0 | [脚本] | DataFrame | DataProfile, fingerprint | stage0_profile.py |
| 缓存查询 | [脚本] | fingerprint | 命中/未命中 | cache.py |
| Stage 1 | [AI] | DataProfile | SceneCode | stage1_scene.py |
| Stage 2 | [脚本] | SceneCode, DataProfile | Prompt | stage2_router.py |
| Stage 3 | [AI] | Prompt | FieldSignalSequence | stage3_semantic.py |
| Stage 4 | [脚本] | signal_sequence | 操作链 | stage4_assemble.py |
| Stage 5 | [脚本] | DataFrame, 操作链 | 清洗后 DataFrame | stage5_execute.py |

**流程说明**：
1. `data/dirty/*.csv` 进入 Stage 0，提取元信息和指纹
2. 查询缓存，命中则跳过 Stage 1-3，未命中继续
3. Stage 1 调用 AI 识别场景（S0-S5）
4. Stage 2 根据场景组装 Prompt
5. Stage 3 调用 AI 识别每个字段类型，输出信号序列（如 "IIGADDNT"）
6. Stage 4 组装操作链
7. Stage 5 执行操作链，产出清洗后数据
8. 输出到 `data/clean/*.csv`

### 链路二：分类变量分析

```
data/categorical/input/*.csv
    │
    ▼
┌──────────────────────────────────────────────┐
│ Stage 0: 提取 DataProfile（字段名、类型、样本）  │ [脚本]
│ 缓存检查：*_type.json 存在且更新？              │ [脚本]
└──────────────────────────────────────────────┘
    │ (未命中)
    ▼
┌──────────────────────────────────────────────┐
│ 第一层 AI：筛选分类变量                         │ [AI]
│ 第二层 AI：判断有序/无序 + 排序                  │ [AI]
│ 写入 *_type.json                               │ [脚本]
└──────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────┐
│ R 脚本：根据变量类型选择统计方法                  │ [R]
│ 有序/有序 → Spearman                           │
│ 无序/无序 → Cramer's V                         │
│ 有序/无序 → Kruskal-Wallis                     │
│ 通用兜底 → 卡方检验                             │
└──────────────────────────────────────────────┘
    │
    ▼
data/categorical/output/report.json + report.xlsx
```

---

## 五阶段详解

### Stage 0: 元信息提取 `[脚本]`

**职责**：从原始 DataFrame 提取元信息，为后续阶段提供数据摘要。

**输入**：
```
DataFrame
```

**处理**：
1. 遍历每一列，提取字段名、数据类型、样本值、缺失率
2. 生成数据指纹（用于缓存命中判断）

**输出**：
```
DataProfile(
    fields: list[FieldProfile]
)

FieldProfile:
    - name: str        # 字段名
    - type: str        # "string" | "int" | "float"
    - samples: list[str]  # 去重样本值，最多20个
    - null_ratio: float   # 缺失率 0.0~1.0
)
```

**关键文件**：`signalchain/stage0_profile.py`

---

### Stage 1: 场景识别 `[AI]`

**职责**：根据数据结构特征，判断数据属于哪种业务场景。

**输入**（来自 Stage 0）：
```
DataProfile
    - field_count: int
    - field_names: list[str]
    - type_summary: str   # 如 "3string,1int"
    - null_ratio_summary: str  # 如 "0.00,0.05"
```

**处理**：
1. 构建场景识别 Prompt
2. 调用 AI，返回场景码

**输出**：
```
SceneCode: str  # "S0" | "S1" | "S2" | "S3" | "S4" | "S5"
```

**AI 调用示例**：
```
输入: "数据概览：
      - 字段数：8
      - 字段名：['id', 'gender', 'age', 'dept', 'drug', 'diagnosis', 'date', 'amount']
      - 类型分布：7string,1float
      - 缺失率：0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00"

输出: "S1"
```

**关键文件**：`signalchain/stage1_scene.py`

---

### Stage 2: 路由与 Prompt 组装 `[脚本]`

**职责**：根据场景码查表获取配置，组装字段语义识别 Prompt。

**输入**：
```
SceneCode: str
DataProfile: DataProfile
```

**处理**：
1. 用 SceneCode 查 `ROUTING_TABLE`，获取 `SceneConfig`
2. 提取每个字段的压缩样本
3. 尝试本地预判字段类型（查 `FIELD_NAME_HINTS` 表）
4. 组装完整 Prompt

**SceneConfig 结构**：
```python
@dataclass
class SceneConfig:
    scene_name: str                 # 场景中文名
    prompt_template: str            # Prompt 模板
    valid_codes: set[str]          # 该场景下合法的信号码
    operations: dict[str, str]      # 信号码 → 操作名映射
```

**输出**：
```
Prompt: str
```

**关键文件**：`signalchain/stage2_router.py`

---

### Stage 3: 字段语义识别 `[AI]`

**职责**：识别每个字段的语义类型，返回信号序列。

**输入**（来自 Stage 2）：
```
Prompt: str
```

**处理**：
1. 调用 AI，返回信号序列
2. 校验输出合法性

**校验规则**：
1. 长度必须等于字段数
2. 每个字符必须属于当前场景的 `valid_codes`
3. 非法字符替换为 "X"

**输出**：
```
FieldSignalSequence: str  # 如 "IIGADDNT"（每位对应一个字段）
```

**AI 调用示例**：
```
输入: "场景：医疗数据
      选项：I=编号 G=性别 A=年龄 D=科室 N=药品名 C=诊断码 T=时间 X=其他
      规则：字段名是判断语义的唯一依据，样本值仅供确认。

      字段:
      1. id(string): P001, P002, P003
      2. gender(string): 男, 女, 男
      3. age(int): 45, 32, 28
      ..."

输出: "IIGADDNT"
```

**关键文件**：`signalchain/stage3_semantic.py`

---

### Stage 4: 执行计划组装 `[脚本]`

**职责**：根据信号序列，从注册表获取操作实例，组装操作链。

**输入**：
```
field_names: list[str]
signal_sequence: str
scene_config: SceneConfig
```

**处理**：
1. 遍历 signal_sequence
2. 用信号码查 `scene_config.operations`，获取操作名
3. 用操作名查 `OPERATION_REGISTRY`，获取 Operation 实例

**操作注册表**：
```python
OPERATION_REGISTRY = {
    "pass_through": PassThrough(),
    "normalize_gender": GenderNormalizer(),
    "extract_age": AgeExtractor(),
    "normalize_department": DepartmentNormalizer(),
    "normalize_drug_name": DrugNameNormalizer(),
    "validate_icd10": ICD10Validator(),
    "parse_datetime": DateTimeParser(),
    "split_currency": CurrencySplitter(),
    "validate_email": EmailValidator(),
    "validate_phone": PhoneValidator(),
    "normalize_log_level": LogLevelNormalizer(),
    "validate_coordinates": CoordinatesValidator(),
}
```

**输出**：
```
operations: list[(字段名, Operation)]
```

**关键文件**：`signalchain/stage4_assemble.py`

---

### Stage 5: 本地执行 `[脚本]`

**职责**：执行操作链，产出清洗后数据。

**输入**：
```
df: DataFrame
operations: list[(字段名, Operation)]
```

**处理**：
1. **全局符号清理**：所有字段去除无意义符号（!?@#$%等，保留@因为邮箱需要）
2. **逐字段执行**：按操作链顺序执行每个操作
3. **分列处理**：如果操作返回 DataFrame（1:N），创建新列

**全局符号清理规则**：
- **保留**：字母、数字、空格、@ . - + _ $ / : , % ，
- **清除**：! ? ~ # ^ & * ( ) [ ] { } < > " ' ` ; = | \ 及全角符号

**分列操作示例**：`split_currency` 将金额列拆分为 `amount_value` 和 `amount_currency`

**输出**：
```
(清洗后 DataFrame, QualityReport)

QualityReport:
    - records: list[FieldReport]
        - col_name: str
        - op_name: str
        - changed: int   # 变化的行数
        - errors: int    # 错误的行数
```

**关键文件**：`signalchain/stage5_execute.py`

---

## 缓存机制

### 缓存结构

```json
{
  "_code_hash": "a1b2c3d4e5f6g7h8",
  "entries": {
    "fingerprint1": {
      "scene_code": "S1",
      "signal_sequence": "IIGADDNT"
    },
    "fingerprint2": {
      "scene_code": "S3",
      "signal_sequence": "IIXGAPE"
    }
  }
}
```

### 指纹生成算法

```
fingerprint = MD5(
    field_names_sorted + ":" +
    MD5(samples1)[:8] + "," +
    MD5(samples2)[:8] + "," + ...
)
```

指纹包含样本值的原因：相同字段名但不同样本值可能需要不同的 AI 决策。

### 缓存失效策略

缓存文件附带 `_code_hash`，每次加载时检查：

```python
def _code_hash() -> str:
    """计算当前代码配置的哈希"""
    h = hashlib.sha256()
    h.update(ROUTING_TABLE)    # 路由表变化
    h.update(SIGNAL_STANDARD_NAMES)  # 标准列名变化
    h.update(OPERATION_REGISTRY)     # 操作注册表变化
    return h.hexdigest()[:16]
```

**任何代码配置变更都会导致全量缓存失效**。

### 缓存命中流程

缓存命中时，跳过 AI 调用阶段：

1. `extract_profile(df)` 生成 fingerprint
2. `cache.get(fingerprint)` 查询缓存
3. **命中**：跳过 Stage 1-3，直接执行 Stage 4-5
4. **未命中**：
   - 调用 Stage 1-3 的 AI
   - `cache.put(fingerprint, result)` 写入缓存
   - 执行 Stage 4-5

**缓存失效**：任何代码配置变更（路由表、标准列名、操作注册表）都会导致全量缓存失效。

---

## 语义知识库

`knowledge.py` 定义了各种字段类型的语义知识，供 Operation 使用。

### 知识库结构

```python
SEMANTIC_KNOWLEDGE = {
    "gender": {
        "male_values": ["男", "M", "Male", "1", "帅哥", ...],
        "female_values": ["女", "F", "Female", "0", ...],
        "unknown_values": ["??", "未知", "保密", ...],
    },
    "department": {
        "mappings": {
            "心内": "心内科",
            "Cardiology": "心内科",
            ...
        }
    },
    "email": {
        "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    },
    "phone": {
        "cn_mobile_pattern": r"^1[3-9]\d{9}$",
        "cn_landline_pattern": r"^0\d{2,3}-?\d{7,8}$",
    },
    ...
}
```

### 知识库的用途

1. **GenderNormalizer**：使用 male/female/unknown_values 映射
2. **DepartmentNormalizer**：使用 department.mappings 归一化
3. **EmailValidator**：使用 email.pattern 验证格式
4. **PhoneValidator**：使用 phone.pattern 验证格式
5. **LogLevelNormalizer**：使用 log_level.mappings 归一化

---

## 操作基类

所有 Operation 必须继承 `base.Operation`，实现以下接口：

```python
from abc import ABC, abstractmethod
import pandas as pd

class Operation(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """操作名称"""
        ...

    @property
    def splits_column(self) -> bool:
        """是否产出一列以上（分列操作）"""
        return False

    @abstractmethod
    def execute(self, data: pd.Series) -> pd.Series | pd.DataFrame:
        """执行操作"""
        ...
```

### 普通操作（1:1）

返回 `pd.Series`，与输入列数相同：

```python
class GenderNormalizer(Operation):
    @property
    def name(self) -> str:
        return "normalize_gender"

    def execute(self, data: pd.Series) -> pd.Series:
        # 转换逻辑
        return normalized_series
```

### 分列操作（1:N）

设置 `splits_column = True`，返回 `pd.DataFrame`：

```python
class CurrencySplitter(Operation):
    @property
    def name(self) -> str:
        return "split_currency"

    @property
    def splits_column(self) -> bool:
        return True

    def execute(self, data: pd.Series) -> pd.DataFrame:
        # 返回多列
        return pd.DataFrame({
            "amount_value": [...],
            "amount_currency": [...]
        })
```

---

## 定制开发指南

### 场景一：新增字段类型

**目标**：添加对"地址"字段的支持。

**步骤 1**：在 `knowledge.py` 添加地址知识

```python
SEMANTIC_KNOWLEDGE["address"] = {
    "patterns": [
        r"\d+号.*路.*号",
        r".*省.*市.*区",
    ],
    "standard_format": "省市区街道"
}
```

**步骤 2**：在 `models.py` 添加信号码

```python
CODE_LABELS["B"] = "地址"  # B 可以是任意未使用的字母

VALID_FIELD_CODES.add("B")
```

**步骤 3**：创建 AddressNormalizer 操作

```python
# signalchain/operations/address.py
class AddressNormalizer(Operation):
    @property
    def name(self) -> str:
        return "normalize_address"

    def execute(self, data: pd.Series) -> pd.Series:
        # 地址标准化逻辑
        return normalized_series
```

**步骤 4**：在 `registry.py` 注册

```python
from signalchain.operations.address import AddressNormalizer

OPERATION_REGISTRY["normalize_address"] = AddressNormalizer()
```

**步骤 5**：在 `stage2_router.py` 更新路由表

```python
# 在 S1 医疗场景中添加 B（地址）
"S1": SceneConfig(
    valid_codes={"G", "A", "D", "N", "C", "T", "B", "I", "X"},
    operations={..., "B": "normalize_address", ...}
)
```

**步骤 6**：添加字段名预判（可选）

```python
# stage2_router.py
FIELD_NAME_HINTS["address"] = "B"
FIELD_NAME_HINTS["住址"] = "B"
```

---

### 场景二：修改现有知识库

**目标**：添加新的性别表达方式。

**文件**：`signalchain/knowledge.py`

```python
"gender": {
    "male_values": [..., "雄", "公"],
    "female_values": [..., "雌", "母"],
    ...
}
```

---

### 场景三：新增场景

**目标**：为电商数据创建专属场景 S6。

**步骤 1**：在 `models.py` 添加场景码

```python
VALID_SCENE_CODES = {"S0", "S1", "S2", "S3", "S4", "S5", "S6"}
```

**步骤 2**：在 `stage2_router.py` 添加场景配置

```python
SCENE_REFERENCES["S6"] = "I=订单号, M=金额, T=日期, G=买家性别, ..."

"S6": SceneConfig(
    scene_name="电商数据",
    prompt_template=FIELD_SEMANTIC_TEMPLATE,
    valid_codes={"I", "M", "T", "G", "X"},
    operations={
        "I": "pass_through",
        "M": "split_currency",
        "T": "parse_datetime",
        "G": "normalize_gender",
        "X": "pass_through",
    },
)
```

---

### 场景四：自定义AI客户端

**目标**：接入其他 AI 服务（如本地模型）。

**文件**：`signalchain/ai_client.py`

```python
class LocalModelClient:
    """本地模型客户端"""

    def __init__(self, model_path: str):
        # 加载本地模型
        ...

    def call(self, prompt: str) -> str:
        # 调用本地模型
        return result
```

**使用**：

```python
pipeline = SignalChainPipeline(
    ai_client=LocalModelClient(model_path="/path/to/model")
)
```

---

## 数据规范

### 输入规范

- **格式**：CSV 文件（UTF-8 编码）
- **首行**：必须为列名
- **字段**：建议不超过 50 列
- **行数**：无限制，但样本值最多采集 20 个

### 输出规范

- **格式**：CSV 文件（UTF-8-sig 编码，带 BOM）
- **列名**：统一为标准名称（id, gender, age 等）
- **空值**：保存为空字符串（不是 NaN）
- **浮点数**：避免 float64 推断，强制转为 object 类型

### 信号序列规范

- **长度**：必须等于字段数量
- **字符集**：每字符必须属于当前场景的 valid_codes
- **顺序**：与字段顺序一一对应

---

## 调试与日志

### 日志级别

```python
logging.getLogger("signalchain").setLevel(logging.DEBUG)  # 查看详细日志
logging.getLogger("signalchain").setLevel(logging.ERROR) # 只看错误
```

### 关键日志位置

- Stage 0：`fingerprint={...}`
- Stage 1：`scene_code={...}`
- Stage 2：`prompt assembled for '{scene_name}'`
- Stage 3：`signal_sequence={...}`
- Stage 4/5：`Renaming columns: {...}`

### 质量报告

```python
clean, report = pipeline.run(df)
print(report.summary())

# 输出示例：
# ============================================================
# SignalChain 质量报告
# ============================================================
#   [OK] id               | pass_through          | changed=0 errors=0
#   [OK] gender           | normalize_gender       | changed=5 errors=0
#   [ERR] email           | validate_email        | changed=0 errors=2
#   --------------------------------------------------------
#   总计: changed=12, errors=2
# ============================================================
```

---

## 性能考量

### Token 消耗

**数据清洗**：

| 阶段 | 输入 Token | 输出 Token |
|------|----------|----------|
| Stage 1 场景识别 | ~30 | 1 |
| Stage 3 字段识别 | ~60 + N*3 | N |

**分类变量分析**：

| 阶段 | 输入 Token | 输出 Token |
| --- | --- | --- |
| 第一层 筛选分类变量 | ~80 | ~5 |
| 第二层 有序判断 | ~100 | ~10 |

- N = 字段数量
- 缓存命中时 = 0 Token

### 执行速度

| 操作 | 耗时 |
|------|------|
| Stage 0 | < 1ms |
| Stage 1 (AI) | 500ms-2s |
| Stage 2 | < 1ms |
| Stage 3 (AI) | 500ms-2s |
| Stage 4 | < 1ms |
| Stage 5 | 1-10ms/字段 |
| 分类 AI (2次) | 1-3s |
| R 统计分析 | 1-5s |

### 缓存收益

首次处理后，相同数据结构的文件可在 10ms 内完成（纯本地执行）。分类变量分析同理，`*_type.json` 存在时跳过 AI 调用。
