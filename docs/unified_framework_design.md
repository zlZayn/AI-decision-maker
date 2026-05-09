# SignalChain 统一框架设计文档（初步计划，已过期）

> AI 信号链决策框架：AI 只传递极简信号码，本地负责查表执行
> 核心目标：单次运行总 Token < 100，缓存命中时零 Token

---

## 目录

1. [设计哲学](#设计哲学)
2. [数据结构契约](#数据结构契约)
3. [系统架构与逻辑链](#系统架构与逻辑链)
4. [信号码系统规范](#信号码系统规范)
5. [Stage 0: 本地元信息提取](#stage-0-本地元信息提取)
6. [Stage 1: AI 场景识别节点](#stage-1-ai-场景识别节点)
7. [Stage 2: 本地信号路由与 Prompt 组装](#stage-2-本地信号路由与-prompt-组装)
8. [Stage 3: AI 字段语义识别节点](#stage-3-ai-字段语义识别节点)
9. [Stage 4: 执行计划组装与校验](#stage-4-执行计划组装与校验)
10. [Stage 5: 本地执行引擎](#stage-5-本地执行引擎)
11. [数据指纹与缓存](#数据指纹与缓存)
12. [语义知识库](#语义知识库)
13. [安全与回退机制](#安全与回退机制)
14. [完整数据流示例](#完整数据流示例)
15. [性能基准与验证清单](#性能基准与验证清单)
16. [实现路线](#实现路线)

---

## 设计哲学

### 三条铁律

1. **AI 只做语义判断，不做模式匹配**
   - 脚本擅长：正则、映射表、数值范围检查
   - AI 擅长：理解字段含义、推断数据意图、处理模糊语义
   - 分界线：凡是能用确定性算法解决的，绝不交给 AI

2. **AI 输出信号码，不输出执行代码**
   - AI 输出：单字符信号码（如 `S1`, `G`, `A`）
   - 本地执行：查表调用预置的纯函数
   - 好处：AI 输出极短且可校验，本地函数经过测试

3. **所有本地操作必须是纯函数**
   - 无副作用、幂等、输入输出可预测
   - 便于测试、缓存、回滚

### 为什么脚本不能替代 AI

| 场景 | 脚本方案 | AI 方案 | 为什么脚本不行 |
|------|---------|---------|---------------|
| 字段名是 "sex" | 正则匹配 /sex/i | 语义理解 "sex" = "gender" | 脚本只能匹配模式，不能理解同义词 |
| 样本值有 "帅哥" | 需要庞大的映射表 | 理解 "帅哥" 属于男性 | 新词汇不断出现，映射表无法覆盖 |
| 字段名是 "dept" | 无法判断 department 还是 deposit | 根据样本值推断是科室 | 缩写歧义，脚本无法上下文推断 |
| 年龄值 "约30" | 需要复杂的正则规则 | 理解 "约" 是修饰词，提取 30 | 自然语言的模糊表达 |
| 药品 "阿莫西林0.25g" | 需要药品知识库 | 理解这是 "药品名+剂量" | 需要领域知识 |

---

## 数据结构契约

整个框架各阶段之间传递的数据必须遵循统一的结构定义。以下是核心数据结构：

### DataProfile（Stage 0 输出，贯穿全流程）

这是 Stage 0 生成的唯一数据结构，同时服务于指纹生成、AI 场景识别、AI 字段语义识别三个用途。

```python
@dataclass
class FieldProfile:
    name: str            # 字段名，如 "gender"
    type: str            # 数据类型，如 "string", "int", "float"
    samples: list[str]   # 去重后的样本值，最多 20 个
    null_ratio: float    # 缺失率，0.0 ~ 1.0

@dataclass
class DataProfile:
    fields: list[FieldProfile]  # 所有字段的画像
```

**派生属性（从 fields 计算，不单独存储）：**

```python
@property
def field_count(self) -> int:
    return len(self.fields)

@property
def field_names(self) -> list[str]:
    return [f.name for f in self.fields]

@property
def type_summary(self) -> str:
    # 如 "3string,1int,1float"
    counter = Counter(f.type for f in self.fields)
    return ",".join(f"{count}{dtype}" for dtype, count in counter.items())

@property
def null_ratio_summary(self) -> str:
    # 如 "0.00,0.05,0.02,0.00,0.10"
    return ",".join(f"{f.null_ratio:.2f}" for f in self.fields)
```

### SceneCode（Stage 1 输出）

```python
SceneCode = str  # 单个字符串，取值范围 {"S0", "S1", "S2", "S3", "S4", "S5"}
```

### SceneConfig（Stage 2 输出，从路由表查表得到）

```python
@dataclass
class SceneConfig:
    scene_name: str               # 场景中文名，如 "医疗数据"
    prompt_template: str          # 字段语义识别的 Prompt 模板
    valid_codes: set[str]         # 该场景下合法的字段信号码集合
    operations: dict[str, str]    # 信号码 → 操作名映射，如 {"G": "normalize_gender"}
```

### FieldSignalSequence（Stage 3 输出）

```python
FieldSignalSequence = str  # 字符串，长度等于字段数，每个字符 ∈ VALID_FIELD_CODES
                           # 如 "IGADN"
```

### CacheEntry（缓存存储的内容）

```python
@dataclass
class CacheEntry:
    scene_code: str              # 场景信号码，如 "S1"
    signal_sequence: str         # 字段信号序列，如 "IGADN"
```

**关键设计**：缓存必须同时存储 `scene_code` 和 `signal_sequence`。因为信号码的含义依赖于场景——同一个 `G` 在 S1（医疗）中映射到 `normalize_gender`，但在 S2（财务）中不存在。只存信号序列会导致缓存命中时无法确定正确的操作表。

---

## 系统架构与逻辑链

```
输入数据 (CSV/JSON/Excel)
    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 0: 本地元信息提取                                            │
│  输入：原始 DataFrame                                               │
│  输出：DataProfile（含 fields 数组，每字段有 name/type/samples/     │
│        null_ratio）                                                 │
│  副产物：fingerprint（从 DataProfile 计算，用于缓存查找）            │
│  Token：0（纯本地）                                                 │
└─────────────────────────────────────────────────────────────────────┘
    ↓ DataProfile + fingerprint
┌─────────────────────────────────────────────────────────────────────┐
│  缓存查找                                                           │
│  输入：fingerprint                                                  │
│  命中 → 取出 CacheEntry(scene_code, signal_sequence)               │
│        → 跳转 Stage 4                                              │
│  未命中 → 继续 Stage 1                                             │
│  Token：0（纯本地）                                                 │
└─────────────────────────────────────────────────────────────────────┘
    ↓ 缓存未命中，DataProfile
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 1: AI 场景识别节点                                           │
│  输入：从 DataProfile 派生的摘要（field_count, field_names,         │
│        type_summary, null_ratio_summary）                           │
│  输出：SceneCode（1 个字符串，如 "S1"）                             │
│  校验：输出必须 ∈ VALID_SCENE_CODES，否则回退到 "S0"               │
│  约束：Prompt 强制只输出 1 个代码，temperature=0                    │
│  Token：~30 输入 + 1 输出                                          │
└─────────────────────────────────────────────────────────────────────┘
    ↓ SceneCode
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 2: 本地信号路由与 Prompt 组装                                │
│  输入：SceneCode + DataProfile                                      │
│  动作：                                                             │
│    1. 用 SceneCode 查 ROUTING_TABLE → 得到 SceneConfig              │
│    2. 从 DataProfile 提取每字段样本，调用 compress_samples 压缩     │
│    3. 用 SceneConfig.prompt_template + 压缩样本 → 组装完整 Prompt   │
│  输出：组装好的 Prompt 字符串                                       │
│  Token：0（纯本地）                                                 │
└─────────────────────────────────────────────────────────────────────┘
    ↓ Prompt 字符串
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 3: AI 字段语义识别节点                                       │
│  输入：Stage 2 组装的 Prompt                                        │
│  输出：FieldSignalSequence（字符串，长度 = field_count，            │
│        每字符 ∈ VALID_FIELD_CODES）                                 │
│  校验：                                                             │
│    1. 长度必须 == field_count                                       │
│    2. 每字符必须 ∈ 当前场景的 valid_codes（不是全局白名单）         │
│  校验失败 → 回退为 "X" * field_count                               │
│  约束：Prompt 强制只输出字符序列，无空格，temperature=0             │
│  Token：~60 输入 + field_count 输出                                │
└─────────────────────────────────────────────────────────────────────┘
    ↓ FieldSignalSequence
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 4: 执行计划组装与校验                                        │
│  输入：SceneCode + FieldSignalSequence + SceneConfig                │
│  动作：                                                             │
│    1. 将 (fingerprint, CacheEntry(scene_code, signal_sequence))     │
│       写入缓存                                                      │
│    2. 遍历 signal_sequence，用 SceneConfig.operations 查表：        │
│       信号码 → 操作名 → OPERATION_REGISTRY 中的 Operation 实例     │
│    3. 组装成 pandas 操作链                                          │
│  输出：可执行的操作链                                               │
│  Token：0（纯本地）                                                 │
└─────────────────────────────────────────────────────────────────────┘
    ↓ 操作链
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 5: 本地执行引擎                                              │
│  输入：DataFrame + 操作链                                           │
│  动作：逐字段执行操作，异常时保留原值                               │
│  输出：清洗后的 DataFrame + 质量报告                                │
│  Token：0（纯本地）                                                 │
└─────────────────────────────────────────────────────────────────────┘
    ↓
输出数据 + 质量报告
```

### 各阶段接口汇总

| 阶段 | 输入 | 输出 | Token |
|------|------|------|-------|
| Stage 0 | DataFrame | DataProfile + fingerprint | 0 |
| 缓存查找 | fingerprint | CacheEntry 或 None | 0 |
| Stage 1 | DataProfile 摘要 | SceneCode | ~31 |
| Stage 2 | SceneCode + DataProfile | Prompt 字符串 | 0 |
| Stage 3 | Prompt 字符串 | FieldSignalSequence | ~65 |
| Stage 4 | SceneCode + FieldSignalSequence + SceneConfig | 操作链 | 0 |
| Stage 5 | DataFrame + 操作链 | 清洗后 DataFrame | 0 |

---

## 信号码系统规范

### 设计原则

1. **单字符**：只使用 `[A-Z0-9]`，共 36 种可能
2. **语义无关**：AI 不知道信号码含义，只知道"选哪个"
3. **本地映射**：含义由本地路由表定义，可动态更新
4. **版本控制**：信号码表版本化，确保兼容性

### 场景信号码 (Stage 1 输出)

| 信号码 | 场景名称 | 说明 |
|--------|---------|------|
| S0 | 未知场景 | 无法匹配时的默认回退 |
| S1 | 医疗数据 | 患者、诊断、药品、科室 |
| S2 | 财务数据 | 金额、成本、货币、交易 |
| S3 | 用户数据 | 用户、姓名、邮箱、手机 |
| S4 | 日志数据 | 时间戳、日志级别、消息 |
| S5 | 地理数据 | 经纬度、地址、城市 |

### 字段语义信号码 (Stage 3 输出)

| 信号码 | 语义含义 | 适用场景 | 对应操作 | 知识库键 |
|--------|---------|---------|---------|---------|
| G | 性别 | S1, S3 | normalize_gender | gender |
| A | 年龄 | S1, S3 | extract_age | - |
| D | 科室/部门 | S1 | normalize_department | department |
| N | 药品名 | S1 | normalize_drug_name | drug_name |
| C | 诊断码 | S1 | validate_icd10 | icd10 |
| T | 时间/日期 | S1, S2, S4 | parse_datetime | - |
| M | 金额 | S2 | normalize_currency | - |
| E | 邮箱 | S3 | validate_email | - |
| P | 手机号 | S3 | validate_phone | - |
| L | 日志级别 | S4 | normalize_log_level | - |
| R | 经纬度 | S5 | validate_coordinates | - |
| I | ID/编号 | 全部 | pass_through | - |
| X | 未知/其他 | 全部 | pass_through | - |

**信号码与场景的关系**：同一个信号码在不同场景中含义相同（如 `G` 永远是性别），但并非所有场景都支持所有信号码。每个场景的 `valid_codes` 定义了该场景下 AI 被允许输出的信号码子集。

---

## Stage 0: 本地元信息提取

### 输入

原始 pandas DataFrame。

### 输出

`DataProfile` 对象（定义见 [数据结构契约](#数据结构契约)）。

### 实现逻辑

```python
def extract_profile(df: pd.DataFrame, max_samples: int = 20) -> DataProfile:
    """从 DataFrame 提取 DataProfile，零 Token"""
    fields = []
    for col in df.columns:
        # 采样：去重，保留最多 max_samples 个
        unique_values = df[col].dropna().unique()
        samples = [str(v) for v in unique_values[:max_samples]]

        # 类型推断
        dtype = df[col].dtype
        if dtype in ("int64", "int32"):
            type_name = "int"
        elif dtype in ("float64", "float32"):
            type_name = "float"
        else:
            type_name = "string"

        # 缺失率
        null_ratio = df[col].isna().mean()

        fields.append(FieldProfile(
            name=col,
            type=type_name,
            samples=samples,
            null_ratio=round(null_ratio, 4)
        ))

    return DataProfile(fields=fields)
```

### 指纹计算

从 `DataProfile` 直接计算，不依赖额外数据：

```python
def generate_fingerprint(profile: DataProfile) -> str:
    """从 DataProfile 生成指纹，用于缓存命中判断"""
    field_names = ",".join(sorted(f.name for f in profile.fields))
    sample_hashes = ",".join(
        hashlib.md5(",".join(sorted(f.samples)).encode()).hexdigest()[:8]
        for f in profile.fields
    )
    return hashlib.md5(f"{field_names}:{sample_hashes}".encode()).hexdigest()
```

**指纹包含样本值的原因**：相同字段名但不同样本值（如性别字段一个含 "帅哥"，一个不含）可能需要不同的 AI 决策。仅用字段名做指纹会误命中。

---

## Stage 1: AI 场景识别节点

### 输入

从 `DataProfile` 派生的摘要信息。不传完整 DataProfile，控制 Token。

```python
def build_scene_prompt(profile: DataProfile) -> str:
    """从 DataProfile 构建场景识别 Prompt"""
    return f"""数据概览：
- 字段数：{profile.field_count}
- 字段名：{profile.field_names}
- 类型分布：{profile.type_summary}
- 缺失率：{profile.null_ratio_summary}

从以下选项中选择最匹配的场景代码：
S1=医疗数据 S2=财务数据 S3=用户数据 S4=日志数据 S5=地理数据 S0=未知

要求：
1. 只输出 1 个代码（如 S1）
2. 不要输出任何解释文字
3. 不要输出标点符号

输出："""
```

### 输出校验

```python
VALID_SCENE_CODES = {"S0", "S1", "S2", "S3", "S4", "S5"}

def validate_scene_code(raw_output: str) -> str:
    """校验 AI 输出的场景信号码"""
    # 提取：去除空白，取前 2 个字符（S0-S5 都是 2 字符）
    cleaned = raw_output.strip()[:2]
    if cleaned in VALID_SCENE_CODES:
        return cleaned
    return "S0"  # 回退到未知场景
```

### Token 估算

- 输入：~30 tokens
- 输出：1 token（`S1` 是 1 个 token）
- 合计：~31 tokens

---

## Stage 2: 本地信号路由与 Prompt 组装

### 输入

- `SceneCode`：Stage 1 的输出
- `DataProfile`：Stage 0 的输出

### 动作

1. 用 `SceneCode` 查 `ROUTING_TABLE` 得到 `SceneConfig`
2. 从 `DataProfile` 提取每字段样本，调用 `compress_samples` 压缩
3. 用 `SceneConfig.prompt_template` + 压缩样本组装完整 Prompt

### 路由表

```python
ROUTING_TABLE: dict[str, SceneConfig] = {
    "S0": SceneConfig(
        scene_name="未知数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S0,
        valid_codes={"I", "X"},
        operations={"I": "pass_through", "X": "pass_through"}
    ),
    "S1": SceneConfig(
        scene_name="医疗数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S1,
        valid_codes={"G", "A", "D", "N", "C", "T", "I", "X"},
        operations={
            "G": "normalize_gender", "A": "extract_age",
            "D": "normalize_department", "N": "normalize_drug_name",
            "C": "validate_icd10", "T": "parse_datetime",
            "I": "pass_through", "X": "pass_through"
        }
    ),
    "S2": SceneConfig(
        scene_name="财务数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S2,
        valid_codes={"M", "T", "I", "X"},
        operations={
            "M": "normalize_currency", "T": "parse_datetime",
            "I": "pass_through", "X": "pass_through"
        }
    ),
    "S3": SceneConfig(
        scene_name="用户数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S3,
        valid_codes={"G", "A", "E", "P", "I", "X"},
        operations={
            "G": "normalize_gender", "A": "extract_age",
            "E": "validate_email", "P": "validate_phone",
            "I": "pass_through", "X": "pass_through"
        }
    ),
    "S4": SceneConfig(
        scene_name="日志数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S4,
        valid_codes={"T", "L", "I", "X"},
        operations={
            "T": "parse_datetime", "L": "normalize_log_level",
            "I": "pass_through", "X": "pass_through"
        }
    ),
    "S5": SceneConfig(
        scene_name="地理数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S5,
        valid_codes={"R", "I", "X"},
        operations={
            "R": "validate_coordinates",
            "I": "pass_through", "X": "pass_through"
        }
    ),
}
```

### Prompt 模板示例（S1 医疗场景）

```python
FIELD_SEMANTIC_TEMPLATE_S1 = """场景：{scene_name}

字段样本（每字段最多 5 个值）：

{field_blocks}

要求：
1. 为每个字段选择 1 个代码
2. 按字段顺序输出，不要有空格（如 IGADN）
3. 不要输出任何解释

输出："""
```

### Prompt 组装逻辑

```python
def compress_samples(values: list[str], max_count: int = 5) -> list[str]:
    """压缩样本值，保留信息密度最高的样本"""
    unique = list(set(values))

    def info_score(v: str) -> int:
        score = 0
        if any(c in v for c in "?!@#$%"): score += 3
        if len(v) > 20 or len(v) < 2: score += 2
        if any(c.isdigit() for c in v) and any(c.isalpha() for c in v): score += 2
        return score

    unique.sort(key=info_score, reverse=True)
    return unique[:max_count]


def build_field_semantic_prompt(profile: DataProfile, scene_config: SceneConfig) -> str:
    """组装字段语义识别的完整 Prompt"""
    code_options = _format_code_options(scene_config.valid_codes)

    field_blocks = []
    for i, field in enumerate(profile.fields, 1):
        compressed = compress_samples(field.samples, max_count=5)
        options_str = " ".join(f"{code}={label}" for code, label in code_options)
        field_blocks.append(
            f"字段{i}: {field.name}\n"
            f"样本: {compressed}\n"
            f"选项: {options_str}"
        )

    return scene_config.prompt_template.format(
        scene_name=scene_config.scene_name,
        field_blocks="\n\n".join(field_blocks)
    )


def _format_code_options(valid_codes: set[str]) -> list[tuple[str, str]]:
    """将 valid_codes 转换为 AI 可读的选项列表"""
    CODE_LABELS = {
        "G": "性别", "A": "年龄", "D": "科室", "N": "药品名",
        "C": "诊断码", "T": "时间", "M": "金额", "E": "邮箱",
        "P": "手机号", "L": "日志级别", "R": "经纬度",
        "I": "编号", "X": "其他"
    }
    # 固定顺序，X 始终放在最后
    ordered = sorted(valid_codes - {"X"}) + (["X"] if "X" in valid_codes else [])
    return [(code, CODE_LABELS[code]) for code in ordered]
```

### Token 估算

Stage 2 是纯本地操作，Token = 0。但组装出的 Prompt 会被 Stage 3 的 AI 调用消耗 Token。

---

## Stage 3: AI 字段语义识别节点

### 输入

Stage 2 组装的 Prompt 字符串。

### 输出校验

```python
def validate_field_signal_sequence(
    raw_output: str,
    field_count: int,
    valid_codes: set[str]
) -> str:
    """
    校验 AI 输出的字段信号序列。

    校验规则：
    1. 去除空白后长度必须 == field_count
    2. 每个字符必须 ∈ 当前场景的 valid_codes（不是全局白名单）
    """
    cleaned = raw_output.strip().replace(" ", "")

    # 规则 1：长度校验
    if len(cleaned) != field_count:
        return "X" * field_count

    # 规则 2：白名单校验（使用场景级别的 valid_codes）
    for ch in cleaned:
        if ch not in valid_codes:
            return "X" * field_count

    return cleaned
```

**关键设计**：校验使用 `scene_config.valid_codes`（场景白名单），而非全局 `VALID_FIELD_CODES`。原因：如果 AI 在财务场景（S2）下输出了 `G`（性别），虽然 `G` 是合法的字段信号码，但在财务场景下没有意义，应该被拒绝。

### Token 估算

- 输入：~60 tokens（Prompt）
- 输出：field_count tokens（每个信号码 1 个 token）
- 合计：~60 + field_count tokens

---

## Stage 4: 执行计划组装与校验

### 输入

- `SceneCode`：Stage 1 输出
- `FieldSignalSequence`：Stage 3 输出
- `SceneConfig`：Stage 2 中从路由表查到的配置

### 动作

1. **缓存写入**：将 `(fingerprint, CacheEntry(scene_code, signal_sequence))` 写入缓存
2. **操作链组装**：遍历信号序列，查表得到操作名，再从 `OPERATION_REGISTRY` 获取操作实例

```python
def assemble_operations(
    signal_sequence: str,
    scene_config: SceneConfig,
    registry: dict[str, Operation]
) -> list[tuple[str, Operation]]:
    """
    组装操作链。

    返回：[(字段名, 操作实例), ...]
    """
    ops = []
    for col_name, code in zip(df.columns, signal_sequence):
        op_name = scene_config.operations.get(code, "pass_through")
        op = registry.get(op_name, registry["pass_through"])
        ops.append((col_name, op))
    return ops
```

### 校验

操作链组装本身不需要额外校验，因为：
- 信号码已在 Stage 3 校验过，保证 ∈ `valid_codes`
- `operations` 映射表覆盖了 `valid_codes` 中的每个码
- `OPERATION_REGISTRY` 包含了所有可能出现的操作名

但如果出现映射缺失（代码 bug），应 fallback 到 `pass_through`：

```python
op_name = scene_config.operations.get(code)  # 不给默认值
if op_name is None or op_name not in registry:
    logger.error(f"Missing operation for code {code} in scene {scene_code}, fallback to pass_through")
    op = registry["pass_through"]
else:
    op = registry[op_name]
```

---

## Stage 5: 本地执行引擎

### 输入

- 原始 DataFrame
- Stage 4 组装的操作链

### 执行逻辑

```python
def execute_pipeline(
    df: pd.DataFrame,
    operations: list[tuple[str, Operation]]
) -> tuple[pd.DataFrame, QualityReport]:
    """
    执行操作链，返回清洗后的 DataFrame 和质量报告。
    """
    report = QualityReport()
    result = df.copy()

    for col_name, op in operations:
        try:
            original = result[col_name]
            result[col_name] = op.execute(original)
            changed = (original != result[col_name]).sum()
            report.record(col_name, op.name, changed=changed, errors=0)
        except Exception as e:
            logger.warning(f"Field '{col_name}' op '{op.name}' failed: {e}, keeping original")
            report.record(col_name, op.name, changed=0, errors=1)

    return result, report
```

### 操作基类

```python
from abc import ABC, abstractmethod

class Operation(ABC):
    """所有本地操作的基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """操作名称，用于日志和报告"""
        pass

    @abstractmethod
    def execute(self, data: pd.Series) -> pd.Series:
        """
        执行操作。纯函数，不修改输入。

        Args:
            data: 输入列
        Returns:
            处理后的列
        Raises:
            ValueError: 输入数据不合法时抛出，由执行引擎捕获
        """
        pass
```

**注意**：信号码架构中 AI 不输出参数，因此 Operation 基类没有 `validate_params` 方法。如果未来需要参数化操作（如指定日期格式），应通过 `SEMANTIC_KNOWLEDGE` 或场景配置提供默认参数，而非让 AI 输出参数。

---

## 数据指纹与缓存

### 缓存结构

```python
class SignalCache:
    """
    缓存结构：fingerprint → CacheEntry(scene_code, signal_sequence)

    为什么必须存 scene_code：
    同一个信号码在不同场景下映射到不同操作。
    如 "T" 在 S1(医疗) 中是 parse_datetime，在 S4(日志) 中也是 parse_datetime，
    但 S1 的 operations 包含 {G,A,D,N,C,T,I,X}，S4 只包含 {T,L,I,X}。
    如果只存 signal_sequence，缓存命中时不知道用哪张操作表。
    """
    def __init__(self, cache_file: str = "signal_cache.json"):
        self.cache_file = cache_file
        self.cache: dict[str, CacheEntry] = self._load()

    def get(self, fingerprint: str) -> CacheEntry | None:
        """命中返回 CacheEntry，未命中返回 None"""
        entry = self.cache.get(fingerprint)
        if entry is None:
            return None
        return CacheEntry(**entry)  # 从 dict 反序列化

    def put(self, fingerprint: str, entry: CacheEntry):
        """写入缓存"""
        self.cache[fingerprint] = {"scene_code": entry.scene_code, "signal_sequence": entry.signal_sequence}
        self._save()

    def _load(self) -> dict:
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self):
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, indent=2, ensure_ascii=False)
```

### 缓存版本兼容

当信号码表或路由表发生变更时（如新增信号码、修改映射），旧缓存可能失效。通过版本号管理：

```python
CACHE_VERSION = "1.0"

class SignalCache:
    def _load(self) -> dict:
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("_version") != CACHE_VERSION:
                return {}  # 版本不匹配，清空缓存
            return data.get("entries", {})
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self):
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump({"_version": CACHE_VERSION, "entries": self.cache}, f, indent=2, ensure_ascii=False)
```

---

## 语义知识库

### 定义

语义知识库是本地预置的"世界知识"。AI 只输出信号码（如 `G`），操作执行时从知识库获取具体映射规则。

```python
SEMANTIC_KNOWLEDGE = {
    "gender": {
        "male_values": ["男", "M", "Male", "男生", "男士", "1", "帅哥"],
        "female_values": ["女", "F", "Female", "女生", "女士", "0"],
        "unknown_values": ["??", "未知", "不详", "保密"]
    },
    "department": {
        "mappings": {
            "心内": "心内科",
            "心内一区": "心内科",
            "Cardiology": "心内科",
            "外科": "外科",
            "妇科": "妇科"
        }
    },
    "drug_name": {
        "pattern": r"^[一-龥a-zA-Z]+",
        "dose_pattern": r"(\d+\.?\d*)\s*(mg|g|ml|片|粒)"
    },
    "icd10": {
        "pattern": r"^[A-Z]\d{2}(\.\d+)?$"
    }
}
```

### 知识库与操作的绑定

每个 Operation 通过 `knowledge_key` 关联到知识库中的条目。绑定关系在操作注册时确定：

```python
class GenderNormalizer(Operation):
    name = "normalize_gender"
    knowledge_key = "gender"  # 关联到 SEMANTIC_KNOWLEDGE["gender"]

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        mapping = {}
        for v in knowledge["male_values"]:
            mapping[v] = "男"
        for v in knowledge["female_values"]:
            mapping[v] = "女"
        for v in knowledge["unknown_values"]:
            mapping[v] = None
        return data.map(lambda x: mapping.get(str(x).strip(), str(x).strip()))
```

### 信号码 → 操作名 → 知识库 的完整链路

```
AI 输出信号码 "G"
    ↓ 查 SceneConfig.operations
操作名 "normalize_gender"
    ↓ 查 OPERATION_REGISTRY
GenderNormalizer 实例
    ↓ 读 knowledge_key
"gender"
    ↓ 查 SEMANTIC_KNOWLEDGE
{"male_values": [...], "female_values": [...], ...}
    ↓ 执行映射
"M" → "男", "F" → "女"
```

每一环都有明确的输入输出契约，不存在隐式依赖。

---

## 安全与回退机制

### 三层约束

```
Layer 1: Prompt 约束
  - AI 只能看到压缩后的样本值，看不到完整数据
  - 敏感字段（身份证、手机号）自动脱敏后再采样
  - Prompt 强制输出格式（单代码 / 字符序列）
  - temperature=0 确保可复现

Layer 2: 输出校验
  - Stage 1：SceneCode 必须 ∈ VALID_SCENE_CODES
  - Stage 3：FieldSignalSequence 长度必须 == field_count
  - Stage 3：每个字符必须 ∈ 当前场景的 valid_codes（不是全局白名单）
  - 校验失败 → 回退到安全默认值

Layer 3: 执行沙箱
  - 操作名必须在 OPERATION_REGISTRY 中
  - 所有操作都是纯函数，无副作用
  - 单字段异常不影响其他字段
  - 异常被捕获，该字段保留原值
```

### 三层回退

```
Level 1: AI 输出格式异常（如输出了 "医疗数据" 而非 "S1"）
  → validate_scene_code / validate_field_signal_sequence 返回默认值
  → SceneCode 回退到 "S0"
  → FieldSignalSequence 回退到 "X" * field_count
  → 所有字段走 pass_through，数据原样保留

Level 2: AI 输出不在白名单（如 S2 场景输出了 "G"）
  → validate_field_signal_sequence 检测到非法字符
  → 回退到 "X" * field_count
  → 记录 WARNING 日志

Level 3: 本地执行异常（如 normalize_gender 遇到非预期值）
  → execute_pipeline 捕获 Exception
  → 该字段保留原值，其他字段继续处理
  → 记录 WARNING 日志，写入 QualityReport
```

### Pipeline 主流程（含所有回退）

```python
class SignalChainPipeline:
    def __init__(self):
        self.cache = SignalCache()
        self.registry = OPERATION_REGISTRY
        self.routing = ROUTING_TABLE

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, QualityReport]:
        # Stage 0: 元信息提取
        profile = extract_profile(df)
        fingerprint = generate_fingerprint(profile)

        # 缓存查找
        cached = self.cache.get(fingerprint)
        if cached is not None:
            logger.info(f"Cache hit: {fingerprint}")
            scene_config = self.routing.get(cached.scene_code, self.routing["S0"])
            ops = assemble_operations(cached.signal_sequence, scene_config, self.registry)
            return execute_pipeline(df, ops)

        # Stage 1: 场景识别
        scene_prompt = build_scene_prompt(profile)
        raw_scene = call_ai(scene_prompt)  # 返回字符串
        scene_code = validate_scene_code(raw_scene)

        # Stage 2: 路由 + Prompt 组装
        scene_config = self.routing.get(scene_code, self.routing["S0"])
        field_prompt = build_field_semantic_prompt(profile, scene_config)

        # Stage 3: 字段语义识别
        raw_signals = call_ai(field_prompt)  # 返回字符串
        signal_sequence = validate_field_signal_sequence(
            raw_signals,
            field_count=profile.field_count,
            valid_codes=scene_config.valid_codes
        )

        # Stage 4: 缓存写入 + 操作链组装
        self.cache.put(fingerprint, CacheEntry(scene_code, signal_sequence))
        ops = assemble_operations(signal_sequence, scene_config, self.registry)

        # Stage 5: 执行
        return execute_pipeline(df, ops)
```

---

## 完整数据流示例

### 输入数据

```csv
patient_id,gender,age,dept_name,drug_name
P001,M,30,心内,阿莫西林
P002,F,30岁,外科,甲硝唑
P003,Male,约30,妇科,未知药品
```

### Stage 0: 本地元信息提取

```python
profile = extract_profile(df)
# DataProfile(
#   fields=[
#     FieldProfile(name="patient_id", type="string", samples=["P001","P002","P003"], null_ratio=0.0),
#     FieldProfile(name="gender",     type="string", samples=["M","F","Male"],        null_ratio=0.0),
#     FieldProfile(name="age",        type="string", samples=["30","30岁","约30"],     null_ratio=0.0),
#     FieldProfile(name="dept_name",  type="string", samples=["心内","外科","妇科"],    null_ratio=0.0),
#     FieldProfile(name="drug_name",  type="string", samples=["阿莫西林","甲硝唑","未知药品"], null_ratio=0.0),
#   ]
# )

fingerprint = generate_fingerprint(profile)
# "a1b2c3d4e5f6..."

cached = cache.get(fingerprint)
# None — 缓存未命中
```

### Stage 1: AI 场景识别

**Prompt（~30 tokens）：**
```
数据概览：
- 字段数：5
- 字段名：['patient_id', 'gender', 'age', 'dept_name', 'drug_name']
- 类型分布：5string
- 缺失率：0.00,0.00,0.00,0.00,0.00

从以下选项中选择最匹配的场景代码：
S1=医疗数据 S2=财务数据 S3=用户数据 S4=日志数据 S5=地理数据 S0=未知

要求：
1. 只输出 1 个代码（如 S1）
2. 不要输出任何解释文字
3. 不要输出标点符号

输出：
```

**AI 输出：** `S1`

**校验：** `S1` ∈ `VALID_SCENE_CODES`，通过。`scene_code = "S1"`

### Stage 2: 本地路由 + Prompt 组装

```python
scene_config = ROUTING_TABLE["S1"]
# SceneConfig(
#   scene_name="医疗数据",
#   valid_codes={"G","A","D","N","C","T","I","X"},
#   operations={"G":"normalize_gender", "A":"extract_age", ...}
# )

# 压缩样本（此例中每字段最多 3 个样本，未超上限，无需裁剪）

prompt = build_field_semantic_prompt(profile, scene_config)
```

**组装后的 Prompt（~60 tokens）：**
```
场景：医疗数据

字段样本（每字段最多 5 个值）：

字段1: patient_id
样本: ['P001', 'P002', 'P003']
选项: A=年龄 D=科室 G=性别 C=诊断码 N=药品名 T=时间 I=编号 X=其他

字段2: gender
样本: ['M', 'F', 'Male']
选项: A=年龄 D=科室 G=性别 C=诊断码 N=药品名 T=时间 I=编号 X=其他

字段3: age
样本: ['30', '30岁', '约30']
选项: A=年龄 D=科室 G=性别 C=诊断码 N=药品名 T=时间 I=编号 X=其他

字段4: dept_name
样本: ['心内', '外科', '妇科']
选项: A=年龄 D=科室 G=性别 C=诊断码 N=药品名 T=时间 I=编号 X=其他

字段5: drug_name
样本: ['阿莫西林', '甲硝唑', '未知药品']
选项: A=年龄 D=科室 G=性别 C=诊断码 N=药品名 T=时间 I=编号 X=其他

要求：
1. 为每个字段选择 1 个代码
2. 按字段顺序输出，不要有空格（如 IGADN）
3. 不要输出任何解释

输出：
```

### Stage 3: AI 字段语义识别

**AI 输出：** `IGADN`

**校验：**
- 长度 = 5 == `field_count`(5)，通过
- `I` ∈ `{"G","A","D","N","C","T","I","X"}`，通过
- `G` ∈ `{"G","A","D","N","C","T","I","X"}`，通过
- `A` ∈ `{"G","A","D","N","C","T","I","X"}`，通过
- `D` ∈ `{"G","A","D","N","C","T","I","X"}`，通过
- `N` ∈ `{"G","A","D","N","C","T","I","X"}`，通过

`signal_sequence = "IGADN"`

### Stage 4: 缓存写入 + 操作链组装

```python
# 缓存写入
cache.put(fingerprint, CacheEntry(scene_code="S1", signal_sequence="IGADN"))

# 操作链组装
# I → "pass_through"   → PassThrough()
# G → "normalize_gender" → GenderNormalizer()
# A → "extract_age"     → AgeExtractor()
# D → "normalize_department" → DepartmentNormalizer()
# N → "normalize_drug_name"  → DrugNameNormalizer()
```

### Stage 5: 本地执行

```
patient_id: ["P001", "P002", "P003"]     → pass_through → 不变
gender:     ["M", "F", "Male"]           → normalize_gender → ["男", "女", "男"]
age:        ["30", "30岁", "约30"]        → extract_age → [30, 30, 30]
dept_name:  ["心内", "外科", "妇科"]       → normalize_department → ["心内科", "外科", "妇科"]
drug_name:  ["阿莫西林", "甲硝唑", "未知药品"] → normalize_drug_name → ["阿莫西林", "甲硝唑", None]
```

### 第二次运行（相同数据）— 缓存命中

```python
profile = extract_profile(df)
fingerprint = generate_fingerprint(profile)  # "a1b2c3d4e5f6..." — 与上次相同
cached = cache.get(fingerprint)
# CacheEntry(scene_code="S1", signal_sequence="IGADN") — 命中！

scene_config = ROUTING_TABLE["S1"]  # 用缓存中的 scene_code 查表
ops = assemble_operations("IGADN", scene_config, registry)
result, report = execute_pipeline(df, ops)
# 总 Token = 0
```

### Token 消耗汇总

| 运行方式 | Stage 1 | Stage 3 | 总计 |
|---------|---------|---------|------|
| 首次运行 | ~31 | ~65 | ~96 |
| 缓存命中 | 0 | 0 | **0** |

---

## 性能基准与验证清单

### 性能基准

| 指标 | 目标 | 说明 |
|------|------|------|
| 单次运行 Token | < 100 | 包含所有 AI 调用 |
| 缓存命中时 Token | 0 | 数据指纹完全匹配 |
| 场景识别准确率 | > 90% | 测试集 100 条 |
| 字段语义识别准确率 | > 85% | 测试集 500 个字段 |
| 本地执行速度 | > 10000 行/秒 | 纯 pandas 操作 |
| 端到端延迟 | < 5 秒 | 包含 AI 调用 + 本地执行 |

### 必须验证的假设

- [ ] AI 在 temperature=0 时，相同输入输出相同信号码
- [ ] AI 能正确理解字段名的语义（场景识别准确率 > 90%）
- [ ] AI 能正确理解样本值的语义（字段识别准确率 > 85%）
- [ ] 信号码系统的 36 种可能足够覆盖常见场景
- [ ] 缓存命中率在稳定数据模式下可达 80%+
- [ ] 本地执行引擎性能满足 > 10000 行/秒

---

## 实现路线

### Phase 1: 核心框架 (MVP)

1. **数据结构**：`FieldProfile`, `DataProfile`, `CacheEntry`, `SceneConfig`
2. **Stage 0**：`extract_profile`, `generate_fingerprint`
3. **缓存**：`SignalCache`（含版本管理）
4. **Stage 1**：`build_scene_prompt`, `validate_scene_code`, `call_ai`
5. **Stage 2**：`compress_samples`, `build_field_semantic_prompt`, `ROUTING_TABLE`（S0-S3）
6. **Stage 3**：`validate_field_signal_sequence`
7. **Stage 4**：`assemble_operations`
8. **Stage 5**：`execute_pipeline`
9. **操作库**：`PassThrough`, `GenderNormalizer`, `AgeExtractor`, `DepartmentNormalizer`, `ICD10Validator`
10. **Pipeline**：`SignalChainPipeline.run`（含完整回退链）

### Phase 2: 扩展能力

1. 补全 S4（日志）、S5（地理）场景的路由和操作
2. 增加更多字段语义信号码和对应操作
3. 语义知识库扩展（更多领域映射）
4. QualityReport 增强（统计、可视化）
5. 缓存 LRU 淘汰策略

### Phase 3: 高级特性

1. 增量学习（根据用户反馈优化 AI 决策）
2. 多模型支持（不同场景用不同模型）
3. 分布式执行（大数据量并行处理）
4. 信号码热更新（不停机动态扩展场景）

---

## 总结

SignalChain 统一框架的逻辑链：

```
DataFrame
  → Stage 0: extract_profile → DataProfile + fingerprint
  → 缓存查找: fingerprint → CacheEntry? → 命中则跳到 Stage 4
  → Stage 1: DataProfile 摘要 → AI → SceneCode (如 "S1")
  → Stage 2: SceneCode → 查 ROUTING_TABLE → SceneConfig
           + DataProfile.fields → compress_samples → Prompt
  → Stage 3: Prompt → AI → FieldSignalSequence (如 "IGADN")
  → Stage 4: signal_sequence + SceneConfig.operations → 操作链
           + 缓存写入 CacheEntry("S1", "IGADN")
  → Stage 5: DataFrame + 操作链 → 清洗后 DataFrame
```

每一步的输入输出都有明确的数据结构契约：

| 契约 | 生产者 | 消费者 |
|------|--------|--------|
| DataProfile | Stage 0 | Stage 1, Stage 2, 指纹计算 |
| fingerprint | Stage 0 | 缓存查找, 缓存写入 |
| SceneCode | Stage 1 | Stage 2 路由, 缓存写入 |
| SceneConfig | Stage 2 (查表) | Stage 3 Prompt 组装, Stage 4 操作组装 |
| Prompt 字符串 | Stage 2 | Stage 3 (AI 调用) |
| FieldSignalSequence | Stage 3 | Stage 4 操作组装, 缓存写入 |
| CacheEntry | Stage 4 (写入) | 缓存查找 (读取) |
| 操作链 | Stage 4 | Stage 5 (执行) |

> **AI 的"智慧"体现在从模糊信息中做出正确分类，**
> **本地的"力量"体现在从确定信号中高速安全执行。**
> **信号码是两者之间的唯一契约，极简、规范、不可篡改。**
