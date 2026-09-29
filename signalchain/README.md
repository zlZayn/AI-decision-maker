# signalchain/ — 数据清洗与分类分析核心

- 职责：两条链路的核心实现（清洗 pipeline + 分类变量分析）
- __init__.py：对外导出 SignalChainPipeline 与核心数据类
- pipeline.py：SignalChainPipeline，清洗编排（含 cache_file / evaluator / escalate_to_system2 参数），被根脚本与 e2e 测试依赖
- ai_client.py：DeepSeekV4Client（**系统二**），AI 调用封装（model/api_key/base_url/thinking）
- system1.py：**系统一**接入层 —— Evaluator 协议、JevEvaluator、MockEvaluator、certainty 数学、GatePolicy
- fastpath.py：清洗链路系统一快通道（一次请求定场景+字段，低置信批量升级系统二）
- categorical_system1.py：分类链路系统一通道（noul 筛分类变量 + noul 判有序 + score 定顺序）
- categorical.py：CategoricalClassifier，分类变量两层 AI 识别
- cache.py：指纹缓存（signal_cache.json），被 pipeline 依赖
- models.py：信号码/场景码常量，被 stage* 依赖
- knowledge.py：字段语义知识库，被 operations/* 依赖
- stage0_profile.py → stage5_execute.py：五阶段清洗链（profile→scene→router→semantic→assemble→execute）
- operations/：base.py 基类 + registry.py 注册表 + 12 个字段操作（文件清单与信号码对应见 [operations/README.md](operations/README.md)）
- run_categorical_analysis.R：分类链路统计脚本，需 R 环境（见 [../README.md](../README.md)）
- [SYSTEM1.md](SYSTEM1.md)：系统一的完整说明（为何用 Jev、契约、门控、成本口径、实测与已知边界），
  按 §0–§13 编号，外部文档按此引用（如 §13.4）
- 变更影响路由：改这里 → 同步根 [../AGENTS.md](../AGENTS.md) 快照/坑 + 契约变更写 [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- 规则与约束 → 见 [AGENTS.md](AGENTS.md)

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
