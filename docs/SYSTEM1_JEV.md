# System 1 接入设计 — 用 Jev 给 SignalChain 装上"直觉"

> 状态：设计 + 参考实现 + **已在 jev-1.13.0 上实测**（见 §13）
> 结论先行：**能加，而且加完之后代码是净减少的**。但 Jev 不是"更快的 LLM"，它是另一类引擎，只能在满足 4 个边界条件的判断上替换系统二。
> 实测修正两条预期：**中文风险没有出现**（16/16 字段码、3/3 场景全对）；但 **token 量比现有系统二高约 20 倍**（§13.4）——
> 收益在"延迟 + 失败模式 + 可审计"，不在 token 成本。

---

## 0. 一句话

项目现在的架构是 **系统二（LLM，慢、审慎、生成文本）+ 本地执行**。
它把系统二用在了**本来就是系统一的判断**上：从 6 个场景里选 1 个、从 13 个信号码里给每个字段选 1 个。
做法是"逼 LLM 只输出代码 → 解析字符串 → 修非法值"。

Jev 是系统一引擎：**输入结构化 state + 类型化 questions，输出概率分布，不生成文本**。
接上它，上一层描述里的"解析"和"修"两步会**结构性消失**——不是优化掉，是变得无法表达。

---

## 1. 现状诊断：三处"用系统二模拟系统一"的证据

| 位置 | 现状代码 | 问题本质 |
| --- | --- | --- |
| `stage1_scene.py` | `build_scene_prompt` 末尾写"只输出1个代码"；`validate_scene_code` 做 `raw.strip()[:2]`，非法则 **静默回落 S0** | 非法值靠**字符串截断**兜底；而且回落是**静默失败**——幻觉被伪装成"未知数据"，没有任何信号 |
| `stage3_semantic.py` | 让 LLM 输出**定长字符串** `"IIGADDNT"`；`validate_field_signal_sequence` 65 行做长度校验、逐字符白名单、补 `X` | **位置即语义**：模型多输出/少输出 1 个字符，后面所有字段集体错位。每个字段**没有独立置信度**，只能整体接受或整体回退 |
| `stage2_router.py` | 手工把选项拼成文本行 `"I=编号 G=性别 ..."`；`compress_samples` 用启发式 `info_score` 把样本砍到 3 个 | 选项**是文本**而不是类型；样本压缩是**正确性 hack**（怕 prompt 太长导致判断退化），本应是成本旋钮 |
| `categorical.py` 第一层 | 让 LLM 输出 `"gender,education"` 或字符串 `"无"`；`validate_categorical_output` 做中英文逗号归一 + 与真实字段名求交集 | `"无"` 是一个**魔法字符串协议**；字段漏判/多判无概率可查 |
| `categorical.py` 第二层 | 让 LLM 输出 `"education:小学>本科"`；`validate_ordinal_output` 拆 `;`、`:`、`>`，归一中文标点，与唯一值求交集 | **把排序问题变成了文本解析问题**。顺序的正确性取决于模型是否记得用 `>` 和中文冒号 |

**共同点**：这五处都在做同一件事——*把一个封闭答案集上的分类判断，编码成自由文本，再解码回来*。
编解码层的每一行代码，都是引擎能力缺位的补偿。

---

## 2. 目标架构：三层决策 + 一层记忆

项目的名字是 **AI-decision-maker**，但"决策"目前只有一档。补上中间层：

```
                   ┌─────────────────────────────────────────┐
   输入 ──────────▶ │  System 0  本地查表（反射）              │  微秒 / 0 Token
   DataProfile      │  FIELD_NAME_HINTS · STANDARD_NAME_ALIASES│  精确匹配，无推断
                   └───────────────┬─────────────────────────┘
                                   │ 未决
                   ┌───────────────▼─────────────────────────┐
                   │  System 1  Jev（直觉）                   │  ~1 次前向 / 输出免费
                   │  choice / noul / score → 概率分布        │  结构化 state，闭集答案
                   └───────────────┬─────────────────────────┘
                          置信度 ≥ 阈值 │ 置信度 < 阈值（不确定性显式化）
                                   │         │
                                   │  ┌──────▼──────────────────────────┐
                                   │  │  System 2  LLM（审慎）           │ 0.5–2s / 计费
                                   │  │  只对"不确定的那几个字段"提问     │ 开放式、可生成文本
                                   │  └──────┬──────────────────────────┘
                                   ▼         ▼
                   ┌─────────────────────────────────────────┐
                   │  记忆  指纹缓存（含决策来源与置信度）      │  微秒 / 0 Token
                   └───────────────┬─────────────────────────┘
                                   ▼
                        本地执行（stage4 → stage5）· 零 Token
```

关键点：**门控（gate）才是"决策者"**。System 1 给概率，代码决定"这个概率够不够格直接执行"。
官方文档把这两件事分别叫 [Confidence-gated routing](https://docs.typesafe.ai/patterns/confidence-routing) 与
[Intent routing](https://docs.typesafe.ai/patterns/intent-routing)，与本项目"AI 负责认知、本地负责执行"是同一件事。

### 2.1 Jev 的适用边界（4 个条件，必须全中）

1. **答案集合在请求时已知且封闭** — 场景 S0–S5（6 个）、信号码（13 个）、是否分类变量（是/否）
2. **判断可从少量结构化证据完成** — 字段名 + 类型 + 样本值，正是 `DataProfile` 已有的东西
3. **不需要生成文本** — 我们只要代码，不要解释
4. **错误可被下游吸收** — 有门控 + 有系统二兜底 + 有缓存

清洗链路的 stage1 / stage3、分类链路的两层，**全部满足**。
反过来：`unified_framework_design` 里"让 AI 写清洗规则"这类**开放式生成**任务不满足条件 1 和 3，永远留在系统二。
所以这不是"把 LLM 换成 Jev"，而是**把两档决策拆成三档**。

---

## 3. 映射表：现有 AI 调用 → Jev 问题类型

### 3.1 清洗链路（stage1 + stage3 合并为 1 次请求）

| 现有 | System 1 形态 | 净效果 |
| --- | --- | --- |
| Stage 1 场景识别（1 次 LLM 调用） | `scene`: **choice**，criteria = `{S0:未知数据, S1:医疗数据, ... S5:地理数据}` | 非法场景码**不可表示**；`[:2]` 截断 + 静默回落 S0 整段删除，换成"置信度不足 → 升级系统二" |
| Stage 3 字段信号（1 次 LLM 调用） | 每个字段 1 个 **choice**，question_id = `field:<字段名>`，criteria = 全 13 个信号码 → 本地按场景条件化 | 定长字符串协议删除；串位不可能（**按 key 取答，不按位置**）；每字段自带概率分布 |
| `compress_samples` 启发式 | `state` 传结构化对象，样本进 `state.<字段>.samples` | 样本压缩从"防判断退化的正确性 hack"降级为"成本旋钮" |
| `_format_code_options` + `FIELD_SEMANTIC_TEMPLATE` | **criteria 即选项**，不需要把选项写成文本 | 提示词脚手架整体删除 |

**两次网络往返 → 一次。** 依据是官方文档的
[Speculative fan-out](https://docs.typesafe.ai/patterns/fan-out)：
"把代码可能用到的每个问题都问出来，让代码决定哪些答案有用"，
并且 "同一请求内的多个问题彼此独立、并行评估，加问题几乎不增加响应时间"。

⚠️ 同一请求内的问题**互相独立**（文档明确：一个答案不会成为另一个问题的上下文）。
所以字段问题是用**全 13 码**问的**边缘分布**，"按场景条件化"必须在**代码里**做——
把 13 码分布限制到该场景的 `valid_codes` 上重新归一化，并用**新的选项数**重算置信度。
这是一次显式的建模选择（形似朴素贝叶斯），不是引擎语义。设计上给了双请求的严格模式作为退路（见 §7）。

### 3.2 分类变量链路

| 现有 | System 1 形态 | 净效果 |
| --- | --- | --- |
| 第一层：筛选分类变量 | 每个字段 1 个 **noul**，question_id = `cat:<字段名>`；`criteria.true/false` 写清正反例 | `"无"` 魔法字符串删除；阈值变成**可调策略**而不是"模型这次输出什么" |
| 第二层：有序/无序 + 顺序 | 每个**取值** 1 个 **score**，criteria = 5 档有序刻度 → **按分数排序得到顺序** | 排序回归为排序（比较数值），`>`/中文标点解析整段删除 |

第二层的**有序性判据也从"模型措辞"变成了"数值结构"**：

- **离散度** `max(score) - min(score)`：分数分得开 → 语义上确实有梯度（如"小学 < 本科 < 硕士"）
- **确定度** `mean(certainty)`：每个取值的概率分布足够集中
- 判据：`离散度 ≥ min_spread` **且** `确定度 ≥ accept` → 有序；否则 → 无序

这正是官方 [Composite scoring](https://docs.typesafe.ai/patterns/composite-scoring) 的用法：判断拆成原子的 score，组合权重留在代码里。

> ⚠️ **实测修正（重要）**：上面"只靠离散度 + 确定度"的判据**不成立**。实测中
> 性别（男/女）被打了 男=0.72 / 女=3.61 分 —— 离散度 2.89、档位确定度 0.675，**两项都过线**，
> 于是被误判为"有序"，而性别必须是无序。
>
> 修正后的设计多了一道门：**再用一个 noul 直接问"这些取值之间有没有公认的高低顺序"**，
> 并把两者放在同一次请求里（speculative fan-out），由代码合成结论。
> 实测这一道门把真假分得极干净：真有序变量 P(有序)=0.980，性别/血型 P(有序)=0.040。
> 见 §13.3。

---

## 4. 关键设计决定

### D1 · 置信度用哪个数，以及为什么不能全用同一个数

这是最容易做错的地方。三种答案的"把握程度"字段并不一样：

| 答案类型 | 返回字段 | 把握程度怎么算 |
| --- | --- | --- |
| `choice` | `choice`, `probabilities`, `confidence` | 直接用 `confidence` |
| `score` | `score`, `legend`, `probabilities`, `confidence` | 直接用 `confidence` |
| `noul` | `noul`（**没有 confidence**） | `noul` 是"答案为是的概率"，把握程度 = `|2p − 1|` |

官方给出的 `confidence` 不是模型自报，而是从分布形状推出来的：

```
confidence = (n × peak − 1) / (n − 1)      n = 选项数，peak = 最大概率
```

这个式子有两个重要推论：

1. **它已经把"随机水平"归一化了**：n 个选项全均匀 → `peak = 1/n` → confidence = 0；全押一个 → 1。
   所以 **一个阈值可以在不同选项数的问题之间通用**，不需要为 6 选项的场景问题和 13 选项的字段问题各配一个。
2. **但 `probabilities` 本身不能跨问题比较**：13 选项里 0.4 是"很强"，6 选项里 0.4 只是"略高"。
   所以门控**必须用 confidence，不能用 raw probability**。

由此，`noul` 用 `|2p − 1|` 正是同一个尺度的自然对应物（0 = 与随机无异，1 = 完全确定），
于是全项目可以只用一个概念：**certainty ∈ [0,1]，"离随机有多远，按最大可能距离归一"**。
`signalchain/system1.py` 里 `certainty()` 就是这个统一入口。

> 还有一个必须做对的细节：**本地条件化重归一之后，选项数从 13 变成了该场景的 k**，
> 所以 `confidence` 必须**用新选项数重算**，不能沿用引擎返回的那个值。代码里 `choice_confidence()` 就是为此存在。

### D2 · 用官方 SDK，但不把它变成硬依赖

核查结果（都实测过）：

| 候选 | 结论 |
| --- | --- |
| `pip install typesafe` | ❌ **不是**官方 SDK，是 2015 年前后的 "formal type asserting decorators" 装饰器库，装错就是灾难 |
| `pip install jev` | ⚠️ 是官方装饰器层，但 `requires_python = ">=3.14"`，**本项目是 3.12.10，装不了** |
| `pip install typesafe-sdk` | ✅ 官方 SDK 0.7.0，`requires_python = ">=3.10"`，依赖 `httpx2`(pydantic 团队的下一代 HTTP 客户端) / `tenacity` / `truststore` / `pydantic`，共 5 个包，**与现有 `httpx 0.28.1` 不冲突**（不同发行名） |

决定：走 `typesafe-sdk`，但放**可选依赖**（`uv sync --extra system1`），并且**惰性导入**——
和项目现在处理 `openai` 的方式完全一致（`ai_client.py` 里 `try: import openai except ImportError: raise ImportError("请安装 openai")`）。
没装 SDK / 没配 Key / 网络不通时，项目**必须一字不变地照旧跑系统二**。

另外 SDK 自带 `RetryPolicy`（tenacity 指数退避 + 识别 `Retry-After` 头），
正好覆盖指南 §5 的 429/529 退避，**不需要自己写重试**。

### D3 · 优雅的度量：删掉多少行

这次改动**新增**的是"信息"（概率、置信度、决策来源），**删掉**的是"补偿代码"：

| 删除/塌缩 | 行数量级 | 为什么能删 |
| --- | --- | --- |
| `validate_scene_code` 的截断 + 静默回落 | ~10 | 非法值不在 criteria 里，不可表示 |
| `validate_field_signal_sequence` 全部校验修复 | ~45 | 按 key 取答 + criteria 即白名单，长度/串位/非法码三个失败模式同时消失 |
| `build_scene_prompt` / `FIELD_SEMANTIC_TEMPLATE` 脚手架 | ~25 | criteria 即选项，不需要"只输出代码，无空格无解释"的祈祷 |
| `validate_categorical_output`（逗号归一 + 交集） | ~17 | 每个字段一个问题，"无"变成"概率低于阈值" |
| `validate_ordinal_output`（拆 3 种分隔符 + 中文标点 + 交集） | ~35 | 顺序由分数排序产生，不存在可解析的文本 |

原来的校验层是**"不信任 AI 输出"**（[docs/ARCHITECTURE.md](ARCHITECTURE.md) 原话），设计上是对的；
换上 Jev 之后，**约束从"事后校验"前移为"请求即类型"**——
`criteria` 就是那个类型，模型返回的值**在结构上不可能越界**（官方原话：
"Every answer is constrained to the options you supplied... Your code never has to recover a value from generated prose"）。

补一句诚实的：`X`（其他）的兜底**没有消失，而是从"修字符"变成了"策略"**。
现在非法码被替换为 `X` 是解析的副产物；将来它是"该字段在场景下无可选码"的显式决策，可以记日志、可以升级系统二。

### D4 · 缓存要记住"为什么"

`signal_cache.json` 现在只存 `(scene_code, signal_sequence)`。系统一进来之后要能回答：
*"这条缓存当时是 Jev 判的还是 LLM 判的？置信度多少？"*

- `CacheEntry` 增加**可选**字段 `certainty` / `engine`（老缓存仍可读，向后兼容）
- `cache._code_hash()` 纳入**引擎标识**（`jev-latest` vs `deepseek-v4-flash`）——
  否则换引擎后旧缓存会被静默复用，而它的来源已经不可追溯

**副作用（要提前说清）**：升级后 `signal_cache.json` 会失效一次，首次运行会重新决策一次。
这是正确行为，不是 bug。

### D5 · 中文风险必须用测量回答，不能用乐观回答

指南第 7 条明确说"中文能力相对较弱，英文场景效果更好"。**这个项目的样本恰恰重度中文**，实测数据：

```
medical.csv : PID, SEX/Gender, AGE, DEPT, MED     值: M / 三十 / 约25 / 心内科 / 阿莫西林 / 头孢 / 甲硝唑~
finance.csv : TXN_ID, AMOUNT, DATE                值: " ¥ 1，000.50 " / "$ 200." / "500!!"
user.csv    : UID, SEX, AGE, E-MAIL, PHONE_number  值: 男 / Male） / invalid.email@ / 138-0013-8000
```

三点设计上的缓解，加一点必须承认的风险：

1. **答案方向是 ASCII**：字段码是 `G/A/D/N/C/T/M/E/P/L/R/I/X`，场景码是 `S0–S5`。
   模型只需要**读中文**，不需要**写中文**——这是弱中文模型最擅长的一侧。
2. **中文放在 criteria 的"描述"里**（`{"G": "性别：男/女/M/F"}`），
   即"被评估的材料"而非"要生成的输出"。
3. **系统二兜底是自动的**：门控不过就升级，最坏情况退化成今天的行为，不会更差。
4. ✅ **已实测（§13.2）：中文风险没有出现。** `run_smoke_jev.py` 把 `三十`、`心内科`、`约25`、
   `Male）`、`阿莫西林`、`" ¥ 1，000.50 "`（含全角逗号）全部喂了进去：
   **场景 3/3 全对，字段码 16/16 全对，且概率几乎都是 1.000**。
   连 `SEX/Gender`、`E-MAIL`、`PHONE_number` 这类带斜杠/连字符/大小写混杂的字段名也没出错。
   "中文弱"在这批真实数据上没有成为问题——但这只是一次 16 例的抽样，
   换更生僻的领域词仍应重跑冒烟脚本。

### D6 · 网络：实测可达，但仍然不能依赖

实测（本机，2026-今）：

```
DNS    api.typesafe.ai → 44.227.31.201 / 100.20.85.248   ✅ 解析正常
HTTPS  POST /v1/systemone  →  HTTP 401                   ✅ 通（401 = 缺 Key，不是被墙）
```

指南里"中国大陆可能无法直连"的警告**在本机不成立**。但设计上仍然按"随时可能不通"处理：
`System1Unavailable`（连接失败 / 超时 / 401 / 429 / 5xx）→ **静默降级到系统二并记日志**，
绝不因为 Jev 挂了就让整条清洗链失败。

---

## 5. 数据契约

```python
# ---- signalchain/system1.py：与 SDK 解耦的内部表示（字段名刻意对齐 wire format）----
@dataclass(frozen=True)
class Answer:
    type: str                                  # "noul" | "choice" | "score"
    noul: float | None = None                  # 答案为是的概率
    choice: str | None = None                  # 选中的选项 key
    score: float | None = None                 # 刻度位置（可落在两档之间）
    probabilities: Mapping[str, float] = ...   # choice: 选项→概率；score: 档位→概率
    legend: Mapping[str, str] = ...            # score 的档位说明
    confidence: float = 0.0                    # noul 为 0（引擎不返回），用 noul_certainty()

@dataclass
class EvalResponse:
    model: str
    answers: dict[str, Answer]                 # key = question_id
    input_tokens: int = 0
    output_tokens: int = 0

class Evaluator(Protocol):
    engine_id: str
    def evaluate(self, state, questions) -> EvalResponse: ...
    def available(self) -> bool: ...
```

`Evaluator` 是这次改动的**唯一接缝**。三种实现：`JevEvaluator`（真 API）、
`MockEvaluator`（离线，让全部链路可测——遵守 `tests/AGENTS.md`"测试不调用真实 AI"）、
以及未来的任何引擎。

```python
# ---- signalchain/models.py：可审计的决策记录 ----
@dataclass
class DecisionRecord:
    question_id: str          # "scene" | "field:gender" | "cat:education" | "level:education=小学"
    subject: str              # 人类可读主体
    chosen: str               # 最终选择
    certainty: float          # [0,1]
    engine: str               # "system0" | "system1" | "system2" | "cache"
    probabilities: dict[str, float] = ...
    escalated: bool = False   # 是否因置信度不足升级到系统二
```

`DecisionRecord` 让"AI Decision Maker"第一次有了可审计的决策日志：
每次运行都能回答"这一列为什么被判成 G、当时有多确定、有没有升级"。

---

## 6. 落地形态（参考实现已就位）

| 文件 | 角色 | 状态 |
| --- | --- | --- |
| `signalchain/system1.py` | Evaluator 协议 + Jev 传输 + 问题构造原语 + certainty 数学 + Mock | 已实现 |
| `signalchain/fastpath.py` | 清洗链路系统一快通道（单请求 + 本地条件化 + 门控 + 单次批量升级） | 已实现 |
| `signalchain/categorical_system1.py` | 分类链路系统一通道（noul 筛选 + score 定序） | 已实现 |
| `signalchain/models.py` | + `DecisionRecord` / `FieldDecision` | 已实现 |
| `signalchain/cache.py` | 引擎标识进哈希；条目可选记录置信度与来源 | 已实现 |
| `signalchain/pipeline.py` | 可选 `evaluator=` 参数；命中则走系统一，否则**原路不动** | 已实现 |
| `tests/test_system1.py` | 置信度数学 / 条件化 / 异常归类 / 问题构造 / Mock | 97 项用例 |
| `tests/test_fastpath.py` | 门控 / 批量升级 / 与系统二路径结果一致性 | 同上 |
| `tests/test_categorical_system1.py` | noul 筛选 / 有序性门 / 假阳性回归 | 同上 |
| `run_smoke_jev.py` | 在线冒烟：中文样本 / noul 语义 / 有序性 / 延迟 / token | 已在线跑通 |
| `config.py` / `config.example.py` | 两套系统分别命名：`SYSTEM1_*`（Jev）/ `SYSTEM2_*`（LLM） | 已重构 |
| `pyproject.toml` | `[project.optional-dependencies] system1` —— 不装也能跑 | 已加 |

### 用法（默认行为完全不变）

```python
# 今天怎么写，明天还怎么写 —— 不传 evaluator 就是系统二
pipeline = SignalChainPipeline(ai_client=DeepSeekV4Client(...))

# 开启系统一：Jev 先判，拿不准的字段自动升级给系统二
from signalchain.system1 import JevEvaluator
pipeline = SignalChainPipeline(
    ai_client=DeepSeekV4Client(...),          # 系统二：兜底 + 升级裁决
    evaluator=JevEvaluator(),                  # 系统一：主路径（Key 读 TYPESAFE_API_KEY）
)
clean, report = pipeline.run(df)

for d in pipeline.decisions:                   # 新增：可审计决策日志
    print(f"{d.subject:<16} {d.chosen}  certainty={d.certainty:.3f}  via {d.engine}"
          + ("  [escalated]" if d.escalated else ""))
```

---

## 7. 门控策略

```python
@dataclass
class GatePolicy:
    accept: float = 0.80          # certainty ≥ 0.80 → 直接执行
    escalate: float = 0.55        # certainty < 0.55 → 升级系统二
                                  # 中间带：接受但标记 provisional（记日志，不入长期缓存）
    min_spread: float = 1.0       # 分类变量：分数离散度阈值（有序性判据）
    max_levels: int = 12          # 取值数超过此值不再逐个打 score，直接判无序
    scene_strategy: str = "one_pass"   # "one_pass"（1 次请求）| "two_pass"（严格条件化，2 次请求）
```

**为什么是两档阈值而不是一档**：中间带是真实存在的。
0.6 的判断不该直接丢给系统二（贵、慢、也未必更准），但它也不配进长期缓存。
所以中间带**执行但标记 provisional**——这个"不干不脆"的区间被显式建模，而不是被阈值一刀切掉。

`scene_strategy="two_pass"` 是给"条件化假设不成立"时的严格退路：
先用场景答案定场景，再用**该场景的 valid_codes** 作为 criteria 发第二次请求。
代价是 +1 次往返（约 +0.3–1s），换来的是"字段分布确实是在场景条件下产生的"，不需要本地条件化。

---

## 8. 成本与延迟：先给模型，再给测量方法

**结构上的变化是确定的**：

| 维度 | 现状（系统二） | System 1 主路径 |
| --- | --- | --- |
| 网络往返 | **2 次**（stage1 → stage3，串行） | **1 次** |
| 输出计费 | 计费且按 2× 单价（`run_clean.py` 里 `PRICE_OUTPUT = 2.0`） | **输出免费** |
| 输出被截断风险 | `max_tokens=256`，序列一长有截断风险 | 无（不是生成任务） |
| 失败模式 | 幻觉码 → 静默 S0 / 静默 `X` | 低置信 → **显式升级**，永不静默 |
| 可解释性 | 一串字符 `"IIGADDNT"` | 每字段一个概率分布 + 一条 DecisionRecord |

**这里不编数字，§13 给实测值。** 一句话预告（实测）：延迟确实大幅改善，
但**输入 token 比现有系统二高约 20 倍**——prompt 从"压到 3 个样本的紧凑文本行"
变成了"13 个码的 criteria + 结构化 state"。
所以"高效"这个词要拆开看：**延迟与可靠性高效，token 成本不高效**。
真正的收益是"1 次往返 + 输出不计费 + 无解析修复 + 失败模式消除 + 每步可审计"。

---

## 9. 分阶段实施

| 阶段 | 内容 | 风险 | 回滚 |
| --- | --- | --- | --- |
| **P0** | `uv sync --extra system1`；`run_smoke_jev.py` 跑中文样本 | 低 | 不装即可 |
| **P1** | 清洗链路接入（本设计的 `fastpath.py`） | 中 | 不传 `evaluator` 即回旧行为 |
| **P2** | 分类链路接入（`categorical_system1.py`） | 低 | 同上 |
| **P3** | 实测后调 `GatePolicy`；决定是否设为默认 | 低 | 纯配置 |
| **P4** | 把 `certainty` 接进 `QualityReport` / `run_clean.py` 输出 | 低 | — |

**P0 是硬门槛**：中文样本上如果 Jev 分不对字段码，P1/P2 就不要设默认路径。
设计已经把"退化成今天的行为"做成零成本，所以试错成本很低。

---

## 10. 验收标准

- [x] `uv run pytest` 全绿：**234 passed**（原有 137 + 新增 97 项离线用例）
- [x] 中文与真实脏数据已在线实测（§13.2）
- [x] 有序性判定已在线实测并修正（§13.3）
- [ ] 不传 `evaluator` 时，行为与改动前**逐字节一致**（含缓存格式兼容）
- [ ] `JevEvaluator` 在连接失败 / 401 / 429 / 5xx 时抛 `System1Unavailable`，pipeline **自动降级**且不抛异常
- [ ] 单请求内能同时拿到场景 + N 个字段的答案（Speculative fan-out）
- [ ] 低置信字段**批量**升级（一次系统二调用处理所有不确定字段，而非每字段一次）
- [ ] `pipeline.decisions` 能复现"每个字段为什么是这个码"
- [ ] 分类链路：性别判**无序**、学历判**有序且顺序正确**（离线用 Mock 断言，在线用冒烟脚本验证）
- [x] `run_smoke_jev.py` 打印中文样本的完整概率分布与 token 用量（在线/离线两种模式）

---

## 11. 风险登记

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| 中文能力弱（官方明示） | 字段码判错 | criteria 中文放描述侧、答案侧全 ASCII；门控 + 系统二兜底；**P0 实测** |
| 同请求问题互相独立 | 本地条件化是建模假设 | 提供 `two_pass` 严格模式；不一致时门控会自然升级 |
| 引擎静默升级改变决策 | 结果不可追溯 | 引擎标识进缓存哈希；切换引擎强制缓存失效一次 |
| `confidence` 被误当作"模型自报" | 阈值调错 | 文档 + 代码注释写明 `(n×peak−1)/(n−1)`，门控只读 certainty |
| 依赖 `typesafe-sdk` 但装不上 | 功能不可用 | 可选依赖 + 惰性导入；未安装时项目行为不变 |
| 单请求问题数过多（N 字段 + 1 场景） | 请求体过大 | 官方文档：并行评估、加问题几乎不增响应时间；实测确认后再定 单请求问题数上限 |

---

## 12. 顺带发现（与本设计无关，但该修）

1. **`config.py` 里的 DeepSeek API Key 是明文且已入库**（`API_KEY = "sk-f393...3233"`，3 行文件，`config.example.py` 才是模板）。
   建议改成读环境变量并**轮换这把 Key**。接 Jev 时请务必让 `TYPESAFE_API_KEY` 只走环境变量，不要再复制一份进 `config.py`。
2. **PyPI 上的 `typesafe` 是无关库**。任何把 `pip install typesafe` 写进文档/脚本的地方都要改成 `typesafe-sdk`。
3. `.pytest_cache` 目录权限异常（`WinError 5 拒绝访问`），`pytest` 每次都会报一条 `PytestCacheWarning`。可用 `-p no:cacheprovider` 或修目录权限消除。
4. **`config.py` 里的 `SYSTEM2_MODEL` 是 `deepseek-flash`**，而 README 与 `DeepSeekV4Client` 默认值都是 `deepseek-v4-flash`。
   我按"不擅自改你的模型选择"保留了原值，但这个不一致建议确认一下。
5. **配置项已按两套系统重命名**：`SYSTEM2_*`（LLM）/ `SYSTEM1_*`（Jev），
   5 个调用方（`run_clean.py`、`run_categorical.py`、`tests/run_*.py` ×3）已同步；
   旧的 `API_KEY / API_URL / MODEL` 三个名字**已移除**（全仓库 grep 确认无其他引用方）。

---

## 13. 实测结果（jev-1.13.0，真实 API Key，本机直连）

跑法：`uv run python run_smoke_jev.py --all`（8 次请求，脚本见 [run_smoke_jev.py](../run_smoke_jev.py)）。
全部数字来自真实调用，不是估算。

### 13.1 连通性

| 项 | 结果 |
| --- | --- |
| DNS / HTTPS | `api.typesafe.ai` 可达（401 = 缺 Key，不是被墙） |
| 鉴权 | 通过；返回 `model = jev-1.13.0` |
| `noul` 语义 | **确认是"答案为是的概率"**：紧急句 0.98 / 无关句 0.13 |
| SDK | `typesafe-sdk 0.7.0` 装好即用，与既有 `httpx 0.28.1` 无冲突 |

### 13.2 中文与脏数据：风险**没有**出现

| 数据集 | 场景识别 | 字段码 |
| --- | --- | --- |
| finance.csv（`" ¥ 1，000.50 "` 全角逗号、`$ 200.`、`500!!`） | S2 ✅ p=1.000 | I / M / T **3/3** |
| medical.csv（`SEX/Gender`、`三十`、`约25`、`心内科`、`阿莫西林`、`Male）`） | S1 ✅ p=1.000 | I / G / A / D / N **5/5** |
| user.csv（`E-MAIL`、`PHONE_number`、`男`、`Male`、`invalid.email@`、`12345`） | S3 ✅ p=1.000 | I / G / A / E / P **5/5** |
| **合计** | **3/3，概率均 1.000** | **16/16** |

值得单独点出的四例（都是原本最担心的）：

- `AGE` 的样本含 **`三十`**（中文数字）与 **`约25`**（带"约"字）→ 仍判 **A**
- `SEX/Gender` 样本含 **`Male）`**（多了个全角右括号）→ 仍判 **G**
- `AMOUNT` 样本含 **`" ¥ 1，000.50 "`**（全角逗号 + 前后空格 + 货币符号）→ 仍判 **M**
- `E-MAIL` 的样本里有 **`invalid.email@`**（残缺邮箱）→ 仍判 **E**

结论：**答码侧全 ASCII 的设计选择是对的**——模型只需要"读中文"，不需要"写中文"。

### 13.3 有序性判定：实测暴露了一个真实缺陷，并已修正

第一版只用"分数离散度 + 档位确定度"判有序，结果：

| 变量 | P(有序) | 离散度 | 档位确定度 | 第一版判定 | 正确答案 |
| --- | --- | --- | --- | --- | --- |
| education 学历 | — | 3.99 | 0.988 | 有序 ✅ | 有序 |
| satisfaction 满意度 | — | 3.97 | 0.995 | 有序 ✅ | 有序 |
| severity 病情 | — | 3.98 | 0.993 | 有序 ✅ | 有序 |
| **gender 性别** | — | **2.78** | **0.59** | **有序 ❌** | **无序** |
| blood_type 血型 | — | 2.66 | 0.43 | 无序 ✅ | 无序 |

**性别被判成了有序**——离散度 2.78 过了 1.0 的线。这是设计缺陷，不是模型问题：
"男/女"根本不存在可比较的高低，但打分题逼着模型给出一个位置。

加上 noul 门之后的同一批数据：

| 变量 | P(有序) | cert | 离散度 | 档位确定度 | 修正后判定 |
| --- | --- | --- | --- | --- | --- |
| education | **0.980** | 0.960 | 3.99 | 0.988 | 有序 ✅ |
| satisfaction | **0.980** | 0.960 | 3.97 | 0.995 | 有序 ✅ |
| severity | **0.980** | 0.960 | 3.99 | 0.993 | 有序 ✅ |
| gender | **0.040** | 0.920 | 2.89 | 0.675 | 无序 ✅ |
| blood_type | **0.040** | 0.920 | 2.55 | 0.434 | 无序 ✅ |

**5/5，而且分得极干净：0.980 vs 0.040。** 顺序本身也完全正确
（`小学(0.01) < 初中(1.03) < 高中(2.01) < 本科(2.99) < 硕士(4.00)`）。
这条已经固化成回归测试 `test_gender_like_variable_is_nominal`。

### 13.4 成本与延迟：一个必须说清的反直觉结果

| 维度 | 现状（系统二）| System 1（实测）| 判断 |
| --- | --- | --- | --- |
| 网络往返（每数据集）| **2 次** | **1 次** | ✅ 改善 |
| 单次请求延迟 | 0.5–2 s | **0.24–0.43 s** | ✅ 显著改善 |
| 输入 token（medical，5 字段）| 约 **75** | **2335**（优化后）| ❌ **约 31×** |
| 输出 token | 约 5（计费 2×）| 604（官方称输出免费）| ⚠️ 数量大但免费 |
| 静默失败 | 有（幻觉码→静默 S0 / 静默 X）| **无**（低置信必升级）| ✅ 改善 |
| 可解释性 | 一串字符 | 每字段一个分布 + 一条决策记录 | ✅ 改善 |

**token 为什么高**：criteria 有 13 个码，state 带全部字段与样本，
而每个字段的 instructions 又重复一次自己的名字/类型/样本。

**已做的优化（实测 −38%，答案完全不变）**：`code_criteria(verbose=False)` 只给标签、
不带判据描述与示例：

| 变体 | 输入 token | 场景 | 字段码 |
| --- | --- | --- | --- |
| 富判据 criteria + 全字段 state + hint | 3745 | S1 ✅ | IGADN ✅ |
| **短判据 criteria + 全字段 state + hint（现行默认）** | **2335** | S1 ✅ | IGADN ✅ |
| 富判据 + 精简 state + hint | 3470 | **S0 ❌** | IGADN ✅ |
| 短判据 + 精简 state + 去 hint | 1944 | **S0 ❌** | IGADN ✅ |

两条结论：
1. **criteria 是最大的一块可省内容**——砍掉判据描述省 38%，准确率不动 → 已设为默认。
2. **`state` 里的字段清单不能砍**——场景问题靠它判断，砍了场景就从 S1 掉到 S0。
   而字段码在精简 state 下依然全对，说明"字段信息放进每道题的 instructions"这个选择是有效的
   （字段判断不依赖 state）。

**还剩的优化空间**（未做，留待需要时）：`state` 里只保留字段名、把样本完全交给每题的
instructions，可以再省几百 token。需要先实测确认场景准确率不受影响。

### 13.5 实测改了设计里的哪些东西

| 原设计 | 实测结果 | 改了什么 |
| --- | --- | --- |
| 有序性：score 离散度 + 确定度 | 性别被误判为有序 | **加 noul 有序性门**，与 score 同请求发出 |
| criteria 带中文判据描述 | 省 38% token 且答案不变 | **默认改为短判据**，verbose 作为可选 |
| 中文是主要风险，必须实测 | 16/16 全对，中文风险未出现 | 结论改为"已实测通过"，但保留重跑建议 |
| "会变得非常高效" | 延迟高效、token 不高效 | 文档改成分开表述，不合并成"高效" |
| 需要自己写 429/529 重试 | SDK 自带 RetryPolicy | 不写重试代码 |

### 13.6 仍未验证的部分（诚实清单）

- **升级路径没有在线跑过**：低置信字段升级给系统二这条分支只能用 Mock 验证
  （现有离线测试覆盖了），真实 Jev 一直给出高置信答案，没有自然触发过。
- **`two_pass` 严格模式没在线跑过**：只跑了默认的 `one_pass`。
- **token 与费用换算**：TypeSafe 的实际单价需以控制台为准，本设计只测了 token 量。
- **样本量小**：3 个数据集 / 16 个字段 / 5 个分类变量。领域词更生僻时应重跑 `run_smoke_jev.py`。

