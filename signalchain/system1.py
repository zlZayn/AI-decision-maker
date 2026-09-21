"""System 1 决策层 — Jev (TypeSafe System One) 接入

职责边界
--------
本模块只管"把判断发出去、把概率拿回来"，不掺任何业务语义。
业务问题怎么构造，由 fastpath.py（清洗链路）与 categorical_system1.py（分类链路）负责。

为什么需要单独一层
------------------
现有 ai_client.AIClient 的契约是 call(prompt) -> str：文本进、文本出。
Jev 的契约是 evaluate(state, questions) -> 概率分布：结构化进、结构化出。
两者不是同一个接口。硬塞进 AIClient 会让"解析字符串 + 修复非法值"的补偿代码复活，
而那正是本次接入要消灭的东西。所以新增 Evaluator 协议，作为系统一/系统二的分界线。

设计约束
--------
1. typesafe-sdk 惰性导入 —— 未安装时本项目其余功能完全不受影响
   （与 ai_client.OpenAIClient 惰性导入 openai 的处理方式一致）
2. MockEvaluator 让全部链路可离线测试（tests/AGENTS.md：测试不调用真实 AI）
3. System1Unavailable 表示"引擎此刻不可用"，调用方据此降级到系统二，绝不因此失败

参考
----
- 问题类型与答案结构：https://docs.typesafe.ai/primitives
- 置信度公式：https://docs.typesafe.ai/confidence
- 置信度门控：https://docs.typesafe.ai/patterns/confidence-routing
- 单请求多问题：https://docs.typesafe.ai/patterns/fan-out
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, Sequence, runtime_checkable

from signalchain.ai_client import TokenUsage

logger = logging.getLogger(__name__)

# 官方 Python SDK 的发行名。注意：PyPI 上的 "typesafe" 是无关的第三方装饰器库，
# "jev" 是官方装饰器层但要求 Python >= 3.14（本项目 3.12，装不了）。只有它是正确的。
SDK_PACKAGE = "typesafe-sdk"
SDK_INSTALL_HINT = f"请安装官方 SDK: uv sync --extra system1（等价于 uv add {SDK_PACKAGE}）"
API_KEY_ENV = "TYPESAFE_API_KEY"


# ============================================================
# 异常
# ============================================================


class System1Error(Exception):
    """系统一相关的基类异常"""


class System1Unavailable(System1Error):
    """引擎此刻不可用：未装 SDK / 未配 Key / 网络不通 / 429 / 5xx

    语义是"这次别用系统一"，不是"程序有 bug"。
    调用方（pipeline）捕获它并降级到系统二，用户侧不应看到异常。
    """


class System1RequestError(System1Error):
    """请求本身有问题（400/422/响应结构非法）—— 大概率是我们的 bug

    与 System1Unavailable 刻意区分：这个不该被静默吞掉，
    否则"问题构造写错了"会伪装成"引擎不可用"，永远查不出来。
    """


# ============================================================
# 与 SDK 解耦的内部表示
#
# 字段名刻意对齐 wire format（JSON），这样 SDK 适配层与"裸 HTTP"适配层
# 产出的对象完全一致，上层代码不需要知道底下用的哪个传输。
# ============================================================


@dataclass(frozen=True)
class Answer:
    """一道题的回答（只保留本项目用得到的字段）"""

    type: str
    """取值：noul | choice | score"""

    noul: float | None = None
    """noul：答案为"是"的概率（0~1）。noul 没有 confidence 字段，见 noul_certainty()。"""

    choice: str | None = None
    """choice：被选中的选项 key（必然 ∈ criteria 的 key 集合，结构上不可能越界）"""

    score: float | None = None
    """score：在有序刻度上的位置，可落在两档之间"""

    probabilities: Mapping[str, float] = field(default_factory=dict)
    """choice：选项 → 概率；score：档位序号（字符串）→ 概率"""

    legend: Mapping[str, str] = field(default_factory=dict)
    """score：档位序号 → 档位说明"""

    confidence: float = 0.0
    """引擎返回的置信度。注意：本地条件化后这个值会失效，请用 certainty()。"""


@dataclass
class EvalResponse:
    """一次评估请求的完整结果"""

    model: str
    answers: dict[str, Answer] = field(default_factory=dict)
    input_tokens: int = 0
    output_tokens: int = 0


# ============================================================
# 把握程度：全项目只用一个概念
#
# 三种答案的"把握程度"字段并不一样：choice/score 有 confidence，noul 没有。
# 这里把它们统一到同一个尺度 [0,1]：
#   0 = 与随机猜测无异，1 = 完全确定
# 门控（gate）只读这一个数，不需要为每种答案类型配不同阈值。
# ============================================================


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def choice_confidence(probabilities: Mapping[str, float]) -> float:
    """TypeSafe 的 Choice 置信度公式：(n × peak − 1) / (n − 1)

    含义：把"离随机水平多远"按最大可能距离归一。
      n 个选项均匀分布（peak = 1/n）→ 0；概率全押在一个选项 → 1。

    为什么门控必须用它、而不是 raw probability：
      13 个选项里 0.4 是很强的信号，6 个选项里 0.4 只是略高；
      这个式子把选项数的影响消掉了，所以一个阈值可以跨不同选项数的问题复用。

    为什么本地还要重算：
      本地条件化会把选项数从 13 缩到该场景的 k，引擎返回的 confidence 随之失效。
      统一"从 probabilities 现算"可以保证门控永远看到当前分布的真实形状。
    """
    n = len(probabilities)
    if n == 0:
        return 0.0
    if n == 1:
        return 1.0
    peak = max(probabilities.values())
    return _clamp((n * peak - 1.0) / (n - 1.0))


def noul_certainty(probability: float) -> float:
    """noul 的把握程度：|2p − 1|

    noul 只返回"答案为是的概率"，没有 confidence 字段（官方文档明确）。
    |2p−1| 与 choice_confidence 是同一尺度：p=0.5 → 0（完全没主意），
    p=0 或 1 → 1（完全确定）。
    """
    return _clamp(abs(2.0 * probability - 1.0))


def certainty(answer: Answer) -> float:
    """统一的把握程度入口 —— 门控只读这一个函数。"""
    if answer.type == "noul":
        return noul_certainty(answer.noul) if answer.noul is not None else 0.0
    if answer.type in ("choice", "score"):
        # 有分布就用分布现算（条件化后引擎返回值已失效）；没有才退回引擎值
        if answer.probabilities:
            return choice_confidence(answer.probabilities)
        return _clamp(answer.confidence)
    return 0.0


def argmax(probabilities: Mapping[str, float]) -> str | None:
    """取概率最高的选项 key；并列时按 key 排序取第一个，保证可复现。"""
    if not probabilities:
        return None
    return max(sorted(probabilities), key=lambda key: probabilities[key])


def condition_choice(answer: Answer, allowed: Iterable[str]) -> Answer:
    """把 choice 的分布限制到 allowed 上并重新归一化（本地条件化）

    用途：同一请求内的问题彼此独立（官方文档明确：一个答案不会成为另一个问题的上下文），
    所以字段问题是用"全码集"问出来的边缘分布。
    本函数把它条件化到场景的 valid_codes 上，得到
    "已知这是 S1 场景，该字段最可能是哪个码"的条件分布。

    这是一次显式的建模选择（形似朴素贝叶斯），不是引擎语义。
    如果这个假设不成立，可用 GatePolicy.scene_strategy="two_pass" 走严格模式。

    注意：选项数从 len(answer.probabilities) 变成 len(allowed)，
    所以 confidence 必须用新选项数重算 —— certainty() 会自动从新分布现算。

    退化处理：若 allowed 内所有概率都是 0（模型把概率全给了场景外的码），
    则回退为 allowed 上的均匀分布 —— 语义是"确定是这几个之一，但完全不知道是哪个"，
    certainty 自然是 0，门控会把该字段升级给系统二。这比硬塞一个 X 诚实得多。
    """
    allowed_list = list(dict.fromkeys(allowed))  # 去重且保持顺序
    if not allowed_list:
        return answer

    restricted = {key: float(answer.probabilities.get(key, 0.0)) for key in allowed_list}
    total = sum(restricted.values())
    if total <= 0.0:
        uniform = 1.0 / len(allowed_list)
        restricted = {key: uniform for key in allowed_list}
    else:
        restricted = {key: value / total for key, value in restricted.items()}

    return Answer(
        type="choice",
        choice=argmax(restricted),
        probabilities=restricted,
        confidence=choice_confidence(restricted),
        noul=answer.noul,
        score=answer.score,
        legend=answer.legend,
    )


# ============================================================
# 问题构造原语
#
# 返回 plain dict（对齐 wire format）而不是 SDK 类型对象。
# 原因：SDK 的 normalize_questions() 同时接受 dict 与类型对象，
# 用 dict 可以让"未安装 SDK"的场景（测试、离线冒烟）构造问题零依赖。
# ============================================================


def noul(
    instructions: Any,
    true_description: Any = None,
    false_description: Any = None,
) -> dict[str, Any]:
    """是/否判断。返回"答案为是的概率"。

    适用：干净的是非题，且概率本身就是有用信号。
    官方提醒：定义不清时 0.5 无法解释（"Python 强不强"没有定义，
    它只表示是与否各半，不表示"中等水平"）。要测程度请用 score。
    """
    question: dict[str, Any] = {"type": "noul", "instructions": instructions}
    criteria: dict[str, Any] = {}
    if true_description is not None:
        criteria["true"] = true_description
    if false_description is not None:
        criteria["false"] = false_description
    if criteria:
        question["criteria"] = criteria
    return question


def choice(instructions: Any, criteria: Mapping[str, Any]) -> dict[str, Any]:
    """单选。返回选中项 + 全选项概率分布 + 置信度。

    criteria 的 key 就是这个问题的"类型"——模型返回的值不可能越界。
    上限 255 个选项。选项之间应无序；有序请用 score。
    """
    return {"type": "choice", "instructions": instructions, "criteria": dict(criteria)}


def score(instructions: Any, criteria: Sequence[Any]) -> dict[str, Any]:
    """有序评分。返回刻度位置 + 档位说明 + 概率分布 + 置信度

    criteria 是有序数组，2~10 档。答案 score 可落在两档之间。
    """
    return {"type": "score", "instructions": instructions, "criteria": list(criteria)}


# ---- 构造 mock/离线答案的小工具（同时充当 wire format 的活文档）----


def choice_answer_dict(
    value: str, probabilities: Mapping[str, float] | None = None
) -> dict[str, Any]:
    """构造一个 choice 答案的原始 dict"""
    if probabilities is None:
        probabilities = {value: 1.0}
    return {"type": "choice", "choice": value, "probabilities": dict(probabilities)}


def noul_answer_dict(probability: float) -> dict[str, Any]:
    """构造一个 noul 答案的原始 dict"""
    return {"type": "noul", "noul": float(probability)}


def score_answer_dict(
    value: float,
    legend: Sequence[str] | Mapping[str, str] | None = None,
    probabilities: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """构造一个 score 答案的原始 dict"""
    if legend is None:
        legend_map: dict[str, str] = {}
    elif isinstance(legend, Mapping):
        legend_map = {str(k): str(v) for k, v in legend.items()}
    else:
        legend_map = {str(i): str(v) for i, v in enumerate(legend)}
    if probabilities is None:
        # 默认造一个以 round(value) 档为中心的尖峰分布
        peak = int(round(value))
        probabilities = {str(peak): 0.8}
        if legend_map:
            rest = 0.2 / max(1, len(legend_map) - 1)
            for key in legend_map:
                if key != str(peak):
                    probabilities[key] = rest
    return {
        "type": "score",
        "score": float(value),
        "legend": legend_map,
        "probabilities": {str(k): float(v) for k, v in probabilities.items()},
    }


# ============================================================
# Evaluator 协议 —— 系统一/系统二的唯一分界线
# ============================================================


@runtime_checkable
class Evaluator(Protocol):
    """系统一引擎协议。任何实现只要满足它就能接进 pipeline。"""

    usage: TokenUsage

    @property
    def engine_id(self) -> str:
        """引擎标识，参与缓存哈希 —— 换引擎必须让旧缓存失效"""
        ...

    def available(self) -> bool:
        """此刻是否可用（不发起网络请求，只看配置）"""
        ...

    def evaluate(self, state: Any, questions: Mapping[str, Any]) -> EvalResponse:
        """评估一批问题。失败时抛 System1Unavailable / System1RequestError。"""
        ...


# ============================================================
# 门控策略 —— "决策者"本体
#
# 系统一只负责给出概率，代码负责决定"这个概率够不够格直接执行"。
# 官方把这个模式叫 Confidence-gated routing。
# ============================================================


@dataclass
class GatePolicy:
    """系统一有多大把握才允许直接执行

    certainty ∈ [0,1] 由 certainty() 统一给出（choice/score 用分布算，noul 用 |2p−1|）。
    为什么是两档阈值而不是一档：中间带是真实存在的。
    0.6 的判断不该直接丢给系统二（贵、慢、也未必更准），但它也不配进长期缓存。
    所以中间带照常执行但标记 provisional —— 把不干不脆的区间显式建模，而不是一刀切。
    """

    accept: float = 0.80
    """certainty >= accept：直接采用"""

    escalate: float = 0.55
    """certainty < escalate：升级系统二；[escalate, accept) 采用但标记 provisional"""

    min_spread: float = 1.0
    """分类链路：判为"有序"所需的最小分数离散度（max(score) − min(score)）"""

    max_levels: int = 12
    """分类链路：唯一值超过此数就不再逐个打 score，直接判为无序"""

    scene_strategy: str = "one_pass"
    """one_pass：单请求（场景+字段一起问）+ 本地条件化；two_pass：严格条件化，多一次往返"""

    raise_on_request_error: bool = False
    """请求错误（400/422）是否直接抛出。False = 记 ERROR 日志后降级系统二。"""

    def verdict(self, cert: float) -> str:
        """给出三态判定：accept | provisional | escalate"""
        if cert >= self.accept:
            return "accept"
        if cert >= self.escalate:
            return "provisional"
        return "escalate"


# ============================================================
# Jev 实现（官方 typesafe-sdk）
# ============================================================


# 这些异常名表示"引擎此刻不可用" —— 降级到系统二即可
_UNAVAILABLE_ERROR_NAMES = frozenset({
    "TypeSafeAPIConnectionError",
    "TypeSafeAPITimeoutError",
    "TypeSafeAuthenticationError",
    "TypeSafePermissionDeniedError",
    "TypeSafeNotFoundError",
    "TypeSafeRateLimitError",
    "TypeSafeInternalServerError",
    "TypeSafeAPIError",
})

# 这些表示"请求有问题" —— 大概率是我们的 bug，不该被静默吞掉
_REQUEST_ERROR_NAMES = frozenset({
    "TypeSafeBadRequestError",
    "TypeSafeUnprocessableEntityError",
    "TypeSafeAPIResponseValidationError",
})


def classify_error(exc: BaseException) -> type[System1Error]:
    """把 SDK 异常归类为 Unavailable 还是 RequestError

    刻意按类名字符串判断而不是 isinstance：
    SDK 未安装时无法 import 其异常类型，而本函数需要能在离线环境测试。
    这也让适配层对 SDK 小版本改名更耐受。
    """
    name = type(exc).__name__
    if name in _REQUEST_ERROR_NAMES:
        return System1RequestError
    if name in _UNAVAILABLE_ERROR_NAMES:
        return System1Unavailable
    # 裸网络/超时异常（SDK 未包装时）
    if isinstance(exc, (OSError, TimeoutError, ConnectionError)):
        return System1Unavailable
    # 未知异常：不吞，交给上层暴露真实 bug
    return System1RequestError


def _to_answer(raw: Any) -> Answer:
    """把 SDK 答案对象或裸 dict 统一转成 Answer

    两种来源都支持，是为了让传输层可替换（SDK / 裸 HTTP / Mock）。
    """
    if isinstance(raw, dict):
        get = raw.get
    else:
        def get(key: str, default: Any = None) -> Any:
            return getattr(raw, key, default)

    answer_type = get("type")
    if answer_type == "noul":
        value = get("noul")
        return Answer(type="noul", noul=float(value) if value is not None else None)

    probabilities = {
        str(key): float(value) for key, value in (get("probabilities") or {}).items()
    }

    if answer_type == "choice":
        return Answer(
            type="choice",
            choice=get("choice"),
            probabilities=probabilities,
            confidence=float(get("confidence") or 0.0),
        )

    if answer_type == "score":
        legend = {str(key): str(value) for key, value in (get("legend") or {}).items()}
        value = get("score")
        return Answer(
            type="score",
            score=float(value) if value is not None else None,
            probabilities=probabilities,
            legend=legend,
            confidence=float(get("confidence") or 0.0),
        )

    logger.warning(f"Unknown answer type: {answer_type!r}")
    return Answer(type=str(answer_type))


def _sdk_importable() -> bool:
    try:
        import typesafe_sdk  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        logger.debug(f"未安装 {SDK_PACKAGE}，系统一不可用")
        return False
    return True


class JevEvaluator:
    """Jev（TypeSafe System One）客户端

    惰性导入官方 SDK：未安装时构造本对象不会报错，available() 返回 False，
    pipeline 会自动回退到系统二 —— 项目其余功能零影响。

    重试由 SDK 的 RetryPolicy（tenacity + 识别 Retry-After 头）负责，
    覆盖 429/529 的指数退避，本类不自己实现重试。
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ):
        # Key 只从环境变量或显式参数拿 —— 不要复制进 config.py
        self._api_key = (
            api_key if api_key is not None else os.environ.get(API_KEY_ENV, "").strip()
        )
        self._model = model or os.environ.get("TYPESAFE_DEFAULT_MODEL", "").strip() or None
        self._base_url = base_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: Any = None
        self.usage = TokenUsage()
        self.last_model: str = ""
        self.last_question_count: int = 0

    # ---- 标识与可用性 ----

    @property
    def engine_id(self) -> str:
        return f"jev:{self._model or 'latest'}"

    def available(self) -> bool:
        if not self._api_key:
            logger.debug(f"{API_KEY_ENV} 未设置，系统一不可用")
            return False
        return _sdk_importable()

    # ---- 调用 ----

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from typesafe_sdk import TypeSafeClient  # type: ignore[import-not-found]
        except ImportError as exc:
            raise System1Unavailable(f"未安装 TypeSafe SDK。{SDK_INSTALL_HINT}") from exc
        if not self._api_key:
            raise System1Unavailable(
                f"缺少 API Key：请设置环境变量 {API_KEY_ENV}（不要写进 config.py）"
            )
        kwargs: dict[str, Any] = {"api_key": self._api_key}
        if self._model:
            kwargs["model"] = self._model
        if self._base_url:
            kwargs["base_url"] = self._base_url
        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        if self._max_retries is not None:
            from typesafe_sdk import RetryPolicy  # type: ignore[import-not-found]
            kwargs["retry"] = RetryPolicy(max_retries=self._max_retries)
        self._client = TypeSafeClient(**kwargs)
        logger.info(f"JevEvaluator ready: engine_id={self.engine_id}")
        return self._client

    def evaluate(self, state: Any, questions: Mapping[str, Any]) -> EvalResponse:
        if not questions:
            raise System1RequestError("questions 不能为空")
        client = self._ensure_client()
        try:
            response = client.system_one(state=state, questions=dict(questions))
        except Exception as exc:  # noqa: BLE001 — 需要按类名归类后重抛
            error_type = classify_error(exc)
            message = f"Jev 调用失败（{type(exc).__name__}）: {exc}"
            if error_type is System1RequestError:
                raise System1RequestError(message) from exc
            raise System1Unavailable(message) from exc

        answers: dict[str, Answer] = {}
        for question_id, raw in (getattr(response, "answers", None) or {}).items():
            answers[question_id] = _to_answer(raw)

        usage = getattr(response, "usage", None)
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0) if usage else 0
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0) if usage else 0
        self.usage.prompt_tokens += input_tokens
        self.usage.completion_tokens += output_tokens
        self.usage.total_calls += 1
        self.last_model = str(getattr(response, "model", "") or "")
        self.last_question_count = len(questions)

        missing = [qid for qid in questions if qid not in answers]
        if missing:
            # 少答不算致命：调用方按"该题无答案"处理（会走门控升级）
            logger.warning(f"Jev 未返回 {len(missing)} 道题的答案: {missing[:5]}")

        logger.info(
            f"Jev: {len(questions)} questions -> {len(answers)} answers | "
            f"model={self.last_model} in={input_tokens} out={output_tokens}"
        )
        return EvalResponse(
            model=self.last_model,
            answers=answers,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

    def close(self) -> None:
        if self._client is not None and hasattr(self._client, "close"):
            self._client.close()
        self._client = None


# ============================================================
# 离线实现（测试 / 无 Key 演示）
# ============================================================


class MockEvaluator:
    """离线可用的假系统一

    两种用法：
      1. handler(state, questions) -> {question_id: 原始答案 dict}   —— 灵活
      2. answers={question_id: 原始答案 dict}                        —— 静态

    让全部链路（fastpath / categorical_system1 / pipeline）不依赖网络与 Key 就能测试，
    符合 tests/AGENTS.md 的"测试不调用真实 AI"约定。
    """

    def __init__(
        self,
        handler: Any = None,
        answers: Mapping[str, Any] | None = None,
        engine_id: str = "mock:jev",
        available: bool = True,
        model: str = "jev-mock",
        fail_with: BaseException | None = None,
    ):
        self._handler = handler
        self._answers = dict(answers or {})
        self._engine_id = engine_id
        self._available = available
        self._fail_with = fail_with
        self.model = model
        self.usage = TokenUsage()
        self.call_log: list[tuple[Any, dict[str, Any]]] = []

    @property
    def engine_id(self) -> str:
        return self._engine_id

    def available(self) -> bool:
        return self._available

    def evaluate(self, state: Any, questions: Mapping[str, Any]) -> EvalResponse:
        if self._fail_with is not None:
            raise self._fail_with
        if not self._available:
            raise System1Unavailable("mock evaluator 标记为不可用")
        if not questions:
            raise System1RequestError("questions 不能为空")

        self.call_log.append((state, dict(questions)))
        raw_answers = self._handler(state, questions) if self._handler else self._answers

        # 只认被问到的问题（模拟真实引擎：不会凭空多答）
        answers = {
            question_id: _to_answer(raw_answers[question_id])
            for question_id in questions
            if question_id in raw_answers
        }
        self.usage.total_calls += 1
        return EvalResponse(model=self.model, answers=answers, input_tokens=0, output_tokens=0)
