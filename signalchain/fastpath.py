"""清洗链路系统一快通道 —— 一次请求同时定场景与字段信号

原来：Stage 1（LLM 定场景）→ Stage 2（本地组装 prompt）→ Stage 3（LLM 定字段）
现在：一次 Jev 请求拿到 1 个场景答案 + N 个字段答案，本地条件化后组装信号序列。

为什么可以合并
--------------
官方 Speculative fan-out 模式：把代码可能用到的每个问题都问出来，让代码决定哪些答案有用。
同一请求内的问题并行评估，加问题几乎不增加响应时间。

必须说清的一个语义
------------------
同一请求内的问题彼此独立（一个答案不会成为另一个问题的上下文）。
所以字段问题是用"全 13 个信号码"问出来的**边缘分布**，
必须由本地条件化到场景的 valid_codes 上（system1.condition_choice）。
这是显式的建模选择，不是引擎保证。
需要严格条件化时用 GatePolicy.scene_strategy="two_pass"（多一次往返，字段问题直接带场景码集）。

与现有链路的关系
----------------
不改变 stage2 / stage4 / stage5 的任何契约：本模块的产物就是
(scene_code, signal_sequence) —— 正好是 SignalChainPipeline 需要的两样东西。
低置信字段的升级复用 stage2 的 prompt 模板与 stage3 的校验器，不另写一套。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from signalchain.models import (
    CODE_LABELS,
    VALID_SCENE_CODES,
    DataProfile,
    DecisionRecord,
    FieldProfile,
)
from signalchain.stage1_scene import build_scene_prompt, validate_scene_code
from signalchain.stage2_router import (
    FIELD_NAME_HINTS,
    ROUTING_TABLE,
    build_field_semantic_prompt,
)
from signalchain.stage3_semantic import validate_field_signal_sequence
from signalchain.system1 import (
    Answer,
    EvalResponse,
    Evaluator,
    GatePolicy,
    System1Unavailable,
    argmax,
    choice,
    certainty,
    condition_choice,
)

logger = logging.getLogger(__name__)

SCENE_QUESTION_ID = "scene"
FIELD_QUESTION_PREFIX = "field:"

# 信号码的固定展示顺序 —— 保证 criteria 顺序稳定（可复现）
CODE_ORDER: tuple[str, ...] = ("I", "G", "A", "D", "N", "C", "T", "M", "E", "P", "L", "R", "X")

# 各信号码的判据描述（系统一视角：模型读的是这些中文描述，答的是 ASCII 码）
CODE_HINTS: dict[str, str] = {
    "I": "唯一标识符 / 主键，如 P001、U001、T001",
    "G": "性别，取值如 男、女、M、F、Male、Female",
    "A": "年龄（岁数），可能含中文数字或约数，如 30、三十、约25",
    "D": "医疗科室名称，如 心内科、外科、ICU",
    "N": "药品或药物名称，如 阿莫西林、头孢、甲硝唑",
    "C": "诊断编码（ICD-10 一类）",
    "T": "时间日期，如 2024-01-15、2024/01/20",
    "M": "金额或货币数值，可能带货币符号与千分位，如 ¥1,000.50、$200",
    "E": "电子邮箱地址（含 @）",
    "P": "手机号或电话号码，如 138-0013-8000",
    "L": "日志级别，如 DEBUG、INFO、WARN、ERROR",
    "R": "经纬度坐标（小数值）",
    "X": "以上都不是（自由文本、地址、备注等）",
}


def field_question_id(field_name: str) -> str:
    return f"{FIELD_QUESTION_PREFIX}{field_name}"


# ============================================================
# 问题构造
# ============================================================


def code_criteria(
    codes: Iterable[str] | None = None, verbose: bool = False
) -> dict[str, str]:
    """信号码选项

    verbose=False（默认）：只给标签。实测（jev-1.13.0，medical.csv，6 个问题）
      输入 token 从 3745 降到 2335（−38%），而场景与 5 个字段码的答案完全不变。
      criteria 是每次请求里最大的一块重复内容，所以它是最值得省的。
    verbose=True：附上判据描述与示例。字段名高度异常、样本难以判断时再开。

    从 models.CODE_LABELS 派生，所以新增信号码只需按既有约定同步 models.py 与
    operations/，本模块不会成为"第 5 个必须同步的地方"——
    没有对应 CODE_HINTS 的码会自动退化为只显示标签，不会 KeyError。
    """
    selected = [c for c in CODE_ORDER if codes is None or c in set(codes)]
    if codes is not None:
        selected += [c for c in codes if c not in CODE_ORDER]
    criteria: dict[str, str] = {}
    for code in selected:
        label = CODE_LABELS.get(code, code)
        hint = CODE_HINTS.get(code, "") if verbose else ""
        criteria[code] = f"{label}：{hint}" if hint else label
    return criteria


def scene_criteria() -> dict[str, str]:
    """场景选项：从 ROUTING_TABLE 派生 —— 新增场景不需要再改这里

    每个场景的描述里列出它支持的信号码，帮助模型把"场景"和"字段类型"两件事对齐。
    """
    criteria: dict[str, str] = {}
    for scene_code in sorted(VALID_SCENE_CODES):
        config = ROUTING_TABLE.get(scene_code)
        if config is None:
            continue
        labels = "、".join(
            CODE_LABELS.get(c, c) for c in CODE_ORDER if c in config.valid_codes
        )
        criteria[scene_code] = f"{config.scene_name}（典型字段：{labels}）"
    return criteria


def build_state(profile: DataProfile) -> dict[str, Any]:
    """共享上下文：数据集概览。场景判断主要看它。"""
    return {
        "dataset": {
            "field_count": profile.field_count,
            "type_summary": profile.type_summary,
            "fields": [
                {
                    "name": f.name,
                    "type": f.type,
                    "samples": list(f.samples[:5]),
                }
                for f in profile.fields
            ],
        }
    }


def field_instructions(field: FieldProfile, hint_code: str | None = None) -> dict[str, Any]:
    """单字段问题的结构化 instructions

    为什么把字段信息放进 instructions，而不是用 state 路径引用：
    真实字段名含斜杠、连字符、大写等（如 SEX/Gender、E-MAIL、PHONE_number），
    而官方推荐用反引号点路径引用 state 里的字段（如 fields.PID）。
    路径转义容易出错，所以让每道题自带结构化的被评估材料，问题与数据放在同一个对象里。
    """
    payload: dict[str, Any] = {
        "field": {
            "name": field.name,
            "type": field.type,
            "samples": list(field.samples[:5]),
        },
        "question": "该字段的语义类型对应哪个代码？字段名是主要依据，样本值用于确认。",
    }
    if hint_code:
        payload["local_hint"] = f"本地字段名预查表建议 {hint_code}，仅供参考，可以推翻"
    return payload


def build_questions(
    profile: DataProfile,
    include_scene: bool = True,
    codes: Iterable[str] | None = None,
    verbose_criteria: bool = False,
) -> dict[str, Any]:
    """构造一次请求里的全部问题

    Args:
        profile: 数据画像
        include_scene: 是否包含场景问题
        codes: 字段问题的候选码集。None = 全 13 码（one_pass 用）；
               传入场景的 valid_codes = 严格条件化（two_pass 用）
        verbose_criteria: 是否在 criteria 里附带判据描述（见 code_criteria）
    """
    questions: dict[str, Any] = {}
    if include_scene:
        questions[SCENE_QUESTION_ID] = choice(
            "整个数据集属于哪一种业务场景？以字段名的组合为主要依据。",
            scene_criteria(),
        )

    criteria = code_criteria(codes, verbose=verbose_criteria)
    used_ids: set[str] = set()
    for field_profile in profile.fields:
        question_id = field_question_id(field_profile.name)
        if question_id in used_ids:
            # DataFrame 允许重名列：加后缀避免问题 id 冲突（否则两列会拿到同一个答案）
            suffix = 2
            while f"{question_id}#{suffix}" in used_ids:
                suffix += 1
            logger.warning(f"重复字段名 {field_profile.name!r}，问题 id 追加后缀 #{suffix}")
            question_id = f"{question_id}#{suffix}"
        used_ids.add(question_id)
        hint = FIELD_NAME_HINTS.get(field_profile.name.lower().strip())
        questions[question_id] = choice(field_instructions(field_profile, hint), criteria)
    return questions


# ============================================================
# 结果
# ============================================================


@dataclass(frozen=True)
class _FieldOutcome:
    """单个字段的系统一判定结果（判定阶段与收尾阶段之间的值对象）

    分开的理由：判定只关心"答案 + 把握程度"，收尾才决定"采用 / 升级 / 落默认值"。
    合成一个对象后，决策记录可以一次成型，不必先建再原地改。
    """

    field: FieldProfile
    code: str
    certainty: float
    verdict: str
    probabilities: dict[str, float] = field(default_factory=dict)


@dataclass
class FastPathResult:
    """系统一快通道的产物（形状与系统二路径完全一致）"""

    scene_code: str
    signal_sequence: str
    decisions: list[DecisionRecord] = field(default_factory=list)
    escalated_fields: list[str] = field(default_factory=list)
    scene_certainty: float = 0.0
    engine: str = "system1"
    model: str = ""

    def summary(self) -> str:
        lines = [
            f"scene={self.scene_code} (certainty={self.scene_certainty:.3f})",
            f"signals={self.signal_sequence}",
        ]
        for record in self.decisions:
            lines.append("  " + record.summary())
        if self.escalated_fields:
            lines.append(f"  escalated: {', '.join(self.escalated_fields)}")
        return "\n".join(lines)


# ============================================================
# 决策器
# ============================================================


class System1Decider:
    """系统一决策器：一次请求定场景与字段，低置信自动升级系统二

    返回 None 表示"这次别用系统一"—— pipeline 会原路走系统二，
    所以系统一永远不是单点故障。
    """

    def __init__(
        self,
        evaluator: Evaluator,
        fallback_client: Any = None,
        policy: GatePolicy | None = None,
        verbose_criteria: bool = False,
    ):
        """
        Args:
            evaluator: 系统一引擎（JevEvaluator / MockEvaluator / 任何满足协议的实现）
            fallback_client: 系统二客户端（ai_client.AIClient）。None 表示不升级，
                             低置信字段直接落 X（pass_through）并记日志。
            policy: 门控策略
            verbose_criteria: criteria 是否附带判据描述。默认 False（实测省 38% 输入
                              token 且答案不变）；字段名高度异常、样本难以判断时再开。
        """
        self.evaluator = evaluator
        self.fallback = fallback_client
        self.policy = policy or GatePolicy()
        self.verbose_criteria = verbose_criteria

    # ---- 对外入口 ----

    def decide(self, profile: DataProfile) -> FastPathResult | None:
        if self.evaluator is None or not self.evaluator.available():
            logger.info("系统一不可用，回退系统二原路径")
            return None
        if profile.field_count == 0:
            logger.info("空数据集，无需系统一决策")
            return None

        state = build_state(profile)
        one_pass = self.policy.scene_strategy != "two_pass"

        # ---- 第 1 次请求：场景（one_pass 时字段问题搭同一次请求的车）----
        response = self._evaluate(state, self._opening_questions(profile, one_pass))
        if response is None:
            return None

        scene_code, scene_certainty, scene_record = self._resolve_scene(response, profile)
        scene_config = ROUTING_TABLE.get(scene_code, ROUTING_TABLE["S0"])

        # ---- two_pass：用该场景的合法码重新问一遍字段 ----
        if not one_pass:
            response = self._evaluate(state, self._field_questions(profile, scene_config))
            if response is None:
                return None

        # ---- 逐字段判定 + 低置信收尾 ----
        outcomes = self._decide_fields(response, profile, scene_config, one_pass)
        signal_sequence, field_records, escalated_names = self._finalize(
            outcomes, scene_config, scene_code
        )
        logger.info(f"系统一快通道：scene={scene_code} signals={signal_sequence}")

        return FastPathResult(
            scene_code=scene_code,
            signal_sequence=signal_sequence,
            decisions=[scene_record, *field_records],
            escalated_fields=escalated_names,
            scene_certainty=scene_certainty,
            engine="system1",
            model=getattr(response, "model", ""),
        )

    # ---- 步骤 ----

    def _opening_questions(self, profile: DataProfile, one_pass: bool) -> dict[str, Any]:
        """第 1 次请求的问题

        one_pass：场景 + 全部字段一起问（speculative fan-out）
        two_pass：只问场景，字段留到知道场景后再用合法码集问
        """
        if one_pass:
            return build_questions(
                profile, include_scene=True, verbose_criteria=self.verbose_criteria
            )
        return {SCENE_QUESTION_ID: self._scene_question()}

    def _field_questions(self, profile: DataProfile, scene_config: Any) -> dict[str, Any]:
        """严格模式下的字段问题：criteria 限定为该场景的合法码"""
        return build_questions(
            profile,
            include_scene=False,
            codes=scene_config.valid_codes,
            verbose_criteria=self.verbose_criteria,
        )

    def _decide_fields(
        self,
        response: EvalResponse,
        profile: DataProfile,
        scene_config: Any,
        one_pass: bool,
    ) -> list["_FieldOutcome"]:
        """逐字段：条件化 → 定码 → 门控。只判定，不改结果。"""
        outcomes: list[_FieldOutcome] = []
        for field in profile.fields:
            answer = self._find_field_answer(response, field)
            if answer is None:
                logger.info(f"字段 {field.name!r} 无答案，转入升级候选")
                outcomes.append(_FieldOutcome(field, code="X", certainty=0.0, verdict="escalate"))
                continue

            if one_pass:
                # 同请求内的字段问题是全码集上的边缘分布，按场景条件化后才可解释
                answer = condition_choice(answer, scene_config.valid_codes)
            cert = certainty(answer)
            outcomes.append(
                _FieldOutcome(
                    field=field,
                    code=answer.choice or "X",
                    certainty=cert,
                    verdict=self.policy.verdict(cert),
                    probabilities=dict(answer.probabilities),
                )
            )
        return outcomes

    def _finalize(
        self,
        outcomes: list["_FieldOutcome"],
        scene_config: Any,
        scene_code: str,
    ) -> tuple[str, list[DecisionRecord], list[str]]:
        """低置信项收尾：批量升级，或落保守默认值

        两条路的记录必须区分开 —— 没升级就不许写 escalated=True / engine=system2。
        记录在这里一次成型，不做"先建后改"。
        """
        uncertain = [o for o in outcomes if o.verdict == "escalate"]
        resolved: dict[str, str] = {}

        if uncertain and self.fallback is not None:
            resolved = self._escalate(
                [o.field for o in uncertain], scene_config, scene_code
            )
        elif uncertain:
            logger.warning(
                f"{len(uncertain)} 个字段置信度不足且未启用系统二升级，"
                f"落保守默认值 X（pass_through）: {[o.field.name for o in uncertain]}"
            )

        codes: list[str] = []
        records: list[DecisionRecord] = []
        escalated: list[str] = []
        for outcome in outcomes:
            name = outcome.field.name
            if outcome.verdict != "escalate":
                code, engine, was_escalated = outcome.code, "system1", False
            elif name in resolved:
                # 系统二真的给了答案
                code, engine, was_escalated = resolved[name], "system2", True
                escalated.append(name)
            else:
                # 无系统二可用，或升级调用失败 → 不猜、不改
                code, engine, was_escalated = "X", "system1", False

            codes.append(code)
            records.append(
                DecisionRecord(
                    question_id=field_question_id(name),
                    subject=name,
                    chosen=code,
                    certainty=outcome.certainty,
                    engine=engine,
                    probabilities=outcome.probabilities,
                    escalated=was_escalated,
                    verdict=outcome.verdict,
                )
            )
        return "".join(codes), records, escalated

    def _scene_question(self) -> dict[str, Any]:
        return choice(
            "整个数据集属于哪一种业务场景？以字段名的组合为主要依据。",
            scene_criteria(),
        )

    # ---- 内部 ----

    def _evaluate(self, state: Any, questions: Mapping[str, Any]) -> EvalResponse | None:
        """发请求；引擎不可用则返回 None（降级信号）

        System1RequestError 刻意不在这里捕获 —— 那意味着我们的问题构造写错了，
        直接抛出便于定位。SignalChainPipeline 会在更外层统一降级，
        保证"用户的数据照常清洗完"，同时开发者能看到真实原因。
        """
        try:
            return self.evaluator.evaluate(state, questions)
        except System1Unavailable as exc:
            logger.warning(f"系统一不可用，回退系统二原路径：{exc}")
            return None

    def _find_field_answer(self, response: EvalResponse, field_profile: FieldProfile) -> Answer | None:
        answer = response.answers.get(field_question_id(field_profile.name))
        if answer is not None:
            return answer
        # 重名列的后缀形式
        for question_id, candidate in response.answers.items():
            if question_id.startswith(field_question_id(field_profile.name) + "#"):
                return candidate
        return None

    def _resolve_scene(
        self, response: EvalResponse, profile: DataProfile
    ) -> tuple[str, float, DecisionRecord]:
        """定场景：系统一先判，置信不足则升级系统二"""
        answer = response.answers.get(SCENE_QUESTION_ID)
        cert = certainty(answer) if answer is not None else 0.0
        choice_value = (answer.choice if answer is not None else None) or "S0"
        verdict = self.policy.verdict(cert)

        record = DecisionRecord(
            question_id=SCENE_QUESTION_ID,
            subject="scene",
            chosen=choice_value,
            certainty=cert,
            engine="system1",
            probabilities=dict(answer.probabilities) if answer is not None else {},
            verdict=verdict,
        )

        if verdict != "escalate" or self.fallback is None:
            if verdict == "escalate":
                # 无系统二可升级：保留系统一的答案，但用校验器兜底（与旧行为一致）
                logger.warning(f"场景置信度 {cert:.3f} 偏低且未配置系统二，沿用 {choice_value}")
                choice_value = validate_scene_code(choice_value)
            return choice_value, cert, record

        # 升级：复用系统二原有的场景 prompt 与校验器，不另写一套
        logger.info(f"场景置信度 {cert:.3f} 不足，升级系统二裁决")
        try:
            raw = self.fallback.call(build_scene_prompt(profile))
        except Exception as exc:  # noqa: BLE001 — 升级失败不应中断整条链路
            logger.error(f"系统二场景裁决失败，沿用系统一答案：{exc}")
            return validate_scene_code(choice_value), cert, record

        escalated_code = validate_scene_code(raw)
        record.chosen = escalated_code
        record.engine = "system2"
        record.escalated = True
        return escalated_code, cert, record

    def _escalate(
        self,
        uncertain: list[FieldProfile],
        scene_config: Any,
        scene_code: str,
    ) -> dict[str, str]:
        """把不确定字段批量交给系统二（一次调用处理全部）

        复用 stage2 的 prompt 模板与 stage3 的校验器：
        做法是把不确定字段装成一个"只有这些字段"的 DataProfile，
        直接喂给既有的 build_field_semantic_prompt —— 于是不新增任何 prompt 代码，
        系统一升级后的提问方式与今天完全一致。
        """
        subset = DataProfile(fields=list(uncertain))
        prompt = build_field_semantic_prompt(subset, scene_config, scene_code)
        try:
            raw = self.fallback.call(prompt)
        except Exception as exc:  # noqa: BLE001 — 升级失败不应中断整条链路
            logger.error(f"系统二字段裁决失败，这些字段落 X：{exc}")
            return {}

        sequence = validate_field_signal_sequence(
            raw, len(uncertain), scene_config.valid_codes
        )
        logger.info(f"系统二批量裁决 {len(uncertain)} 个字段 -> {sequence}")
        return {f.name: code for f, code in zip(uncertain, sequence)}


