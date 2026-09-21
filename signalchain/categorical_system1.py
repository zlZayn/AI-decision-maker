"""分类变量链路系统一通道 —— noul 判有序性 + score 定顺序

替换原来的两层文本协议
----------------------
  原来第一层：让 LLM 输出 "gender,education" 或字符串 "无"，再按逗号拆分求交集
  原来第二层：让 LLM 输出 "education:小学>本科"，再拆分号/冒号/大于号并归一中文标点

  现在第一层：每个字段 1 个 noul（是不是分类变量）
  现在第二层：每个变量 1 个 noul（取值之间有没有公认顺序）+ 每个取值 1 个 score（在刻度上的位置）

两个关键设计决定
----------------
1. **有序性用 noul 判，顺序用 score 求，两者放在同一次请求里**（speculative fan-out），
   最后由代码决定用哪些答案。这是官方文档推荐的用法：
   把代码可能用到的每个问题都问出来，让代码决定哪些答案有用。

2. **为什么不能只靠 score 的离散度**：实测（jev-1.13.0）发现，
   只用"分数离散度 >= 阈值 且 确定度 >= 阈值"判有序，会把"男/女"误判为有序 ——
   模型给出 男=0.70 / 女=3.48，离散度 2.78、确定度 0.59，两项都过线。
   而加一道 noul 门（取值之间是否存在公认的高低顺序）能从源头掐掉这类假阳性，
   而且对无序变量还能省掉逐值打分的成本。
   实测对照：真有序变量（学历/满意度/病情）确定度 0.98~0.99，男/女只有 0.59，血型 0.43 —— 可分。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import pandas as pd

from signalchain.categorical import (
    CategoricalClassifier,
    ClassificationResult,
    extract_unique_values,
)
from signalchain.models import DataProfile, DecisionRecord, FieldProfile
from signalchain.system1 import (
    EvalResponse,
    Evaluator,
    GatePolicy,
    System1Unavailable,
    certainty,
    noul,
    score,
)

logger = logging.getLogger(__name__)

CAT_QUESTION_PREFIX = "cat:"
ORDINAL_QUESTION_PREFIX = "ordinal:"
LEVEL_QUESTION_PREFIX = "level:"

# 通用有序刻度：不绑定具体变量，所以同一个刻度能复用到任意变量上（composite scoring）
# 刻意不给具体语义（不说"低/高"指什么），只描述"在变量内部相对位置"。
ORDINALITY_LEVELS: tuple[str, ...] = (
    "该变量内部最低的一档",
    "偏低的一档",
    "中间档",
    "偏高的一档",
    "该变量内部最高的一档",
)


def _dataset_state(profile: DataProfile) -> dict[str, Any]:
    """两层请求共用的数据集概览"""
    return {
        "dataset": {
            "field_count": profile.field_count,
            "fields": [
                {
                    "name": field.name,
                    "type": field.type,
                    "distinct_count": len(field.samples),
                    "samples": list(field.samples[:8]),
                }
                for field in profile.fields
            ],
        }
    }


def cat_question_id(field_name: str) -> str:
    return f"{CAT_QUESTION_PREFIX}{field_name}"


def ordinal_question_id(field_name: str) -> str:
    return f"{ORDINAL_QUESTION_PREFIX}{field_name}"


def level_question_id(field_name: str, value: str) -> str:
    return f"{LEVEL_QUESTION_PREFIX}{field_name}={value}"


# ============================================================
# 问题构造
# ============================================================


def build_categorical_questions(profile: DataProfile) -> dict[str, Any]:
    """第一层：每个字段 1 个 noul —— 是不是分类变量

    官方提醒：定义不清时 noul 的 0.5 无法解释。所以正反例都写清楚，
    把"连续数值 / 唯一标识符 / 自由文本 / 时间"这几类明确排除掉。
    """
    questions: dict[str, Any] = {}
    for field_profile in profile.fields:
        questions[cat_question_id(field_profile.name)] = noul(
            {
                "field": {
                    "name": field_profile.name,
                    "type": field_profile.type,
                    "distinct_samples": list(field_profile.samples[:8]),
                    "distinct_count": len(field_profile.samples),
                },
                "question": (
                    "该字段是不是分类变量？分类变量 = 取值来自一个有限的离散类别集合，"
                    "而不是连续数值、唯一标识符、自由文本或时间。"
                    "注意：用 1/2/3 这种数字表示等级的字段仍然是分类变量。"
                ),
            },
            true_description=(
                "取值是有限个离散类别，例如 男/女、A/B/O、很不满意~很满意、"
                "或用 1/2/3 编码的等级"
            ),
            false_description=(
                "取值是连续数值（年龄、金额、评分小数）、唯一标识符（编号、订单号）、"
                "自由文本（地址、备注、邮箱）、药品名这类开放式名称、或时间日期"
            ),
        )
    return questions


def build_ordinal_questions(
    categorical_fields: Iterable[str],
    unique_values: Mapping[str, list[str]],
    max_levels: int = 12,
) -> dict[str, Any]:
    """第二层：每个变量 1 个 noul（有没有顺序）+ 每个取值 1 个 score（在哪一档）

    取值超过 max_levels 的变量不发 score 问题（问题数会爆），只留 noul —— 
    若 noul 判有序但没有分数可用，调用方会保守地判为无序。
    """
    questions: dict[str, Any] = {}
    for name in categorical_fields:
        values = list(unique_values.get(name, []))
        if len(values) < 2:
            continue
        questions[ordinal_question_id(name)] = noul(
            {
                "variable": name,
                "values": values,
                "question": (
                    "这个变量的取值之间，是否存在公认的高低 / 程度 / 等级顺序？"
                    "判断依据是这些取值在社会常识或行业惯例中的顺序，"
                    "而不是它们在数据里出现的先后。"
                ),
            },
            true_description=(
                "存在公认顺序，例如 学历（小学<初中<本科<硕士）、"
                "满意度（很不满意<一般<很满意）、病情（轻<中<重）"
            ),
            false_description=(
                "不存在公认顺序，只是并列的类别，例如 性别（男/女）、"
                "血型（A/B/O/AB）、城市名、颜色"
            ),
        )
        if len(values) > max_levels:
            continue
        for value in values:
            questions[level_question_id(name, value)] = score(
                {
                    "variable": name,
                    "all_values_in_this_variable": values,
                    "value": value,
                    "question": (
                        "在这个变量的语义刻度上，该取值处于什么位置？"
                        "只依据它在变量内部相对其他取值的高低排序，"
                        "不要依据它在现实世界中的绝对数值。"
                    ),
                },
                list(ORDINALITY_LEVELS),
            )
    return questions


# ============================================================
# 结果
# ============================================================


@dataclass
class System1Classification:
    """分类链路的系统一产物"""

    result: ClassificationResult
    decisions: list[DecisionRecord] = field(default_factory=list)
    borderline: list[str] = field(default_factory=list)
    scores: dict[str, dict[str, float]] = field(default_factory=dict)
    """有序变量 -> {取值: 分数}，便于人工核对顺序是否合理"""

    model: str = ""

    def summary(self) -> str:
        lines = []
        for name, order in self.result.ordinal.items():
            scored = self.scores.get(name, {})
            chain = " < ".join(f"{value}({scored.get(value, 0):.2f})" for value in order)
            lines.append(f"  [有序] {name}: {chain}")
        for name in self.result.nominal:
            lines.append(f"  [无序] {name}")
        if self.borderline:
            lines.append(f"  borderline（置信度不足）: {', '.join(self.borderline)}")
        return "\n".join(lines)


# ============================================================
# 分类器
# ============================================================


class System1CategoricalClassifier:
    """分类变量识别与定序（系统一主路径）

    返回 None 表示"这次别用系统一"，调用方应回退到 CategoricalClassifier（系统二）。
    """

    def __init__(
        self,
        evaluator: Evaluator,
        fallback: CategoricalClassifier | None = None,
        policy: GatePolicy | None = None,
    ):
        self.evaluator = evaluator
        self.fallback = fallback
        self.policy = policy or GatePolicy()
        self.decisions: list[DecisionRecord] = []

    # ---- 对外入口 ----

    def classify(
        self, df: pd.DataFrame, profile: DataProfile
    ) -> System1Classification | None:
        if self.evaluator is None or not self.evaluator.available():
            logger.info("系统一不可用，回退系统二分类器")
            return None
        if profile.field_count == 0:
            return None

        self.decisions = []
        state = _dataset_state(profile)

        # ---- 第一层：noul 筛选分类变量 ----
        first = self._evaluate(state, build_categorical_questions(profile))
        if first is None:
            return None

        categorical_fields, borderline = self._select_categorical_fields(first, profile)
        logger.info(f"系统一第一层：分类变量 = {categorical_fields}（borderline={borderline}）")

        if not categorical_fields:
            return System1Classification(
                result=ClassificationResult(ordinal={}, nominal=[]),
                decisions=list(self.decisions),
                borderline=borderline,
                model=first.model,
            )

        # ---- 脚本：提取唯一值（不修改原 df）----
        unique_values = extract_unique_values(df, categorical_fields)

        # ---- 第二层：noul 判有序性 + score 定顺序（同一次请求）----
        second = self._evaluate(
            state,
            build_ordinal_questions(
                categorical_fields, unique_values, max_levels=self.policy.max_levels
            ),
        )
        if second is None:
            return None

        ordinal, scores_by_name = self._classify_levels(
            second, categorical_fields, unique_values
        )
        result = ClassificationResult(
            ordinal=ordinal,
            nominal=[name for name in categorical_fields if name not in ordinal],
        )

        # ---- 升级：把 borderline 字段交给系统二复判（只采纳这些字段的结论）----
        if borderline and self.fallback is not None:
            result = self._merge_fallback(df, profile, result, borderline)

        return System1Classification(
            result=result,
            decisions=list(self.decisions),
            borderline=borderline,
            scores=scores_by_name,
            model=second.model,
        )

    # ---- 步骤 ----

    def _select_categorical_fields(
        self, response: EvalResponse, profile: DataProfile
    ) -> tuple[list[str], list[str]]:
        """第一层：逐字段读 noul，返回（分类变量, borderline）

        概率过半即认定为分类变量；把握不足的记进 borderline，交给系统二复判（若启用）。
        """
        categorical_fields: list[str] = []
        borderline: list[str] = []
        for field in profile.fields:
            answer = response.answers.get(cat_question_id(field.name))
            probability = answer.noul if answer is not None and answer.noul is not None else 0.0
            cert = certainty(answer) if answer is not None else 0.0
            is_categorical = probability >= 0.5
            verdict = self.policy.verdict(cert)
            if verdict == "escalate":
                borderline.append(field.name)
            self.decisions.append(
                DecisionRecord(
                    question_id=cat_question_id(field.name),
                    subject=field.name,
                    chosen="分类变量" if is_categorical else "非分类",
                    certainty=cert,
                    engine="system1",
                    probabilities={"P(分类变量)": probability},
                    verdict=verdict,
                )
            )
            if is_categorical:
                categorical_fields.append(field.name)
        return categorical_fields, borderline

    def _classify_levels(
        self,
        response: EvalResponse,
        categorical_fields: list[str],
        unique_values: Mapping[str, list[str]],
    ) -> tuple[dict[str, list[str]], dict[str, dict[str, float]]]:
        """第二层：逐变量判有序并定序，返回（有序变量→顺序, 有序变量→分数）"""
        ordinal: dict[str, list[str]] = {}
        scores_by_name: dict[str, dict[str, float]] = {}
        for name in categorical_fields:
            answer = response.answers.get(ordinal_question_id(name))
            probability = answer.noul if answer is not None and answer.noul is not None else 0.0
            cert = certainty(answer) if answer is not None else 0.0
            verdict = self.policy.verdict(cert)

            scores, level_certainties = self._level_scores(
                response, name, list(unique_values.get(name, []))
            )
            spread = (max(scores.values()) - min(scores.values())) if scores else 0.0
            mean_level_certainty = (
                sum(level_certainties) / len(level_certainties) if level_certainties else 0.0
            )
            is_ordinal = self._is_ordinal(
                probability, cert, scores, spread, mean_level_certainty
            )

            self.decisions.append(
                DecisionRecord(
                    question_id=ordinal_question_id(name),
                    subject=f"{name}（有序性）",
                    chosen="有序" if is_ordinal else "无序",
                    certainty=cert,
                    engine="system1",
                    probabilities={"P(有序)": probability},
                    verdict=verdict,
                )
            )
            logger.info(
                f"系统一第二层：{name} 判为{'有序' if is_ordinal else '无序'} "
                f"(P={probability:.3f} cert={cert:.3f} spread={spread:.2f} "
                f"level_cert={mean_level_certainty:.3f})"
                + (f" -> {sorted(scores, key=scores.get)}" if is_ordinal else "")
            )
            if is_ordinal and scores:
                order = sorted(scores, key=scores.get)
                ordinal[name] = order
                scores_by_name[name] = {value: scores[value] for value in order}
        return ordinal, scores_by_name

    @staticmethod
    def _level_scores(
        response: EvalResponse, name: str, values: list[str]
    ) -> tuple[dict[str, float], list[float]]:
        """逐取值读 score，返回（取值 → 刻度位置, 各取值的把握程度）"""
        scores: dict[str, float] = {}
        certainties: list[float] = []
        for value in values:
            answer = response.answers.get(level_question_id(name, value))
            if answer is None or answer.score is None:
                continue
            scores[value] = float(answer.score)
            certainties.append(certainty(answer))
        return scores, certainties

    def _is_ordinal(
        self,
        probability: float,
        cert: float,
        scores: Mapping[str, float],
        spread: float,
        mean_level_certainty: float,
    ) -> bool:
        """有序的四个必要条件

        1. noul 认为有公认顺序（概率过半）   2. noul 本身够确定
        3. 分数拉得开                       4. 每个取值的分数够确定
        第 1、2 条就是从源头掐掉"男/女被判有序"这类假阳性的那道门。
        """
        return (
            probability >= 0.5
            and cert >= self.policy.escalate
            and len(scores) >= 2
            and spread >= self.policy.min_spread
            and mean_level_certainty >= self.policy.accept
        )

    # ---- 内部 ----

    def _evaluate(
        self, state: Any, questions: Mapping[str, Any]
    ) -> EvalResponse | None:
        if not questions:
            return EvalResponse(model="")
        try:
            return self.evaluator.evaluate(state, questions)
        except System1Unavailable as exc:
            logger.warning(f"系统一不可用，回退系统二分类器：{exc}")
            return None

    def _merge_fallback(
        self,
        df: pd.DataFrame,
        profile: DataProfile,
        result: ClassificationResult,
        borderline: list[str],
    ) -> ClassificationResult:
        """borderline 字段交给系统二复判，只采纳这些字段的结论

        复用既有的 CategoricalClassifier（它会对整个数据集做两次调用），
        但只把结果用在不确定的字段上 —— 确定的字段保持系统一的答案，
        避免"因为一个字段不确定就整份推翻重来"。
        """
        logger.info(f"{len(borderline)} 个字段置信度不足，交系统二复判：{borderline}")
        try:
            fallback_result = self.fallback.classify(df, profile)
        except Exception as exc:  # noqa: BLE001 — 升级失败不应中断链路
            logger.error(f"系统二复判失败，沿用系统一结论：{exc}")
            return result

        borderline_set = set(borderline)
        ordinal = dict(result.ordinal)
        nominal = list(result.nominal)
        for name in borderline:
            if name in fallback_result.ordinal:
                ordinal[name] = fallback_result.ordinal[name]
                if name in nominal:
                    nominal.remove(name)
            elif name in fallback_result.nominal:
                if name not in nominal:
                    nominal.append(name)
                ordinal.pop(name, None)
        for record in self.decisions:
            subject = record.subject.split("（")[0]
            if subject in borderline_set:
                record.engine = "system2"
                record.escalated = True
        return ClassificationResult(ordinal=ordinal, nominal=nominal)
