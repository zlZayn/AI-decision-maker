"""分类链路系统一通道测试 —— noul 判有序性 + score 定顺序

其中 test_gender_like_variable_is_nominal 是针对实测假阳性的回归测试：
只用"分数离散度"判有序时，"男/女"会被误判为有序（男 0.72 / 女 3.61，离散度 2.89）。
"""

import pandas as pd
import pytest

from signalchain.categorical import ClassificationResult
from signalchain.categorical_system1 import (
    ORDINALITY_LEVELS,
    System1CategoricalClassifier,
    build_categorical_questions,
    build_ordinal_questions,
    cat_question_id,
    level_question_id,
    ordinal_question_id,
)
from signalchain.models import DataProfile, FieldProfile
from signalchain.stage0_profile import extract_profile
from signalchain.system1 import (
    GatePolicy,
    MockEvaluator,
    System1Unavailable,
    choice_answer_dict,
    noul_answer_dict,
    score_answer_dict,
)


def peaked_score(value: float, peak: float = 0.95) -> dict:
    """造一个"档位判断很确定"的 score 答案

    默认 peak=0.95 → choice_confidence = (5 × 0.95 − 1) / 4 = 0.9375，过 0.80 的档位门。
    实测的真有序变量在 0.988~0.995，无序变量在 0.43~0.675 —— 0.80 落在中间的空档里。
    """
    peak_key = str(int(round(value)))
    rest = (1.0 - peak) / (len(ORDINALITY_LEVELS) - 1)
    probabilities = {
        str(index): (peak if str(index) == peak_key else rest)
        for index in range(len(ORDINALITY_LEVELS))
    }
    return score_answer_dict(value, ORDINALITY_LEVELS, probabilities)


def make_profile(names_and_samples) -> DataProfile:
    return DataProfile(
        fields=[
            FieldProfile(name=name, type="string", samples=samples, null_ratio=0.0)
            for name, samples in names_and_samples
        ]
    )


FRAME = pd.DataFrame(
    {
        "gender": ["男", "女", "男", "女"],
        "education": ["小学", "本科", "硕士", "高中"],
        "amount": ["1", "2", "3", "4"],
    }
)

CASE = {
    "gender": ["男", "女"],
    "education": ["小学", "高中", "本科", "硕士"],
    "amount": ["1", "2", "3", "4"],
}


def handler(state, questions):
    """假引擎：gender 无序、education 有序、amount 非分类变量"""
    answers = {
        cat_question_id("gender"): noul_answer_dict(0.97),
        cat_question_id("education"): noul_answer_dict(0.98),
        cat_question_id("amount"): noul_answer_dict(0.04),
        # 有序性 noul
        ordinal_question_id("gender"): noul_answer_dict(0.04),
        ordinal_question_id("education"): noul_answer_dict(0.98),
    }
    # gender：分数被拉开（离散度 2.89）且每档都很确定 —— 只差 noul 这一道门
    # 刻意用 peaked_score，让这个用例真正隔离"noul 门"的作用：
    # 离散度与档位确定度全部过线，唯一的拦截者是 P(有序)=0.04。
    answers[level_question_id("gender", "男")] = peaked_score(0.72)
    answers[level_question_id("gender", "女")] = peaked_score(3.61)
    # education：分数拉得开且每档都确定
    for value, score in (("小学", 0.01), ("高中", 2.01), ("本科", 2.99), ("硕士", 4.0)):
        answers[level_question_id("education", value)] = peaked_score(score)
    # amount 不是分类变量，不会问它的取值
    return answers


class TestQuestionBuilding:
    def test_one_noul_per_field(self):
        profile = make_profile([("gender", ["男", "女"]), ("amount", ["1", "2"])])
        questions = build_categorical_questions(profile)
        assert set(questions) == {cat_question_id("gender"), cat_question_id("amount")}
        assert all(q["type"] == "noul" for q in questions.values())

    def test_noul_defines_both_outcomes(self):
        """定义不清时 noul 的 0.5 无法解释，所以正反例都要写清"""
        profile = make_profile([("gender", ["男", "女"])])
        criteria = build_categorical_questions(profile)[cat_question_id("gender")]["criteria"]
        assert set(criteria) == {"true", "false"}

    def test_ordinal_questions_are_noul_plus_scores(self):
        questions = build_ordinal_questions(["education"], CASE, max_levels=12)
        assert ordinal_question_id("education") in questions
        assert questions[ordinal_question_id("education")]["type"] == "noul"
        for value in CASE["education"]:
            assert questions[level_question_id("education", value)]["type"] == "score"

    def test_single_value_variable_is_skipped(self):
        questions = build_ordinal_questions(["solo"], {"solo": ["only"]}, max_levels=12)
        assert questions == {}

    def test_too_many_levels_skips_scores_but_keeps_noul(self):
        values = [f"v{i}" for i in range(20)]
        questions = build_ordinal_questions(["wide"], {"wide": values}, max_levels=12)
        assert ordinal_question_id("wide") in questions
        assert not any(qid.startswith(level_question_id("wide", "")) for qid in questions)

    def test_score_criteria_is_ordered_and_within_limits(self):
        questions = build_ordinal_questions(["education"], CASE, max_levels=12)
        criteria = questions[level_question_id("education", "小学")]["criteria"]
        assert 2 <= len(criteria) <= 10  # 官方限制：2~10 档
        assert criteria == list(ORDINALITY_LEVELS)


class TestClassification:
    def test_end_to_end_classification(self):
        profile = extract_profile(FRAME)
        classifier = System1CategoricalClassifier(MockEvaluator(handler=handler))
        outcome = classifier.classify(FRAME, profile)

        assert outcome is not None
        assert outcome.result.ordinal == {"education": ["小学", "高中", "本科", "硕士"]}
        assert outcome.result.nominal == ["gender"]

    def test_gender_like_variable_is_nominal(self):
        """回归测试：分数离散度很大（2.89）但无公认顺序 → 必须判无序

        只用 spread + 档位确定度 判有序会在这里出错（实测确定度 0.675 也过了 0.55 的线），
        noul 门（P(有序)=0.04）才是真正起作用的那道判据。
        """
        profile = extract_profile(FRAME)
        outcome = System1CategoricalClassifier(MockEvaluator(handler=handler)).classify(
            FRAME, profile
        )
        assert "gender" not in outcome.result.ordinal
        assert "gender" in outcome.result.nominal

        gender_record = next(
            r for r in outcome.decisions if r.subject.startswith("gender（")
        )
        assert gender_record.chosen == "无序"
        assert gender_record.probabilities["P(有序)"] == pytest.approx(0.04)

    def test_scores_are_recorded_for_audit(self):
        profile = extract_profile(FRAME)
        outcome = System1CategoricalClassifier(MockEvaluator(handler=handler)).classify(
            FRAME, profile
        )
        scores = outcome.scores["education"]
        assert scores["小学"] < scores["高中"] < scores["本科"] < scores["硕士"]

    def test_non_categorical_field_is_excluded(self):
        profile = extract_profile(FRAME)
        outcome = System1CategoricalClassifier(MockEvaluator(handler=handler)).classify(
            FRAME, profile
        )
        record = next(r for r in outcome.decisions if r.subject == "amount")
        assert record.chosen == "非分类"

    def test_order_comes_from_sorting_not_insertion(self):
        """顺序必须由分数排序产生，与取值在数据里出现的先后无关"""
        frame = pd.DataFrame({"rank": ["高", "低", "中", "低"]})
        profile = extract_profile(frame)

        def ordered_handler(state, questions):
            return {
                cat_question_id("rank"): noul_answer_dict(0.95),
                ordinal_question_id("rank"): noul_answer_dict(0.95),
                level_question_id("rank", "低"): peaked_score(0.0),
                level_question_id("rank", "中"): peaked_score(2.0),
                level_question_id("rank", "高"): peaked_score(4.0),
            }

        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=ordered_handler)
        ).classify(frame, profile)
        assert outcome.result.ordinal == {"rank": ["低", "中", "高"]}

    def test_low_level_certainty_blocks_ordinal(self):
        """档位分数本身不确定时也不该判有序（第二道门）"""
        frame = pd.DataFrame({"x": ["a", "b", "c"]})
        profile = extract_profile(frame)
        flat = {str(i): 1.0 / len(ORDINALITY_LEVELS) for i in range(len(ORDINALITY_LEVELS))}

        def uncertain_handler(state, questions):
            return {
                cat_question_id("x"): noul_answer_dict(0.95),
                ordinal_question_id("x"): noul_answer_dict(0.95),
                level_question_id("x", "a"): score_answer_dict(0.0, ORDINALITY_LEVELS, flat),
                level_question_id("x", "b"): score_answer_dict(2.0, ORDINALITY_LEVELS, flat),
                level_question_id("x", "c"): score_answer_dict(4.0, ORDINALITY_LEVELS, flat),
            }

        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=uncertain_handler)
        ).classify(frame, profile)
        assert outcome.result.nominal == ["x"]

    def test_no_categorical_field_returns_empty(self):
        frame = pd.DataFrame({"amount": ["1", "2", "3"]})
        profile = extract_profile(frame)

        def none_handler(state, questions):
            return {cat_question_id("amount"): noul_answer_dict(0.03)}

        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=none_handler)
        ).classify(frame, profile)
        assert outcome.result == ClassificationResult(ordinal={}, nominal=[])

    def test_unavailable_returns_none(self):
        profile = extract_profile(FRAME)
        classifier = System1CategoricalClassifier(MockEvaluator(available=False))
        assert classifier.classify(FRAME, profile) is None

    def test_engine_error_returns_none(self):
        profile = extract_profile(FRAME)
        classifier = System1CategoricalClassifier(
            MockEvaluator(fail_with=System1Unavailable("不通"))
        )
        assert classifier.classify(FRAME, profile) is None

    def test_borderline_fields_escalate_to_system2(self):
        profile = extract_profile(FRAME)

        class StubFallback:
            calls = 0

            def classify(self, df, profile):
                StubFallback.calls += 1
                return ClassificationResult(ordinal={}, nominal=["gender", "education"])

        def borderline_handler(state, questions):
            answers = handler(state, questions)
            # gender 的分类判定很不确定 → borderline
            answers[cat_question_id("gender")] = noul_answer_dict(0.52)
            return answers

        StubFallback.calls = 0
        classifier = System1CategoricalClassifier(
            MockEvaluator(handler=borderline_handler), fallback=StubFallback()
        )
        outcome = classifier.classify(FRAME, profile)

        assert outcome.borderline == ["gender"]
        assert StubFallback.calls == 1
        # 系统二说 gender 无序 → 采纳；education 由系统一判为有序 → 不被推翻
        assert "gender" in outcome.result.nominal
        assert outcome.result.ordinal == {"education": ["小学", "高中", "本科", "硕士"]}
        assert any(r.escalated for r in outcome.decisions if r.subject.startswith("gender"))

    def test_summary_is_readable(self):
        profile = extract_profile(FRAME)
        outcome = System1CategoricalClassifier(MockEvaluator(handler=handler)).classify(
            FRAME, profile
        )
        assert "[有序] education" in outcome.summary()
        assert "[无序] gender" in outcome.summary()


class TestPolicyKnobs:
    def test_zero_spread_blocks_ordinal(self):
        """分数完全没拉开 → 无梯度可言 → 无序"""
        frame = pd.DataFrame({"x": ["a", "b", "c"]})
        profile = extract_profile(frame)

        def flat_scores(state, questions):
            return {
                cat_question_id("x"): noul_answer_dict(0.9),
                ordinal_question_id("x"): noul_answer_dict(0.9),
                level_question_id("x", "a"): peaked_score(2.0),
                level_question_id("x", "b"): peaked_score(2.0),
                level_question_id("x", "c"): peaked_score(2.0),
            }

        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=flat_scores),
            policy=GatePolicy(min_spread=1.0),
        ).classify(frame, profile)
        assert outcome.result.nominal == ["x"]


def _borderline_handler(state, questions):
    """把 gender 的分类判定做得很不确定，用于触发 borderline"""
    answers = handler(state, questions)
    answers[cat_question_id("gender")] = noul_answer_dict(0.52)
    return answers


class TestStandalone:
    """纯系统一（fallback=None）：不确定项不进系统二复判，按概率照常采用"""

    def test_standalone_keeps_decision(self):
        profile = extract_profile(FRAME)
        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=_borderline_handler), fallback=None
        ).classify(FRAME, profile)

        assert outcome.borderline == ["gender"]      # 仍然被记录
        assert "gender" in outcome.result.nominal    # 但按概率照常采用
        assert outcome.result.ordinal == {"education": ["小学", "高中", "本科", "硕士"]}
        assert not any(r.escalated for r in outcome.decisions)

    def test_standalone_records_escalate_verdict_without_escalating(self):
        """决策日志要能区分该升级与已升级：verdict=escalate 而 escalated=False"""
        profile = extract_profile(FRAME)
        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=_borderline_handler), fallback=None
        ).classify(FRAME, profile)
        record = next(r for r in outcome.decisions if r.subject == "gender")
        assert record.verdict == "escalate"
        assert record.escalated is False
        assert record.engine == "system1"

    def test_chained_mode_does_escalate(self):
        """对照：给了 fallback 才会真的复判"""
        profile = extract_profile(FRAME)

        class StubFallback:
            calls = 0

            def classify(self, df, profile):
                StubFallback.calls += 1
                return ClassificationResult(ordinal={}, nominal=["gender"])

        StubFallback.calls = 0
        outcome = System1CategoricalClassifier(
            MockEvaluator(handler=_borderline_handler), fallback=StubFallback()
        ).classify(FRAME, profile)
        assert StubFallback.calls == 1
        assert any(r.escalated for r in outcome.decisions if r.subject == "gender")
