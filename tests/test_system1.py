"""System 1 层测试 —— 置信度数学、问题构造、条件化、异常归类、Mock

全部离线（不调用真实 AI），符合 tests/AGENTS.md 约定。
"""

from types import SimpleNamespace

import pytest

from signalchain.system1 import (
    Answer,
    EvalResponse,
    GatePolicy,
    JevEvaluator,
    MockEvaluator,
    System1RequestError,
    System1Unavailable,
    _to_answer,
    argmax,
    certainty,
    choice,
    choice_answer_dict,
    choice_confidence,
    classify_error,
    condition_choice,
    noul,
    noul_answer_dict,
    noul_certainty,
    score,
    score_answer_dict,
)


class TestConfidenceMath:
    """置信度公式必须与官方一致：(n × peak − 1) / (n − 1)"""

    def test_uniform_distribution_is_zero(self):
        # n 个选项全均匀 → 与随机猜测无异 → 0
        assert choice_confidence({str(i): 1 / 13 for i in range(13)}) == pytest.approx(0.0)

    def test_single_option_is_one(self):
        assert choice_confidence({"only": 1.0}) == 1.0

    def test_all_mass_on_one_of_two(self):
        # (2*1 - 1) / (2-1) = 1
        assert choice_confidence({"a": 1.0, "b": 0.0}) == pytest.approx(1.0)

    def test_matches_documented_formula_for_three_options(self):
        # 官方文档示例：(3 × 0.9 − 1) / 2 = 0.85
        assert choice_confidence({"a": 0.9, "b": 0.06, "c": 0.04}) == pytest.approx(0.85)

    def test_option_count_is_normalised_away(self):
        """同一个 peak，在不同选项数下 confidence 不同 —— 这正是它可比的原因

        13 选项里 0.4 是很强的信号（远高于 1/13 的随机水平），
        6 选项里 0.4 只是略高（随机水平 1/6）。所以门控必须看 confidence 而不是 raw probability。
        """
        peak_13 = {"0": 0.4, **{str(i): 0.6 / 12 for i in range(1, 13)}}
        peak_6 = {"a": 0.4, **{f"c{i}": 0.6 / 5 for i in range(5)}}
        assert choice_confidence(peak_13) > choice_confidence(peak_6)

    def test_empty_is_zero(self):
        assert choice_confidence({}) == 0.0

    def test_clamped_to_unit_interval(self):
        assert choice_confidence({"a": 0.5, "b": 0.5}) == 0.0
        assert 0.0 <= choice_confidence({"a": 0.999, "b": 0.001}) <= 1.0


class TestNoulCertainty:
    """noul 没有 confidence 字段，用 |2p − 1| 映射到同一尺度"""

    def test_half_is_coin_flip(self):
        assert noul_certainty(0.5) == 0.0

    def test_extremes_are_certain(self):
        assert noul_certainty(0.0) == 1.0
        assert noul_certainty(1.0) == 1.0

    def test_symmetric(self):
        assert noul_certainty(0.9) == pytest.approx(noul_certainty(0.1))
        assert noul_certainty(0.9) == pytest.approx(0.8)


class TestCertaintyDispatch:
    def test_noul_uses_probability(self):
        assert certainty(Answer(type="noul", noul=0.93)) == pytest.approx(0.86)

    def test_noul_without_value_is_zero(self):
        assert certainty(Answer(type="noul", noul=None)) == 0.0

    def test_choice_recomputes_from_distribution(self):
        """刻意忽略引擎返回的 confidence，从分布现算 —— 条件化后引擎值会失效"""
        answer = Answer(
            type="choice",
            choice="G",
            probabilities={"G": 0.9, "A": 0.1},
            confidence=0.123,
        )
        assert certainty(answer) == pytest.approx(0.8)

    def test_score_falls_back_to_engine_confidence(self):
        answer = Answer(type="score", score=3.0, probabilities={}, confidence=0.77)
        assert certainty(answer) == pytest.approx(0.77)

    def test_unknown_type_is_zero(self):
        assert certainty(Answer(type="mystery")) == 0.0


class TestConditionChoice:
    """本地条件化 —— 系统一单请求方案的核心数学"""

    def test_renormalises_onto_allowed(self):
        answer = Answer(
            type="choice",
            choice="G",
            probabilities={"G": 0.5, "A": 0.3, "X": 0.2},
        )
        conditioned = condition_choice(answer, ["G", "A"])
        assert sum(conditioned.probabilities.values()) == pytest.approx(1.0)
        assert conditioned.probabilities["G"] == pytest.approx(0.625)
        assert conditioned.probabilities["A"] == pytest.approx(0.375)

    def test_confidence_uses_new_option_count(self):
        """选项数从 3 缩到 2，confidence 必须按新选项数重算"""
        answer = Answer(
            type="choice",
            choice="G",
            probabilities={"G": 0.5, "A": 0.3, "X": 0.2},
        )
        conditioned = condition_choice(answer, ["G", "A"])
        # (2 × 0.625 − 1) / (2 − 1) = 0.25，而不是原来的 (3×0.5−1)/2 = 0.25 的选项数 3
        assert certainty(conditioned) == pytest.approx(0.25)
        assert len(conditioned.probabilities) == 2

    def test_out_of_scope_mass_becomes_uniform_not_silent_x(self):
        """概率全落在场景外的码上 → 回退为合法码上的均匀分布，certainty=0 交给门控

        这比旧实现"非法字符直接替换成 X"诚实：旧做法会静默降级，
        新做法把"确定是这几个之一但不知道是哪个"显式表达出来。
        """
        answer = Answer(type="choice", choice="G", probabilities={"G": 1.0})
        conditioned = condition_choice(answer, ["M", "T"])
        assert set(conditioned.probabilities) == {"M", "T"}
        assert conditioned.probabilities["M"] == pytest.approx(0.5)
        assert certainty(conditioned) == pytest.approx(0.0)

    def test_empty_allowed_returns_original(self):
        answer = Answer(type="choice", choice="G", probabilities={"G": 1.0})
        assert condition_choice(answer, []) is answer

    def test_deduplicates_allowed(self):
        answer = Answer(type="choice", choice="G", probabilities={"G": 0.6, "A": 0.4})
        conditioned = condition_choice(answer, ["G", "A", "G"])
        assert len(conditioned.probabilities) == 2


class TestArgmax:
    def test_picks_highest(self):
        assert argmax({"a": 0.1, "b": 0.8}) == "b"

    def test_tie_is_deterministic(self):
        assert argmax({"b": 0.5, "a": 0.5}) == "a"

    def test_empty_is_none(self):
        assert argmax({}) is None


class TestErrorClassification:
    """按类名归类（SDK 未安装时无法 isinstance，且对改名更耐受）"""

    def test_rate_limit_is_unavailable(self):
        exc = type("TypeSafeRateLimitError", (Exception,), {})()
        assert classify_error(exc) is System1Unavailable

    def test_auth_is_unavailable(self):
        exc = type("TypeSafeAuthenticationError", (Exception,), {})()
        assert classify_error(exc) is System1Unavailable

    def test_connection_is_unavailable(self):
        exc = type("TypeSafeAPIConnectionError", (Exception,), {})()
        assert classify_error(exc) is System1Unavailable

    def test_unprocessable_is_request_error(self):
        exc = type("TypeSafeUnprocessableEntityError", (Exception,), {})()
        assert classify_error(exc) is System1RequestError

    def test_bare_network_errors_are_unavailable(self):
        assert classify_error(TimeoutError()) is System1Unavailable
        assert classify_error(ConnectionError()) is System1Unavailable

    def test_unknown_error_is_not_swallowed(self):
        """未知异常归为 RequestError（会被 pipeline 记 ERROR 后降级），不被当成正常的不可用"""
        assert classify_error(ValueError("boom")) is System1RequestError


class TestToAnswer:
    """SDK 对象与裸 dict 都要能转 —— 这是传输层可替换的前提"""

    def test_dict_choice(self):
        answer = _to_answer(
            {"type": "choice", "choice": "G", "probabilities": {"G": 1.0}, "confidence": 0.9}
        )
        assert answer.type == "choice"
        assert answer.choice == "G"
        assert answer.probabilities == {"G": 1.0}

    def test_dict_noul(self):
        assert _to_answer({"type": "noul", "noul": 0.87}).noul == pytest.approx(0.87)

    def test_dict_score_coerces_int_keys_to_str(self):
        """SDK 的 legend/probabilities 是 int 键，内部统一成 str 以对齐 wire format"""
        answer = _to_answer(
            {
                "type": "score",
                "score": 2.4,
                "legend": {0: "low", 4: "high"},
                "probabilities": {0: 0.1, 4: 0.9},
            }
        )
        assert answer.score == pytest.approx(2.4)
        assert answer.legend == {"0": "low", "4": "high"}
        assert answer.probabilities == {"0": 0.1, "4": 0.9}

    def test_sdk_style_object(self):
        raw = SimpleNamespace(
            type="choice",
            choice="M",
            probabilities={"M": 0.8, "X": 0.2},
            confidence=0.6,
        )
        answer = _to_answer(raw)
        assert answer.choice == "M"
        assert answer.confidence == pytest.approx(0.6)

    def test_unknown_type_is_tolerated(self):
        assert _to_answer({"type": "future_kind"}).type == "future_kind"


class TestQuestionBuilders:
    """构造出的问题必须直接是 wire format（SDK 与裸 HTTP 都能吃）"""

    def test_noul_minimal(self):
        assert noul("是不是？") == {"type": "noul", "instructions": "是不是？"}

    def test_noul_with_criteria(self):
        question = noul("是不是？", true_description="是的样子", false_description="否的样子")
        assert question["criteria"] == {"true": "是的样子", "false": "否的样子"}

    def test_choice(self):
        question = choice("选哪个？", {"a": "甲", "b": "乙"})
        assert question == {
            "type": "choice",
            "instructions": "选哪个？",
            "criteria": {"a": "甲", "b": "乙"},
        }

    def test_score(self):
        question = score("打几分？", ["低", "中", "高"])
        assert question["type"] == "score"
        assert question["criteria"] == ["低", "中", "高"]

    def test_structured_instructions_are_preserved(self):
        payload = {"field": {"name": "X"}, "question": "是什么？"}
        assert choice(payload, {"a": None})["instructions"] is payload

    def test_answer_dict_helpers(self):
        assert choice_answer_dict("G")["probabilities"] == {"G": 1.0}
        assert noul_answer_dict(0.5)["noul"] == 0.5
        scored = score_answer_dict(2.0, ["低", "中", "高"])
        assert scored["legend"] == {"0": "低", "1": "中", "2": "高"}
        assert sum(scored["probabilities"].values()) == pytest.approx(1.0)


class TestGatePolicy:
    def test_three_state_verdict(self):
        policy = GatePolicy(accept=0.8, escalate=0.55)
        assert policy.verdict(0.95) == "accept"
        assert policy.verdict(0.80) == "accept"
        assert policy.verdict(0.60) == "provisional"
        assert policy.verdict(0.54) == "escalate"


class TestMockEvaluator:
    def test_answers_only_asked_questions(self):
        evaluator = MockEvaluator(
            answers={"q1": noul_answer_dict(0.9), "unasked": noul_answer_dict(1.0)}
        )
        response = evaluator.evaluate({"x": 1}, {"q1": noul("?")})
        assert set(response.answers) == {"q1"}

    def test_records_calls(self):
        evaluator = MockEvaluator(answers={"q1": noul_answer_dict(0.9)})
        evaluator.evaluate({"x": 1}, {"q1": noul("?")})
        assert len(evaluator.call_log) == 1
        assert evaluator.usage.total_calls == 1

    def test_unavailable_raises(self):
        with pytest.raises(System1Unavailable):
            MockEvaluator(available=False).evaluate({}, {"q": noul("?")})

    def test_fail_with_is_raised(self):
        failure = System1Unavailable("网络不通")
        with pytest.raises(System1Unavailable):
            MockEvaluator(fail_with=failure).evaluate({}, {"q": noul("?")})

    def test_empty_questions_is_request_error(self):
        with pytest.raises(System1RequestError):
            MockEvaluator().evaluate({}, {})


class TestJevEvaluatorAvailability:
    def test_no_key_is_unavailable(self, monkeypatch):
        monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
        evaluator = JevEvaluator(api_key="")
        assert evaluator.available() is False

    def test_engine_id_includes_model(self):
        assert JevEvaluator(api_key="k", model="jev-latest").engine_id == "jev:jev-latest"

    def test_engine_id_defaults(self):
        assert JevEvaluator(api_key="k").engine_id == "jev:latest"

    def test_missing_key_raises_unavailable_on_call(self, monkeypatch):
        monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
        with pytest.raises(System1Unavailable):
            JevEvaluator(api_key="").evaluate({"a": 1}, {"q": noul("?")})
