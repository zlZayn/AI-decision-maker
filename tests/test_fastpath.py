"""清洗链路系统一快通道测试 —— 问题构造、门控、升级、与系统二路径一致性

全部离线（MockEvaluator + MockAIClient），不调用真实 AI。
"""

import os
import tempfile

import pandas as pd
import pytest

from signalchain.ai_client import MockAIClient
from signalchain.cache import SignalCache
from signalchain.fastpath import (
    CODE_ORDER,
    FIELD_QUESTION_PREFIX,
    SCENE_QUESTION_ID,
    System1Decider,
    build_questions,
    build_state,
    code_criteria,
    field_question_id,
    scene_criteria,
)
from signalchain.models import (
    CODE_LABELS,
    VALID_SCENE_CODES,
    DataProfile,
    FieldProfile,
)
from signalchain.stage2_router import ROUTING_TABLE
from signalchain.pipeline import SignalChainPipeline
from signalchain.stage0_profile import extract_profile
from signalchain.system1 import (
    MockEvaluator,
    System1Unavailable,
    choice_answer_dict,
    noul_answer_dict,
)


# ============================================================
# 夹具
# ============================================================

MEDICAL = {
    "patient_id": ["P001", "P002", "P003"],
    "gender": ["M", "F", "男"],
    "age": ["30", "30岁", "45"],
    "dept_name": ["心内", "外科", "ICU"],
    "drug_name": ["阿莫西林", "甲硝唑", "头孢"],
}
MEDICAL_FIELDS = ["patient_id", "gender", "age", "dept_name", "drug_name"]
MEDICAL_CODES = "IGADN"


def medical_frame() -> pd.DataFrame:
    return pd.DataFrame(MEDICAL)


class TwoCallMockAI(MockAIClient):
    """系统二替身：第 1 次调用答场景，第 2 次答字段序列（与 test_pipeline.py 同构）"""

    def __init__(self, scene_code: str, signal_sequence: str):
        super().__init__()
        self.scene_code = scene_code
        self.signal_sequence = signal_sequence
        self._calls = 0

    def call(self, prompt: str) -> str:
        self.call_log.append(prompt)
        self._calls += 1
        return self.scene_code if self._calls == 1 else self.signal_sequence


def confident(value: str, codes=CODE_ORDER, peak: float = 1.0) -> dict:
    """造一个把概率全押在 value 上的 choice 答案"""
    rest = (1.0 - peak) / max(1, len(codes) - 1)
    probabilities = {code: (peak if code == value else rest) for code in codes}
    return choice_answer_dict(value, probabilities)


def medical_handler(state, questions):
    """一个"判得对"的假引擎：按字段名给出正确答案"""
    mapping = {
        "patient_id": "I",
        "gender": "G",
        "age": "A",
        "dept_name": "D",
        "drug_name": "N",
    }
    answers = {SCENE_QUESTION_ID: confident("S1", sorted(VALID_SCENE_CODES))}
    for name, code in mapping.items():
        answers[field_question_id(name)] = confident(code)
    return answers


# ============================================================
# 问题构造
# ============================================================


class TestCriteria:
    def test_terse_criteria_are_labels_only(self):
        """默认简短：实测省 38% 输入 token 而答案不变"""
        criteria = code_criteria()
        assert criteria["G"] == CODE_LABELS["G"]
        assert "：" not in criteria["G"]

    def test_verbose_criteria_carry_hints(self):
        criteria = code_criteria(verbose=True)
        assert criteria["G"].startswith(CODE_LABELS["G"])
        assert "性别" in criteria["G"] and len(criteria["G"]) > len(CODE_LABELS["G"])

    def test_criteria_order_is_stable(self):
        assert list(code_criteria()) == list(CODE_ORDER)

    def test_criteria_subset_follows_scene(self):
        criteria = code_criteria(ROUTING_TABLE["S2"].valid_codes)
        assert set(criteria) == ROUTING_TABLE["S2"].valid_codes

    def test_scene_criteria_derived_from_routing_table(self):
        """场景选项从 ROUTING_TABLE 派生：新增场景不需要再改这里"""
        criteria = scene_criteria()
        assert set(criteria) == VALID_SCENE_CODES
        for code, config in ROUTING_TABLE.items():
            assert config.scene_name in criteria[code]


class TestBuildQuestions:
    def test_one_question_per_field_plus_scene(self):
        profile = extract_profile(medical_frame())
        questions = build_questions(profile)
        assert SCENE_QUESTION_ID in questions
        assert len(questions) == profile.field_count + 1
        for name in MEDICAL_FIELDS:
            assert field_question_id(name) in questions

    def test_field_question_carries_structured_instructions(self):
        """字段信息放进 instructions 而不是 state 路径：真实字段名含 / 和 -，路径转义易错"""
        profile = extract_profile(medical_frame())
        questions = build_questions(profile)
        instructions = questions[field_question_id("gender")]["instructions"]
        assert instructions["field"]["name"] == "gender"
        assert instructions["field"]["samples"]
        assert "question" in instructions

    def test_local_hint_is_attached_when_known(self):
        profile = extract_profile(medical_frame())
        questions = build_questions(profile)
        instructions = questions[field_question_id("gender")]["instructions"]
        assert "local_hint" in instructions and "G" in instructions["local_hint"]

    def test_two_pass_restricts_criteria_to_scene(self):
        profile = extract_profile(medical_frame())
        questions = build_questions(
            profile, include_scene=False, codes=ROUTING_TABLE["S1"].valid_codes
        )
        assert SCENE_QUESTION_ID not in questions
        assert set(questions[field_question_id("gender")]["criteria"]) == ROUTING_TABLE[
            "S1"
        ].valid_codes

    def test_duplicate_field_names_get_distinct_ids(self):
        """DataFrame 允许重名列；不处理的话两列会拿到同一个答案"""
        profile = DataProfile(
            fields=[
                FieldProfile(name="x", type="string", samples=["a"], null_ratio=0.0),
                FieldProfile(name="x", type="string", samples=["b"], null_ratio=0.0),
            ]
        )
        questions = build_questions(profile, include_scene=False)
        assert len(questions) == 2

    def test_state_carries_dataset_overview(self):
        profile = extract_profile(medical_frame())
        state = build_state(profile)
        assert state["dataset"]["field_count"] == 5
        assert len(state["dataset"]["fields"]) == 5


# ============================================================
# 决策器
# ============================================================


class TestSystem1Decider:
    def test_high_certainty_is_accepted(self):
        profile = extract_profile(medical_frame())
        decider = System1Decider(MockEvaluator(handler=medical_handler))
        result = decider.decide(profile)
        assert result is not None
        assert result.scene_code == "S1"
        assert result.signal_sequence == MEDICAL_CODES
        assert result.escalated_fields == []
        assert all(record.verdict == "accept" for record in result.decisions)
        assert all(record.engine == "system1" for record in result.decisions)
        assert len(result.decisions) == profile.field_count + 1

    def test_unavailable_evaluator_returns_none(self):
        profile = extract_profile(medical_frame())
        assert System1Decider(MockEvaluator(available=False)).decide(profile) is None

    def test_engine_error_returns_none(self):
        """引擎报不可用 → 返回 None 让 pipeline 走系统二，绝不抛出去"""
        profile = extract_profile(medical_frame())
        decider = System1Decider(MockEvaluator(fail_with=System1Unavailable("不通")))
        assert decider.decide(profile) is None

    def test_empty_profile_returns_none(self):
        assert System1Decider(MockEvaluator()).decide(DataProfile(fields=[])) is None

    def test_single_request_for_scene_and_fields(self):
        """Stage 1 + Stage 3 两次往返压成一次 —— 这是本设计的主要收益"""
        profile = extract_profile(medical_frame())
        evaluator = MockEvaluator(handler=medical_handler)
        System1Decider(evaluator).decide(profile)
        assert len(evaluator.call_log) == 1
        _, questions = evaluator.call_log[0]
        assert len(questions) == profile.field_count + 1

    def test_low_certainty_field_is_escalated_in_one_batch(self):
        """置信度不足的字段批量升级：一次系统二调用处理全部不确定字段"""
        profile = extract_profile(medical_frame())

        def handler(state, questions):
            answers = medical_handler(state, questions)
            # 把 gender 变成完全平坦的分布 → certainty = 0 → 必然升级
            flat = {code: 1.0 / len(CODE_ORDER) for code in CODE_ORDER}
            answers[field_question_id("gender")] = choice_answer_dict("G", flat)
            return answers

        # 升级时系统二只被调用一次（问那个子集），所以替身必须"永远返回序列"
        fallback = MockAIClient(responses={"选项": "G"})
        decider = System1Decider(MockEvaluator(handler=handler), fallback_client=fallback)
        result = decider.decide(profile)

        assert result is not None
        assert result.escalated_fields == ["gender"]
        assert result.signal_sequence == MEDICAL_CODES
        gender_record = next(r for r in result.decisions if r.subject == "gender")
        assert gender_record.escalated is True
        assert gender_record.engine == "system2"
        assert gender_record.verdict == "escalate"
        # 升级只调用一次，且只问那一个字段
        assert len(fallback.call_log) == 1
        assert "gender" in fallback.call_log[0]

    def test_no_fallback_falls_back_to_x(self):
        profile = extract_profile(medical_frame())

        def handler(state, questions):
            answers = medical_handler(state, questions)
            answers[field_question_id("age")] = choice_answer_dict(
                "A", {code: 1.0 / len(CODE_ORDER) for code in CODE_ORDER}
            )
            return answers

        decider = System1Decider(MockEvaluator(handler=handler), fallback_client=None)
        result = decider.decide(profile)
        assert result is not None
        assert result.signal_sequence == "IGXDN"

    def test_out_of_scene_code_is_conditioned_away(self):
        """单请求下字段问题用全 13 码问，场景外的高概率必须在本地被条件化掉"""
        profile = extract_profile(medical_frame())

        def handler(state, questions):
            answers = medical_handler(state, questions)
            # 场景是 S1，但模型给 age 押了 S2 才合法的 M
            answers[field_question_id("age")] = confident("M")
            return answers

        decider = System1Decider(MockEvaluator(handler=handler), fallback_client=None)
        result = decider.decide(profile)
        assert result is not None
        # M 不在 S1 的 valid_codes 里 → 条件化后退化为合法码上的均匀分布 → certainty 0 → X
        assert result.signal_sequence[2] == "X"

    def test_low_certainty_scene_escalates_to_system2(self):
        profile = extract_profile(medical_frame())

        def handler(state, questions):
            answers = medical_handler(state, questions)
            answers[SCENE_QUESTION_ID] = choice_answer_dict(
                "S1", {code: 1.0 / len(VALID_SCENE_CODES) for code in VALID_SCENE_CODES}
            )
            return answers

        fallback = TwoCallMockAI(scene_code="S2", signal_sequence="I")
        decider = System1Decider(MockEvaluator(handler=handler), fallback_client=fallback)
        result = decider.decide(profile)
        assert result is not None
        assert result.scene_code == "S2"
        scene_record = next(r for r in result.decisions if r.subject == "scene")
        assert scene_record.escalated is True

    def test_two_pass_makes_second_request(self):
        """严格模式：先定场景，再用该场景的合法码二次提问"""
        profile = extract_profile(medical_frame())
        evaluator = MockEvaluator(handler=medical_handler)
        from signalchain.system1 import GatePolicy

        decider = System1Decider(
            evaluator, policy=GatePolicy(scene_strategy="two_pass")
        )
        result = decider.decide(profile)
        assert result is not None
        assert len(evaluator.call_log) == 2
        _, second_questions = evaluator.call_log[1]
        assert SCENE_QUESTION_ID not in second_questions
        assert set(second_questions[field_question_id("gender")]["criteria"]) == ROUTING_TABLE[
            "S1"
        ].valid_codes

    def test_result_summary_is_readable(self):
        profile = extract_profile(medical_frame())
        result = System1Decider(MockEvaluator(handler=medical_handler)).decide(profile)
        assert "scene=S1" in result.summary()
        assert MEDICAL_CODES in result.summary()


# ============================================================
# Pipeline 集成：系统一与系统二必须产出同一份清洗结果
# ============================================================


class TestPipelineIntegration:
    def test_system1_path_matches_system2_path(self):
        """最强的一致性断言：换了决策引擎，清洗结果必须完全一致"""
        with tempfile.TemporaryDirectory() as tmp:
            frame_a = medical_frame()
            pipeline_s2 = SignalChainPipeline(
                ai_client=TwoCallMockAI("S1", MEDICAL_CODES),
                cache_file=os.path.join(tmp, "s2.json"),
            )
            out_s2, _ = pipeline_s2.run(frame_a)

            frame_b = medical_frame()
            pipeline_s1 = SignalChainPipeline(
                ai_client=TwoCallMockAI("S1", MEDICAL_CODES),
                cache_file=os.path.join(tmp, "s1.json"),
                evaluator=MockEvaluator(handler=medical_handler),
            )
            out_s1, _ = pipeline_s1.run(frame_b)

            pd.testing.assert_frame_equal(out_s1, out_s2)
            assert pipeline_s1.decisions and not pipeline_s2.decisions

    def test_system1_path_produces_expected_cleaning(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = SignalChainPipeline(
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=medical_handler),
            )
            result, _ = pipeline.run(medical_frame())
            assert list(result["gender"]) == ["男", "女", "男"]
            assert list(result["age"]) == [30, 30, 45]
            assert result["department"].iloc[0] == "心内科"

    def test_unavailable_system1_degrades_to_system2(self):
        with tempfile.TemporaryDirectory() as tmp:
            fallback_ai = TwoCallMockAI("S1", MEDICAL_CODES)
            pipeline = SignalChainPipeline(
                ai_client=fallback_ai,
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(available=False),
            )
            result, _ = pipeline.run(medical_frame())
            assert list(result["gender"]) == ["男", "女", "男"]
            assert len(fallback_ai.call_log) == 2  # 两次系统二调用都发生了

    def test_engine_partitions_are_independent(self):
        """两套系统的缓存分区互不覆盖：各自保留自己的决策

        早期实现用单一 _code_hash 整体比对，交替跑会让后一个引擎冲掉前一个的条目，
        导致系统一每次都要重新问 Jev。
        """
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.json")

            evaluator = MockEvaluator(handler=medical_handler)
            pipeline_s1 = SignalChainPipeline(cache_file=path, evaluator=evaluator)
            assert pipeline_s1._engine_id() == "mock:jev"
            pipeline_s1.run(medical_frame())
            assert len(evaluator.call_log) == 1

            # 同一份数据、同一个 fingerprint：系统二必须自己判一次，而不是吃系统一的缓存
            pipeline_s2 = SignalChainPipeline(
                ai_client=TwoCallMockAI("S1", MEDICAL_CODES), cache_file=path
            )
            assert pipeline_s2._engine_id() == "system2"
            pipeline_s2.run(medical_frame())
            assert len(pipeline_s2.ai.call_log) == 2

            # 关键：回到系统一，它的缓存必须还在（旧实现会被系统二冲掉）
            pipeline_s1_again = SignalChainPipeline(
                cache_file=path, evaluator=MockEvaluator(handler=medical_handler)
            )
            pipeline_s1_again.run(medical_frame())
            assert pipeline_s1_again.decisions[0].engine == "cache"

            # 两个分区同时存在于同一个缓存文件
            assert SignalCache(path, namespace="system2").namespaces() == ["mock:jev", "system2"]

    def test_cache_hit_records_decision_with_engine_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.json")
            pipeline = SignalChainPipeline(
                cache_file=path, evaluator=MockEvaluator(handler=medical_handler)
            )
            pipeline.run(medical_frame())
            first_decisions = len(pipeline.decisions)
            assert first_decisions > 0

            pipeline.run(medical_frame())
            assert len(pipeline.decisions) == 1
            assert pipeline.decisions[0].engine == "cache"

    def test_cache_entry_stores_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.json")
            pipeline = SignalChainPipeline(
                cache_file=path, evaluator=MockEvaluator(handler=medical_handler)
            )
            pipeline.run(medical_frame())
            cache = SignalCache(path, namespace="mock:jev")
            entries = cache.items()
            assert len(entries) == 1
            assert entries[0][1].engine == "system1"
            assert entries[0][1].certainty == pytest.approx(1.0)

    def test_no_evaluator_means_no_decisions(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = SignalChainPipeline(
                ai_client=TwoCallMockAI("S1", MEDICAL_CODES),
                cache_file=os.path.join(tmp, "c.json"),
            )
            pipeline.run(medical_frame())
            assert pipeline.decisions == []
            assert pipeline.decider is None


# ============================================================
# 两套系统解耦：默认互不依赖，串联是显式选择
# ============================================================


class TestDecoupling:
    """系统一与系统二各自能单独跑；串联必须显式开启"""

    def test_default_does_not_chain(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = SignalChainPipeline(
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=medical_handler),
            )
            assert pipeline.escalate_to_system2 is False
            # 关键：decider 拿不到系统二客户端，物理上无法串联
            assert pipeline.decider.fallback is None

    def test_chaining_is_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = SignalChainPipeline(
                ai_client=TwoCallMockAI("S1", MEDICAL_CODES),
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=medical_handler),
                escalate_to_system2=True,
            )
            assert pipeline.escalate_to_system2 is True
            assert pipeline.decider.fallback is not None

    def test_standalone_never_calls_system2(self):
        """纯系统一：低置信字段落保守默认值，系统二一次都不被调用"""
        frame = medical_frame()

        def handler(state, questions):
            answers = medical_handler(state, questions)
            # age 给平坦分布 → certainty = 0 → 若串联就会升级
            answers[field_question_id("age")] = choice_answer_dict(
                "A", {code: 1.0 / len(CODE_ORDER) for code in CODE_ORDER}
            )
            return answers

        with tempfile.TemporaryDirectory() as tmp:
            system2 = MockAIClient()
            pipeline = SignalChainPipeline(
                ai_client=system2,
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=handler),
                escalate_to_system2=False,
            )
            result, _ = pipeline.run(frame)

            assert system2.call_log == []          # 系统二完全没参与
            assert result["department"].iloc[0] == "心内科"   # 置信字段照常清洗
            age_record = next(r for r in pipeline.decisions if r.subject == "age")
            assert age_record.verdict == "escalate"
            assert age_record.escalated is False   # 未被升级
            assert age_record.engine == "system1"

    def test_standalone_keeps_system1_scene_answer(self):
        """纯系统一下场景低置信：保留系统一答案并用校验器兜底，不找系统二"""
        frame = medical_frame()

        def handler(state, questions):
            answers = medical_handler(state, questions)
            answers[SCENE_QUESTION_ID] = choice_answer_dict(
                "S1", {code: 1.0 / len(VALID_SCENE_CODES) for code in VALID_SCENE_CODES}
            )
            return answers

        with tempfile.TemporaryDirectory() as tmp:
            system2 = MockAIClient()
            pipeline = SignalChainPipeline(
                ai_client=system2,
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=handler),
                escalate_to_system2=False,
            )
            pipeline.run(frame)
            assert system2.call_log == []
            scene_record = next(r for r in pipeline.decisions if r.subject == "scene")
            assert scene_record.chosen == "S1"
            assert scene_record.engine == "system1"
            assert scene_record.escalated is False

    def test_escalation_failure_does_not_claim_success(self):
        """启用串联但系统二调用抛异常：落 X，记录不得写成 engine=system2"""
        frame = medical_frame()

        def handler(state, questions):
            answers = medical_handler(state, questions)
            answers[field_question_id("age")] = choice_answer_dict(
                "A", {code: 1.0 / len(CODE_ORDER) for code in CODE_ORDER}
            )
            return answers

        class ExplodingAI(MockAIClient):
            def call(self, prompt: str) -> str:
                raise RuntimeError("系统二不可用")

        with tempfile.TemporaryDirectory() as tmp:
            pipeline = SignalChainPipeline(
                ai_client=ExplodingAI(),
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=handler),
                escalate_to_system2=True,
            )
            pipeline.run(frame)
            age_record = next(r for r in pipeline.decisions if r.subject == "age")
            assert age_record.chosen == "X"
            assert age_record.escalated is False
            assert age_record.engine == "system1"
            assert age_record.verdict == "escalate"

    def test_chained_mode_does_call_system2(self):
        """对照：显式开启串联后，同一个低置信字段确实会走系统二"""
        frame = medical_frame()

        def handler(state, questions):
            answers = medical_handler(state, questions)
            answers[field_question_id("age")] = choice_answer_dict(
                "A", {code: 1.0 / len(CODE_ORDER) for code in CODE_ORDER}
            )
            return answers

        with tempfile.TemporaryDirectory() as tmp:
            system2 = MockAIClient(responses={"选项": "A"})
            pipeline = SignalChainPipeline(
                ai_client=system2,
                cache_file=os.path.join(tmp, "c.json"),
                evaluator=MockEvaluator(handler=handler),
                escalate_to_system2=True,
            )
            pipeline.run(frame)
            assert len(system2.call_log) == 1
            age_record = next(r for r in pipeline.decisions if r.subject == "age")
            assert age_record.escalated is True
            assert age_record.engine == "system2"
