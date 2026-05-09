"""Stage 2 测试 — 路由与 Prompt 组装"""

from signalchain.stage2_router import (
    ROUTING_TABLE,
    compress_samples,
    build_field_semantic_prompt,
    _format_code_options,
)
from signalchain.models import DataProfile, FieldProfile


class TestRoutingTable:
    """测试路由表配置"""

    def test_all_scene_codes_exist(self):
        for code in ["S0", "S1", "S2", "S3", "S4", "S5"]:
            assert code in ROUTING_TABLE

    def test_s1_medical_config(self):
        config = ROUTING_TABLE["S1"]
        assert config.scene_name == "医疗数据"
        assert "G" in config.valid_codes
        assert "A" in config.valid_codes
        assert "D" in config.valid_codes
        assert "N" in config.valid_codes
        assert config.operations["G"] == "normalize_gender"

    def test_s0_fallback_config(self):
        config = ROUTING_TABLE["S0"]
        assert config.valid_codes == {"I", "X"}
        assert all(v == "pass_through" for v in config.operations.values())

    def test_operations_cover_valid_codes(self):
        """每个场景的 operations 必须覆盖 valid_codes"""
        for code, config in ROUTING_TABLE.items():
            assert config.valid_codes == set(config.operations.keys()), (
                f"Scene {code}: operations keys {set(config.operations.keys())} "
                f"don't match valid_codes {config.valid_codes}"
            )


class TestCompressSamples:
    """测试样本压缩函数"""

    def test_basic_compression(self):
        values = ["a", "b", "c", "d", "e", "f"]
        result = compress_samples(values, max_count=3)
        assert len(result) <= 3

    def test_dedup(self):
        values = ["a", "a", "b", "b", "c"]
        result = compress_samples(values, max_count=5)
        assert len(result) == 3

    def test_info_score_priority(self):
        """信息密度高的样本优先保留"""
        values = ["hello", "test@#$%", "world"]
        result = compress_samples(values, max_count=2)
        # "test@#$%" 的 info_score 更高，应该被保留
        assert "test@#$%" in result

    def test_empty_input(self):
        result = compress_samples([], max_count=5)
        assert result == []


class TestBuildFieldSemanticPrompt:
    """测试 Prompt 组装"""

    def test_prompt_contains_field_info(self):
        profile = DataProfile(fields=[
            FieldProfile(name="gender", type="string", samples=["M", "F"], null_ratio=0.0),
            FieldProfile(name="age", type="string", samples=["30"], null_ratio=0.0),
        ])
        scene_config = ROUTING_TABLE["S1"]
        prompt = build_field_semantic_prompt(profile, scene_config, scene_code="S1")

        assert "gender" in prompt
        assert "age" in prompt
        assert "医疗数据" in prompt

    def test_prompt_contains_code_options(self):
        profile = DataProfile(fields=[
            FieldProfile(name="x", type="string", samples=["a"], null_ratio=0.0),
        ])
        scene_config = ROUTING_TABLE["S1"]
        prompt = build_field_semantic_prompt(profile, scene_config, scene_code="S1")

        # 应该包含 S1 场景的有效码
        assert "G=" in prompt
        assert "I=" in prompt
        assert "X=" in prompt


class TestFormatCodeOptions:
    """测试代码选项格式化"""

    def test_x_always_last(self):
        options = _format_code_options({"G", "A", "X"})
        codes = [code for code, _ in options]
        assert codes[-1] == "X"

    def test_order_without_x(self):
        options = _format_code_options({"G", "A", "D"})
        codes = [code for code, _ in options]
        assert codes == sorted(["G", "A", "D"])
