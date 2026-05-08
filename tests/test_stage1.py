"""Stage 1 测试 — 场景识别"""

from signalchain.stage1_scene import build_scene_prompt, validate_scene_code
from signalchain.models import DataProfile, FieldProfile


class TestBuildScenePrompt:
    """测试 build_scene_prompt 函数"""

    def test_prompt_contains_field_info(self):
        profile = DataProfile(fields=[
            FieldProfile(name="patient_id", type="string", samples=["P001"], null_ratio=0.0),
            FieldProfile(name="gender", type="string", samples=["M", "F"], null_ratio=0.0),
        ])
        prompt = build_scene_prompt(profile)

        assert "patient_id" in prompt
        assert "gender" in prompt
        assert "S1" in prompt
        assert "S0" in prompt

    def test_prompt_contains_scene_options(self):
        profile = DataProfile(fields=[
            FieldProfile(name="x", type="string", samples=["a"], null_ratio=0.0),
        ])
        prompt = build_scene_prompt(profile)

        assert "S1=医疗数据" in prompt
        assert "S2=财务数据" in prompt
        assert "S3=用户数据" in prompt
        assert "S0=未知" in prompt


class TestValidateSceneCode:
    """测试 validate_scene_code 函数"""

    def test_valid_codes(self):
        assert validate_scene_code("S1") == "S1"
        assert validate_scene_code("S0") == "S0"
        assert validate_scene_code("S5") == "S5"

    def test_whitespace_handling(self):
        assert validate_scene_code("  S1  ") == "S1"
        assert validate_scene_code("S1\n") == "S1"

    def test_invalid_code_fallback(self):
        assert validate_scene_code("S9") == "S0"
        assert validate_scene_code("XX") == "S0"
        assert validate_scene_code("医疗数据") == "S0"

    def test_long_output_truncated(self):
        assert validate_scene_code("S1医疗数据") == "S1"

    def test_empty_output_fallback(self):
        assert validate_scene_code("") == "S0"
