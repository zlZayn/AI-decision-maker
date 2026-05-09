"""Stage 4 测试 — 执行计划组装"""

from signalchain.stage4_assemble import assemble_operations
from signalchain.models import SceneConfig
from signalchain.operations.pass_through import PassThrough


class TestAssembleOperations:
    """测试 assemble_operations 函数"""

    def test_basic_assembly(self):
        field_names = ["patient_id", "gender", "age", "dept_name", "drug_name"]
        signal_sequence = "IGADN"
        scene_config = SceneConfig(
            scene_name="测试",
            prompt_template="",
            valid_codes={"I", "G", "A", "D", "N", "X"},
            operations={
                "I": "pass_through",
                "G": "normalize_gender",
                "A": "extract_age",
                "D": "normalize_department",
                "N": "normalize_drug_name",
                "X": "pass_through",
            },
        )
        ops = assemble_operations(field_names, signal_sequence, scene_config)

        assert len(ops) == 5
        assert ops[0][0] == "patient_id"
        assert ops[0][1].name == "pass_through"
        assert ops[1][0] == "gender"
        assert ops[1][1].name == "normalize_gender"

    def test_fallback_on_missing_operation(self):
        """操作名不在 registry 中时回退到 pass_through"""
        field_names = ["col1"]
        signal_sequence = "G"
        scene_config = SceneConfig(
            scene_name="测试",
            prompt_template="",
            valid_codes={"G"},
            operations={"G": "nonexistent_operation"},
        )
        ops = assemble_operations(field_names, signal_sequence, scene_config)

        assert ops[0][1].name == "pass_through"

    def test_fallback_on_missing_mapping(self):
        """信号码不在 operations 映射中时回退到 pass_through"""
        field_names = ["col1"]
        signal_sequence = "Z"  # 不在 operations 中
        scene_config = SceneConfig(
            scene_name="测试",
            prompt_template="",
            valid_codes={"Z"},
            operations={},  # 空映射
        )
        ops = assemble_operations(field_names, signal_sequence, scene_config)

        assert ops[0][1].name == "pass_through"

    def test_custom_registry(self):
        """使用自定义 registry"""
        custom_registry = {"pass_through": PassThrough()}
        field_names = ["col1"]
        signal_sequence = "X"
        scene_config = SceneConfig(
            scene_name="测试",
            prompt_template="",
            valid_codes={"X"},
            operations={"X": "pass_through"},
        )
        ops = assemble_operations(field_names, signal_sequence, scene_config, custom_registry)

        assert ops[0][1].name == "pass_through"
