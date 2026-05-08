"""Stage 3 测试 — 字段语义识别校验"""

from signalchain.stage3_semantic import validate_field_signal_sequence


class TestValidateFieldSignalSequence:
    """测试 validate_field_signal_sequence 函数"""

    def test_valid_sequence(self):
        result = validate_field_signal_sequence("IGADN", 5, {"I", "G", "A", "D", "N", "X"})
        assert result == "IGADN"

    def test_too_short_pads_with_x(self):
        """过短 → 补 X"""
        result = validate_field_signal_sequence("IGA", 5, {"I", "G", "A", "X"})
        assert result == "IGAXX"

    def test_too_long_truncates(self):
        """过长 → 截取前 field_count 个字符"""
        result = validate_field_signal_sequence("IGADNXX", 5, {"I", "G", "A", "D", "N", "X"})
        assert result == "IGADN"

    def test_invalid_code_replaced_per_char(self):
        """非法字符逐个替换为 X（不整体回退）"""
        # G 在 S2（财务）场景中不合法
        result = validate_field_signal_sequence("MGTI", 4, {"M", "T", "I", "X"})
        assert result == "MXTI"

    def test_whitespace_handling(self):
        result = validate_field_signal_sequence("I G A D N", 5, {"I", "G", "A", "D", "N", "X"})
        assert result == "IGADN"

    def test_empty_output_fallback(self):
        result = validate_field_signal_sequence("", 3, {"I", "X"})
        assert result == "XXX"

    def test_all_valid_codes_in_scene(self):
        """S1 场景的完整校验"""
        s1_codes = {"G", "A", "D", "N", "C", "T", "I", "X"}
        result = validate_field_signal_sequence("IGADN", 5, s1_codes)
        assert result == "IGADN"

    def test_scene_level_validation(self):
        """不同场景对同一信号序列的校验结果不同"""
        s1_codes = {"G", "A", "D", "N", "C", "T", "I", "X"}
        s2_codes = {"M", "T", "I", "X"}

        # "M" 在 S2 合法，在 S1 不合法（替换为X）
        assert validate_field_signal_sequence("MI", 2, s2_codes) == "MI"
        assert validate_field_signal_sequence("MI", 2, s1_codes) == "XI"

    def test_newline_handling(self):
        """AI 有时在输出中加换行"""
        result = validate_field_signal_sequence("IGA\nDN", 5, {"I", "G", "A", "D", "N", "X"})
        assert result == "IGADN"

    def test_six_chars_for_five_fields(self):
        """AI 多输出一个字符的场景（如 IDGADN → 截取前5个 → IDGAD）"""
        result = validate_field_signal_sequence("IDGADN", 5, {"I", "G", "A", "D", "N", "X"})
        assert result == "IDGAD"
