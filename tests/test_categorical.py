"""分类变量有序判断 — 单元测试"""

from __future__ import annotations

import pandas as pd

from signalchain.categorical import (
    CategoricalClassifier,
    ClassificationResult,
    build_categorical_prompt,
    build_ordinal_prompt,
    validate_categorical_output,
    validate_ordinal_output,
    extract_unique_values,
    apply_categorical_type,
)
from signalchain.models import DataProfile, FieldProfile
from signalchain.ai_client import MockAIClient


# ============================================================
# 辅助工具
# ============================================================


class SequenceMockAI(MockAIClient):
    """按调用顺序返回预设响应的 MockAI

    第一次 call → 第一层（分类变量筛选）
    第二次 call → 第二层（有序/无序判断）
    """

    def __init__(self, responses: list[str]):
        super().__init__()
        self._responses = list(responses)
        self._call_count = 0

    def call(self, prompt: str) -> str:
        self.call_log.append(prompt)
        if self._call_count < len(self._responses):
            resp = self._responses[self._call_count]
            self._call_count += 1
            return resp
        return "无"


def _make_profile(fields: list[tuple[str, str, list[str]]]) -> DataProfile:
    """快捷创建 DataProfile。参数：(name, type, samples)"""
    return DataProfile(fields=[
        FieldProfile(name=n, type=t, samples=s, null_ratio=0.0)
        for n, t, s in fields
    ])


def _make_df(columns: dict[str, list]) -> pd.DataFrame:
    return pd.DataFrame(columns)


# ============================================================
# Prompt 构建测试
# ============================================================


class TestBuildCategoricalPrompt:
    def test_contains_field_info(self):
        profile = _make_profile([
            ("gender", "string", ["M", "F"]),
            ("age", "int", ["30", "25"]),
        ])
        prompt = build_categorical_prompt(profile)
        assert "gender(string, 2种)" in prompt
        assert "age(int, 2种)" in prompt
        assert "M, F" in prompt

    def test_contains_instructions(self):
        profile = _make_profile([("x", "string", ["a"])])
        prompt = build_categorical_prompt(profile)
        assert "分类变量" in prompt
        assert "无" in prompt


class TestBuildOrdinalPrompt:
    def test_contains_field_values(self):
        prompt = build_ordinal_prompt(
            ["education", "gender"],
            {"education": ["小学", "本科"], "gender": ["M", "F"]},
        )
        assert "education: 小学, 本科" in prompt
        assert "gender: M, F" in prompt

    def test_truncates_many_values(self):
        many = [f"v{i}" for i in range(50)]
        prompt = build_ordinal_prompt(["col"], {"col": many})
        assert "共50个" in prompt


# ============================================================
# 输出校验测试
# ============================================================


class TestValidateCategoricalOutput:
    def test_valid_output(self):
        result = validate_categorical_output(
            "gender,education", ["patient_id", "gender", "education", "age"]
        )
        assert result == ["gender", "education"]

    def test_no_categorical(self):
        assert validate_categorical_output("无", ["a", "b"]) == []

    def test_empty_output(self):
        assert validate_categorical_output("", ["a"]) == []

    def test_filters_invalid_names(self):
        result = validate_categorical_output(
            "gender,nonexistent,age", ["gender", "age"]
        )
        assert result == ["gender", "age"]

    def test_deduplicates(self):
        result = validate_categorical_output(
            "gender,gender,age", ["gender", "age"]
        )
        assert result == ["gender", "age"]

    def test_preserves_order(self):
        result = validate_categorical_output(
            "age,gender", ["gender", "age"]
        )
        assert result == ["age", "gender"]

    def test_whitespace_handling(self):
        result = validate_categorical_output(
            " gender , education ", ["gender", "education"]
        )
        assert result == ["gender", "education"]

    def test_chinese_comma(self):
        result = validate_categorical_output(
            "gender，education", ["gender", "education"]
        )
        assert result == ["gender", "education"]


class TestValidateOrdinalOutput:
    def test_valid_output(self):
        raw = "education:小学>初中>高中>本科;satisfaction:不满意>一般>满意"
        result = validate_ordinal_output(
            raw,
            ["education", "satisfaction", "gender"],
            {
                "education": ["小学", "初中", "高中", "本科"],
                "satisfaction": ["不满意", "一般", "满意"],
                "gender": ["M", "F"],
            },
        )
        assert result == {
            "education": ["小学", "初中", "高中", "本科"],
            "satisfaction": ["不满意", "一般", "满意"],
        }

    def test_no_ordinal(self):
        assert validate_ordinal_output(
            "无", ["gender"], {"gender": ["M", "F"]}
        ) == {}

    def test_empty_output(self):
        assert validate_ordinal_output(
            "", ["gender"], {"gender": ["M", "F"]}
        ) == {}

    def test_filters_invalid_field(self):
        raw = "nonexistent:小学>本科"
        result = validate_ordinal_output(
            raw, ["education"], {"education": ["小学", "本科"]}
        )
        assert result == {}

    def test_filters_invalid_values(self):
        """顺序中包含不存在的值，应被过滤"""
        raw = "education:小学>博士>本科"
        result = validate_ordinal_output(
            raw,
            ["education"],
            {"education": ["小学", "本科"]},
        )
        # "博士" 不在唯一值中，被过滤；"小学" 和 "本科" 保留
        assert result == {"education": ["小学", "本科"]}

    def test_single_ordinal(self):
        raw = "education:小学>本科"
        result = validate_ordinal_output(
            raw, ["education"], {"education": ["小学", "本科"]}
        )
        assert result == {"education": ["小学", "本科"]}

    def test_malformed_segment_skipped(self):
        """缺少冒号的段应被跳过"""
        raw = "education:小学>本科;invalid_segment"
        result = validate_ordinal_output(
            raw, ["education"], {"education": ["小学", "本科"]}
        )
        assert result == {"education": ["小学", "本科"]}

    def test_chinese_punctuation(self):
        """中文冒号和分号应被正确处理"""
        raw = "education：小学>本科；satisfaction：不满意>满意"
        result = validate_ordinal_output(
            raw,
            ["education", "satisfaction"],
            {
                "education": ["小学", "本科"],
                "satisfaction": ["不满意", "满意"],
            },
        )
        assert result == {
            "education": ["小学", "本科"],
            "satisfaction": ["不满意", "满意"],
        }


# ============================================================
# 脚本步骤测试
# ============================================================


class TestExtractUniqueValues:
    def test_basic(self):
        df = _make_df({"gender": ["M", "F", "M"], "city": ["北京", "上海", "广州"]})
        result = extract_unique_values(df, ["gender", "city"])
        assert set(result["gender"]) == {"M", "F"}
        assert set(result["city"]) == {"北京", "上海", "广州"}

    def test_handles_nan(self):
        df = _make_df({"gender": ["M", None, "F", None]})
        result = extract_unique_values(df, ["gender"])
        assert set(result["gender"]) == {"M", "F"}

    def test_missing_column_skipped(self):
        df = _make_df({"gender": ["M", "F"]})
        result = extract_unique_values(df, ["gender", "nonexistent"])
        assert "nonexistent" not in result

    def test_does_not_modify_df(self):
        df = _make_df({"gender": ["M", "F"]})
        original_dtype = df["gender"].dtype
        extract_unique_values(df, ["gender"])
        assert df["gender"].dtype == original_dtype


class TestApplyCategoricalType:
    def test_ordinal_ordered(self):
        df = _make_df({"education": ["高中", "小学", "本科"]})
        result = apply_categorical_type(
            df, ["education"], {"education": ["小学", "高中", "本科"]}
        )
        assert result["education"].cat.ordered is True
        # 小学 < 高中 < 本科
        assert result["education"].iloc[1] < result["education"].iloc[0]

    def test_nominal_unordered(self):
        df = _make_df({"gender": ["M", "F"]})
        result = apply_categorical_type(df, ["gender"], {})
        assert result["gender"].cat.ordered is False

    def test_does_not_modify_original(self):
        df = _make_df({"gender": ["M", "F"]})
        apply_categorical_type(df, ["gender"], {})
        assert df["gender"].dtype.name != "category"


# ============================================================
# 主类集成测试
# ============================================================


class TestCategoricalClassifier:
    def test_mixed_fields(self):
        """同时包含有序、无序、非分类变量"""
        df = _make_df({
            "patient_id": ["P001", "P002", "P003"],
            "gender": ["M", "F", "M"],
            "education": ["小学", "本科", "高中"],
            "satisfaction": ["满意", "不满意", "非常满意"],
            "age": [30, 25, 42],
        })
        profile = _make_profile([
            ("patient_id", "string", ["P001", "P002", "P003"]),
            ("gender", "string", ["M", "F"]),
            ("education", "string", ["小学", "本科", "高中"]),
            ("satisfaction", "string", ["满意", "不满意", "非常满意"]),
            ("age", "int", ["30", "25", "42"]),
        ])

        mock = SequenceMockAI([
            "gender,education,satisfaction",
            "education:小学>高中>本科;satisfaction:不满意>满意>非常满意",
        ])
        classifier = CategoricalClassifier(mock)
        result = classifier.classify(df, profile)

        assert isinstance(result, ClassificationResult)
        assert "education" in result.ordinal
        assert "satisfaction" in result.ordinal
        assert result.nominal == ["gender"]

    def test_no_categorical(self):
        """没有分类变量"""
        df = _make_df({"age": [30, 25], "amount": [100, 200]})
        profile = _make_profile([
            ("age", "int", ["30", "25"]),
            ("amount", "int", ["100", "200"]),
        ])

        mock = SequenceMockAI(["无"])
        result = CategoricalClassifier(mock).classify(df, profile)

        assert result.ordinal == {}
        assert result.nominal == []
        assert result.all_categorical == []

    def test_all_nominal(self):
        """全部是无序变量"""
        df = _make_df({"gender": ["M", "F"], "city": ["北京", "上海"]})
        profile = _make_profile([
            ("gender", "string", ["M", "F"]),
            ("city", "string", ["北京", "上海"]),
        ])

        mock = SequenceMockAI(["gender,city", "无"])
        result = CategoricalClassifier(mock).classify(df, profile)

        assert result.ordinal == {}
        assert set(result.nominal) == {"gender", "city"}

    def test_all_ordinal(self):
        """全部是有序变量"""
        df = _make_df({
            "education": ["小学", "本科"],
            "satisfaction": ["满意", "不满意"],
        })
        profile = _make_profile([
            ("education", "string", ["小学", "本科"]),
            ("satisfaction", "string", ["满意", "不满意"]),
        ])

        mock = SequenceMockAI([
            "education,satisfaction",
            "education:小学>本科;satisfaction:不满意>满意",
        ])
        result = CategoricalClassifier(mock).classify(df, profile)

        assert set(result.ordinal.keys()) == {"education", "satisfaction"}
        assert result.nominal == []

    def test_ai_output_with_extra_text(self):
        """AI 输出包含多余空白和中文逗号时，校验应能处理"""
        df = _make_df({"gender": ["M", "F"], "city": ["北京", "上海"]})
        profile = _make_profile([
            ("gender", "string", ["M", "F"]),
            ("city", "string", ["北京", "上海"]),
        ])

        mock = SequenceMockAI([
            " gender，city \n",  # 中文逗号 + 首尾空白
            "无",
        ])
        result = CategoricalClassifier(mock).classify(df, profile)

        assert set(result.nominal) == {"gender", "city"}
        assert result.ordinal == {}

    def test_prompt_log_captured(self):
        """验证 prompt 被记录到 call_log"""
        df = _make_df({"gender": ["M", "F"]})
        profile = _make_profile([("gender", "string", ["M", "F"])])

        mock = SequenceMockAI(["gender", "无"])
        CategoricalClassifier(mock).classify(df, profile)

        assert len(mock.call_log) == 2
        assert "字段列表" in mock.call_log[0]
        assert "分类变量" in mock.call_log[1]
