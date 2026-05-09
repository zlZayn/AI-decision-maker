"""Stage 0 测试 — 元信息提取与指纹生成"""

import pandas as pd

from signalchain.stage0_profile import extract_profile, generate_fingerprint


class TestExtractProfile:
    """测试 extract_profile 函数"""

    def test_basic_dataframe(self):
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "score": [85.5, 92.3, 78.1],
        })
        profile = extract_profile(df)

        assert profile.field_count == 3
        assert profile.field_names == ["name", "age", "score"]
        assert profile.fields[0].type == "string"
        assert profile.fields[1].type == "int"
        assert profile.fields[2].type == "float"

    def test_null_ratio(self):
        df = pd.DataFrame({
            "col1": [1, None, 3, None, 5],
            "col2": ["a", "b", "c", "d", "e"],
        })
        profile = extract_profile(df)

        assert profile.fields[0].null_ratio == 0.4  # 2/5
        assert profile.fields[1].null_ratio == 0.0

    def test_samples_dedup(self):
        df = pd.DataFrame({
            "col": ["a", "b", "a", "c", "b", "a"],
        })
        profile = extract_profile(df)

        # 去重后的样本
        assert len(profile.fields[0].samples) == 3
        assert set(profile.fields[0].samples) == {"a", "b", "c"}

    def test_samples_max_limit(self):
        df = pd.DataFrame({
            "col": [str(i) for i in range(30)],
        })
        profile = extract_profile(df, max_samples=20)

        assert len(profile.fields[0].samples) == 20

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        profile = extract_profile(df)

        assert profile.field_count == 0
        assert profile.fields == []

    def test_derived_properties(self):
        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [25, 30],
            "score": [85.5, 92.3],
        })
        profile = extract_profile(df)

        assert profile.type_summary == "1string,1int,1float"
        assert len(profile.null_ratio_summary.split(",")) == 3


class TestGenerateFingerprint:
    """测试 generate_fingerprint 函数"""

    def test_same_data_same_fingerprint(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df2 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        p1 = extract_profile(df1)
        p2 = extract_profile(df2)

        assert generate_fingerprint(p1) == generate_fingerprint(p2)

    def test_different_data_different_fingerprint(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df2 = pd.DataFrame({"a": [1, 2], "b": ["x", "z"]})

        p1 = extract_profile(df1)
        p2 = extract_profile(df2)

        assert generate_fingerprint(p1) != generate_fingerprint(p2)

    def test_different_columns_different_fingerprint(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df2 = pd.DataFrame({"a": [1, 2], "c": ["x", "y"]})

        p1 = extract_profile(df1)
        p2 = extract_profile(df2)

        assert generate_fingerprint(p1) != generate_fingerprint(p2)
