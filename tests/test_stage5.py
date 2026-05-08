"""Stage 5 测试 — 本地执行引擎"""

import pandas as pd
import pytest

from signalchain.stage5_execute import execute_pipeline, QualityReport
from signalchain.operations.pass_through import PassThrough
from signalchain.operations.gender import GenderNormalizer
from signalchain.operations.age import AgeExtractor


class TestExecutePipeline:
    """测试 execute_pipeline 函数"""

    def test_pass_through(self):
        df = pd.DataFrame({"col": ["a", "b", "c"]})
        ops = [("col", PassThrough())]
        result, report = execute_pipeline(df, ops)

        assert list(result["col"]) == ["a", "b", "c"]
        assert report.records[0].errors == 0

    def test_gender_normalization(self):
        df = pd.DataFrame({"gender": ["M", "F", "Male", "帅哥"]})
        ops = [("gender", GenderNormalizer())]
        result, report = execute_pipeline(df, ops)

        assert list(result["gender"]) == ["男", "女", "男", "男"]
        assert report.records[0].errors == 0

    def test_age_extraction(self):
        df = pd.DataFrame({"age": ["30", "30岁", "约30"]})
        ops = [("age", AgeExtractor())]
        result, report = execute_pipeline(df, ops)

        assert list(result["age"]) == [30, 30, 30]

    def test_multiple_operations(self):
        df = pd.DataFrame({
            "id": ["P001", "P002"],
            "gender": ["M", "F"],
        })
        ops = [
            ("id", PassThrough()),
            ("gender", GenderNormalizer()),
        ]
        result, report = execute_pipeline(df, ops)

        assert list(result["id"]) == ["P001", "P002"]
        assert list(result["gender"]) == ["男", "女"]
        assert len(report.records) == 2

    def test_exception_handling(self):
        """操作异常时保留原值"""

        class FailingOperation(PassThrough):
            @property
            def name(self):
                return "failing_op"

            def execute(self, data):
                raise ValueError("test error")

        df = pd.DataFrame({"col": ["original"]})
        ops = [("col", FailingOperation())]
        result, report = execute_pipeline(df, ops)

        # 应该保留原值
        assert list(result["col"]) == ["original"]
        assert report.records[0].errors == 1

    def test_original_df_not_modified(self):
        """执行管线不应修改原始 DataFrame"""
        df = pd.DataFrame({"gender": ["M", "F"]})
        ops = [("gender", GenderNormalizer())]
        result, report = execute_pipeline(df, ops)

        # 原始数据不变
        assert list(df["gender"]) == ["M", "F"]
        assert list(result["gender"]) == ["男", "女"]


class TestQualityReport:
    """测试 QualityReport"""

    def test_summary(self):
        report = QualityReport()
        report.record("col1", "pass_through", changed=0, errors=0)
        report.record("col2", "normalize_gender", changed=5, errors=0)

        summary = report.summary()
        assert "col1" in summary
        assert "col2" in summary
        assert "changed=5" in summary
