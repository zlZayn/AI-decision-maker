"""Pipeline 集成测试"""

import pandas as pd
import tempfile

from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import MockAIClient
from signalchain.stage5_execute import QualityReport


class TestSignalChainPipeline:
    """测试 SignalChainPipeline 完整流程"""

    def test_medical_data_full_pipeline(self):
        """医疗数据完整管线测试"""
        mock_ai = MockAIClient(responses={
            "字段名": "S1",
            "patient_id": "IGADN",
        })

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=f.name)

            df = pd.DataFrame({
                "patient_id": ["P001", "P002"],
                "gender": ["M", "F"],
                "age": ["30", "30岁"],
                "dept_name": ["心内", "外科"],
                "drug_name": ["阿莫西林", "甲硝唑"],
            })

            result, report = pipeline.run(df)

            # 性别应被标准化
            assert list(result["gender"]) == ["男", "女"]
            # 年龄应被提取
            assert list(result["age"]) == [30, 30]
            # 科室应被标准化
            assert result["dept_name"].iloc[0] == "心内科"

    def test_cache_hit_second_run(self):
        """第二次运行应命中缓存"""
        call_count = 0

        class CountingMockAI(MockAIClient):
            def call(self, prompt):
                nonlocal call_count
                call_count += 1
                return super().call(prompt)

        mock_ai = CountingMockAI(responses={
            "字段名": "S1",
            "patient_id": "IGADN",
        })

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=f.name)

            df = pd.DataFrame({
                "patient_id": ["P001", "P002"],
                "gender": ["M", "F"],
                "age": ["30", "30岁"],
                "dept_name": ["心内", "外科"],
                "drug_name": ["阿莫西林", "甲硝唑"],
            })

            # 第一次运行
            pipeline.run(df)
            first_call_count = call_count

            # 第二次运行（缓存命中）
            pipeline.run(df)
            second_call_count = call_count

            # 缓存命中时 AI 不应被再次调用
            assert second_call_count == first_call_count

    def test_run_local_mode(self):
        """本地模式测试（跳过 AI）"""
        df = pd.DataFrame({
            "patient_id": ["P001", "P002"],
            "gender": ["M", "F"],
            "age": ["30", "30岁"],
            "dept_name": ["心内", "外科"],
            "drug_name": ["阿莫西林", "甲硝唑"],
        })

        result, report = SignalChainPipeline.run_local(df, "S1", "IGADN")

        assert list(result["gender"]) == ["男", "女"]
        assert list(result["age"]) == [30, 30]

    def test_invalid_scene_fallback(self):
        """无效场景码回退到 S0"""
        mock_ai = MockAIClient(responses={
            "字段名": "INVALID",  # 将被校验为 S0
            "col1": "IX",        # S0 只有 I 和 X
        })

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=f.name)

            df = pd.DataFrame({
                "col1": ["a", "b"],
                "col2": ["c", "d"],
            })

            result, report = pipeline.run(df)

            # S0 场景下所有字段 pass_through
            assert list(result["col1"]) == ["a", "b"]
            assert list(result["col2"]) == ["c", "d"]

    def test_empty_dataframe(self):
        """空 DataFrame 处理"""
        mock_ai = MockAIClient()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=f.name)

            df = pd.DataFrame()
            result, report = pipeline.run(df)

            assert len(result.columns) == 0
