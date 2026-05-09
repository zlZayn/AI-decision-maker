"""Pipeline 集成测试"""

import pandas as pd
import tempfile

from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import MockAIClient


class SequenceMockAI(MockAIClient):
    """按调用顺序返回预设响应的MockAI

    第一次 call → 场景识别（返回 scene_code）
    第二次 call → 字段语义识别（返回 signal_sequence）
    """

    def __init__(self, scene_code: str, signal_sequence: str):
        super().__init__()
        self.scene_code = scene_code
        self.signal_sequence = signal_sequence
        self._call_count = 0

    def call(self, prompt: str) -> str:
        self._call_count += 1
        if self._call_count == 1:
            return self.scene_code
        else:
            return self.signal_sequence


class TestSignalChainPipeline:
    """测试 SignalChainPipeline 完整流程"""

    def test_medical_data_full_pipeline(self):
        """医疗数据完整管线测试"""
        mock_ai = SequenceMockAI(scene_code="S1", signal_sequence="IGADN")

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
            # 科室应被标准化（字段名已被标准化为 department）
            assert result["department"].iloc[0] == "心内科"

    def test_cache_hit_second_run(self):
        """第二次运行应命中缓存"""
        call_count = 0

        class CountingMockAI(SequenceMockAI):
            def call(self, prompt):
                nonlocal call_count
                call_count += 1
                return super().call(prompt)

        mock_ai = CountingMockAI(scene_code="S1", signal_sequence="IGADN")

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
        mock_ai = SequenceMockAI(scene_code="INVALID", signal_sequence="IX")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=f.name)

            df = pd.DataFrame({
                "col1": ["a", "b"],
                "col2": ["c", "d"],
            })

            result, report = pipeline.run(df)

            # S0 场景下 I→id 重命名，X 保留原列名
            assert list(result["id"]) == ["a", "b"]
            assert list(result["col2"]) == ["c", "d"]

    def test_empty_dataframe(self):
        """空 DataFrame 处理"""
        mock_ai = MockAIClient()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=f.name)

            df = pd.DataFrame()
            result, report = pipeline.run(df)

            assert len(result.columns) == 0
