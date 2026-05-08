"""SignalChain 使用示例 — 演示完整的信号链管线"""

import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import MockAIClient


def demo_medical_data():
    """医疗数据清洗示例"""
    print("=" * 60)
    print("SignalChain 医疗数据清洗示例")
    print("=" * 60)

    # 构建模拟 AI 客户端（实际使用时替换为 OpenAIClient）
    mock_ai = MockAIClient(responses={
        # 场景识别 Prompt 中包含 "patient" 等关键词 → 返回 S1
        "字段名": "S1",
        # 字段语义识别 Prompt 中包含 "gender" → 返回 IGADN
        "patient_id": "IGADN",
    })

    # 创建管线
    pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=":memory:")

    # 构建测试数据
    df = pd.DataFrame({
        "patient_id": ["P001", "P002", "P003", "P004"],
        "gender": ["M", "F", "Male", "帅哥"],
        "age": ["30", "30岁", "约30", "25Y"],
        "dept_name": ["心内", "外科", "妇科", "ICU"],
        "drug_name": ["阿莫西林0.25g", "甲硝唑", "未知药品", "头孢"],
    })

    print("\n原始数据：")
    print(df.to_string())

    # 执行管线
    result, report = pipeline.run(df)

    print("\n清洗后数据：")
    print(result.to_string())

    print("\n" + report.summary())

    # 第二次运行（缓存命中）
    print("\n" + "-" * 60)
    print("第二次运行（相同数据，应命中缓存）：")
    result2, report2 = pipeline.run(df)
    print(report2.summary())


def demo_local_mode():
    """本地模式 — 跳过 AI，直接指定场景和信号序列"""
    print("\n" + "=" * 60)
    print("SignalChain 本地模式示例")
    print("=" * 60)

    df = pd.DataFrame({
        "patient_id": ["P001", "P002"],
        "gender": ["M", "F"],
        "age": ["30", "30岁"],
        "dept_name": ["心内", "外科"],
        "drug_name": ["阿莫西林", "甲硝唑"],
    })

    # 直接指定场景码和信号序列，跳过 AI
    result, report = SignalChainPipeline.run_local(df, scene_code="S1", signal_sequence="IGADN")

    print("\n清洗后数据：")
    print(result.to_string())
    print("\n" + report.summary())


def demo_user_data():
    """用户数据清洗示例"""
    print("\n" + "=" * 60)
    print("SignalChain 用户数据清洗示例")
    print("=" * 60)

    mock_ai = MockAIClient(responses={
        "字段名": "S3",
        "user_id": "IGAEI",
    })

    pipeline = SignalChainPipeline(ai_client=mock_ai, cache_file=":memory:")

    df = pd.DataFrame({
        "user_id": ["U001", "U002", "U003"],
        "gender": ["男", "F", "1"],
        "age": ["25", "30岁", "约28"],
        "email": ["test@example.com", "invalid-email", "user@mail.cn"],
        "phone": ["13800138000", "12345", "13912345678"],
    })

    print("\n原始数据：")
    print(df.to_string())

    result, report = pipeline.run(df)

    print("\n清洗后数据：")
    print(result.to_string())
    print("\n" + report.summary())


if __name__ == "__main__":
    demo_medical_data()
    demo_local_mode()
    demo_user_data()
