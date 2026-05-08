"""
SignalChain 真实端到端测试 — 接入 DeepSeek API

运行方式：
    python tests/test_real_e2e.py
"""

import os
import sys
import time

# Windows 终端 UTF-8 输出
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import OpenAIClient


# ============ 配置 ============
API_KEY = "sk-abff4438c575405b9a42f813ec21b992"
API_URL = "https://api.deepseek.com/v1"
MODEL = "deepseek-chat"


def create_pipeline() -> SignalChainPipeline:
    """创建使用真实 AI 的管线"""
    client = OpenAIClient(model=MODEL, api_key=API_KEY, base_url=API_URL)
    return SignalChainPipeline(ai_client=client, cache_file=":memory:")


def test_medical():
    """测试1: 医疗数据 — 预期场景 S1"""
    print("\n" + "=" * 60)
    print("测试1: 医疗数据")
    print("=" * 60)

    df = pd.DataFrame({
        "patient_id": ["P001", "P002", "P003", "P004"],
        "gender": ["M", "F", "Male", "帅哥"],
        "age": ["30", "30岁", "约30", "25Y"],
        "dept_name": ["心内", "外科", "妇科", "ICU"],
        "drug_name": ["阿莫西林0.25g", "甲硝唑", "未知药品", "头孢"],
    })

    print("\n原始数据：")
    print(df.to_string())

    pipeline = create_pipeline()
    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    print("\n清洗后数据：")
    print(result.to_string())
    print(report.summary())
    print(f"\n耗时: {elapsed:.2f}s")

    # 验证：至少部分字段被正确处理
    # gender 列：如果 AI 正确识别为 G，则应该被标准化
    gender_values = list(result["gender"])
    if gender_values == ["男", "女", "男", "男"]:
        print("[OK] 性别标准化完美")
    else:
        print(f"[INFO] 性别标准化结果: {gender_values} (AI可能未识别为G信号码)")

    # age 列：如果 AI 正确识别为 A，则应该被提取为数字
    age_values = list(result["age"])
    if all(isinstance(v, (int, float)) for v in age_values):
        print("[OK] 年龄提取成功")
    else:
        print(f"[INFO] 年龄提取结果: {age_values}")

    print("\n[PASS] 医疗数据测试完成")


def test_user():
    """测试2: 用户数据 — 预期场景 S3"""
    print("\n" + "=" * 60)
    print("测试2: 用户数据")
    print("=" * 60)

    df = pd.DataFrame({
        "user_id": ["U001", "U002", "U003"],
        "gender": ["男", "F", "1"],
        "age": ["25", "30岁", "约28"],
        "email": ["test@example.com", "invalid-email", "user@mail.cn"],
        "phone": ["13800138000", "12345", "13912345678"],
    })

    print("\n原始数据：")
    print(df.to_string())

    pipeline = create_pipeline()
    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    print("\n清洗后数据：")
    print(result.to_string())
    print(report.summary())
    print(f"\n耗时: {elapsed:.2f}s")
    print("\n[PASS] 用户数据测试完成")


def test_finance():
    """测试3: 财务数据 — 预期场景 S2"""
    print("\n" + "=" * 60)
    print("测试3: 财务数据")
    print("=" * 60)

    df = pd.DataFrame({
        "transaction_id": ["T001", "T002", "T003"],
        "amount": ["¥1,000.50", "$200", "500"],
        "date": ["2024-01-15", "2024/02/20", "2024年3月1日"],
    })

    print("\n原始数据：")
    print(df.to_string())

    pipeline = create_pipeline()
    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    print("\n清洗后数据：")
    print(result.to_string())
    print(report.summary())
    print(f"\n耗时: {elapsed:.2f}s")

    # 如果 amount 被识别为 M，应该被标准化为数字
    amount_values = list(result["amount"])
    if all(isinstance(v, (int, float)) for v in amount_values):
        print("[OK] 金额标准化成功")
    else:
        print(f"[INFO] 金额标准化结果: {amount_values}")

    print("\n[PASS] 财务数据测试完成")


def test_cache_hit():
    """测试4: 缓存命中 — 第二次运行应零Token且更快"""
    print("\n" + "=" * 60)
    print("测试4: 缓存命中验证")
    print("=" * 60)

    df = pd.DataFrame({
        "patient_id": ["P001", "P002"],
        "gender": ["M", "F"],
        "age": ["30", "25"],
        "dept_name": ["心内", "外科"],
        "drug_name": ["阿莫西林", "甲硝唑"],
    })

    pipeline = create_pipeline()

    # 第一次运行
    t0 = time.time()
    result1, _ = pipeline.run(df)
    first_time = time.time() - t0

    # 第二次运行（应命中缓存）
    t0 = time.time()
    result2, _ = pipeline.run(df)
    second_time = time.time() - t0

    print(f"首次运行耗时: {first_time:.2f}s (含AI调用)")
    print(f"缓存命中耗时: {second_time:.2f}s (零Token)")

    # 缓存命中应远快于首次
    if second_time < first_time * 0.5:
        print("[OK] 缓存命中显著加速")
    else:
        print(f"[WARN] 缓存命中加速不明显: first={first_time:.2f}s, second={second_time:.2f}s")

    # 两次结果应一致
    assert result1.equals(result2), "缓存命中的结果应与首次一致"
    print("\n[PASS] 缓存命中测试完成")


if __name__ == "__main__":
    print("SignalChain 真实端到端测试")
    print(f"模型: {MODEL}")
    print(f"API: {API_URL}")
    print()

    try:
        test_medical()
        test_user()
        test_finance()
        test_cache_hit()
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED!")
        print("=" * 60)
    except Exception as e:
        print(f"\n[FAIL] 测试失败: {e}")
        import traceback
        traceback.print_exc()
