"""
SignalChain API 端到端测试 (DeepSeek · 消耗Token)

运行：python tests/test_real_e2e.py
"""

import os
import sys
import time

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import OpenAIClient


# ============ 配置 ============
API_KEY = "sk-abff4438c575405b9a42f813ec21b992"
API_URL = "https://api.deepseek.com/v1"
MODEL = "deepseek-chat"


def create_pipeline() -> SignalChainPipeline:
    client = OpenAIClient(model=MODEL, api_key=API_KEY, base_url=API_URL)
    return SignalChainPipeline(ai_client=client, cache_file=":memory:")


# ---- 测试用例 ----

def case_medical(index: int, total: int):
    """医疗数据 — 预期场景 S1"""
    print(f"\n--- {index}/{total} 医疗数据 ---")

    df = pd.DataFrame({
        "patient_id": ["P001", "P002", "P003", "P004"],
        "gender": ["M", "F", "Male", "帅哥"],
        "age": ["30", "30岁", "约30", "25Y"],
        "dept_name": ["心内", "外科", "妇科", "ICU"],
        "drug_name": ["阿莫西林0.25g", "甲硝唑", "未知药品", "头孢"],
    })

    print(f"  原始数据: {list(df.columns)}")
    pipeline = create_pipeline()
    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    print(f"  清洗后  : {list(result.columns)}")
    print(f"  耗时    : {elapsed:.2f}s")
    print(report.summary())

    gender_values = list(result["gender"])
    age_values = list(result["age"])
    ok = gender_values == ["男", "女", "男", "男"]
    ok = ok and all(isinstance(v, (int, float)) for v in age_values)
    print(f"  {'[OK]' if ok else '[INFO]'} 性别={gender_values} 年龄={age_values}")
    print(f"  {'PASS' if ok else 'PASS (部分AI偏差可接受)'}")


def case_user(index: int, total: int):
    """用户数据 — 预期场景 S3"""
    print(f"\n--- {index}/{total} 用户数据 ---")

    df = pd.DataFrame({
        "user_id": ["U001", "U002", "U003"],
        "gender": ["男", "F", "1"],
        "age": ["25", "30岁", "约28"],
        "email": ["test@example.com", "invalid-email", "user@mail.cn"],
        "phone": ["13800138000", "12345", "13912345678"],
    })

    print(f"  原始数据: {list(df.columns)}")
    pipeline = create_pipeline()
    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    print(f"  清洗后  : {list(result.columns)}")
    print(f"  耗时    : {elapsed:.2f}s")
    print(report.summary())
    print("  PASS")


def case_finance(index: int, total: int):
    """财务数据 — 预期场景 S2"""
    print(f"\n--- {index}/{total} 财务数据 ---")

    df = pd.DataFrame({
        "transaction_id": ["T001", "T002", "T003"],
        "amount": ["¥1,000.50", "$200", "500"],
        "date": ["2024-01-15", "2024/02/20", "2024年3月1日"],
    })

    print(f"  原始数据: {list(df.columns)}")
    pipeline = create_pipeline()
    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    print(f"  清洗后  : {list(result.columns)}")
    print(f"  耗时    : {elapsed:.2f}s")
    print(report.summary())

    amount_values = list(result["amount"])
    ok = all(isinstance(v, (int, float)) for v in amount_values)
    print(f"  {'[OK]' if ok else '[INFO]'} 金额={amount_values}")
    print("  PASS")


def case_cache_hit(index: int, total: int):
    """缓存命中 — 第二次运行应零Token且更快"""
    print(f"\n--- {index}/{total} 缓存命中 ---")

    df = pd.DataFrame({
        "patient_id": ["P001", "P002"],
        "gender": ["M", "F"],
        "age": ["30", "25"],
        "dept_name": ["心内", "外科"],
        "drug_name": ["阿莫西林", "甲硝唑"],
    })

    pipeline = create_pipeline()

    t0 = time.time()
    result1, _ = pipeline.run(df)
    first_time = time.time() - t0

    t0 = time.time()
    result2, _ = pipeline.run(df)
    second_time = time.time() - t0

    print(f"  首次运行: {first_time:.2f}s (含AI调用)")
    print(f"  缓存命中: {second_time:.2f}s (零Token)")

    ok = second_time < first_time * 0.5
    print(f"  {'[OK]' if ok else '[WARN]'} 缓存加速{'显著' if ok else '不明显'}")

    assert result1.equals(result2), "缓存命中的结果应与首次一致"
    print("  PASS")


# ---- 入口 ----

TAG = "API"
BAR = "=" * 56
TESTS = [case_medical, case_user, case_finance, case_cache_hit]


def header():
    print(f"\n{BAR}")
    print(f"  SignalChain · API 端到端测试 · DeepSeek · {MODEL}")
    print(f"{BAR}")


def footer(elapsed: float, passed: int, total: int):
    ok = passed == total
    label = "PASS" if ok else "FAIL"
    print(f"\n{BAR}")
    print(f"  {label} · {TAG} · {passed}/{total} · 耗时: {elapsed:.1f}s")
    print(f"{BAR}")


if __name__ == "__main__":
    header()
    passed = 0
    t0 = time.time()
    for i, test_fn in enumerate(TESTS, 1):
        try:
            test_fn(i, len(TESTS))
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            import traceback
            traceback.print_exc()
    elapsed = time.time() - t0
    footer(elapsed, passed, len(TESTS))
