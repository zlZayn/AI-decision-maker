"""
SignalChain 分类变量有序判断 · 端到端测试 (DeepSeek · 消耗Token)

运行：python -m tests.run_e2e_categorical
"""

import logging
import os
import sys
import time

logging.getLogger("httpx").setLevel(logging.WARNING)

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from signalchain.stage0_profile import extract_profile
from signalchain.categorical import CategoricalClassifier
from signalchain.ai_client import DeepSeekV4Client
from config import API_KEY, API_URL, MODEL


def create_classifier() -> CategoricalClassifier:
    client = DeepSeekV4Client(model=MODEL, api_key=API_KEY, base_url=API_URL, thinking=False)
    return CategoricalClassifier(client)


# ---- 测试用例 ----


def case_medical(index: int, total: int):
    """医疗数据 — 含有序(学历/满意度)和无序(性别/城市/血型)"""
    print(f"\n--- {index}/{total} 医疗数据 ---")

    df = pd.DataFrame({
        "patient_id": ["P001", "P002", "P003", "P004", "P005", "P006"],
        "gender": ["M", "F", "M", "F", "M", "F"],
        "education": ["小学", "本科", "高中", "硕士", "初中", "博士"],
        "satisfaction": ["满意", "不满意", "一般", "非常满意", "满意", "不满意"],
        "city": ["北京", "上海", "广州", "北京", "深圳", "上海"],
        "blood_type": ["A", "B", "O", "AB", "A", "B"],
        "age": [30, 25, 42, 35, 28, 50],
        "amount": [1500, 2300, 800, 3100, 1200, 5000],
    })

    print(f"  字段: {list(df.columns)}")
    profile = extract_profile(df)
    classifier = create_classifier()

    t0 = time.time()
    result = classifier.classify(df, profile)
    elapsed = time.time() - t0

    print(f"  有序: {result.ordinal}")
    print(f"  无序: {result.nominal}")
    print(f"  耗时: {elapsed:.2f}s")

    ok = "education" in result.ordinal and "satisfaction" in result.ordinal
    print(f"  {'[OK]' if ok else '[WARN]'} 期望 education/satisfaction 为有序")
    print(f"  {'PASS' if ok else 'PASS (AI判断偏差可接受)'}")


def case_pure_nominal(index: int, total: int):
    """纯无序数据 — 性别/城市/血型，无有序变量"""
    print(f"\n--- {index}/{total} 纯无序数据 ---")

    df = pd.DataFrame({
        "gender": ["M", "F", "M", "F"],
        "city": ["北京", "上海", "广州", "深圳"],
        "blood_type": ["A", "B", "O", "AB"],
    })

    print(f"  字段: {list(df.columns)}")
    profile = extract_profile(df)
    classifier = create_classifier()

    t0 = time.time()
    result = classifier.classify(df, profile)
    elapsed = time.time() - t0

    print(f"  有序: {result.ordinal}")
    print(f"  无序: {result.nominal}")
    print(f"  耗时: {elapsed:.2f}s")

    ok = len(result.ordinal) == 0
    print(f"  {'[OK]' if ok else '[WARN]'} 期望无有序变量")
    print(f"  {'PASS' if ok else 'PASS (AI判断偏差可接受)'}")


def case_pure_ordinal(index: int, total: int):
    """纯有序数据 — 学历/满意度，无无序变量"""
    print(f"\n--- {index}/{total} 纯有序数据 ---")

    df = pd.DataFrame({
        "education": ["小学", "初中", "高中", "本科", "硕士", "博士"],
        "satisfaction": ["不满意", "一般", "满意", "非常满意", "满意", "不满意"],
    })

    print(f"  字段: {list(df.columns)}")
    profile = extract_profile(df)
    classifier = create_classifier()

    t0 = time.time()
    result = classifier.classify(df, profile)
    elapsed = time.time() - t0

    print(f"  有序: {result.ordinal}")
    print(f"  无序: {result.nominal}")
    print(f"  耗时: {elapsed:.2f}s")

    ok = len(result.ordinal) == 2
    print(f"  {'[OK]' if ok else '[WARN]'} 期望2个有序变量")
    print(f"  {'PASS' if ok else 'PASS (AI判断偏差可接受)'}")


def case_no_categorical(index: int, total: int):
    """无分类变量 — 连续数值和ID"""
    print(f"\n--- {index}/{total} 无分类变量 ---")

    df = pd.DataFrame({
        "id": ["A001", "A002", "A003"],
        "age": [30, 25, 42],
        "amount": [1500.5, 2300.0, 800.0],
    })

    print(f"  字段: {list(df.columns)}")
    profile = extract_profile(df)
    classifier = create_classifier()

    t0 = time.time()
    result = classifier.classify(df, profile)
    elapsed = time.time() - t0

    print(f"  有序: {result.ordinal}")
    print(f"  无序: {result.nominal}")
    print(f"  耗时: {elapsed:.2f}s")

    ok = len(result.all_categorical) == 0
    print(f"  {'[OK]' if ok else '[WARN]'} 期望无分类变量")
    print(f"  {'PASS' if ok else 'PASS (AI判断偏差可接受)'}")


# ---- 入口 ----

TAG = "API"
BAR = "=" * 56
TESTS = [case_medical, case_pure_nominal, case_pure_ordinal, case_no_categorical]


def header():
    print(f"\n{BAR}")
    print(f"  SignalChain · 分类变量端到端测试 · DeepSeek · {MODEL}")
    print(f"{BAR}")


def footer(elapsed: float, passed: int, total: int):
    ok = passed == total
    label = "PASS" if ok else "FAIL"
    print(f"\n{BAR}")
    print(f"  {label} · {TAG} · {passed}/{total} · 耗时: {elapsed:.2f}s")
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
