"""
SignalChain 分类变量统计分析

读取 data/categorical/input/ 下的 CSV，AI 判断分类变量类型，
R 脚本执行 4 种统计检验。

Usage:
    python run_categorical.py              # use cache
    python run_categorical.py --no-cache   # force re-classify
"""

import json
import logging
import os
import subprocess
import sys
import time

import pandas as pd

from signalchain.stage0_profile import extract_profile
from signalchain.ai_client import DeepSeekV4Client, MockAIClient
from signalchain.categorical import CategoricalClassifier
from signalchain.categorical_system1 import System1CategoricalClassifier, System1Classification
from config import SYSTEM2_API_KEY, SYSTEM2_BASE_URL, SYSTEM2_MODEL

logging.getLogger("signalchain").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for s in (sys.stdout, sys.stderr):
        if hasattr(s, "reconfigure"):
            s.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(ROOT, "data", "categorical", "input")
OUTPUT_DIR = os.path.join(ROOT, "data", "categorical", "output")
R_SCRIPT = os.path.join(ROOT, "signalchain", "run_categorical_analysis.R")

BAR = "=" * 62
DASH = "-" * 62


def _pad(s: str, width: int) -> str:
    extra = sum(1 for c in s if ord(c) > 127)
    return s + " " * max(0, width - len(s) - extra)


def _calc_cost(prompt: int, completion: int) -> float:
    return (prompt * 1.0 + completion * 2.0) / 1_000_000


def _read_cache(out_path: str) -> dict | None:
    """读取缓存的 type JSON，不存在或解析失败返回 None"""
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _cache_hit(csv_path: str, out_path: str) -> dict | None:
    """检查缓存：out_path 存在且比 CSV 更新时返回缓存内容"""
    if not os.path.exists(out_path):
        return None
    csv_mtime = os.path.getmtime(csv_path)
    out_mtime = os.path.getmtime(out_path)
    if out_mtime <= csv_mtime:
        return None
    return _read_cache(out_path)


def _build_classifier(use_system1: bool, escalate: bool, client):
    """构造分类器。两套系统互不依赖，返回 (classifier, 模式标签)。

    - 默认：纯系统二
    - --system1：纯系统一（不构造系统二客户端）
    - --system1 --escalate：串联（系统一不确定的字段交系统二复判）

    任何一步不可用都回退系统二，并打印原因。
    """
    if not use_system1:
        return CategoricalClassifier(client), "system 2 only"

    try:
        from config import SYSTEM1_API_KEY, SYSTEM1_BASE_URL, SYSTEM1_MODEL
    except ImportError:
        print("  [WARN] config.py 里没有 SYSTEM1_* 配置，回退系统二")
        return CategoricalClassifier(client), "system 2 only"

    if not SYSTEM1_API_KEY:
        print("  [WARN] SYSTEM1_API_KEY 为空，回退系统二")
        return CategoricalClassifier(client), "system 2 only"

    from signalchain.system1 import JevEvaluator

    evaluator = JevEvaluator(
        api_key=SYSTEM1_API_KEY, base_url=SYSTEM1_BASE_URL, model=SYSTEM1_MODEL
    )
    if not evaluator.available():
        print("  [WARN] 系统一不可用（未装 typesafe-sdk？请 uv sync --extra system1），回退系统二")
        return CategoricalClassifier(client), "system 2 only"

    fallback = CategoricalClassifier(client) if escalate else None
    tag = "system 1 + escalate to system 2" if escalate else "system 1 only"
    return System1CategoricalClassifier(evaluator, fallback=fallback), tag


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SignalChain Categorical Analysis")
    parser.add_argument("--no-cache", action="store_true", help="force re-classify all files")
    parser.add_argument(
        "--system1", action="store_true",
        help="use System 1 (Jev) ONLY for classification (needs uv sync --extra system1)",
    )
    parser.add_argument(
        "--escalate", action="store_true",
        help="let System 1 escalate uncertain fields to System 2 "
             "(opt-in chaining; requires --system1)",
    )
    args = parser.parse_args()
    system1_only = args.system1 and not args.escalate

    csv_files = sorted(
        f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")
    )
    if not csv_files:
        print(f"  [ERR] No CSV files in {INPUT_DIR}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---- 分类器（两套系统互不依赖）----
    # 纯系统一时不构造系统二客户端，连 DeepSeek Key 都不需要
    client = (
        MockAIClient()
        if system1_only
        else DeepSeekV4Client(
            model=SYSTEM2_MODEL, api_key=SYSTEM2_API_KEY,
            base_url=SYSTEM2_BASE_URL, thinking=False,
        )
    )
    classifier, classifier_tag = _build_classifier(args.system1, args.escalate, client)

    print(BAR)
    print("  Categorical Variable Analysis")
    print(f"  system 2 (LLM): {'OFF' if system1_only else 'ON'}  |  model: {SYSTEM2_MODEL}")
    print(f"  mode: {classifier_tag}")
    print(BAR)
    print(f"\n  input : {os.path.relpath(INPUT_DIR, ROOT)} ({len(csv_files)} files)")
    print(f"  output: {os.path.relpath(OUTPUT_DIR, ROOT)}")

    results = []

    for filename in csv_files:
        filepath = os.path.join(INPUT_DIR, filename)
        df = pd.read_csv(filepath)
        profile = extract_profile(df)

        out_name = filename.replace(".csv", "_type.json")
        out_path = os.path.join(OUTPUT_DIR, out_name)
        decisions: list = []

        # ---- 缓存检查 ----
        cached = None if args.no_cache else _cache_hit(filepath, out_path)
        cache_label = ""

        if cached:
            # 缓存命中，跳过 AI
            from signalchain.categorical import ClassificationResult
            result = ClassificationResult(
                ordinal=cached.get("ordinal", {}),
                nominal=cached.get("nominal", []),
            )
            elapsed = 0.0
            cache_label = "  [cache]"
        else:
            # 调用分类器
            t0 = time.time()
            outcome = classifier.classify(df, profile)
            elapsed = time.time() - t0

            if isinstance(outcome, System1Classification):
                result = outcome.result
                decisions = outcome.decisions
            elif outcome is None:
                # 系统一此刻不可用：本文件回退系统二，不中断整批
                print("  [WARN] 系统一不可用，本文件回退系统二")
                result = CategoricalClassifier(client).classify(df, profile)
            else:
                result = outcome

            # 写入分类结果 JSON
            output = {"ordinal": result.ordinal, "nominal": result.nominal}
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)

        results.append({
            "name": filename,
            "rows": len(df),
            "ordinal": result.ordinal,
            "nominal": result.nominal,
            "elapsed": elapsed,
            "cached": cached is not None,
        })

        # 打印
        print(f"\n  {DASH}")
        header = f"  {filename}  ({len(df)} rows)"
        if cache_label:
            header += cache_label
        print(header)

        if result.ordinal:
            for name, order in result.ordinal.items():
                print(f"  [有序]  {name}: {' < '.join(order)}")
        if result.nominal:
            for name in result.nominal:
                print(f"  [无序]  {name}")
        for record in decisions:
            print(f"    {record.summary()}")

    # ---- 决策逻辑：根据变量类型选择最佳方法 ----
    for r in results:
        ordinal_set = set(r["ordinal"].keys())
        nominal_set = set(r["nominal"])
        # 取前两个分类变量（排除 id 等）
        cats = []
        for name in list(r["ordinal"].keys()) + r["nominal"]:
            cats.append(name)
            if len(cats) == 2:
                break
        if len(cats) < 2:
            r["method"] = "N/A"
            r["decision"] = ""
            continue
        c1, c2 = cats[0], cats[1]
        t1 = "有序" if c1 in ordinal_set else "无序"
        t2 = "有序" if c2 in ordinal_set else "无序"
        if t1 == "有序" and t2 == "有序":
            r["method"] = "Spearman"
            r["decision"] = "有序vs有序 -> Spearman rho"
        elif t1 == "无序" and t2 == "无序":
            r["method"] = "Cramer V"
            r["decision"] = "无序vs无序 -> Cramer's V"
        else:
            r["method"] = "Kruskal"
            r["decision"] = f"有序vs无序 -> Kruskal-Wallis"

    # ---- AI 汇总 ----
    usage = client.usage
    cost = _calc_cost(usage.prompt_tokens, usage.completion_tokens)
    cached_count = sum(1 for r in results if r["cached"])
    called_count = len(results) - cached_count
    total_time = sum(r["elapsed"] for r in results)

    print(f"\n  {DASH}")
    print(f"  AI Classification & Method Selection")
    print(f"  {DASH}")
    print(f"  {'file':<14s} {'rows':>4s} {'types':<10s} {'method':<10s} {'status':<7s}  {'result'}")
    print(f"  {'-' * 14} {'-' * 4} {'-' * 10} {'-' * 10} {'-' * 7}  {'-' * 24}")
    for r in results:
        cats = []
        for name in list(r["ordinal"].keys()) + r["nominal"]:
            t = "有序" if name in r["ordinal"] else "无序"
            cats.append(f"[{t}]{name}")
        cat_str = ", ".join(cats) if cats else "-"

        # 类型组合
        ordinal_set = set(r["ordinal"].keys())
        nominal_set = set(r["nominal"])
        type_labels = []
        for name in list(r["ordinal"].keys()) + r["nominal"]:
            type_labels.append("有序" if name in ordinal_set else "无序")
        types_str = "/".join(type_labels[:2]) if len(type_labels) >= 2 else "?"

        status = "cache" if r["cached"] else f"{r['elapsed']:.2f}s"
        method = r.get("method", "?")
        print(f"  {_pad(r['name'], 14)} {r['rows']:>4d} {_pad(types_str, 10)} {_pad(method, 10)} {status:>7s}  {cat_str}")
    print(f"  {'-' * 14} {'-' * 4} {'-' * 10} {'-' * 10} {'-' * 7}  {'-' * 24}")

    print(f"\n{BAR}")
    print("  Summary")
    print(BAR)
    print(f"  files: {len(results)}  |  called: {called_count}  |  cached: {cached_count}  |  time: {total_time:.2f}s  |  cost: {cost:.6f}")
    if usage.prompt_tokens:
        print(f"  tokens: in={usage.prompt_tokens}  out={usage.completion_tokens}")
    print(BAR)

    # ---- 调用 R ----
    print(f"\n{BAR}")
    print("  Statistical Analysis (R)")
    print(BAR + "\n")

    proc = subprocess.run(
        ["Rscript", R_SCRIPT, INPUT_DIR, OUTPUT_DIR],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if proc.stdout:
        print(proc.stdout)
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr)
        print(f"\n  [ERR] R script failed (exit code: {proc.returncode})")
    else:
        print(f"\n{BAR}")
        print("  done")
        print(BAR)


if __name__ == "__main__":
    main()
