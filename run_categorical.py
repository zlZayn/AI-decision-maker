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
from signalchain.ai_client import DeepSeekV4Client
from signalchain.categorical import CategoricalClassifier
from config import API_KEY, API_URL, MODEL

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


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SignalChain Categorical Analysis")
    parser.add_argument("--no-cache", action="store_true", help="force re-classify all files")
    args = parser.parse_args()

    csv_files = sorted(
        f for f in os.listdir(INPUT_DIR) if f.endswith(".csv")
    )
    if not csv_files:
        print(f"  [ERR] No CSV files in {INPUT_DIR}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(BAR)
    print("  Categorical Variable Analysis")
    print(f"  model: {MODEL} | thinking: OFF")
    print(BAR)
    print(f"\n  input : {os.path.relpath(INPUT_DIR, ROOT)} ({len(csv_files)} files)")
    print(f"  output: {os.path.relpath(OUTPUT_DIR, ROOT)}")

    # ---- AI 分类每个文件 ----
    client = DeepSeekV4Client(
        model=MODEL, api_key=API_KEY, base_url=API_URL, thinking=False,
    )
    classifier = CategoricalClassifier(client)

    results = []

    for filename in csv_files:
        filepath = os.path.join(INPUT_DIR, filename)
        df = pd.read_csv(filepath)
        profile = extract_profile(df)

        out_name = filename.replace(".csv", "_type.json")
        out_path = os.path.join(OUTPUT_DIR, out_name)

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
            # 调用 AI
            t0 = time.time()
            result = classifier.classify(df, profile)
            elapsed = time.time() - t0

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
    print(f"  total: {len(results)} files | {called_count} called | {cached_count} cached | {total_time:.2f}s | cost: {cost:.6f}")
    if usage.prompt_tokens:
        print(f"  tokens: in={usage.prompt_tokens}  out={usage.completion_tokens}")

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
