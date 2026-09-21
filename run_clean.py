"""
SignalChain Data Cleaning Tool

Reads CSV files from data/dirty/, cleans them, and writes to data/clean/.

Usage:
    python run_clean.py                  # clean all files (use cache)
    python run_clean.py --no-cache       # clean all files (skip cache)
    python run_clean.py medical          # clean a specific file
    python run_clean.py medical --no-cache
"""

import logging
import os
import sys
import time
import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import DeepSeekV4Client, MockAIClient
from config import SYSTEM2_API_KEY, SYSTEM2_BASE_URL, SYSTEM2_MODEL

logging.getLogger("signalchain").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for s in (sys.stdout, sys.stderr):
        if hasattr(s, "reconfigure"):
            s.reconfigure(encoding="utf-8", errors="replace")


ROOT = os.path.dirname(os.path.abspath(__file__))
DIRTY_DIR = os.path.join(ROOT, "data", "dirty")
CLEAN_DIR = os.path.join(ROOT, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)


PRICE_INPUT = 1.0
PRICE_OUTPUT = 2.0

BAR = "=" * 62
DASH = "-" * 62


def _build_evaluator(use_system1: bool):
    """按需构造系统一引擎；不可用时返回 None，pipeline 自动走系统二"""
    if not use_system1:
        return None
    try:
        from config import SYSTEM1_API_KEY, SYSTEM1_BASE_URL, SYSTEM1_MODEL
    except ImportError:
        print("  [WARN] config.py 里没有 SYSTEM1_* 配置，忽略 --system1")
        return None
    if not SYSTEM1_API_KEY:
        print("  [WARN] SYSTEM1_API_KEY 为空，忽略 --system1")
        return None
    from signalchain.system1 import JevEvaluator

    evaluator = JevEvaluator(
        api_key=SYSTEM1_API_KEY, base_url=SYSTEM1_BASE_URL, model=SYSTEM1_MODEL
    )
    if not evaluator.available():
        print("  [WARN] 系统一不可用（未装 typesafe-sdk？请 uv sync --extra system1），忽略 --system1")
        return None
    return evaluator


def _build_gate_policy():
    """从 config.py 读门控阈值；缺省用 GatePolicy 默认值（0.80 / 0.55）"""
    from signalchain.system1 import GatePolicy

    try:
        from config import SYSTEM1_ACCEPT, SYSTEM1_ESCALATE
    except ImportError:
        return GatePolicy()
    return GatePolicy(accept=SYSTEM1_ACCEPT, escalate=SYSTEM1_ESCALATE)


def _pipeline(
    use_system1: bool = False, escalate: bool = False
) -> tuple[SignalChainPipeline, DeepSeekV4Client]:
    """两套系统互不依赖：

    - 默认：纯系统二
    - --system1：纯系统一（不构造系统二客户端，连 DeepSeek Key 都不需要）
    - --system1 --escalate：串联（系统一低置信时升级给系统二）
    """
    evaluator = _build_evaluator(use_system1)

    if evaluator is not None and not escalate:
        pipeline = SignalChainPipeline(
            cache_file=os.path.join(ROOT, "signal_cache.json"),
            evaluator=evaluator,
            gate_policy=_build_gate_policy(),
            escalate_to_system2=False,
        )
        return pipeline, MockAIClient()

    client = DeepSeekV4Client(
        model=SYSTEM2_MODEL, api_key=SYSTEM2_API_KEY,
        base_url=SYSTEM2_BASE_URL, thinking=False,
    )
    pipeline = SignalChainPipeline(
        ai_client=client,
        cache_file=os.path.join(ROOT, "signal_cache.json"),
        evaluator=evaluator,
        gate_policy=_build_gate_policy(),
        escalate_to_system2=escalate,
    )
    return pipeline, client


def _fmt(v):
    s = str(v)
    return s if len(s) <= 16 else s[:13] + "..."


def _calc_cost(prompt: int, completion: int) -> float:
    return (prompt * PRICE_INPUT + completion * PRICE_OUTPUT) / 1_000_000


def _pad(s: str, width: int) -> str:
    extra = sum(1 for c in s if ord(c) > 127)
    return s + " " * max(0, width - len(s) - extra)


def clean_file(
    filepath: str, use_system1: bool = False, escalate: bool = False
) -> dict | None:
    basename = os.path.splitext(os.path.basename(filepath))[0]
    dirty = pd.read_csv(filepath)
    dirty_cols = list(dirty.columns)

    pipeline, client = _pipeline(use_system1, escalate)
    t0 = time.time()
    try:
        clean, report = pipeline.run(dirty)
        elapsed = time.time() - t0
    except Exception as e:
        print(f"\n  {basename}.csv  [ERR] {e}")
        return None

    for col in clean.columns:
        clean[col] = clean[col].astype(object)
    clean.to_csv(
        os.path.join(CLEAN_DIR, f"{basename}_clean.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    usage = client.usage
    cost = _calc_cost(usage.prompt_tokens, usage.completion_tokens)

    old_set, new_set = set(dirty_cols), set(clean.columns)
    gained = sorted(new_set - old_set)
    lost = sorted(old_set - new_set)

    # --- header ---
    print(f"\n  {basename}.csv")
    print(f"  {DASH}")
    print(f"  {len(dirty)} rows x {len(dirty.columns)} cols  -->  {len(clean)} rows x {len(clean.columns)} cols")
    print(f"  time: {elapsed:.2f}s  |  calls: {usage.total_calls}  |  cost: {cost:.6f}")
    if usage.prompt_tokens or usage.completion_tokens:
        print(f"  tokens: in={usage.prompt_tokens}  out={usage.completion_tokens}  reason={usage.reasoning_tokens}")

    # 系统一的 token 与系统二分开报：两者的量级差一个数量级，混在一起看不清
    s1_usage = pipeline.evaluator.usage if pipeline.evaluator is not None else None
    if s1_usage is not None and (s1_usage.prompt_tokens or s1_usage.completion_tokens):
        print(
            f"  system1 tokens: calls={s1_usage.total_calls}  in={s1_usage.prompt_tokens}  "
            f"out={s1_usage.completion_tokens}  (TypeSafe 输出不计费)"
        )
    if gained:
        print(f"  + added:  {', '.join(gained)}")
    if lost:
        print(f"  - removed: {', '.join(lost)}")

    # --- field changes ---
    change_rows = []
    shown_prefixes = set()
    for r in report.records:
        if r.changed == 0 and r.errors == 0:
            continue
        if r.col_name in dirty.columns:
            orig_col = r.col_name
            samples = []
            for i in range(len(dirty)):
                dv = dirty[orig_col].iloc[i]
                cv = clean[r.col_name].iloc[i]
                if dv != cv and (pd.notna(dv) or pd.notna(cv)):
                    samples.append(f"{_fmt(dv)} -> {_fmt(cv)}")
            if samples:
                preview = "  ".join(samples[:2])
                if len(samples) > 2:
                    preview += f"  (+{len(samples) - 2} more)"
                change_rows.append([r.col_name, r.op_name, r.changed, r.errors, preview])
        elif r.col_name in gained and r.changed > 0:
            vals = [_fmt(clean[r.col_name].iloc[i]) for i in range(min(3, len(clean)))]
            preview = "  ".join(vals) + ("..." if len(clean) > 3 else "")
            change_rows.append([r.col_name, r.op_name, r.changed, r.errors, preview])
        elif r.col_name not in dirty.columns:
            prefix = r.col_name.split("_")[0]
            if prefix not in shown_prefixes:
                shown_prefixes.add(prefix)
                for c in sorted(gained):
                    if c.startswith(prefix):
                        vals = [_fmt(clean[c].iloc[i]) for i in range(min(3, len(clean)))]
                        preview = "  ".join(vals) + ("..." if len(clean) > 3 else "")
                        change_rows.append([c, r.op_name, r.changed, r.errors, preview])

    if change_rows:
        print(f"\n  {'field':<20s} {'operation':<22s} {'chg':>4s} {'err':>4s}  samples")
        print(f"  {'-' * 20} {'-' * 22} {'-' * 4} {'-' * 4}  {'-' * 26}")
        for row in change_rows:
            field, op, chg, err, samples = row
            print(f"  {_pad(field, 20)} {op:<22s} {chg:>4d} {err:>4d}  {samples}")

    # --- 系统一决策日志（只有启用系统一时才有内容）---
    if pipeline.decisions:
        print("\n  decisions (system 1):")
        for record in pipeline.decisions:
            print(f"    {record.summary()}")

    total_changed = sum(r.changed for r in report.records)
    total_errors = sum(r.errors for r in report.records)
    status = "OK" if total_errors == 0 else f"ERR({total_errors})"
    print(f"\n  [{status}] changed={total_changed}  ->  data/clean/{basename}_clean.csv")

    return {
        "name": f"{basename}.csv",
        "rows": len(dirty),
        "cols_old": len(dirty.columns),
        "cols_new": len(clean.columns),
        "elapsed": elapsed,
        "calls": usage.total_calls,
        "prompt": usage.prompt_tokens,
        "completion": usage.completion_tokens,
        "reasoning": usage.reasoning_tokens,
        "cost": cost,
        "changed": total_changed,
        "errors": total_errors,
        "s1_calls": s1_usage.total_calls if s1_usage is not None else 0,
        "s1_in": s1_usage.prompt_tokens if s1_usage is not None else 0,
        "s1_out": s1_usage.completion_tokens if s1_usage is not None else 0,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SignalChain Data Cleaning Tool")
    parser.add_argument("file", nargs="?", help="clean a specific file (without .csv)")
    parser.add_argument("--no-cache", action="store_true", help="clear cache before cleaning")
    parser.add_argument(
        "--system1", action="store_true",
        help="use System 1 (Jev) ONLY: scene + fields in ONE request, "
             "low-confidence items fall back to conservative defaults "
             "(needs uv sync --extra system1)",
    )
    parser.add_argument(
        "--escalate", action="store_true",
        help="let System 1 escalate low-confidence items to System 2 "
             "(opt-in chaining; requires --system1)",
    )
    args = parser.parse_args()

    # clear cache if requested
    cache_path = os.path.join(ROOT, "signal_cache.json")
    if args.no_cache and os.path.exists(cache_path):
        os.remove(cache_path)
        print(f"  cache cleared: {cache_path}")

    if args.file:
        files = [os.path.join(DIRTY_DIR, f"{args.file}.csv")]
        if not os.path.exists(files[0]):
            print(f"[ERR] file not found: {files[0]}")
            sys.exit(1)
    else:
        files = sorted(
            f
            for f in (os.path.join(DIRTY_DIR, f) for f in os.listdir(DIRTY_DIR))
            if f.endswith(".csv")
        )
    if not files:
        print("[ERR] no CSV files in data/dirty/")
        sys.exit(1)

    print(BAR)
    print("  SignalChain Cleaner")
    print(f"  system 2 (LLM): {'ON' if (not args.system1 or args.escalate) else 'OFF'}  |  model: {SYSTEM2_MODEL}")
    if not args.system1:
        mode = "system 2 only"
    elif args.escalate:
        mode = "system 1 + escalate to system 2"
    else:
        mode = "system 1 only (low confidence -> conservative defaults)"
    print(f"  mode: {mode}")
    print(BAR)

    t0 = time.time()
    results = []
    for fp in files:
        r = clean_file(fp, use_system1=args.system1, escalate=args.escalate)
        if r:
            results.append(r)
    elapsed = time.time() - t0

    passed = len(results)
    failed = len(files) - passed

    # --- summary ---
    if results:
        total_prompt = sum(r["prompt"] for r in results)
        total_completion = sum(r["completion"] for r in results)
        total_reasoning = sum(r["reasoning"] for r in results)
        total_cost = sum(r["cost"] for r in results)
        total_changed = sum(r["changed"] for r in results)
        total_errors = sum(r["errors"] for r in results)

        print(f"\n{BAR}")
        print("  Summary")
        print(BAR)

        # per-file table
        hdr = f"  {'file':<16s} {'size':<12s} {'time':>7s} {'calls':>5s} {'in':>5s} {'out':>5s} {'reason':>6s} {'cost':>10s} {'changed':>7s}"
        print(hdr)
        print(f"  {'-' * 16} {'-' * 12} {'-' * 7} {'-' * 5} {'-' * 5} {'-' * 5} {'-' * 6} {'-' * 10} {'-' * 7}")
        for r in results:
            size_str = f"{r['rows']}x{r['cols_old']}->{r['cols_new']}"
            print(
                f"  {r['name']:<16s} "
                f"{size_str:<12s} "
                f"{r['elapsed']:>6.2f}s "
                f"{r['calls']:>5d} "
                f"{r['prompt']:>5d} "
                f"{r['completion']:>5d} "
                f"{r['reasoning']:>6d} "
                f"{r['cost']:>10.6f} "
                f"{r['changed']:>7d}"
            )
        print(f"  {'-' * 16} {'-' * 12} {'-' * 7} {'-' * 5} {'-' * 5} {'-' * 5} {'-' * 6} {'-' * 10} {'-' * 7}")
        total_calls = sum(r["calls"] for r in results)
        print(
            f"  {'TOTAL':<16s} "
            f"{'':>12s} "
            f"{elapsed:>6.2f}s "
            f"{total_calls:>5d} "
            f"{total_prompt:>5d} "
            f"{total_completion:>5d} "
            f"{total_reasoning:>6d} "
            f"{total_cost:>10.6f} "
            f"{total_changed:>7d}"
        )

        total_s1_in = sum(r.get("s1_in", 0) for r in results)
        total_s1_out = sum(r.get("s1_out", 0) for r in results)
        total_s1_calls = sum(r.get("s1_calls", 0) for r in results)
        if total_s1_calls:
            print(
                f"\n  system1 total: calls={total_s1_calls}  in={total_s1_in}  out={total_s1_out}"
                f"  (system 2 calls={total_calls})"
            )

        if total_errors:
            print(f"\n  WARNING: {total_errors} errors")

    print(f"\n{BAR}")
    label = f"done {passed}/{len(files)}" + (f", failed {failed}" if failed else "")
    print(f"  {label} | time {elapsed:.2f}s")
    print(BAR)
