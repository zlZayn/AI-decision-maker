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
from signalchain.ai_client import DeepSeekV4Client
from config import API_KEY, API_URL, MODEL

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


def _pipeline() -> tuple[SignalChainPipeline, DeepSeekV4Client]:
    client = DeepSeekV4Client(model=MODEL, api_key=API_KEY, base_url=API_URL, thinking=False)
    pipeline = SignalChainPipeline(ai_client=client, cache_file=os.path.join(ROOT, "signal_cache.json"))
    return pipeline, client


def _fmt(v):
    s = str(v)
    return s if len(s) <= 16 else s[:13] + "..."


def _calc_cost(prompt: int, completion: int) -> float:
    return (prompt * PRICE_INPUT + completion * PRICE_OUTPUT) / 1_000_000


def _pad(s: str, width: int) -> str:
    extra = sum(1 for c in s if ord(c) > 127)
    return s + " " * max(0, width - len(s) - extra)


def clean_file(filepath: str) -> dict | None:
    basename = os.path.splitext(os.path.basename(filepath))[0]
    dirty = pd.read_csv(filepath)
    dirty_cols = list(dirty.columns)

    pipeline, client = _pipeline()
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
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SignalChain Data Cleaning Tool")
    parser.add_argument("file", nargs="?", help="clean a specific file (without .csv)")
    parser.add_argument("--no-cache", action="store_true", help="clear cache before cleaning")
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
    print(f"  model: {MODEL} | thinking: OFF")
    print(BAR)

    t0 = time.time()
    results = []
    for fp in files:
        r = clean_file(fp)
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

        if total_errors:
            print(f"\n  WARNING: {total_errors} errors")

    print(f"\n{BAR}")
    label = f"done {passed}/{len(files)}" + (f", failed {failed}" if failed else "")
    print(f"  {label} | time {elapsed:.2f}s")
    print(BAR)
