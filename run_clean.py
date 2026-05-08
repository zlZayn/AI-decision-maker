"""
SignalChain 数据清洗工具

读取 data/dirty/ 下的脏数据，清洗后写入 data/clean/。

用法：
    python run_clean.py              # 清洗所有文件
    python run_clean.py medical      # 只清洗指定文件
"""

import logging
import os, sys, time

logging.getLogger("signalchain").setLevel(logging.ERROR)

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for s in (sys.stdout, sys.stderr):
        if hasattr(s, "reconfigure"):
            s.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from signalchain.pipeline import SignalChainPipeline
from signalchain.ai_client import OpenAIClient

ROOT = os.path.dirname(os.path.abspath(__file__))
DIRTY_DIR = os.path.join(ROOT, "data", "dirty")
CLEAN_DIR = os.path.join(ROOT, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

API_KEY = "sk-abff4438c575405b9a42f813ec21b992"
API_URL = "https://api.deepseek.com/v1"
MODEL = "deepseek-chat"
BAR = "=" * 56


def _pipeline() -> SignalChainPipeline:
    return SignalChainPipeline(
        ai_client=OpenAIClient(model=MODEL, api_key=API_KEY, base_url=API_URL),
        cache_file=os.path.join(ROOT, "signal_cache.json"),
    )


def _fmt(v):
    s = str(v)
    # 日期时间去掉尾随 00:00:00
    s = s.replace(" 00:00:00", "")
    return s if len(s) <= 20 else s[:17] + "..."


def clean_file(filepath: str) -> bool:
    basename = os.path.splitext(os.path.basename(filepath))[0]
    dirty = pd.read_csv(filepath)
    dirty_cols = list(dirty.columns)

    pipeline = _pipeline()
    t0 = time.time()
    try:
        clean, report = pipeline.run(dirty)
        elapsed = time.time() - t0
    except Exception as e:
        print(f"\n  {basename}.csv  [{len(dirty)}×{len(dirty.columns)}]  [ERR] {e}")
        return False

    # 修复: 确保混合类型列（字符串 + NaN）保存时不会变成 float64
    # 将所有列转换为 object dtype，这样 NaN 会被保存为空字符串
    for col in clean.columns:
        clean[col] = clean[col].astype(object)

    clean.to_csv(os.path.join(CLEAN_DIR, f"{basename}_clean.csv"),
                 index=False, encoding="utf-8-sig")

    print(f"\n  {basename}.csv  [{len(dirty)}×{len(dirty.columns)}] → [{len(clean)}×{len(clean.columns)}]  {elapsed:.1f}s")

    # 列结构变化
    old_set, new_set = set(dirty_cols), set(clean.columns)
    gained = new_set - old_set
    lost = old_set - new_set
    if gained:
        print(f"    新增列: {', '.join(sorted(gained))}")
    if lost:
        print(f"    消失列: {', '.join(sorted(lost))}")

    # 逐字段变化
    shown_prefixes = set()
    for r in report.records:
        if r.changed == 0 and r.errors == 0:
            continue
        # 重命名查找：脏数据中的原列名
        orig_found = None
        for lo in lost:
            if lo not in dirty.columns:
                continue
            # 看该列清洗后是否出现在新列集里
            if r.col_name in gained and r.col_name.lower() == lo.lower():
                # 仅为辅助显示，不参与精确比对
                pass
        # 优先找 dirty 中同名的列
        if r.col_name in dirty.columns:
            orig_col = r.col_name
            changes = []
            for i in range(len(dirty)):
                dv = dirty[orig_col].iloc[i]
                cv = clean[r.col_name].iloc[i]
                if dv != cv and (pd.notna(dv) or pd.notna(cv)):
                    changes.append(f"{_fmt(dv)}→{_fmt(cv)}")
            if changes:
                suff = f" ...(+{len(changes)-2})" if len(changes) > 2 else ""
                print(f"    {r.col_name:16s} {r.op_name:22s}  {'  '.join(changes[:2])}{suff}")
        elif r.col_name in gained and r.changed > 0:
            # 重命名/新增列：直接展示清洗后值
            vals = [_fmt(clean[r.col_name].iloc[i]) for i in range(len(clean))]
            suff = "..." if len(vals) > 3 else ""
            print(f"    {r.col_name:24s} {'  '.join(vals[:3])}{suff}")
        elif r.col_name not in dirty.columns:
            # 分列新列
            prefix = r.col_name.split("_")[0]
            if prefix not in shown_prefixes:
                shown_prefixes.add(prefix)
                for c in sorted(gained):
                    if c.startswith(prefix):
                        vals = [_fmt(clean[c].iloc[i]) for i in range(len(clean))]
                        print(f"    {c:24s} {'  '.join(vals[:3])}{'...' if len(vals) > 3 else ''}")

    print(f"    累计变化: {sum(r.changed for r in report.records)} | 错误: {sum(r.errors for r in report.records)}")
    print(f"    保存: data/clean/{basename}_clean.csv")
    return True


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    if target:
        files = [os.path.join(DIRTY_DIR, f"{target}.csv")]
        if not os.path.exists(files[0]):
            print(f"[ERR] 文件不存在: {files[0]}")
            sys.exit(1)
    else:
        files = sorted(f for f in (os.path.join(DIRTY_DIR, f)
                                   for f in os.listdir(DIRTY_DIR)) if f.endswith(".csv"))
    if not files:
        print("[ERR] data/dirty/ 下没有 CSV 文件")
        sys.exit(1)

    print(f"{BAR}\n  SignalChain 清洗工具\n{BAR}")
    t0 = time.time()
    passed = failed = 0
    for fp in files:
        if clean_file(fp):
            passed += 1
        else:
            failed += 1
    elapsed = time.time() - t0
    print(f"\n{BAR}\n  DONE · {passed}/{len(files)} 通过{f' · {failed} 失败' if failed else ''} · {elapsed:.1f}s\n{BAR}")
