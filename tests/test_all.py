"""
SignalChain 全量测试 = test_local + test_real_e2e

运行：python tests/test_all.py
"""

import os
import sys
import subprocess
import time

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = [
    ("1/2 本地单元测试 (MockAI)", os.path.join(ROOT, "tests", "test_local.py")),
    ("2/2 API 端到端测试 (DeepSeek)", os.path.join(ROOT, "tests", "test_real_e2e.py")),
]

TAG = "ALL"
BAR = "=" * 56


def header():
    print(f"\n{BAR}")
    print(f"  SignalChain · 全量测试 (2 阶段)")
    print(f"{BAR}")


def footer(elapsed: float, failed_label: str | None):
    label = "FAIL" if failed_label else "PASS"
    extra = f" · 失败: {failed_label}" if failed_label else ""
    print(f"\n{BAR}")
    print(f"  {label} · {TAG}{extra} · 总耗时: {elapsed:.2f}s")
    print(f"{BAR}")


if __name__ == "__main__":
    header()
    t0 = time.time()
    failed = None
    for label, script in SCRIPTS:
        print(f"\n--- {label} ---")
        result = subprocess.run([sys.executable, script], cwd=ROOT)
        if result.returncode != 0:
            failed = label
            break
    elapsed = time.time() - t0
    footer(elapsed, failed)
    sys.exit(1 if failed else 0)
