"""
SignalChain 本地单元测试 (MockAI · 零Token)

运行：python tests/test_local.py
"""

import os
import sys
import time

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

TAG = "LOCAL"
BAR = "=" * 56


def header():
    print(f"\n{BAR}")
    print(f"  SignalChain · 本地单元测试 · MockAI · 零Token")
    print(f"{BAR}")


def footer(ok: bool, elapsed: float, extra: str = ""):
    label = "PASS" if ok else "FAIL"
    print(f"\n{BAR}")
    print(f"  {label} · {TAG} {extra}耗时: {elapsed:.2f}s")
    print(f"{BAR}")


if __name__ == "__main__":
    import pytest

    header()
    t0 = time.time()
    code = pytest.main(["tests/", "-v", "--tb=short"])
    elapsed = time.time() - t0
    footer(code == 0, elapsed)
    sys.exit(code)
