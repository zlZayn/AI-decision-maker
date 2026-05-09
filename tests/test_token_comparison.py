"""
SignalChain V4 思考模式 Token 消耗对比

对比 deepseek-v4-flash 在思考/不思考模式下的 token 用量和费用。
输入 token 用本地 tokenizer 离线计算，输出 token 按实际 API 返回。

运行：python tests/test_token_comparison.py
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
from signalchain.ai_client import DeepSeekV4Client
from signalchain.tokenizer import count_tokens
from config import API_KEY, API_URL, MODEL


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIRTY_DIR = os.path.join(ROOT, "data", "dirty")

# ===== 单价（元/百万tokens） =====
PRICE_INPUT = 1.0
PRICE_OUTPUT = 2.0

BAR = "=" * 64


def load_dirty_data() -> dict[str, pd.DataFrame]:
    cases = {}
    if not os.path.isdir(DIRTY_DIR):
        print("[WARN] data/dirty/ 目录不存在")
        return cases
    for f in sorted(os.listdir(DIRTY_DIR)):
        if f.endswith(".csv"):
            name = os.path.splitext(f)[0]
            cases[name] = pd.read_csv(os.path.join(DIRTY_DIR, f))
    return cases


def run_case(df: pd.DataFrame, thinking: bool) -> dict:
    """跑单个用例，返回 token 用量和耗时"""
    client = DeepSeekV4Client(
        api_key=API_KEY,
        base_url=API_URL,
        model=MODEL,
        thinking=thinking,
    )
    pipeline = SignalChainPipeline(ai_client=client, cache_file=":memory:")

    # 先用 tokenizer 离线算输入 token
    pipeline_for_prompts = SignalChainPipeline(
        ai_client=DeepSeekV4Client(
            api_key=API_KEY,
            base_url=API_URL,
            model=MODEL,
            thinking=thinking,
        ),
        cache_file=":memory:",
    )
    pipeline_for_prompts.run(df)
    input_tokens_offline = sum(count_tokens(p) for p in pipeline_for_prompts.prompt_log)

    t0 = time.time()
    result, report = pipeline.run(df)
    elapsed = time.time() - t0

    u = client.usage
    return {
        "input_offline": input_tokens_offline,
        "input_api": u.prompt_tokens,
        "output": u.completion_tokens,
        "reasoning": u.reasoning_tokens,
        "calls": u.total_calls,
        "elapsed": elapsed,
        "result": result,
    }


def calc_cost(input_t: int, output_t: int) -> float:
    return (input_t * PRICE_INPUT + output_t * PRICE_OUTPUT) / 1_000_000


def main():
    test_cases = load_dirty_data()
    if not test_cases:
        print("[ERR] data/dirty/ 下没有 CSV 文件")
        return

    print(f"\n{BAR}")
    print("  V4 思考模式 ON vs OFF Token 对比")
    print(f"  模型: {MODEL}")
    print(f"  数据: data/dirty/ ({len(test_cases)} 个文件)")
    print(f"{BAR}\n")

    total = {"thinking": None, "no_thinking": None}
    case_results = {"thinking": {}, "no_thinking": {}}

    for thinking in [True, False]:
        mode = "thinking" if thinking else "no_thinking"
        label = "思考模式 ON " if thinking else "思考模式 OFF"
        total_input_offline = 0
        total_input_api = 0
        total_output = 0
        total_reasoning = 0
        total_elapsed = 0

        print(f"\n{BAR}")
        print(f"  {label}")
        print(f"{BAR}")

        for name, df in test_cases.items():
            r = run_case(df, thinking)
            total_input_offline += r["input_offline"]
            total_input_api += r["input_api"]
            total_output += r["output"]
            total_reasoning += r["reasoning"]
            total_elapsed += r["elapsed"]

            case_results[mode][name] = r["result"]

            print(
                f"  {name:<16s} input={r['input_api']:>4d} output={r['output']:>4d} reasoning={r['reasoning']:>4d} {r['elapsed']:.1f}s"
            )

        total_cost = calc_cost(total_input_api, total_output)
        print(f"  {'-' * 52}")
        print(
            f"  {'合计':<16s} input={total_input_api:>4d} output={total_output:>4d} reasoning={total_reasoning:>4d} {total_elapsed:.2f}s"
        )
        print(f"  离线 input 估算: {total_input_offline} tokens")
        print(f"  费用: {total_cost:.6f} 元")

        total[mode] = {
            "input": total_input_api,
            "output": total_output,
            "reasoning": total_reasoning,
            "cost": total_cost,
            "elapsed": total_elapsed,
        }

    # 对比
    t_on = total["thinking"]
    t_off = total["no_thinking"]
    print(f"\n{BAR}")
    print("  对比")
    print(f"{BAR}")
    diff_input = t_off["input"] - t_on["input"]
    diff_output = t_off["output"] - t_on["output"]
    diff_reasoning = t_off["reasoning"] - t_on["reasoning"]
    diff_cost = t_off["cost"] - t_on["cost"]
    diff_elapsed = t_off["elapsed"] - t_on["elapsed"]

    def _d(v: float, fmt: str) -> str:
        return "0" if v == 0 else f"{v:{fmt}}"

    df_cmp = pd.DataFrame(
        {
            "思考 ON": [
                t_on["input"],
                t_on["output"],
                t_on["reasoning"],
                f"{t_on['cost']:.6f}",
                f"{t_on['elapsed']:.2f}s",
            ],
            "思考 OFF": [
                t_off["input"],
                t_off["output"],
                t_off["reasoning"],
                f"{t_off['cost']:.6f}",
                f"{t_off['elapsed']:.2f}s",
            ],
            "差值": [
                _d(diff_input, "+d"),
                _d(diff_output, "+d"),
                _d(diff_reasoning, "+d"),
                _d(diff_cost, "+.6f"),
                _d(diff_elapsed, "+.2f"),
            ],
        },
        index=["input tokens", "output tokens", "reasoning", "费用", "耗时"],
    )
    df_cmp.index.name = "指标"
    print(df_cmp.to_string())

    if t_on["cost"] > 0:
        saving = (1 - t_off["cost"] / t_on["cost"]) * 100
        print(f"\n  关闭思考省 {saving:.1f}% 费用")

    # 输出内容对比
    print(f"\n{BAR}")
    print("  输出内容对比 (思考 ON vs OFF)")
    print(f"{BAR}")
    match_count = 0
    total_cases = len(case_results["thinking"])
    rows = []
    for name in case_results["thinking"]:
        df_on = case_results["thinking"][name]
        df_off = case_results["no_thinking"][name]
        ok = df_on.equals(df_off)
        if ok:
            match_count += 1
        diff_cols = []
        if not ok:
            for col in df_on.columns:
                if col in df_off.columns:
                    if (
                        df_on[col].astype(str).tolist()
                        != df_off[col].astype(str).tolist()
                    ):
                        diff_cols.append(col)
        rows.append(
            {
                "用例": name,
                "结果": "一致" if ok else "不一致",
                "差异列": ", ".join(diff_cols) if diff_cols else "",
            }
        )
    df_match = pd.DataFrame(rows).set_index("用例")
    print(df_match.to_string())
    print(
        f"\n  汇总: {total_cases} 个用例, {match_count} 一致, {total_cases - match_count} 不一致"
    )
    print(f"\n{BAR}\n")


if __name__ == "__main__":
    main()
