"""Jev 冒烟测试 —— 用真实数据验证系统一引擎（接入前的硬门槛）

为什么单独一个脚本
------------------
接入前必须先回答一个问题：Jev 判得对中文脏数据吗？官方明示"中文能力相对较弱"，
而本项目的字段名与样本值重度中文（心内科、三十、约25、Male））。
本脚本把这个问题从业务逻辑里隔离出来：直接拿 data/dirty/ 的真实字段名与样本值
构造问题，打印完整概率分布，不做任何条件化或门控，以便看清引擎的原始行为。

四项检查
--------
  1. 连通性 + noul 语义：确认 Key 有效；确认 noul 的 0~1 是不是"答案为是的概率"
  2. 真实数据集：一次请求同时问场景 + N 个字段（speculative fan-out）
  3. 有序性判定：用 score 给中文分类变量的取值打分，看分数能否区分有序与无序
  4. 分类变量筛选：用 noul 判断字段是不是分类变量

用法
----
  uv run python run_smoke_jev.py                # 真实调用（读 config.py 的 SYSTEM1_API_KEY）
  uv run python run_smoke_jev.py --offline      # 离线：MockEvaluator，不联网不花钱
  uv run python run_smoke_jev.py --all          # 三个数据集都测
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import pandas as pd

from signalchain.fastpath import (
    SCENE_QUESTION_ID,
    build_questions,
    build_state,
    field_question_id,
    scene_criteria,
)
from signalchain.stage0_profile import extract_profile
from signalchain.categorical_system1 import (
    build_ordinal_questions,
    level_question_id,
    ordinal_question_id,
)
from signalchain.system1 import (
    Evaluator,
    JevEvaluator,
    MockEvaluator,
    certainty,
    choice,
    choice_answer_dict,
    noul,
    noul_answer_dict,
    noul_certainty,
    score,
    score_answer_dict,
)

if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.abspath(__file__))
DIRTY_DIR = os.path.join(ROOT, "data", "dirty")

BAR = "=" * 78
DASH = "-" * 78

# ---- 第 3 项检查用的合成中文分类变量 ----
# 前三个应当判为有序，后两个应当判为无序
ORDINAL_CASES: dict[str, list[str]] = {
    "education": ["小学", "初中", "高中", "本科", "硕士"],
    "satisfaction": ["很不满意", "不满意", "一般", "满意", "很满意"],
    "severity": ["轻微", "轻度", "中度", "重度", "危重"],
    "gender": ["男", "女"],
    "blood_type": ["A", "B", "AB", "O"],
}

# 通用有序刻度：不绑定具体变量，所以同一个刻度可以复用到任意变量上（composite scoring）
LEVELS = ["最低档", "偏低", "中等", "偏高", "最高档"]


def _pad(text: str, width: int) -> str:
    """按显示宽度对齐（中文字符算 2 列）"""
    extra = sum(1 for ch in text if ord(ch) > 127)
    return text + " " * max(0, width - len(text) - extra)


def _fmt_probs(probabilities: dict[str, float], top: int = 3) -> str:
    if not probabilities:
        return "-"
    ranked = sorted(probabilities.items(), key=lambda kv: -kv[1])[:top]
    return "  ".join(f"{key}={value:.3f}" for key, value in ranked)


def _load_config() -> tuple[str, str, str]:
    """从 config.py 读系统一配置（读不到就空）"""
    try:
        from config import SYSTEM1_API_KEY, SYSTEM1_BASE_URL, SYSTEM1_MODEL  # type: ignore
        return SYSTEM1_API_KEY, SYSTEM1_BASE_URL, SYSTEM1_MODEL
    except Exception:
        return "", "https://api.typesafe.ai", "jev-latest"


# 离线演示用：哪些变量"有公认顺序"。真值来自实测（docs/SYSTEM1_JEV.md）
_OFFLINE_ORDINAL = {"education", "satisfaction", "severity", "rank"}
_OFFLINE_CATEGORICAL = {
    "gender", "sex", "SEX", "SEX/Gender", "DEPT", "department", "education",
    "satisfaction", "severity", "rank", "blood_type",
}


def _peaked(value: float, peak: float = 0.95) -> dict:
    """尖峰分布：peak=0.95 → confidence=(5×0.95−1)/4=0.9375，过 0.80 的档位门"""
    peak_key = str(int(round(value)))
    rest = (1.0 - peak) / (len(LEVELS) - 1)
    probabilities = {
        str(index): (peak if str(index) == peak_key else rest)
        for index in range(len(LEVELS))
    }
    return score_answer_dict(value, LEVELS, probabilities)


def _offline_evaluator() -> MockEvaluator:
    """离线假引擎：按问题内容给出"看起来合理"的答案，用于验证链路与打印格式

    刻意让答案分布与实测一致（尖峰、有序性 noul 分明），
    否则 --offline 会打印出一片 MISS，把"演示"误导成"失败"。
    """

    def handler(state: Any, questions: dict[str, Any]) -> dict[str, Any]:
        answers: dict[str, Any] = {}
        for question_id, question in questions.items():
            if question["type"] == "noul":
                instructions = question.get("instructions")
                payload = instructions if isinstance(instructions, dict) else {}
                if question_id.startswith("ordinal:"):
                    variable = payload.get("variable", "")
                    answers[question_id] = noul_answer_dict(
                        0.98 if variable in _OFFLINE_ORDINAL else 0.04
                    )
                elif question_id.startswith("cat:"):
                    field_name = payload.get("field", {}).get("name", "")
                    answers[question_id] = noul_answer_dict(
                        0.95 if field_name in _OFFLINE_CATEGORICAL else 0.10
                    )
                else:
                    # 连通性检查：含 ASAP 的文本判为紧急
                    text = json.dumps(state, ensure_ascii=False)
                    answers[question_id] = noul_answer_dict(0.93 if "ASAP" in text else 0.12)
            elif question["type"] == "choice":
                criteria = list(question["criteria"].keys())
                if question_id == SCENE_QUESTION_ID:
                    answers[question_id] = choice_answer_dict(
                        "S1" if "S1" in criteria else criteria[0],
                        {code: (0.88 if code == "S1" else 0.12 / max(1, len(criteria) - 1)) for code in criteria},
                    )
                else:
                    name = json.dumps(question["instructions"], ensure_ascii=False)
                    picked = "X"
                    for probe, code in (
                        ("SEX", "G"), ("Gender", "G"), ("AGE", "A"), ("DEPT", "D"),
                        ("MED", "N"), ("PID", "I"), ("UID", "I"), ("TXN", "I"),
                        ("AMOUNT", "M"), ("DATE", "T"), ("E-MAIL", "E"), ("PHONE", "P"),
                    ):
                        if probe in name:
                            picked = code
                            break
                    answers[question_id] = choice_answer_dict(
                        picked,
                        {code: (0.91 if code == picked else 0.09 / max(1, len(criteria) - 1)) for code in criteria},
                    )
            elif question["type"] == "score":
                instructions = question["instructions"]
                value = instructions.get("value", "") if isinstance(instructions, dict) else ""
                if value in ("小学", "很不满意", "轻微"):
                    position = 0.0
                elif value in ("初中", "不满意", "轻度"):
                    position = 1.0
                elif value in ("高中", "一般", "中度", "男", "A", "B", "AB", "O"):
                    position = 2.0
                elif value in ("本科", "满意", "重度", "女"):
                    position = 3.0
                else:
                    position = 4.0
                answers[question_id] = _peaked(position)
        return answers

    return MockEvaluator(handler=handler, engine_id="mock:jev", model="jev-mock")


def check_1_connectivity(evaluator: Evaluator) -> bool:
    print(f"\n{BAR}")
    print("  检查 1 / 连通性 + noul 语义")
    print(BAR)

    state = {"message": "Our API integration is returning 500 errors and we cannot process orders. Please fix this ASAP."}
    questions = {
        "urgent": noul(
            "这段话是否表达了时间紧迫性？",
            true_description="明确表示需要尽快处理",
            false_description="没有表达任何紧迫性",
        ),
        "billing": noul(
            "这段话是否关于账单或付款问题？",
            true_description="涉及付款、发票、退款、重复扣费",
            false_description="与账单付款无关",
        ),
    }
    started = time.time()
    response = evaluator.evaluate(state, questions)
    elapsed = time.time() - started

    for question_id in questions:
        answer = response.answers.get(question_id)
        if answer is None:
            print(f"  {question_id:<10s} 无答案")
            continue
        probability = answer.noul or 0.0
        print(
            f"  {question_id:<10s} noul={probability:.4f}  "
            f"certainty={noul_certainty(probability):.4f}"
        )
    print(f"  model={response.model}  tokens: in={response.input_tokens} out={response.output_tokens}  {elapsed:.2f}s")
    print("  语义校验：urgent 应显著高于 billing（urgent 句明确要求 ASAP，billing 句谈的是故障）")
    urgent = response.answers.get("urgent")
    billing = response.answers.get("billing")
    ok = bool(urgent and billing and (urgent.noul or 0) > (billing.noul or 0))
    print(f"  => {'通过' if ok else '未通过（需人工确认 noul 语义）'}")
    return ok


def check_2_dataset(evaluator: Evaluator, filename: str) -> None:
    path = os.path.join(DIRTY_DIR, filename)
    df = pd.read_csv(path)
    profile = extract_profile(df)

    print(f"\n{BAR}")
    print(f"  检查 2 / 真实数据集 {filename}  ({len(df)} 行 x {profile.field_count} 列)")
    print(BAR)

    state = build_state(profile)
    questions = build_questions(profile, include_scene=True)

    started = time.time()
    response = evaluator.evaluate(state, questions)
    elapsed = time.time() - started

    print(f"  请求：1 次  |  问题数：{len(questions)}（1 场景 + {profile.field_count} 字段）"
          f"  |  {elapsed:.2f}s  |  tokens in={response.input_tokens} out={response.output_tokens}")

    scene_answer = response.answers.get(SCENE_QUESTION_ID)
    if scene_answer is not None:
        print(f"\n  场景：{scene_answer.choice}  ({_fmt_probs(dict(scene_answer.probabilities))})")
    else:
        print("\n  场景：无答案")

    print(f"\n  {'字段':<18s} {'码':<4s} {'概率':<34s} 样本")
    print(f"  {'-' * 18} {'-' * 4} {'-' * 34} {'-' * 24}")
    for field_profile in profile.fields:
        answer = response.answers.get(field_question_id(field_profile.name))
        if answer is None:
            print(f"  {_pad(field_profile.name, 18)} {'-':<4s} {'无答案':<34s} {', '.join(field_profile.samples[:3])}")
            continue
        samples = ", ".join(field_profile.samples[:3])
        print(
            f"  {_pad(field_profile.name, 18)} {answer.choice or '-':<4s} "
            f"{_fmt_probs(dict(answer.probabilities)):<34s} {samples}"
        )


def check_3_ordinality(evaluator: Evaluator) -> None:
    print(f"\n{BAR}")
    print("  检查 3 / 有序性判定（noul 判有没有顺序 + score 求顺序，一次请求问完）")
    print(BAR)

    state = {
        "variables": {name: {"values": values} for name, values in ORDINAL_CASES.items()}
    }
    questions = build_ordinal_questions(list(ORDINAL_CASES.keys()), ORDINAL_CASES)

    started = time.time()
    response = evaluator.evaluate(state, questions)
    elapsed = time.time() - started
    print(f"  请求：1 次  |  问题数：{len(questions)}"
          f"（{len(ORDINAL_CASES)} 个有序性 noul + 逐值 score）  |  {elapsed:.2f}s  |  "
          f"tokens in={response.input_tokens} out={response.output_tokens}\n")

    print(f"  {'变量':<14s} {'P(有序)':>8s} {'cert':>6s} {'离散度':>7s} {'档位确定度':>10s}  判定  顺序")
    print(f"  {'-' * 14} {'-' * 8} {'-' * 6} {'-' * 7} {'-' * 10}  {'-' * 6} {'-' * 40}")
    hits = 0
    for name, values in ORDINAL_CASES.items():
        ordinal_answer = response.answers.get(ordinal_question_id(name))
        probability = (
            ordinal_answer.noul
            if ordinal_answer is not None and ordinal_answer.noul is not None
            else 0.0
        )
        cert = noul_certainty(probability)

        scored: list[tuple[str, float]] = []
        level_certainties: list[float] = []
        for value in values:
            answer = response.answers.get(level_question_id(name, value))
            if answer is None or answer.score is None:
                continue
            scored.append((value, float(answer.score)))
            level_certainties.append(certainty(answer))

        spread = (max(s for _, s in scored) - min(s for _, s in scored)) if scored else 0.0
        mean_level_certainty = (
            sum(level_certainties) / len(level_certainties) if level_certainties else 0.0
        )
        # 与 categorical_system1.System1CategoricalClassifier 的判定条件保持一致
        is_ordinal = (
            probability >= 0.5
            and cert >= 0.55
            and len(scored) >= 2
            and spread >= 1.0
            and mean_level_certainty >= 0.80
        )
        expected_ordinal = name in ("education", "satisfaction", "severity")
        hit = is_ordinal == expected_ordinal
        hits += 1 if hit else 0
        chain = " < ".join(
            f"{value}({score_value:+.2f})"
            for value, score_value in sorted(scored, key=lambda kv: kv[1])
        )
        verdict = "有序" if is_ordinal else "无序"
        print(
            f"  {_pad(name, 14)} {probability:>8.3f} {cert:>6.3f} {spread:>7.2f} "
            f"{mean_level_certainty:>10.3f}  {'OK  ' if hit else 'MISS'} {verdict:<4s} {chain}"
        )
    print(f"\n  判定命中：{hits}/{len(ORDINAL_CASES)}"
          f"  （期望：education / satisfaction / severity 有序，gender / blood_type 无序）")


def check_4_categorical(evaluator: Evaluator, filename: str) -> None:
    path = os.path.join(DIRTY_DIR, filename)
    df = pd.read_csv(path)
    profile = extract_profile(df)

    print(f"\n{BAR}")
    print(f"  检查 4 / 分类变量筛选（noul）  {filename}")
    print(BAR)

    state = build_state(profile)
    questions: dict[str, Any] = {}
    for field_profile in profile.fields:
        questions[f"cat:{field_profile.name}"] = noul(
            {
                "field": {
                    "name": field_profile.name,
                    "type": field_profile.type,
                    "samples": list(field_profile.samples[:8]),
                    "distinct_count": len(field_profile.samples),
                },
                "question": (
                    "该字段是不是分类变量？分类变量 = 取值来自一个有限的离散类别集合，"
                    "类别之间没有大小顺序（可以有顺序，但这里只问是不是类别型）。"
                ),
            },
            true_description="取值是有限个离散类别，如 男/女、A/B/O、满意度的几个档位",
            false_description="取值是连续数值、唯一标识符、自由文本（地址、备注）或时间",
        )

    started = time.time()
    response = evaluator.evaluate(state, questions)
    elapsed = time.time() - started
    print(f"  请求：1 次  |  问题数：{len(questions)}  |  {elapsed:.2f}s  |  "
          f"tokens in={response.input_tokens} out={response.output_tokens}\n")

    print(f"  {'字段':<18s} {'P(是分类变量)':>14s} {'certainty':>10s}  判定")
    print(f"  {'-' * 18} {'-' * 14} {'-' * 10}  {'-' * 10}")
    for field_profile in profile.fields:
        answer = response.answers.get(f"cat:{field_profile.name}")
        if answer is None:
            print(f"  {_pad(field_profile.name, 18)} {'-':>14s} {'-':>10s}  无答案")
            continue
        probability = answer.noul or 0.0
        cert = noul_certainty(probability)
        verdict = "分类变量" if probability >= 0.5 and cert >= 0.55 else "非分类"
        print(
            f"  {_pad(field_profile.name, 18)} {probability:>14.4f} {cert:>10.4f}  {verdict}"
        )


def main() -> None:
    argv = sys.argv[1:]
    offline = "--offline" in argv
    files = ["medical.csv"]
    if "--all" in argv:
        files = sorted(f for f in os.listdir(DIRTY_DIR) if f.endswith(".csv"))

    api_key, base_url, model = _load_config()

    print(BAR)
    print("  Jev 冒烟测试 — 系统一引擎验证")
    print(BAR)

    evaluator: Evaluator
    if offline:
        evaluator = _offline_evaluator()
        print("  模式：离线（MockEvaluator，不联网、不消耗额度）")
    else:
        evaluator = JevEvaluator(api_key=api_key or None, base_url=base_url or None, model=model or None)
        print(f"  模式：在线  |  model={model}  |  base_url={base_url}")
        print(f"  API Key：{'已配置 len=' + str(len(api_key)) if api_key else '未配置'}")
        if not api_key:
            print("\n  [ERR] config.py 里的 SYSTEM1_API_KEY 为空。")
            print("        先跑离线模式看链路：uv run python run_smoke_jev.py --offline")
            sys.exit(1)
        if not evaluator.available():
            print("\n  [ERR] 系统一不可用（未安装 typesafe-sdk？请执行 uv sync --extra system1）")
            sys.exit(1)

    started = time.time()
    try:
        check_1_connectivity(evaluator)
        for filename in files:
            check_2_dataset(evaluator, filename)
            check_4_categorical(evaluator, filename)
        check_3_ordinality(evaluator)
    except Exception as exc:  # noqa: BLE001 — 冒烟脚本需要打印原始错误便于定位
        print(f"\n  [ERR] {type(exc).__name__}: {exc}")
        raise

    total = time.time() - started
    print(f"\n{BAR}")
    print("  汇总")
    print(BAR)
    print(f"  engine_id   : {evaluator.engine_id}")
    print(f"  总调用次数  : {evaluator.usage.total_calls}")
    print(f"  输入 tokens : {evaluator.usage.prompt_tokens}")
    print(f"  输出 tokens : {evaluator.usage.completion_tokens}  (TypeSafe 输出免费)")
    print(f"  总耗时      : {total:.2f}s")
    print(BAR)


if __name__ == "__main__":
    main()
