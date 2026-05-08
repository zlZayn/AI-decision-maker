"""Stage 1: AI 场景识别节点

输入：从 DataProfile 派生的摘要（field_count, field_names, type_summary, null_ratio_summary）
输出：SceneCode（1 个字符串，如 "S1"）
校验：输出必须 ∈ VALID_SCENE_CODES，否则回退到 "S0"
约束：Prompt 强制只输出 1 个代码，temperature=0
Token：~30 输入 + 1 输出
"""

from __future__ import annotations

import logging

from signalchain.models import DataProfile, VALID_SCENE_CODES

logger = logging.getLogger(__name__)


def build_scene_prompt(profile: DataProfile) -> str:
    """从 DataProfile 构建场景识别 Prompt"""
    return (
        f"数据概览：\n"
        f"- 字段数：{profile.field_count}\n"
        f"- 字段名：{profile.field_names}\n"
        f"- 类型分布：{profile.type_summary}\n"
        f"- 缺失率：{profile.null_ratio_summary}\n"
        f"\n"
        f"从以下选项中选择最匹配的场景代码：\n"
        f"S1=医疗数据 S2=财务数据 S3=用户数据 S4=日志数据 S5=地理数据 S0=未知\n"
        f"\n"
        f"只输出1个代码，不要输出任何其他文字。\n"
        f"\n"
        f"输出："
    )


def validate_scene_code(raw_output: str) -> str:
    """校验 AI 输出的场景信号码"""
    cleaned = raw_output.strip()[:2]
    if cleaned in VALID_SCENE_CODES:
        return cleaned
    logger.warning(f"Invalid scene code: {raw_output!r}, fallback to S0")
    return "S0"
