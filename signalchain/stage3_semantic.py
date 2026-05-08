"""Stage 3: AI 字段语义识别节点

输入：Stage 2 组装的 Prompt 字符串
输出：FieldSignalSequence（字符串，长度 = field_count，每字符 ∈ VALID_FIELD_CODES）
校验：
  1. 长度必须 == field_count
  2. 每字符必须 ∈ 当前场景的 valid_codes（不是全局白名单）
校验失败 → 回退为 "X" * field_count
约束：Prompt 强制只输出字符序列，无空格，temperature=0
Token：~60 输入 + field_count 输出
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def validate_field_signal_sequence(
    raw_output: str,
    field_count: int,
    valid_codes: set[str],
) -> str:
    """
    校验 AI 输出的字段信号序列。

    校验规则：
    1. 去除空白后长度必须 == field_count
    2. 每个字符必须 ∈ 当前场景的 valid_codes（不是全局白名单）

    关键设计：校验使用 scene_config.valid_codes（场景白名单），
    而非全局 VALID_FIELD_CODES。原因：如果 AI 在财务场景（S2）
    下输出了 G（性别），虽然 G 是合法的字段信号码，
    但在财务场景下没有意义，应该被拒绝。
    """
    cleaned = raw_output.strip().replace(" ", "")

    # 规则 1：长度校验
    if len(cleaned) != field_count:
        logger.warning(
            f"Signal sequence length mismatch: "
            f"expected {field_count}, got {len(cleaned)} ({raw_output!r}), "
            f"fallback to 'X' * {field_count}"
        )
        return "X" * field_count

    # 规则 2：白名单校验（使用场景级别的 valid_codes）
    for ch in cleaned:
        if ch not in valid_codes:
            logger.warning(
                f"Invalid signal code '{ch}' in sequence {cleaned!r}, "
                f"not in valid_codes {valid_codes}, "
                f"fallback to 'X' * {field_count}"
            )
            return "X" * field_count

    return cleaned
