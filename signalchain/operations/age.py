"""AgeExtractor — 从各种年龄表达中提取数值"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation

# 英文数字单词映射
EN_NUMBERS = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
    "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
    "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60, "seventy": 70,
    "eighty": 80, "ninety": 90, "hundred": 100,
}

# 中文数字映射
CN_NUMBERS = {
    "零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
    "十一": 11, "十二": 12, "十三": 13, "十四": 14, "十五": 15,
    "十六": 16, "十七": 17, "十八": 18, "十九": 19, "二十": 20,
    "三十": 30, "四十": 40, "五十": 50, "六十": 60, "七十": 70,
    "八十": 80, "九十": 90,
}


def _parse_number_word(s: str) -> int | None:
    """解析数字单词，如 eighteen → 18, 十二 → 12, 三十三 → 33"""
    raw = s.strip().lower()
    # 去除常见前后缀
    for prefix in ("约", "大概", "around", "about", "approximately"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):].strip()
    for suffix in ("岁", "年", "岁龄", "years", "years old", "year old", "y/o"):
        if raw.endswith(suffix):
            raw = raw[:len(raw) - len(suffix)].strip()
    if not raw:
        return None

    s = raw
    # 英文数字单词
    if s in EN_NUMBERS:
        return EN_NUMBERS[s]
    # 中文数字
    if s in CN_NUMBERS:
        return CN_NUMBERS[s]
    # 组合数字：二十一 → 二十 + 一
    if len(s) == 3 and s[0] in CN_NUMBERS and s[1] == "十" and s[2] in CN_NUMBERS:
        return CN_NUMBERS[s[0]] * 10 + CN_NUMBERS[s[2]]
    # 组合数字：三十三 → 三十 (已在上层命中) + 三
    if len(s) == 2 and s[0] in CN_NUMBERS and s[1] in CN_NUMBERS:
        first, second = CN_NUMBERS[s[0]], CN_NUMBERS[s[1]]
        if first >= 10 and second < 10:
            return first + second
    return None


class AgeExtractor(Operation):
    """
    从各种年龄表达中提取数值。

    示例：
    - "30" → 30
    - "30岁" → 30
    - "约30" → 30
    - "30Y" → 30
    - "3月" → 0 (婴儿月龄，保留为0)
    - "eighteen" → 18
    - "十二" → 12
    - "二十一" → 21
    """

    @property
    def name(self) -> str:
        return "extract_age"

    def execute(self, data: pd.Series) -> pd.Series:
        def extract(val):
            if pd.isna(val):
                return None
            s = str(val).strip()

            # 优先匹配：数字在前（如 "30岁"、"约30"、"30Y"）
            match = re.search(r"(\d+)\s*(?:岁|年|Y|y|岁龄)?", s)
            if match:
                age = int(match.group(1))
                if 0 <= age <= 150:
                    return age

            # 尝试直接解析为整数
            try:
                age = int(float(s))
                if 0 <= age <= 150:
                    return age
            except (ValueError, TypeError):
                pass

            # 英文数字单词 / 中文数字
            parsed = _parse_number_word(s)
            if parsed is not None and 0 <= parsed <= 150:
                return parsed

            return None

        return data.apply(extract)
