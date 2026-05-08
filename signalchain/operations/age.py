"""AgeExtractor — 从各种年龄表达中提取数值"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation


class AgeExtractor(Operation):
    """
    从各种年龄表达中提取数值。

    示例：
    - "30" → 30
    - "30岁" → 30
    - "约30" → 30
    - "30Y" → 30
    - "3月" → 0 (婴儿月龄，保留为0)
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
                # 合理范围校验
                if 0 <= age <= 150:
                    return age

            # 尝试直接解析为整数
            try:
                age = int(float(s))
                if 0 <= age <= 150:
                    return age
            except (ValueError, TypeError):
                pass

            return None

        return data.apply(extract)
