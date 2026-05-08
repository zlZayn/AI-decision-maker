"""CurrencyNormalizer — 金额字段标准化"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class CurrencyNormalizer(Operation):
    """
    金额字段标准化：
    - 去除货币符号（¥、$、€ 等）
    - 去除千分位逗号
    - 统一为浮点数
    """

    knowledge_key = "currency"

    @property
    def name(self) -> str:
        return "normalize_currency"

    def execute(self, data: pd.Series) -> pd.Series:
        def normalize(val):
            if pd.isna(val):
                return None
            s = str(val).strip()

            # 检测货币类型
            currency_type = "CNY"  # 默认人民币
            if re.search(r"[\$USD]", s):
                currency_type = "USD"
            elif re.search(r"[€EUR]", s):
                currency_type = "EUR"

            # 提取数值部分
            cleaned = re.sub(r"[¥￥\$€,，]", "", s)
            cleaned = cleaned.strip()

            # 尝试提取数字
            match = re.search(r"([\d]+\.?\d*)", cleaned)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    pass

            return None

        return data.apply(normalize)
