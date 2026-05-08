"""CurrencySplitter — 金额拆分为 货币符号 + 数值 两列"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation

CURRENCY_MAP = {
    "¥": "CNY", "￥": "CNY", "CNY": "CNY", "RMB": "CNY",
    "$": "USD", "USD": "USD", "US$": "USD",
    "€": "EUR", "EUR": "EUR",
    "£": "GBP", "GBP": "GBP",
}


class CurrencySplitter(Operation):
    """将金额列拆为 value 和 currency_code 两列"""

    @property
    def name(self) -> str:
        return "split_currency"

    @property
    def splits_column(self) -> bool:
        return True

    def execute(self, data: pd.Series) -> pd.DataFrame:
        result = pd.DataFrame(index=data.index)
        result["amount_value"] = None
        result["amount_currency"] = None

        for i, val in enumerate(data):
            if pd.isna(val):
                continue
            s = str(val).strip()

            # 提取货币符号（第一个非数字非逗号非点号的字符）
            currency_code = None
            for ch in s:
                if ch in CURRENCY_MAP:
                    currency_code = CURRENCY_MAP[ch]
                    break
                if ch.isalpha() and ch.isascii():
                    # 检测 USD/EUR/CNY 等字母代码
                    for code_len in range(2, 5):
                        if s[:code_len].upper() in CURRENCY_MAP:
                            currency_code = CURRENCY_MAP[s[:code_len].upper()]
                            break
                    break

            # 提取数值
            cleaned = re.sub(r"[¥￥\$€£,，\s]", "", s)
            # 去掉字母前缀（USD、EUR等）
            cleaned = re.sub(r"^[A-Za-z]{2,4}\s*", "", cleaned)
            # 去除所有多余的句点，只保留最后一个（处理中文千分位逗号转为点的情况）
            cleaned = re.sub(r"\.(?=.*\.)", "", cleaned)
            # 提取纯数字（含小数）
            match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
            value = float(match.group()) if match else None

            result.at[i, "amount_value"] = value
            result.at[i, "amount_currency"] = currency_code

        return result
