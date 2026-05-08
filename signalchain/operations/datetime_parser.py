"""DateTimeParser — 时间/日期字段解析与标准化"""

from __future__ import annotations

import pandas as pd

from signalchain.operations.base import Operation


class DateTimeParser(Operation):
    """
    时间/日期字段解析与标准化。

    尝试多种常见日期格式解析，统一输出为 ISO 8601 格式字符串。
    无法解析的保留原值。
    """

    COMMON_FORMATS = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日 %H:%M",
        "%Y年%m月%d日",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y%m%d",
        "%Y.%m.%d",
        "%Y%m%d%H%M%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
    ]

    @property
    def name(self) -> str:
        return "parse_datetime"

    def execute(self, data: pd.Series) -> pd.Series:
        def parse(val):
            if pd.isna(val):
                return None
            s = str(val).strip()

            # 尝试 pandas 自动解析
            try:
                ts = pd.to_datetime(s)
                return ts.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                pass

            # 尝试各种格式
            from datetime import datetime

            for fmt in self.COMMON_FORMATS:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

            # 无法解析，保留原值
            return s

        return data.apply(parse)
