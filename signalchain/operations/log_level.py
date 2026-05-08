"""LogLevelNormalizer — 日志级别标准化"""

from __future__ import annotations

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class LogLevelNormalizer(Operation):
    """
    日志级别标准化：
    - 将各种日志级别表达统一为大写标准值
    - 不合法的保留原值
    """

    knowledge_key = "log_level"

    @property
    def name(self) -> str:
        return "normalize_log_level"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        mappings = knowledge["mappings"]

        def normalize(val):
            if pd.isna(val):
                return None
            s = str(val).strip()
            if s in mappings:
                return mappings[s]
            return s

        return data.apply(normalize)
