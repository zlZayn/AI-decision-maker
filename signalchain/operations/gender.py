"""GenderNormalizer — 性别字段标准化"""

from __future__ import annotations

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class GenderNormalizer(Operation):
    """将各种性别表达统一为标准值"""

    knowledge_key = "gender"

    @property
    def name(self) -> str:
        return "normalize_gender"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]

        # 构建映射表
        mapping: dict[str, str | None] = {}
        for v in knowledge["male_values"]:
            mapping[v.lower()] = "男"
        for v in knowledge["female_values"]:
            mapping[v.lower()] = "女"
        for v in knowledge["unknown_values"]:
            mapping[v.lower()] = None

        def normalize(val):
            if pd.isna(val):
                return None
            key = str(val).strip().lower()
            if key in mapping:
                return mapping[key]
            # 原始值本身尝试匹配
            return str(val).strip()

        return data.apply(normalize)
