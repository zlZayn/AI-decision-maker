"""DepartmentNormalizer — 科室/部门名称标准化"""

from __future__ import annotations

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class DepartmentNormalizer(Operation):
    """将各种科室缩写/别名统一为标准名称"""

    knowledge_key = "department"

    @property
    def name(self) -> str:
        return "normalize_department"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        mappings = knowledge["mappings"]

        def normalize(val):
            if pd.isna(val):
                return None
            s = str(val).strip()
            # 精确匹配
            if s in mappings:
                return mappings[s]
            # 去除空格后匹配
            s_no_space = s.replace(" ", "")
            if s_no_space in mappings:
                return mappings[s_no_space]
            return s

        return data.apply(normalize)
