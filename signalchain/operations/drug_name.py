"""DrugNameNormalizer — 药品名称标准化"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class DrugNameNormalizer(Operation):
    """
    药品名称标准化：
    1. 提取纯药品名（去除剂量后缀）
    2. 映射常见别名到标准名称
    3. 无法识别的药品保留原名
    """

    knowledge_key = "drug_name"

    @property
    def name(self) -> str:
        return "normalize_drug_name"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        pattern = knowledge["pattern"]
        common_drugs = knowledge["common_drugs"]

        def normalize(val):
            if pd.isna(val):
                return None
            s = str(val).strip()

            # 尝试提取纯药品名
            match = re.match(pattern, s)
            if match:
                drug_name = match.group()
            else:
                drug_name = s

            # 映射常见别名
            if drug_name in common_drugs:
                return common_drugs[drug_name]

            return drug_name

        return data.apply(normalize)
