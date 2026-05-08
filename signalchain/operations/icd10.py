"""ICD10Validator — ICD-10 诊断码校验"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class ICD10Validator(Operation):
    """
    ICD-10 诊断码校验：
    - 合法的 ICD-10 码保留原值
    - 不合法的标记为 None
    - 常见诊断码附带描述信息（通过知识库）
    """

    knowledge_key = "icd10"

    @property
    def name(self) -> str:
        return "validate_icd10"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        icd10_pattern = knowledge["pattern"]

        def validate(val):
            if pd.isna(val):
                return None
            s = str(val).strip().upper()
            if re.match(icd10_pattern, s):
                return s
            return None

        return data.apply(validate)
