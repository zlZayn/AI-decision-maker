"""EmailValidator — 邮箱格式校验"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class EmailValidator(Operation):
    """
    邮箱格式校验：
    - 合法邮箱保留原值
    - 不合法邮箱标记为 None
    """

    knowledge_key = "email"

    @property
    def name(self) -> str:
        return "validate_email"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        email_pattern = knowledge["pattern"]

        def validate(val):
            if pd.isna(val):
                return None
            s = str(val).strip()
            if re.match(email_pattern, s):
                return s
            return None

        return data.apply(validate)
