"""PhoneValidator — 手机号格式校验与标准化"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class PhoneValidator(Operation):
    """
    手机号格式校验与标准化：
    - 校验中国手机号格式
    - 去除多余分隔符
    - 不合法的标记为 None
    """

    knowledge_key = "phone"

    @property
    def name(self) -> str:
        return "validate_phone"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        cn_mobile = knowledge["cn_mobile_pattern"]

        def validate(val):
            if pd.isna(val):
                return None
            s = str(val).strip()
            # 去除常见分隔符
            cleaned = re.sub(r"[\s\-\(\)]", "", s)

            # 校验手机号
            if re.match(cn_mobile, cleaned):
                return cleaned

            return None

        return data.apply(validate)
