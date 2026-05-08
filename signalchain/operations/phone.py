"""PhoneValidator — 手机号/座机/服务号格式校验与标准化"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class PhoneValidator(Operation):
    """
    手机号/座机/服务号格式校验与标准化：
    - 中国大陆手机号 (1xx xxxx xxxx)
    - 带区号座机 (010-88886666)
    - 400/800 服务热线
    - 100xx 运营商客服
    - 不合法的标记为 None
    """

    knowledge_key = "phone"

    @property
    def name(self) -> str:
        return "validate_phone"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        cn_mobile = knowledge["cn_mobile_pattern"]
        cn_landline = knowledge["cn_landline_pattern"]

        def validate(val):
            if pd.isna(val):
                return None
            s = str(val).strip()
            cleaned = re.sub(r"[\s\-\(\)]", "", s)

            if re.match(cn_mobile, cleaned):
                return cleaned
            if re.match(cn_landline, cleaned):
                return cleaned
            if re.match(r"^(400|800)\d{7}$", cleaned):
                return cleaned
            if re.match(r"^100\d{2,3}$", cleaned):
                return cleaned
            return None

        return data.apply(validate)
