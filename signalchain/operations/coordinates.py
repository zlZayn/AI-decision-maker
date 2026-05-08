"""CoordinatesValidator — 经纬度坐标校验"""

from __future__ import annotations

import re

import pandas as pd

from signalchain.operations.base import Operation
from signalchain.knowledge import SEMANTIC_KNOWLEDGE


class CoordinatesValidator(Operation):
    """
    经纬度坐标校验：
    - 校验经纬度数值范围
    - 支持度分秒(DMS)格式转换
    - 不合法的标记为 None
    """

    knowledge_key = "coordinates"

    @property
    def name(self) -> str:
        return "validate_coordinates"

    def execute(self, data: pd.Series) -> pd.Series:
        knowledge = SEMANTIC_KNOWLEDGE[self.knowledge_key]
        lat_range = knowledge["lat_range"]
        lon_range = knowledge["lon_range"]

        def validate(val):
            if pd.isna(val):
                return None
            s = str(val).strip()

            # 尝试直接解析为浮点数
            try:
                coord = float(s)
                if lat_range[0] <= coord <= lat_range[1]:
                    return round(coord, 6)
                if lon_range[0] <= coord <= lon_range[1]:
                    return round(coord, 6)
                return None
            except ValueError:
                pass

            # 尝试解析度分秒格式
            dms_pattern = knowledge["dms_pattern"]
            match = re.match(dms_pattern, s)
            if match:
                degrees = float(match.group(1))
                minutes = float(match.group(2))
                seconds = float(match.group(3))
                decimal = degrees + minutes / 60 + seconds / 3600
                return round(decimal, 6)

            return None

        return data.apply(validate)
