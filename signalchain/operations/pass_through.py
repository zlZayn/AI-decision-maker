"""PassThrough — 保留原值，不执行任何操作"""

from __future__ import annotations

import pandas as pd

from signalchain.operations.base import Operation


class PassThrough(Operation):
    """保留原值操作（I/X 信号码的默认行为）"""

    @property
    def name(self) -> str:
        return "pass_through"

    def execute(self, data: pd.Series) -> pd.Series:
        return data
