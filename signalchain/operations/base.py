"""操作基类"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Operation(ABC):
    """所有本地操作的基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """操作名称"""
        ...

    @property
    def splits_column(self) -> bool:
        """是否产出一列以上（分列操作）"""
        return False

    @abstractmethod
    def execute(self, data: pd.Series) -> pd.Series | pd.DataFrame:
        """执行操作。返回 Series（1:1）或 DataFrame（1:N 分列）。"""
        ...
