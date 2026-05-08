"""操作基类"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Operation(ABC):
    """所有本地操作的基类

    关键设计：
    - 纯函数：无副作用、幂等、输入输出可预测
    - 信号码架构中 AI 不输出参数，因此 Operation 基类没有 validate_params 方法
    - 如果未来需要参数化操作，应通过 SEMANTIC_KNOWLEDGE 或场景配置提供默认参数
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """操作名称，用于日志和报告"""
        ...

    @abstractmethod
    def execute(self, data: pd.Series) -> pd.Series:
        """
        执行操作。纯函数，不修改输入。

        Args:
            data: 输入列
        Returns:
            处理后的列
        Raises:
            ValueError: 输入数据不合法时抛出，由执行引擎捕获
        """
        ...
