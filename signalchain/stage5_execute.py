"""Stage 5: 本地执行引擎

输入：DataFrame + 操作链
动作：逐字段执行操作，异常时保留原值
输出：清洗后的 DataFrame + QualityReport
Token：0（纯本地）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from signalchain.operations.base import Operation

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    """质量报告：记录每个字段的执行情况"""

    records: list[FieldReport] = field(default_factory=list)

    def record(self, col_name: str, op_name: str, changed: int, errors: int) -> None:
        self.records.append(
            FieldReport(col_name=col_name, op_name=op_name, changed=changed, errors=errors)
        )

    def summary(self) -> str:
        lines = ["=" * 60, "SignalChain 质量报告", "=" * 60]
        total_changed = 0
        total_errors = 0
        for r in self.records:
            status = "[OK]" if r.errors == 0 else "[ERR]"
            lines.append(f"  {status} {r.col_name:20s} | {r.op_name:25s} | changed={r.changed} errors={r.errors}")
            total_changed += r.changed
            total_errors += r.errors
        lines.append("-" * 60)
        lines.append(f"  总计: changed={total_changed}, errors={total_errors}")
        lines.append("=" * 60)
        return "\n".join(lines)


@dataclass
class FieldReport:
    """单字段执行报告"""

    col_name: str
    op_name: str
    changed: int
    errors: int


def execute_pipeline(
    df: pd.DataFrame,
    operations: list[tuple[str, Operation]],
) -> tuple[pd.DataFrame, QualityReport]:
    """执行操作链，返回清洗后的 DataFrame 和质量报告"""
    report = QualityReport()
    result = df.copy()

    for col_name, op in operations:
        try:
            original = result[col_name].copy()
            result[col_name] = op.execute(original)
            changed = int((original != result[col_name]).sum())
            report.record(col_name, op.name, changed=changed, errors=0)
        except Exception as e:
            logger.warning(f"Field '{col_name}' op '{op.name}' failed: {e}, keeping original")
            report.record(col_name, op.name, changed=0, errors=1)

    return result, report
