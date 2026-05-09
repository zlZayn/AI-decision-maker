"""Stage 5: 本地执行引擎

支持全局符号清理 + 1:1 操作 + 1:N 分列操作。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import pandas as pd

from signalchain.operations.base import Operation

logger = logging.getLogger(__name__)

# 全局符号清理 — 去除无意义标点（含全角），保留有用字符
# 保留: 字母、数字、空格、@ . - + _ $ ¥ € £ / : , % ，
# 清除: ! ? ！？ ~ ～ # ＊ ^ & * ( ) （ ） 【 】〔〕［］｛｝ < > " ' ` ；; = | \
# 注意: @ 是邮箱的有效字符，不能删除
MEANINGLESS_SYMBOLS = re.compile(
    r'[!?！？~～#＊^&*()（）【】〔〕［］｛｝<>"\'`;；=|\\]'
)


def _clean_text(val):
    """去除字段中无意义符号"""
    if pd.isna(val):
        return None
    s = str(val)
    s = MEANINGLESS_SYMBOLS.sub("", s)   # 移除无意义符号
    s = re.sub(r"\s+", " ", s).strip()    # 合并多余空格
    return s if s else None


@dataclass
class QualityReport:
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
    col_name: str
    op_name: str
    changed: int
    errors: int


def execute_pipeline(
    df: pd.DataFrame,
    operations: list[tuple[str, Operation]],
) -> tuple[pd.DataFrame, QualityReport]:
    """执行操作链"""
    report = QualityReport()
    result = df.copy()

    # ---- 全局符号清理：所有字段先过一遍去无意义符号 ----
    for col in result.columns:
        original = result[col].copy()
        result[col] = original.apply(_clean_text)
        changed = int((original != result[col]).sum())
        if changed:
            report.record(col, "clean_symbols", changed=changed, errors=0)

    # ---- 逐字段执行指定操作 ----
    skip_cols: set[str] = set()

    for col_name, op in operations:
        if col_name in skip_cols:
            continue
        if col_name not in result.columns:
            continue

        try:
            original = result[col_name].copy()
            output = op.execute(original)

            if op.splits_column:
                if isinstance(output, pd.DataFrame):
                    for new_col in output.columns:
                        result[new_col] = output[new_col]
                    result.drop(columns=[col_name], inplace=True)
                    skip_cols.add(col_name)
                    for c in output.columns:
                        if c != col_name:
                            changed = int((pd.Series([None] * len(result)) != output[c]).sum())
                            report.record(c, op.name, changed=changed, errors=0)
                    report.record(col_name, op.name, changed=0, errors=0)
                else:
                    result[col_name] = output
                    changed = int((original != output).sum())
                    report.record(col_name, op.name, changed=changed, errors=0)
            else:
                result[col_name] = output
                changed = int((original != output).sum())
                report.record(col_name, op.name, changed=changed, errors=0)

        except Exception as e:
            logger.warning(f"Field '{col_name}' op '{op.name}' failed: {e}, keeping original")
            report.record(col_name, op.name, changed=0, errors=1)

    return result, report
