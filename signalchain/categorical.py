"""分类变量有序判断 — 独立模块，复用 AIClient"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from signalchain.ai_client import AIClient
from signalchain.models import DataProfile

logger = logging.getLogger(__name__)


# ============================================================
# 数据结构
# ============================================================


@dataclass
class ClassificationResult:
    """分类变量有序判断的最终结果"""

    ordinal: dict[str, list[str]]  # 有序变量 → [从小到大的顺序]
    nominal: list[str]  # 无序变量列表

    @property
    def all_categorical(self) -> list[str]:
        return list(self.ordinal.keys()) + self.nominal


# ============================================================
# Prompt 构建
# ============================================================


def build_categorical_prompt(profile: DataProfile) -> str:
    """第一层 Prompt：从所有字段中筛选分类变量

    输入：DataProfile（Stage 0 产出）
    输出：AI 应返回逗号分隔的字段名，或"无"
    """
    field_lines = []
    for f in profile.fields:
        display = ", ".join(f.samples[:5])
        n_unique = len(f.samples)
        if n_unique > 5:
            display += f"...(共{n_unique}种)"
        field_lines.append(f"  {f.name}({f.type}, {n_unique}种): {display}")
    field_blocks = "\n".join(field_lines)

    return f"""字段列表：
{field_blocks}

从以上字段中，筛选出分类变量（类别数量有限、值为离散集合的字段）。
包括被错误存储为数值的分类变量（如用 1/2/3 表示等级）。
排除连续数值（年龄、金额）、自由文本（邮箱、地址）、ID编号。

只输出分类变量的字段名，用逗号分隔，无空格无解释。
如果没有分类变量，输出：无

输出："""


def build_ordinal_prompt(
    categorical_fields: list[str],
    unique_values: dict[str, list[str]],
) -> str:
    """第二层 Prompt：判断有序/无序，并输出顺序

    输入：分类变量名 + 每个变量的唯一值
    输出：AI 应返回有序变量及顺序，格式为 字段名:值1>值2>值3，多个用分号分隔
    """
    field_lines = []
    for name in categorical_fields:
        values = unique_values[name]
        if len(values) > 30:
            display = ", ".join(values[:30]) + f"...(共{len(values)}个)"
        else:
            display = ", ".join(values)
        field_lines.append(f"  {name}: {display}")
    field_blocks = "\n".join(field_lines)

    return f"""分类变量及其唯一值：
{field_blocks}

判断哪些是有序分类变量（类别之间有明确的大小/等级/程度关系）。
例如：学历（小学<初中<高中）、满意度（不满意<一般<满意<非常满意）。
无序的例子：性别、城市、血型。

对于有序变量，用>连接从小到大的顺序。
格式：字段名:值1>值2>值3，多个有序变量用分号分隔。
如果全部是无序变量，输出：无

输出："""


# ============================================================
# 输出校验
# ============================================================


def validate_categorical_output(raw_output: str, field_names: list[str]) -> list[str]:
    """校验第一层 AI 输出，返回有效的分类变量名列表"""
    cleaned = raw_output.strip()
    if cleaned == "无" or not cleaned:
        return []
    # 统一中英文逗号，拆分
    cleaned = cleaned.replace("，", ",")
    candidates = [s.strip() for s in cleaned.split(",") if s.strip()]
    # 只保留实际存在的字段名，保持原始顺序
    field_set = set(field_names)
    seen: set[str] = set()
    valid = []
    for c in candidates:
        if c in field_set and c not in seen:
            valid.append(c)
            seen.add(c)
    return valid


def validate_ordinal_output(
    raw_output: str,
    categorical_fields: list[str],
    unique_values: dict[str, list[str]],
) -> dict[str, list[str]]:
    """校验第二层 AI 输出，返回 {有序字段: [顺序]}

    AI 输出格式：字段名:值1>值2>值3;字段名:值1>值2>值3
    """
    cleaned = raw_output.strip()
    if cleaned == "无" or not cleaned:
        return {}

    # 统一中文标点
    cleaned = cleaned.replace("；", ";").replace("：", ":")

    result: dict[str, list[str]] = {}
    segments = [s.strip() for s in cleaned.split(";") if s.strip()]

    for seg in segments:
        if ":" not in seg:
            continue
        name_part, order_part = seg.split(":", 1)
        name = name_part.strip()
        if name not in categorical_fields:
            continue
        order = [v.strip() for v in order_part.split(">") if v.strip()]
        if not order:
            continue
        valid_set = set(unique_values.get(name, []))
        order = [v for v in order if v in valid_set]
        if order:
            result[name] = order

    return result


# ============================================================
# 脚本中间步骤
# ============================================================


def extract_unique_values(
    df: pd.DataFrame, categorical_fields: list[str]
) -> dict[str, list[str]]:
    """提取分类变量的唯一值，不修改原 DataFrame"""
    result: dict[str, list[str]] = {}
    for col in categorical_fields:
        if col not in df.columns:
            continue
        uniques = df[col].dropna().unique().tolist()
        result[col] = [str(v) for v in uniques]
    return result


def apply_categorical_type(
    df: pd.DataFrame,
    categorical_fields: list[str],
    ordinal_fields: dict[str, list[str]],
) -> pd.DataFrame:
    """将分类变量转为 pd.Categorical，有序变量按指定顺序编码

    返回新的 DataFrame，不修改原数据。
    """
    result = df.copy()
    for col in categorical_fields:
        if col in ordinal_fields:
            order = ordinal_fields[col]
            result[col] = pd.Categorical(result[col], categories=order, ordered=True)
        else:
            result[col] = result[col].astype("category")
    return result


# ============================================================
# 主类
# ============================================================


class CategoricalClassifier:
    """分类变量有序判断器

    复用 AIClient 协议，两次 AI 调用完成分类。
    """

    def __init__(self, ai_client: AIClient):
        self.ai = ai_client

    def classify(self, df: pd.DataFrame, profile: DataProfile) -> ClassificationResult:
        """
        Args:
            df: 原始数据
            profile: Stage 0 产出的 DataProfile

        Returns:
            ClassificationResult(ordinal={有序字段: 顺序}, nominal=[无序字段])
        """
        # ---- 第一层 AI：筛选分类变量 ----
        prompt1 = build_categorical_prompt(profile)
        raw1 = self.ai.call(prompt1)
        categorical_fields = validate_categorical_output(raw1, profile.field_names)
        logger.info(f"Layer 1: categorical_fields={categorical_fields}")

        if not categorical_fields:
            return ClassificationResult(ordinal={}, nominal=[])

        # ---- 脚本：提取唯一值（不修改原 DataFrame）----
        unique_values = extract_unique_values(df, categorical_fields)

        # ---- 第二层 AI：判断有序/无序 ----
        prompt2 = build_ordinal_prompt(categorical_fields, unique_values)
        raw2 = self.ai.call(prompt2)
        ordinal_fields = validate_ordinal_output(
            raw2, categorical_fields, unique_values
        )
        logger.info(f"Layer 2: ordinal_fields={list(ordinal_fields.keys())}")

        # ---- 反集：无序变量 ----
        nominal_fields = [f for f in categorical_fields if f not in ordinal_fields]

        return ClassificationResult(ordinal=ordinal_fields, nominal=nominal_fields)
