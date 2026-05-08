"""Stage 0: 本地元信息提取

输入：原始 DataFrame
输出：DataProfile + fingerprint
Token：0（纯本地）
"""

from __future__ import annotations

import hashlib

import pandas as pd

from signalchain.models import DataProfile, FieldProfile


def extract_profile(df: pd.DataFrame, max_samples: int = 20) -> DataProfile:
    """从 DataFrame 提取 DataProfile，零 Token"""
    fields: list[FieldProfile] = []

    for col in df.columns:
        # 采样：去重，保留最多 max_samples 个
        unique_values = df[col].dropna().unique()
        samples = [str(v) for v in unique_values[:max_samples]]

        # 类型推断
        dtype = df[col].dtype
        if dtype in ("int64", "int32", "Int64", "Int32"):
            type_name = "int"
        elif dtype in ("float64", "float32", "Float64", "Float32"):
            type_name = "float"
        else:
            type_name = "string"

        # 缺失率
        null_ratio = df[col].isna().mean()

        fields.append(
            FieldProfile(
                name=col,
                type=type_name,
                samples=samples,
                null_ratio=round(null_ratio, 4),
            )
        )

    return DataProfile(fields=fields)


def generate_fingerprint(profile: DataProfile) -> str:
    """从 DataProfile 生成指纹，用于缓存命中判断

    指纹包含样本值的原因：相同字段名但不同样本值（如性别字段一个含"帅哥"，
    一个不含）可能需要不同的 AI 决策。仅用字段名做指纹会误命中。
    """
    field_names = ",".join(sorted(f.name for f in profile.fields))
    sample_hashes = ",".join(
        hashlib.md5(",".join(sorted(f.samples)).encode()).hexdigest()[:8]
        for f in profile.fields
    )
    return hashlib.md5(f"{field_names}:{sample_hashes}".encode()).hexdigest()
