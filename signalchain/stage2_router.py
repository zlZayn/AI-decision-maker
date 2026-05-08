"""Stage 2: 本地信号路由与 Prompt 组装

输入：SceneCode + DataProfile
动作：
  1. 用 SceneCode 查 ROUTING_TABLE → 得到 SceneConfig
  2. 从 DataProfile 提取每字段样本，调用 compress_samples 压缩
  3. 用 SceneConfig.prompt_template + 压缩样本 → 组装完整 Prompt
输出：组装好的 Prompt 字符串
Token：0（纯本地）
"""

from __future__ import annotations

import logging

from signalchain.models import DataProfile, SceneConfig, CODE_LABELS

logger = logging.getLogger(__name__)

# ============================================================
# Prompt 模板
# ============================================================

FIELD_SEMANTIC_TEMPLATE_S0 = """场景：{scene_name}

字段样本（每字段最多 5 个值）：

{field_blocks}

要求：
1. 为每个字段选择 1 个代码
2. 按字段顺序输出，不要有空格（如 IX）
3. 不要输出任何解释

输出："""

FIELD_SEMANTIC_TEMPLATE_S1 = """场景：{scene_name}

字段样本（每字段最多 5 个值）：

{field_blocks}

要求：
1. 为每个字段选择 1 个代码
2. 按字段顺序输出，不要有空格（如 IGADN）
3. 不要输出任何解释

输出："""

FIELD_SEMANTIC_TEMPLATE_S2 = FIELD_SEMANTIC_TEMPLATE_S1.replace("IGADN", "MTIX")
FIELD_SEMANTIC_TEMPLATE_S3 = FIELD_SEMANTIC_TEMPLATE_S1.replace("IGADN", "GAEIX")
FIELD_SEMANTIC_TEMPLATE_S4 = FIELD_SEMANTIC_TEMPLATE_S1.replace("IGADN", "TLIX")
FIELD_SEMANTIC_TEMPLATE_S5 = FIELD_SEMANTIC_TEMPLATE_S1.replace("IGADN", "RIX")


# ============================================================
# 路由表
# ============================================================

ROUTING_TABLE: dict[str, SceneConfig] = {
    "S0": SceneConfig(
        scene_name="未知数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S0,
        valid_codes={"I", "X"},
        operations={"I": "pass_through", "X": "pass_through"},
    ),
    "S1": SceneConfig(
        scene_name="医疗数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S1,
        valid_codes={"G", "A", "D", "N", "C", "T", "I", "X"},
        operations={
            "G": "normalize_gender",
            "A": "extract_age",
            "D": "normalize_department",
            "N": "normalize_drug_name",
            "C": "validate_icd10",
            "T": "parse_datetime",
            "I": "pass_through",
            "X": "pass_through",
        },
    ),
    "S2": SceneConfig(
        scene_name="财务数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S2,
        valid_codes={"M", "T", "I", "X"},
        operations={
            "M": "normalize_currency",
            "T": "parse_datetime",
            "I": "pass_through",
            "X": "pass_through",
        },
    ),
    "S3": SceneConfig(
        scene_name="用户数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S3,
        valid_codes={"G", "A", "E", "P", "I", "X"},
        operations={
            "G": "normalize_gender",
            "A": "extract_age",
            "E": "validate_email",
            "P": "validate_phone",
            "I": "pass_through",
            "X": "pass_through",
        },
    ),
    "S4": SceneConfig(
        scene_name="日志数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S4,
        valid_codes={"T", "L", "I", "X"},
        operations={
            "T": "parse_datetime",
            "L": "normalize_log_level",
            "I": "pass_through",
            "X": "pass_through",
        },
    ),
    "S5": SceneConfig(
        scene_name="地理数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE_S5,
        valid_codes={"R", "I", "X"},
        operations={
            "R": "validate_coordinates",
            "I": "pass_through",
            "X": "pass_through",
        },
    ),
}


# ============================================================
# Prompt 组装
# ============================================================


def compress_samples(values: list[str], max_count: int = 5) -> list[str]:
    """压缩样本值，保留信息密度最高的样本"""

    def info_score(v: str) -> int:
        score = 0
        if any(c in v for c in "?!@#$%"):
            score += 3
        if len(v) > 20 or len(v) < 2:
            score += 2
        if any(c.isdigit() for c in v) and any(c.isalpha() for c in v):
            score += 2
        return score

    unique = list(set(values))
    unique.sort(key=info_score, reverse=True)
    return unique[:max_count]


def _format_code_options(valid_codes: set[str]) -> list[tuple[str, str]]:
    """将 valid_codes 转换为 AI 可读的选项列表"""
    # 固定顺序，X 始终放在最后
    ordered = sorted(valid_codes - {"X"}) + (["X"] if "X" in valid_codes else [])
    return [(code, CODE_LABELS[code]) for code in ordered]


def build_field_semantic_prompt(
    profile: DataProfile, scene_config: SceneConfig
) -> str:
    """组装字段语义识别的完整 Prompt"""
    code_options = _format_code_options(scene_config.valid_codes)

    field_blocks = []
    for i, field in enumerate(profile.fields, 1):
        compressed = compress_samples(field.samples, max_count=5)
        options_str = " ".join(f"{code}={label}" for code, label in code_options)
        field_blocks.append(
            f"字段{i}: {field.name}\n" f"样本: {compressed}\n" f"选项: {options_str}"
        )

    return scene_config.prompt_template.format(
        scene_name=scene_config.scene_name,
        field_blocks="\n\n".join(field_blocks),
    )
