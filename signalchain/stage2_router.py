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
# 本地字段名预判表 — 常见字段名 → 信号码
# ============================================================

FIELD_NAME_HINTS: dict[str, str] = {
    # 编号类 I
    "id": "I", "uuid": "I", "uid": "I", "pid": "I", "oid": "I",
    "patient_id": "I", "user_id": "I", "order_id": "I", "record_id": "I",
    "transaction_id": "I", "row_id": "I", "doc_id": "I", "bill_id": "I", "txn_id": "I",
    # 性别类 G
    "gender": "G", "sex": "G",
    # 年龄类 A
    "age": "A", "age_at_diagnosis": "A", "patient_age": "A", "user_age": "A",
    # 科室类 D
    "dept": "D", "department": "D", "dept_name": "D", "科室": "D",
    # 药品类 N
    "drug": "N", "drug_name": "N", "medicine": "N", "medication": "N", "med": "N",
    "药品": "N", "药名": "N",
    # 诊断码 C
    "diagnosis": "C", "icd": "C", "icd10": "C", "icd_code": "C",
    # 时间类 T
    "date": "T", "time": "T", "datetime": "T", "timestamp": "T",
    "created_at": "T", "updated_at": "T", "birth_date": "T",
    "visit_date": "T", "admission_date": "T", "discharge_date": "T",
    # 金额类 M
    "amount": "M", "price": "M", "cost": "M", "fee": "M", "total": "M",
    "payment": "M", "salary": "M", "revenue": "M", "balance": "M",
    "金额": "M", "价格": "M", "费用": "M",
    # 邮箱类 E
    "email": "E", "mail": "E", "e_mail": "E", "e-mail": "E",
    # 手机类 P
    "phone": "P", "mobile": "P", "telephone": "P", "tel": "P", "cell": "P",
    "手机": "P", "电话": "P",
    # 日志级别 L
    "level": "L", "log_level": "L", "severity": "L",
    # 经纬度 R
    "latitude": "R", "longitude": "R", "lat": "R", "lng": "R", "lon": "R",
    "coords": "R", "coordinates": "R", "location": "R",
}

# 各场景的参考说明
SCENE_REFERENCES: dict[str, str] = {
    "S0": "I=编号, X=其他",
    "S1": "I=编号(ID/编号类), G=性别(M/F/男/女), A=年龄(数字/岁), D=科室, N=药品名, C=诊断码(I10类), T=时间日期, X=其他",
    "S2": "I=编号(ID/编号类), M=金额(货币符号/数值金额), T=时间日期(年月日), X=其他",
    "S3": "I=编号(ID/编号类), G=性别(M/F/男/女), A=年龄(数字/岁), E=邮箱(@符号), P=手机号(11位数字), X=其他",
    "S4": "I=编号(ID/编号类), T=时间日期(年月日时分秒), L=日志级别(DEBUG/INFO/ERROR), X=其他",
    "S5": "I=编号(ID/编号类), R=经纬度(小数坐标值), X=其他",
}

# 统一的 Prompt 模板 — 规则在前，字段在后
FIELD_SEMANTIC_TEMPLATE = """场景：{scene_name}
选项：{code_options}
规则：字段名是判断语义的唯一依据，样本值仅供确认。每个字段选1个代码，按顺序拼接。只输出代码，无空格无解释。
参考：{reference}

{field_blocks}

输出："""

# ============================================================
# 路由表
# ============================================================

ROUTING_TABLE: dict[str, SceneConfig] = {
    "S0": SceneConfig(
        scene_name="未知数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE,
        valid_codes={"I", "X"},
        operations={"I": "pass_through", "X": "pass_through"},
    ),
    "S1": SceneConfig(
        scene_name="医疗数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE,
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
        prompt_template=FIELD_SEMANTIC_TEMPLATE,
        valid_codes={"M", "T", "I", "X"},
        operations={
            "M": "split_currency",
            "T": "parse_datetime",
            "I": "pass_through",
            "X": "pass_through",
        },
    ),
    "S3": SceneConfig(
        scene_name="用户数据",
        prompt_template=FIELD_SEMANTIC_TEMPLATE,
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
        prompt_template=FIELD_SEMANTIC_TEMPLATE,
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
        prompt_template=FIELD_SEMANTIC_TEMPLATE,
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


def _lookup_field_hint(field_name: str) -> str | None:
    """本地字段名预判 — 查表得信号码"""
    return FIELD_NAME_HINTS.get(field_name.lower().strip())


def build_field_semantic_prompt(
    profile: DataProfile, scene_config: SceneConfig, scene_code: str = "S0"
) -> str:
    """组装字段语义识别的完整 Prompt

    关键优化：
    1. 规则+参考在前，字段列表在后 — AI 先理解规则再看字段
    2. 本地字段名预判 — 常见字段名直接标注建议信号码
    3. 每个字段附带 hint=建议码 — 减少AI误判
    """
    code_options = _format_code_options(scene_config.valid_codes)
    options_line = " ".join(f"{code}={label}" for code, label in code_options)
    reference = SCENE_REFERENCES.get(scene_code, "")

    # 构建字段列表，附带本地预判提示
    field_lines = []
    for i, field in enumerate(profile.fields, 1):
        compressed = compress_samples(field.samples, max_count=3)
        samples_str = ",".join(compressed[:3])
        hint = _lookup_field_hint(field.name)
        hint_str = f" [hint:{hint}]" if hint else ""
        field_lines.append(
            f"  {i}. {field.name}({field.type}){hint_str}: {samples_str}"
        )

    field_blocks = "字段:\n" + "\n".join(field_lines)

    return scene_config.prompt_template.format(
        scene_name=scene_config.scene_name,
        code_options=options_line,
        reference=reference,
        field_blocks=field_blocks,
    )


# ============================================================
# 字段名标准化 — AI 信号码 → 标准列名
# ============================================================

SIGNAL_STANDARD_NAMES: dict[str, str] = {
    "I": "id",
    "G": "gender",
    "A": "age",
    "D": "department",
    "N": "drug_name",
    "C": "diagnosis_code",
    "T": "date",
    "M": "amount",
    "E": "email",
    "P": "phone",
    "L": "log_level",
    "R": "coordinate",
    "X": "other",
}

# 标准列名→信号码反向映射（同义别名归一化用）
STANDARD_NAME_ALIASES: dict[str, str] = {
    # 将别名统一映射到标准名
    "sex": "gender", "mail": "email", "e_mail": "email",
    "mobile": "phone", "tel": "phone", "telephone": "phone",
    "patient_id": "id", "user_id": "id", "transaction_id": "id",
    "dept": "department",
    "drug": "drug_name", "medicine": "drug_name",
    "price": "amount", "cost": "amount", "fee": "amount",
    "datetime": "date", "timestamp": "date",
}


def standardize_column_names(
    field_names: list[str],
    signal_sequence: str,
) -> dict[str, str]:
    """
    根据 AI 信号码统一字段名为标准名。

    AI 负责语义识别（"这是邮箱"），本地负责执行标准化（"→email"）。
    这就是 AI 驱动的体现：不管原始名是 E-MAIL / email / 邮箱 / mail，
    只要 AI 标记为 E，一律改为 email。

    Returns: {原名: 新名, ...}
    """
    rename_map = {}
    used_names: set[str] = set()
    for name, code in zip(field_names, signal_sequence):
        # X=未知字段，保留原名
        if code == "X":
            continue
        standard = SIGNAL_STANDARD_NAMES.get(code)
        if not standard or name.strip() == standard:
            continue
        # 多个字段映射到同一标准名时，加数字后缀避免重复
        target = standard
        if target in used_names:
            idx = 2
            while f"{target}{idx}" in used_names:
                idx += 1
            target = f"{target}{idx}"
        used_names.add(target)
        rename_map[name] = target
    return rename_map
