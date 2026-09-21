"""核心数据结构定义 — 各阶段之间的数据契约"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter


@dataclass
class FieldProfile:
    """单字段的画像信息（Stage 0 输出的最小单元）"""

    name: str              # 字段名，如 "gender"
    type: str              # 数据类型，如 "string", "int", "float"
    samples: list[str]     # 去重后的样本值，最多 20 个
    null_ratio: float      # 缺失率，0.0 ~ 1.0


@dataclass
class DataProfile:
    """
    数据集画像（Stage 0 输出，贯穿全流程）。

    同时服务于指纹生成、AI 场景识别、AI 字段语义识别三个用途。
    """

    fields: list[FieldProfile]

    # ---- 派生属性（从 fields 计算，不单独存储） ----

    @property
    def field_count(self) -> int:
        return len(self.fields)

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    @property
    def type_summary(self) -> str:
        """如 '3string,1int,1float'"""
        counter = Counter(f.type for f in self.fields)
        return ",".join(f"{count}{dtype}" for dtype, count in counter.items())

    @property
    def null_ratio_summary(self) -> str:
        """如 '0.00,0.05,0.02,0.00,0.10'"""
        return ",".join(f"{f.null_ratio:.2f}" for f in self.fields)


@dataclass
class CacheEntry:
    """缓存条目（fingerprint → CacheEntry）

    certainty / engine 是系统一接入后新增的可选字段：
    它们回答"这条缓存当时是谁判的、有多确定"。老缓存文件缺这两个字段照样能读。
    """

    scene_code: str          # 场景信号码，如 "S1"
    signal_sequence: str     # 字段信号序列，如 "IGADN"
    certainty: float = 0.0   # 场景决策时的把握程度（0 表示未知/老缓存）
    engine: str = ""         # 决策来源，如 "system1" / "system2"


@dataclass
class DecisionRecord:
    """单条决策的可审计记录

    系统一接入后，"AI 负责认知"这句话第一次有了可回溯的证据：
    每个字段为什么是这个码、当时有多确定、有没有升级给系统二。

    字段语义：
      engine      最终答案由谁给出 —— "system1"（Jev）或 "system2"（LLM 升级裁决）
      certainty   系统一当时的把握程度（∈[0,1]）。即使是 system2 的答案，
                  这里也保留"触发升级时的那个 certainty"——它才是升级的理由。
      verdict     系统一当时的判定：accept | provisional | escalate
      escalated   是否发生了升级（engine == "system2" 时为 True）
    """

    question_id: str              # 问题标识，如 "scene" / "field:gender"
    subject: str                  # 人类可读主体（字段名 / "scene"）
    chosen: str                   # 最终选择（场景码 / 信号码 / 取值）
    certainty: float              # 系统一把握程度 0.0~1.0
    engine: str                   # "system1" | "system2"
    probabilities: dict[str, float] = field(default_factory=dict)
    escalated: bool = False
    verdict: str = ""

    def summary(self) -> str:
        flag = "  [escalated -> system2]" if self.escalated else ""
        return (
            f"{self.subject:<18s} {self.chosen:<4s} "
            f"certainty={self.certainty:.3f} verdict={self.verdict or '-':<11s} "
            f"via {self.engine}{flag}"
        )


@dataclass
class SceneConfig:
    """场景配置（从路由表查表得到）"""

    scene_name: str                     # 场景中文名，如 "医疗数据"
    prompt_template: str                 # 字段语义识别的 Prompt 模板
    valid_codes: set[str]               # 该场景下合法的字段信号码集合
    operations: dict[str, str]          # 信号码 → 操作名映射，如 {"G": "normalize_gender"}


# ============================================================
# 全局常量
# ============================================================

VALID_SCENE_CODES: set[str] = {"S0", "S1", "S2", "S3", "S4", "S5"}

VALID_FIELD_CODES: set[str] = {
    "G", "A", "D", "N", "C", "T", "M", "E", "P", "L", "R", "I", "X",
}

CODE_LABELS: dict[str, str] = {
    "G": "性别",
    "A": "年龄",
    "D": "科室",
    "N": "药品名",
    "C": "诊断码",
    "T": "时间",
    "M": "金额",
    "E": "邮箱",
    "P": "手机号",
    "L": "日志级别",
    "R": "经纬度",
    "I": "编号",
    "X": "其他",
}
