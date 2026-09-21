"""SignalChainPipeline — 信号链决策框架主流程"""

from __future__ import annotations

import logging

import pandas as pd

from signalchain.models import CacheEntry, DecisionRecord
from signalchain.cache import SignalCache
from signalchain.stage0_profile import extract_profile, generate_fingerprint
from signalchain.stage1_scene import build_scene_prompt, validate_scene_code
from signalchain.stage2_router import (
    ROUTING_TABLE,
    build_field_semantic_prompt,
    standardize_column_names,
)
from signalchain.stage3_semantic import validate_field_signal_sequence
from signalchain.stage4_assemble import assemble_operations
from signalchain.stage5_execute import execute_pipeline, QualityReport
from signalchain.ai_client import AIClient, MockAIClient
from signalchain.fastpath import System1Decider
from signalchain.system1 import Evaluator, GatePolicy, System1Error

logger = logging.getLogger(__name__)


class SignalChainPipeline:
    """SignalChain 信号链决策管线

    Stage 0: 元信息提取 → 缓存查找
    Stage 1: AI 场景识别
    Stage 2: 路由 + Prompt 组装
    Stage 3: AI 字段语义识别 → 字段信号序列
    Stage 4: 字段名标准化 + 操作链组装
    Stage 5: 本地执行

    两套系统互不依赖，各自可单独运行：
      - 只传 ai_client（默认）→ 纯系统二，行为与改动前完全一致
      - 只传 evaluator            → 纯系统一，不调用系统二（低置信走保守默认值）
      - 两个都传 + escalate_to_system2=True → 串联（系统一低置信时升级给系统二）

    escalate_to_system2 默认 False：串联是显式选择，不是默认行为。
    """

    def __init__(
        self,
        ai_client: AIClient | None = None,
        cache_file: str = "signal_cache.json",
        evaluator: Evaluator | None = None,
        gate_policy: GatePolicy | None = None,
        verbose_criteria: bool = False,
        escalate_to_system2: bool = False,
    ):
        self.ai = ai_client or MockAIClient()
        self.evaluator = evaluator
        self.escalate_to_system2 = escalate_to_system2
        self.gate_policy = gate_policy or GatePolicy()
        if escalate_to_system2 and ai_client is None:
            logger.warning(
                "escalate_to_system2=True 但没有传 ai_client，"
                "升级会落到 MockAIClient（返回默认值）。请显式传入系统二客户端。"
            )
        self.decider = (
            System1Decider(
                evaluator,
                # 不串联时 fallback_client=None：低置信走保守默认值，绝不碰系统二
                fallback_client=self.ai if escalate_to_system2 else None,
                policy=self.gate_policy,
                verbose_criteria=verbose_criteria,
            )
            if evaluator is not None
            else None
        )
        self.cache = SignalCache(cache_file, namespace=self._engine_id())
        self.routing = ROUTING_TABLE
        self.prompt_log: list[str] = []  # 捕获发送给 AI 的 prompt
        self.decisions: list[DecisionRecord] = []  # 决策日志（系统一才有内容）

    def _engine_id(self) -> str:
        """决策引擎标识，参与缓存哈希 —— 换引擎必须让旧缓存失效"""
        return self.evaluator.engine_id if self.evaluator is not None else "system2"

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, QualityReport]:
        # ---- Stage 0: 元信息提取 ----
        profile = extract_profile(df)
        fingerprint = generate_fingerprint(profile)
        logger.info(f"Stage 0: profile extracted, fingerprint={fingerprint[:12]}...")

        # ---- 缓存查找 ----
        cached = self.cache.get(fingerprint)
        if cached is not None:
            self.decisions = self._cache_decisions(cached)
            return self._from_cache(df, profile, cached)
            # ^ 保证从缓存出来也走了标准化+分列

        logger.info("Cache miss, proceeding to AI stages")

        # ---- 系统一快通道（可选）：一次请求定场景 + 字段，低置信自动升级系统二 ----
        fast = self._try_system1(profile)
        if fast is not None:
            scene_code = fast.scene_code
            signal_sequence = fast.signal_sequence
            self.decisions = list(fast.decisions)
            scene_certainty = fast.scene_certainty
            decided_by = fast.engine
            logger.info(
                f"系统一快通道：scene_code={scene_code} "
                f"signal_sequence={signal_sequence} "
                f"escalated={fast.escalated_fields or '无'}"
            )
        else:
            decided_by = "system2"
            scene_certainty = 0.0
            # ---- Stage 1: 场景识别 ----
            scene_prompt = build_scene_prompt(profile)
            self.prompt_log.append(scene_prompt)
            raw_scene = self.ai.call(scene_prompt)
            scene_code = validate_scene_code(raw_scene)
            logger.info(f"Stage 1: scene_code={scene_code} (raw={raw_scene!r})")

            # ---- Stage 2: 路由 + Prompt 组装 ----
            scene_config = self.routing.get(scene_code, self.routing["S0"])
            field_prompt = build_field_semantic_prompt(profile, scene_config, scene_code)
            logger.info(f"Stage 2: prompt assembled for '{scene_config.scene_name}'")

            # ---- Stage 3: 字段语义识别 ----
            self.prompt_log.append(field_prompt)
            raw_signals = self.ai.call(field_prompt)
            signal_sequence = validate_field_signal_sequence(
                raw_signals, profile.field_count, scene_config.valid_codes
            )
            logger.info(f"Stage 3: signal_sequence={signal_sequence} (raw={raw_signals!r})")

        scene_config = self.routing.get(scene_code, self.routing["S0"])

        # ---- Stage 4: 缓存写入 + 字段名标准化 + 操作链组装 ----
        self.cache.put(
            fingerprint,
            CacheEntry(
                scene_code,
                signal_sequence,
                certainty=scene_certainty,
                engine=decided_by,
            ),
        )
        return self._execute(df, profile.field_names, signal_sequence, scene_config)

    def _try_system1(self, profile):
        """尝试系统一快通道；不可用或出错则返回 None 让调用方走系统二

        注意这里连 System1RequestError 一起捕获：那意味着我们构造的问题有问题，
        但如果就此抛出去，用户的数据清洗就中断了。
        所以记 ERROR 日志（开发时一眼能看到）+ 降级（用户侧无感），两个目标都不放弃。
        """
        if self.decider is None:
            return None
        try:
            return self.decider.decide(profile)
        except System1Error as exc:
            logger.error(f"系统一决策失败，回退系统二原路径：{type(exc).__name__}: {exc}")
            return None

    @staticmethod
    def _cache_decisions(cached: CacheEntry) -> list[DecisionRecord]:
        """把缓存命中还原成一条决策记录 —— 让决策日志对三种来源都连续"""
        return [
            DecisionRecord(
                question_id="scene",
                subject="scene",
                chosen=cached.scene_code,
                certainty=cached.certainty,
                engine="cache",
                verdict="cache",
            )
        ]

    def _from_cache(
        self, df: pd.DataFrame, profile, cached: CacheEntry
    ) -> tuple[pd.DataFrame, QualityReport]:
        scene_config = self.routing.get(cached.scene_code, self.routing["S0"])
        return self._execute(df, profile.field_names, cached.signal_sequence, scene_config)

    def _execute(
        self,
        df: pd.DataFrame,
        field_names: list[str],
        signal_sequence: str,
        scene_config,
    ) -> tuple[pd.DataFrame, QualityReport]:
        """字段名标准化 + 操作链组装 + 执行"""
        # 字段名标准化
        rename_map = standardize_column_names(field_names, signal_sequence)
        if rename_map:
            logger.info(f"Renaming columns: {rename_map}")
            df = df.rename(columns=rename_map)
            # 用新字段名继续
            field_names = [rename_map.get(n, n) for n in field_names]

        ops = assemble_operations(field_names, signal_sequence, scene_config)
        return execute_pipeline(df, ops)

    @staticmethod
    def run_local(
        df: pd.DataFrame,
        scene_code: str,
        signal_sequence: str,
    ) -> tuple[pd.DataFrame, QualityReport]:
        """跳过 AI，直接用指定信号执行"""
        from signalchain.stage0_profile import extract_profile
        profile = extract_profile(df)
        scene_config = ROUTING_TABLE.get(scene_code, ROUTING_TABLE["S0"])
        # 字段名标准化
        rename_map = standardize_column_names(profile.field_names, signal_sequence)
        if rename_map:
            df = df.rename(columns=rename_map)
        field_names = [rename_map.get(n, n) for n in profile.field_names]
        ops = assemble_operations(field_names, signal_sequence, scene_config)
        return execute_pipeline(df, ops)
