"""SignalChainPipeline — 信号链决策框架主流程"""

from __future__ import annotations

import logging

import pandas as pd

from signalchain.models import CacheEntry
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

logger = logging.getLogger(__name__)


class SignalChainPipeline:
    """SignalChain 信号链决策管线

    Stage 0: 元信息提取 → 缓存查找
    Stage 1: AI 场景识别
    Stage 2: 路由 + Prompt 组装
    Stage 3: AI 字段语义识别 → 字段信号序列
    Stage 4: 字段名标准化 + 操作链组装
    Stage 5: 本地执行
    """

    def __init__(
        self,
        ai_client: AIClient | None = None,
        cache_file: str = "signal_cache.json",
    ):
        self.ai = ai_client or MockAIClient()
        self.cache = SignalCache(cache_file)
        self.routing = ROUTING_TABLE
        self.prompt_log: list[str] = []  # 捕获发送给 AI 的 prompt

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, QualityReport]:
        # ---- Stage 0: 元信息提取 ----
        profile = extract_profile(df)
        fingerprint = generate_fingerprint(profile)
        logger.info(f"Stage 0: profile extracted, fingerprint={fingerprint[:12]}...")

        # ---- 缓存查找 ----
        cached = self.cache.get(fingerprint)
        if cached is not None:
            return self._from_cache(df, profile, cached)
            # ^ 保证从缓存出来也走了标准化+分列

        logger.info("Cache miss, proceeding to AI stages")

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

        # ---- Stage 4: 缓存写入 + 字段名标准化 + 操作链组装 ----
        self.cache.put(fingerprint, CacheEntry(scene_code, signal_sequence))
        return self._execute(df, profile.field_names, signal_sequence, scene_config)

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
