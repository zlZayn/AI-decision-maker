"""SignalChainPipeline — 信号链决策框架主流程

完整的5阶段数据清洗管线，含缓存、校验、回退。
"""

from __future__ import annotations

import logging

import pandas as pd

from signalchain.models import CacheEntry
from signalchain.cache import SignalCache
from signalchain.stage0_profile import extract_profile, generate_fingerprint
from signalchain.stage1_scene import build_scene_prompt, validate_scene_code
from signalchain.stage2_router import ROUTING_TABLE, build_field_semantic_prompt
from signalchain.stage3_semantic import validate_field_signal_sequence
from signalchain.stage4_assemble import assemble_operations
from signalchain.stage5_execute import execute_pipeline, QualityReport
from signalchain.ai_client import AIClient, MockAIClient

logger = logging.getLogger(__name__)


class SignalChainPipeline:
    """SignalChain 信号链决策管线

    核心流程：
    DataFrame → Stage 0: 元信息提取 → 缓存查找 → Stage 1: 场景识别 →
    Stage 2: 路由+Prompt组装 → Stage 3: 字段语义识别 →
    Stage 4: 操作链组装 → Stage 5: 本地执行 → 清洗后DataFrame
    """

    def __init__(
        self,
        ai_client: AIClient | None = None,
        cache_file: str = "signal_cache.json",
    ):
        self.ai = ai_client or MockAIClient()
        self.cache = SignalCache(cache_file)
        self.routing = ROUTING_TABLE

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, QualityReport]:
        """执行完整的信号链管线"""

        # ---- Stage 0: 元信息提取 ----
        profile = extract_profile(df)
        fingerprint = generate_fingerprint(profile)
        logger.info(f"Stage 0: profile extracted, fingerprint={fingerprint[:12]}...")

        # ---- 缓存查找 ----
        cached = self.cache.get(fingerprint)
        if cached is not None:
            logger.info(
                f"Cache hit: fingerprint={fingerprint[:12]}..., "
                f"scene={cached.scene_code}, signals={cached.signal_sequence}"
            )
            scene_config = self.routing.get(cached.scene_code, self.routing["S0"])
            ops = assemble_operations(
                profile.field_names, cached.signal_sequence, scene_config
            )
            return execute_pipeline(df, ops)

        logger.info("Cache miss, proceeding to AI stages")

        # ---- Stage 1: 场景识别 ----
        scene_prompt = build_scene_prompt(profile)
        raw_scene = self.ai.call(scene_prompt)
        scene_code = validate_scene_code(raw_scene)
        logger.info(f"Stage 1: scene_code={scene_code} (raw={raw_scene!r})")

        # ---- Stage 2: 路由 + Prompt 组装 ----
        scene_config = self.routing.get(scene_code, self.routing["S0"])
        field_prompt = build_field_semantic_prompt(profile, scene_config)
        logger.info(f"Stage 2: prompt assembled for scene '{scene_config.scene_name}'")

        # ---- Stage 3: 字段语义识别 ----
        raw_signals = self.ai.call(field_prompt)
        signal_sequence = validate_field_signal_sequence(
            raw_signals,
            field_count=profile.field_count,
            valid_codes=scene_config.valid_codes,
        )
        logger.info(f"Stage 3: signal_sequence={signal_sequence} (raw={raw_signals!r})")

        # ---- Stage 4: 缓存写入 + 操作链组装 ----
        self.cache.put(fingerprint, CacheEntry(scene_code, signal_sequence))
        logger.info(f"Stage 4: cache written, assembling operations")
        ops = assemble_operations(profile.field_names, signal_sequence, scene_config)

        # ---- Stage 5: 执行 ----
        result, report = execute_pipeline(df, ops)
        logger.info("Stage 5: pipeline executed")

        return result, report

    @staticmethod
    def run_local(
        df: pd.DataFrame,
        scene_code: str,
        signal_sequence: str,
    ) -> tuple[pd.DataFrame, QualityReport]:
        """跳过 AI，直接用指定的场景码和信号序列执行（用于测试/调试）"""
        profile = extract_profile(df)
        scene_config = ROUTING_TABLE.get(scene_code, ROUTING_TABLE["S0"])
        ops = assemble_operations(profile.field_names, signal_sequence, scene_config)
        return execute_pipeline(df, ops)
