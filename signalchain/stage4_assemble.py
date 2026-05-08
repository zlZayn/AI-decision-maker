"""Stage 4: 执行计划组装与校验

输入：SceneCode + FieldSignalSequence + SceneConfig
动作：
  1. 将 (fingerprint, CacheEntry(scene_code, signal_sequence)) 写入缓存
  2. 遍历 signal_sequence，用 SceneConfig.operations 查表：
     信号码 → 操作名 → OPERATION_REGISTRY 中的 Operation 实例
  3. 组装成 pandas 操作链
输出：可执行的操作链
Token：0（纯本地）
"""

from __future__ import annotations

import logging

from signalchain.models import SceneConfig
from signalchain.operations.base import Operation
from signalchain.operations.registry import OPERATION_REGISTRY

logger = logging.getLogger(__name__)


def assemble_operations(
    field_names: list[str],
    signal_sequence: str,
    scene_config: SceneConfig,
    registry: dict[str, Operation] | None = None,
) -> list[tuple[str, Operation]]:
    """
    组装操作链。

    Args:
        field_names: 字段名列表
        signal_sequence: 字段信号序列，如 "IGADN"
        scene_config: 场景配置
        registry: 操作注册表，默认使用 OPERATION_REGISTRY

    Returns:
        [(字段名, 操作实例), ...]
    """
    if registry is None:
        registry = OPERATION_REGISTRY

    ops: list[tuple[str, Operation]] = []

    for col_name, code in zip(field_names, signal_sequence):
        op_name = scene_config.operations.get(code)  # 不给默认值
        if op_name is None or op_name not in registry:
            logger.error(
                f"Missing operation for code '{code}' in scene "
                f"'{scene_config.scene_name}', fallback to pass_through"
            )
            op = registry["pass_through"]
        else:
            op = registry[op_name]
        ops.append((col_name, op))

    return ops
