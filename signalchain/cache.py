"""数据指纹缓存 — fingerprint → CacheEntry

缓存命中时跳过 Stage 1-3，零 Token 消耗。
自动根据代码配置计算哈希，代码变更自动失效。
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

from signalchain.models import CacheEntry

logger = logging.getLogger(__name__)

DEFAULT_CACHE_FILE = "signal_cache.json"


def _code_hash() -> str:
    """计算当前代码配置的哈希，代码变更时缓存自动失效"""
    # 延迟导入，避免循环依赖
    from signalchain.stage2_router import ROUTING_TABLE, SIGNAL_STANDARD_NAMES
    from signalchain.operations.registry import OPERATION_REGISTRY

    h = hashlib.sha256()
    # 路由表 → 场景名 + valid_codes + operations 映射
    for code, cfg in sorted(ROUTING_TABLE.items()):
        h.update(code.encode())
        h.update(cfg.scene_name.encode())
        h.update("".join(sorted(cfg.valid_codes)).encode())
        h.update(str(sorted(cfg.operations.items())).encode())
    # 标准列名映射
    h.update(str(sorted(SIGNAL_STANDARD_NAMES.items())).encode())
    # 可用操作列表
    h.update(str(sorted(OPERATION_REGISTRY.keys())).encode())
    return h.hexdigest()[:16]


class SignalCache:
    """
    缓存结构：fingerprint → CacheEntry(scene_code, signal_sequence)

    缓存附带代码哈希 _code_hash，代码变更时自动全量失效。
    """

    def __init__(self, cache_file: str | Path = DEFAULT_CACHE_FILE):
        self._in_memory = str(cache_file) == ":memory:"
        self.cache_file = None if self._in_memory else Path(cache_file)
        self.cache: dict[str, dict[str, str]] = {} if self._in_memory else self._load()

    def get(self, fingerprint: str) -> CacheEntry | None:
        entry = self.cache.get(fingerprint)
        if entry is None:
            return None
        return CacheEntry(scene_code=entry["scene_code"], signal_sequence=entry["signal_sequence"])

    def put(self, fingerprint: str, entry: CacheEntry) -> None:
        self.cache[fingerprint] = {
            "scene_code": entry.scene_code,
            "signal_sequence": entry.signal_sequence,
        }
        self._save()

    def _load(self) -> dict[str, dict[str, str]]:
        expected_hash = _code_hash()
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("_code_hash") != expected_hash:
                logger.info("Code changed, cache invalidated")
                return {}
            return data.get("entries", {})
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self) -> None:
        if self._in_memory:
            return
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(
                {"_code_hash": _code_hash(), "entries": self.cache},
                f,
                indent=2,
                ensure_ascii=False,
            )
