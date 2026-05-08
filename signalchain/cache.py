"""数据指纹缓存 — fingerprint → CacheEntry

缓存命中时跳过 Stage 1-3，零 Token 消耗。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from signalchain.models import CacheEntry

logger = logging.getLogger(__name__)

CACHE_VERSION = "1.0"

DEFAULT_CACHE_FILE = "signal_cache.json"


class SignalCache:
    """
    缓存结构：fingerprint → CacheEntry(scene_code, signal_sequence)

    为什么必须存 scene_code：
    同一个信号码在不同场景下映射到不同操作。
    如 "T" 在 S1(医疗) 和 S4(日志) 中操作不同。
    如果只存 signal_sequence，缓存命中时不知道用哪张操作表。
    """

    def __init__(self, cache_file: str | Path = DEFAULT_CACHE_FILE):
        self._in_memory = str(cache_file) == ":memory:"
        self.cache_file = None if self._in_memory else Path(cache_file)
        self.cache: dict[str, dict[str, str]] = {} if self._in_memory else self._load()

    def get(self, fingerprint: str) -> CacheEntry | None:
        """命中返回 CacheEntry，未命中返回 None"""
        entry = self.cache.get(fingerprint)
        if entry is None:
            return None
        return CacheEntry(scene_code=entry["scene_code"], signal_sequence=entry["signal_sequence"])

    def put(self, fingerprint: str, entry: CacheEntry) -> None:
        """写入缓存"""
        self.cache[fingerprint] = {
            "scene_code": entry.scene_code,
            "signal_sequence": entry.signal_sequence,
        }
        self._save()

    def _load(self) -> dict[str, dict[str, str]]:
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("_version") != CACHE_VERSION:
                logger.info("Cache version mismatch, clearing cache")
                return {}
            return data.get("entries", {})
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save(self) -> None:
        if self._in_memory:
            return
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(
                {"_version": CACHE_VERSION, "entries": self.cache},
                f,
                indent=2,
                ensure_ascii=False,
            )
