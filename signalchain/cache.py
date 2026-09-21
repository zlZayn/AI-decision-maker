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


def _code_hash(engine_id: str = "system2") -> str:
    """计算当前代码配置 + 决策引擎的哈希，任一变更都会让缓存自动失效

    engine_id 必须参与哈希：同一个 fingerprint 由 Jev 判和由 LLM 判，
    得到的置信度与来源完全不同。不纳入的话，换引擎之后旧缓存会被静默复用，
    而它的决策来源已经不可追溯 —— 这正好破坏了引入系统一的初衷。
    """
    # 延迟导入，避免循环依赖
    from signalchain.stage2_router import ROUTING_TABLE, SIGNAL_STANDARD_NAMES
    from signalchain.operations.registry import OPERATION_REGISTRY

    h = hashlib.sha256()
    h.update(f"engine={engine_id}".encode())
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

    def __init__(self, cache_file: str | Path = DEFAULT_CACHE_FILE, engine_id: str = "system2"):
        self._in_memory = str(cache_file) == ":memory:"
        self._engine_id = engine_id
        self.cache_file = None if self._in_memory else Path(cache_file)
        self.cache: dict[str, dict[str, str]] = {} if self._in_memory else self._load()

    def get(self, fingerprint: str) -> CacheEntry | None:
        entry = self.cache.get(fingerprint)
        if entry is None:
            return None
        # certainty / engine 是系统一引入后新增的可选字段：老缓存没有也能读
        return CacheEntry(
            scene_code=entry["scene_code"],
            signal_sequence=entry["signal_sequence"],
            certainty=float(entry.get("certainty", 0.0) or 0.0),
            engine=entry.get("engine", "") or "",
        )

    def put(self, fingerprint: str, entry: CacheEntry) -> None:
        payload: dict[str, object] = {
            "scene_code": entry.scene_code,
            "signal_sequence": entry.signal_sequence,
        }
        if entry.engine:
            payload["engine"] = entry.engine
        if entry.certainty:
            payload["certainty"] = entry.certainty
        self.cache[fingerprint] = payload
        self._save()

    def _load(self) -> dict[str, dict[str, str]]:
        expected_hash = _code_hash(self._engine_id)
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
                {"_code_hash": _code_hash(self._engine_id), "entries": self.cache},
                f,
                indent=2,
                ensure_ascii=False,
            )
