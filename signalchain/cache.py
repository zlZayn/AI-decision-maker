"""数据指纹缓存 —— 一个文件，按决策引擎分区

为什么要分区
------------
系统一（Jev）与系统二（LLM）对同一个 fingerprint 会给出不同的决策，来源与置信度都不同，
两者的缓存必须各自独立。早期实现只维护一份全局配置指纹：加载时哈希不匹配就整体丢弃，
保存时又只写自己那部分 —— 于是交替运行会把对方的分区冲掉。

磁盘结构（SCHEMA_VERSION = 2）
------------------------------

    {
      "schema": 2,
      "namespaces": {
        "<决策引擎标识>": {
          "fingerprint": "<16 hex>",
          "entries": {"<数据指纹>": {"scene_code": "...", "signal_sequence": "..."}}
        }
      }
    }

失效规则：加载时逐命名空间比对配置指纹，不匹配的丢弃、匹配的保留。
写入时只更新本命名空间的条目，其余命名空间原样写回。
清空全部缓存仍然是删除 signal_cache.json 一个文件。
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from signalchain.models import CacheEntry

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2
DEFAULT_CACHE_FILE = "signal_cache.json"
DEFAULT_NAMESPACE = "system2"


def config_fingerprint(namespace: str = DEFAULT_NAMESPACE) -> str:
    """当前代码配置 + 命名空间的指纹

    配置或引擎任一变化都会让**该命名空间**的缓存失效。
    namespace（决策引擎标识）必须参与：同一个数据指纹由 Jev 判和由 LLM 判，
    来源与置信度完全不同，混用会让决策不可追溯。

    延迟导入，避免与 stage2_router / operations 形成循环依赖。
    """
    from signalchain.operations.registry import OPERATION_REGISTRY
    from signalchain.stage2_router import ROUTING_TABLE, SIGNAL_STANDARD_NAMES

    parts = [f"namespace={namespace}"]
    for code, scene in sorted(ROUTING_TABLE.items()):
        parts.append(
            f"{code}|{scene.scene_name}|"
            f"{''.join(sorted(scene.valid_codes))}|{sorted(scene.operations.items())}"
        )
    parts.append(str(sorted(SIGNAL_STANDARD_NAMES.items())))
    parts.append(str(sorted(OPERATION_REGISTRY)))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:16]


@dataclass
class _Namespace:
    """一个决策引擎的缓存分区"""

    fingerprint: str
    entries: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {"fingerprint": self.fingerprint, "entries": self.entries}

    @classmethod
    def from_payload(cls, payload: object, expected: str, name: str) -> "_Namespace | None":
        """还原一个分区；结构非法或配置指纹不匹配时返回 None"""
        if not isinstance(payload, dict):
            return None
        entries = payload.get("entries")
        if not isinstance(entries, dict):
            return None
        if payload.get("fingerprint") != expected:
            logger.info(f"命名空间 {name} 的缓存已失效（配置或引擎变更）")
            return None
        return cls(fingerprint=expected, entries=dict(entries))


class SignalCache:
    """按决策引擎分区的指纹缓存

        cache = SignalCache("signal_cache.json", namespace=engine_id)
        cache.put(fingerprint, CacheEntry("S1", "IGADN", certainty=1.0, engine="system1"))
        entry = cache.get(fingerprint)
    """

    def __init__(
        self,
        cache_file: str | Path = DEFAULT_CACHE_FILE,
        namespace: str = DEFAULT_NAMESPACE,
    ):
        self._namespace = namespace
        self._path = None if str(cache_file) == ":memory:" else Path(cache_file)
        self._fingerprint = config_fingerprint(namespace)
        self._namespaces = self._read()
        if namespace not in self._namespaces:
            # 新引擎首次使用，或本命名空间的分区刚刚失效
            self._namespaces[namespace] = _Namespace(self._fingerprint)

    # ---- 公开 API ----

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def path(self) -> Path | None:
        return self._path

    def get(self, fingerprint: str) -> CacheEntry | None:
        return CacheEntry.from_payload(self._entries.get(fingerprint))

    def put(self, fingerprint: str, entry: CacheEntry) -> None:
        self._entries[fingerprint] = entry.to_payload()
        self._write()

    def clear(self) -> None:
        """只清空本命名空间"""
        self._entries.clear()
        self._write()

    def items(self) -> list[tuple[str, CacheEntry]]:
        """本命名空间的全部条目，排查与展示用"""
        restored = (
            (fingerprint, CacheEntry.from_payload(payload))
            for fingerprint, payload in self._entries.items()
        )
        return [(fingerprint, entry) for fingerprint, entry in restored if entry is not None]

    def namespaces(self) -> list[str]:
        """缓存文件里现有哪些引擎的分区"""
        return sorted(self._namespaces)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, fingerprint: object) -> bool:
        return fingerprint in self._entries

    # ---- 内部 ----

    @property
    def _entries(self) -> dict[str, dict[str, Any]]:
        return self._namespaces[self._namespace].entries

    def _read(self) -> dict[str, _Namespace]:
        if self._path is None:
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

        if not isinstance(data, dict) or data.get("schema") != SCHEMA_VERSION:
            # 旧格式或损坏：条目无法归属到引擎，只能整体作废（一次性损失）
            logger.info(f"缓存不是 schema {SCHEMA_VERSION}，整体作废")
            return {}

        raw = data.get("namespaces")
        if not isinstance(raw, dict):
            return {}

        namespaces: dict[str, _Namespace] = {}
        for name, payload in raw.items():
            if not isinstance(name, str):
                continue
            restored = _Namespace.from_payload(payload, config_fingerprint(name), name)
            if restored is not None:
                namespaces[name] = restored
        return namespaces

    def _write(self) -> None:
        if self._path is None:
            return
        payload = {
            "schema": SCHEMA_VERSION,
            "namespaces": {
                name: namespace.to_payload()
                for name, namespace in self._namespaces.items()
            },
        }
        with open(self._path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
