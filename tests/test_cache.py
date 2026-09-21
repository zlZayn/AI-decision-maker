"""缓存测试"""

import json
import os
import tempfile

import pytest

from signalchain.cache import SignalCache
from signalchain.models import CacheEntry


class TestSignalCache:
    """测试 SignalCache"""

    def test_put_and_get(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache = SignalCache(f.name)

            entry = CacheEntry(scene_code="S1", signal_sequence="IGADN")
            cache.put("fingerprint123", entry)

            result = cache.get("fingerprint123")
            assert result is not None
            assert result.scene_code == "S1"
            assert result.signal_sequence == "IGADN"

    def test_cache_miss(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache = SignalCache(f.name)
            result = cache.get("nonexistent")
            assert result is None

    def test_cache_persistence(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

            # 写入
            cache1 = SignalCache(path)
            cache1.put("fp1", CacheEntry("S1", "IGADN"))

            # 重新加载
            cache2 = SignalCache(path)
            result = cache2.get("fp1")
            assert result is not None
            assert result.scene_code == "S1"

    def test_version_mismatch_clears_cache(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            # 写入一个版本不匹配的缓存文件
            json.dump({"_version": "0.0", "entries": {"fp1": {"scene_code": "S1", "signal_sequence": "IGADN"}}}, f)
            path = f.name

        cache = SignalCache(path)
        result = cache.get("fp1")
        assert result is None  # 版本不匹配，缓存清空

    def test_corrupted_file(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            f.write("not valid json{{{")
            path = f.name

        cache = SignalCache(path)
        result = cache.get("any")
        assert result is None  # 文件损坏，返回空缓存


def _read_raw(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


class TestNamespaces:
    """两套系统的缓存必须互相独立

    早期实现只维护一份全局配置指纹：一个引擎读缓存时发现不匹配就整体丢弃，
    保存时又只写自己那部分 —— 于是后跑的引擎会把前一个引擎的条目冲掉。
    """

    def test_namespaces_do_not_clobber(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")

            system2 = SignalCache(path, namespace="system2")
            system2.put("fp_shared", CacheEntry("S1", "IGADN"))

            # 另一个引擎读同一份文件：看不到对方的分区
            jev = SignalCache(path, namespace="jev:jev-latest")
            assert jev.get("fp_shared") is None
            jev.put("fp_shared", CacheEntry("S3", "IGAEP"))

            # 关键：回到第一个引擎，它的条目必须原样还在
            assert SignalCache(path, namespace="system2").get("fp_shared").signal_sequence == "IGADN"
            assert SignalCache(path, namespace="jev:jev-latest").get("fp_shared").signal_sequence == "IGAEP"
            assert jev.namespaces() == ["jev:jev-latest", "system2"]

    def test_items_exposes_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            cache = SignalCache(path, namespace="jev:jev-latest")
            cache.put("fp1", CacheEntry("S1", "IGADN", certainty=0.99, engine="system1"))

            reloaded = SignalCache(path, namespace="jev:jev-latest")
            assert len(reloaded) == 1
            assert "fp1" in reloaded
            fingerprint, entry = reloaded.items()[0]
            assert fingerprint == "fp1"
            assert entry.engine == "system1"
            assert entry.certainty == pytest.approx(0.99)

    def test_stale_namespace_is_dropped_others_kept(self):
        """一个引擎的配置指纹失效，只丢它的分区，别人的保留"""
        from signalchain.cache import SCHEMA_VERSION, config_fingerprint

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "schema": SCHEMA_VERSION,
                        "namespaces": {
                            "system2": {
                                "fingerprint": "stale-fingerprint",
                                "entries": {
                                    "fp_old": {"scene_code": "S1", "signal_sequence": "IGADN"}
                                },
                            },
                            "jev:jev-latest": {
                                "fingerprint": config_fingerprint("jev:jev-latest"),
                                "entries": {
                                    "fp_new": {"scene_code": "S3", "signal_sequence": "IGAEP"}
                                },
                            },
                        },
                    },
                    handle,
                )

            cache = SignalCache(path, namespace="jev:jev-latest")
            assert cache.get("fp_new") is not None  # 匹配的分区保留
            cache.put("fp_extra", CacheEntry("S2", "IMT"))

            namespaces = _read_raw(path)["namespaces"]
            assert set(namespaces) == {"jev:jev-latest"}  # 失效分区被剪掉
            assert set(namespaces["jev:jev-latest"]["entries"]) == {"fp_new", "fp_extra"}

    def test_unknown_schema_is_discarded(self):
        """旧格式没有 schema 字段：条目归属不可考，整体作废且不报错"""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "_code_hash": "deadbeef",
                        "entries": {"fp1": {"scene_code": "S1", "signal_sequence": "IGADN"}},
                    },
                    handle,
                )
            cache = SignalCache(path, namespace="system2")
            assert cache.get("fp1") is None
            assert len(cache) == 0

    def test_malformed_entry_is_a_miss(self):
        """条目结构非法按未命中处理，不抛异常"""
        from signalchain.cache import SCHEMA_VERSION, config_fingerprint

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "schema": SCHEMA_VERSION,
                        "namespaces": {
                            "system2": {
                                "fingerprint": config_fingerprint("system2"),
                                "entries": {
                                    "bad": {"scene_code": "S1"},  # 缺 signal_sequence
                                    "worse": "not-a-dict",
                                },
                            }
                        },
                    },
                    handle,
                )
            cache = SignalCache(path, namespace="system2")
            assert cache.get("bad") is None
            assert cache.get("worse") is None
            assert cache.items() == []

    def test_clear_only_touches_current_namespace(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.json")
            SignalCache(path, namespace="system2").put("fp1", CacheEntry("S1", "IGADN"))
            jev = SignalCache(path, namespace="jev:jev-latest")
            jev.put("fp2", CacheEntry("S3", "IGAEP"))

            jev.clear()
            assert len(jev) == 0
            assert SignalCache(path, namespace="system2").get("fp1") is not None

    def test_memory_mode_is_isolated(self):
        cache = SignalCache(":memory:", namespace="system2")
        cache.put("fp1", CacheEntry("S1", "IGADN"))
        assert cache.get("fp1") is not None
        assert cache.namespaces() == ["system2"]
