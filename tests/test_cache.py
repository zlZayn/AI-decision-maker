"""缓存测试"""

import json
import tempfile
from pathlib import Path

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
