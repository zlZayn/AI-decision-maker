"""DeepSeek Tokenizer — 离线计算 token 数

⚠️ **已知缺陷（2026-09-21 实测）：本 tokenizer 对中文返回 0 token。**

- `tokenize("最低")` / `tokenize("性别")` 都返回空列表
- 词表 128000 条里 CJK 词条数为 0，`unk_id` 为 None，`byte_fallback` 为 false
  → 中文字符被静默丢弃，不报错
- 官方完整词表目录 tests/deepseek_tokenizer/deepseek_v4_tokenizer/ 行为完全相同，
  替换词表不能解决

**后果**：中文占比越高，低估越严重。因此本函数

- ❌ 不能用于跨引擎 token 比较（曾据此得出错误的"同口径 4.0× / 13.3×"，已作废）
- ❌ 不能用于中文文本的绝对 token 估算
- ✅ 只可用于纯 ASCII 文本，或作为"哪一份 payload 更长"的粗略排序参考

跨引擎比较请一律使用各引擎 API 自报的 token 数。
"""

from __future__ import annotations

import os

_TOKENIZER = None
_TOKENIZER_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "deepseek_tokenizer", "tokenizer",
)


def _get_tokenizer():
    global _TOKENIZER
    if _TOKENIZER is None:
        import transformers
        _TOKENIZER = transformers.AutoTokenizer.from_pretrained(
            _TOKENIZER_DIR, trust_remote_code=True,
        )
    return _TOKENIZER


def count_tokens(text: str) -> int:
    """计算文本的 token 数

    ⚠️ 对中文返回 0（见模块 docstring）。不要用它做跨引擎比较或中文绝对估算。
    """
    tokenizer = _get_tokenizer()
    return len(tokenizer.encode(text))


def count_tokens_batch(texts: list[str]) -> list[int]:
    """批量计算 token 数"""
    tokenizer = _get_tokenizer()
    return [len(tokenizer.encode(t)) for t in texts]
