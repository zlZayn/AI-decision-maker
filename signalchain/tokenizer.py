"""DeepSeek Tokenizer — 离线计算 token 数"""

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
    """计算文本的 token 数"""
    tokenizer = _get_tokenizer()
    return len(tokenizer.encode(text))


def count_tokens_batch(texts: list[str]) -> list[int]:
    """批量计算 token 数"""
    tokenizer = _get_tokenizer()
    return [len(tokenizer.encode(t)) for t in texts]
