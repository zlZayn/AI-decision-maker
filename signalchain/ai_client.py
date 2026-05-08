"""AI 客户端封装 — 统一调用接口"""

from __future__ import annotations

import logging
from typing import Protocol

logger = logging.getLogger(__name__)


class AIClient(Protocol):
    """AI 客户端协议，只要实现 call 方法即可"""

    def call(self, prompt: str) -> str:
        """调用 AI，返回文本响应"""
        ...


class OpenAIClient:
    """基于 OpenAI API 的客户端实现"""

    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_key: str | None = None,
        base_url: str | None = None,
    ):
        try:
            import openai
        except ImportError:
            raise ImportError("请安装 openai: pip install openai")

        self.model = model
        self.client = openai.OpenAI(api_key=api_key, base_url=base_url)

    def call(self, prompt: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=32,
        )
        return response.choices[0].message.content.strip()


class MockAIClient:
    """用于测试的模拟 AI 客户端"""

    def __init__(self, responses: dict[str, str] | None = None):
        self.responses = responses or {}
        self.call_log: list[str] = []

    def call(self, prompt: str) -> str:
        self.call_log.append(prompt)
        # 尝试匹配预定义响应，否则默认返回 S0
        for key, value in self.responses.items():
            if key in prompt:
                return value
        return "S0"
