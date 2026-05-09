"""AI 客户端封装 — 统一调用接口"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol, Any

logger = logging.getLogger(__name__)


@dataclass
class TokenUsage:
    """累计 token 消耗"""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int = 0
    total_calls: int = 0

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def add(self, response_usage: Any) -> None:
        """从 API response.usage 累加"""
        self.prompt_tokens += response_usage.prompt_tokens
        self.completion_tokens += response_usage.completion_tokens
        self.total_calls += 1
        details = getattr(response_usage, "completion_tokens_details", None)
        if details:
            self.reasoning_tokens += getattr(details, "reasoning_tokens", 0) or 0

    def summary(self) -> str:
        return (
            f"calls={self.total_calls} | "
            f"prompt={self.prompt_tokens} | "
            f"completion={self.completion_tokens} | "
            f"reasoning={self.reasoning_tokens} | "
            f"total={self.total_tokens}"
        )


class AIClient(Protocol):
    """AI 客户端协议，只要实现 call 方法即可"""

    usage: TokenUsage

    def call(self, prompt: str) -> str:
        """调用 AI，返回文本响应"""
        ...


class OpenAIClient:
    """基于 OpenAI API 的客户端实现

    支持 deepseek-chat、deepseek-v4-flash 等模型。
    项目默认使用 DeepSeekV4Client（deepseek-v4-flash + 思考关闭）。
    """

    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_key: str | None = None,
        base_url: str | None = None,
        max_tokens: int = 32,
        extra_body: dict[str, Any] | None = None,
    ):
        try:
            import openai
        except ImportError:
            raise ImportError("请安装 openai: pip install openai")

        self.model = model
        self.max_tokens = max_tokens
        self.extra_body = extra_body or {}
        self.client = openai.OpenAI(api_key=api_key, base_url=base_url)
        self.usage = TokenUsage()

    def call(self, prompt: str) -> str:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": self.max_tokens,
        }
        if self.extra_body:
            kwargs["extra_body"] = self.extra_body

        try:
            response = self.client.chat.completions.create(**kwargs)
            if response.usage:
                self.usage.add(response.usage)
            content = response.choices[0].message.content
            return content.strip() if content else ""
        except Exception as e:
            logger.error(f"API call failed: {e}")
            raise


class DeepSeekV4Client(OpenAIClient):
    """DeepSeek V4 系列专用客户端（推理模型）

    模型默认开启思考模式，会消耗 reasoning_tokens。
    本项目默认 thinking=False，适合信号链等短输出场景，省约50%费用。
    如需开启，设 thinking=True 并可通过 thinking_level 控制强度。
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str = "deepseek-v4-flash",
        max_tokens: int = 256,
        thinking: bool = False,
        thinking_level: str = "high",
    ):
        if thinking:
            extra_body = {
                "thinking": {
                    "type": "enabled",
                    "level": thinking_level
                }
            }
        else:
            extra_body = {
                "thinking": {"type": "disabled"}
            }

        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://api.deepseek.com",
            max_tokens=max_tokens,
            extra_body=extra_body,
        )
        self.thinking = thinking
        logger.info(
            f"DeepSeekV4Client initialized: model={model}, "
            f"thinking={thinking}, max_tokens={max_tokens}"
        )


class MockAIClient:
    """用于测试的模拟 AI 客户端"""

    def __init__(self, responses: dict[str, str] | None = None):
        self.responses = responses or {}
        self.call_log: list[str] = []
        self.usage = TokenUsage()

    def call(self, prompt: str) -> str:
        self.call_log.append(prompt)
        for key, value in self.responses.items():
            if key in prompt:
                return value
        return "S0"
