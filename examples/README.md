# examples/ — 示例代码索引

- 职责：演示 SignalChain 数据清洗链路的正确用法，可直接复制进项目脚本
- demo.py：三个示例（医疗数据、本地模式、用户数据），运行 `uv run python examples/demo.py`
- 依赖：pandas + signalchain（MockAIClient，无真实 API 调用，离线可跑）
- 变更影响路由：改示例 → 同步根 [../AGENTS.md](../AGENTS.md) 待办/坑
- 规则与约束 → 见 [AGENTS.md](AGENTS.md)