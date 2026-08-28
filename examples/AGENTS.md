# examples/ — 规则层

继承根规则，见 [../AGENTS.md](../AGENTS.md)。

examples/ 特有约束：
- 示例必须可离线独立运行：AI 调用一律用 MockAIClient，不依赖真实 API Key（见 [README.md](README.md)）
- 示例保持单文件、小体积，不引入新依赖