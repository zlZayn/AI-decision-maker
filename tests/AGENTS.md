# tests/ — 规则层

继承根规则，见 [../AGENTS.md](../AGENTS.md)。

tests/ 特有约束：
- pytest 只收集 test_*.py；run_*.py 是手动脚本（e2e/基准），依赖真实 API Key 与网络，不进 pytest
- 新增用例命名 test_<模块>.py，与 signalchain 对应模块同名
- 测试不调用真实 AI：现有 test_*.py 全部离线（mock），保持这一约定
- 文件映射与特殊说明 → [README.md](README.md)