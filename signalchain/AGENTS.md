# signalchain/ — 规则层

继承根规则，见 [../AGENTS.md](../AGENTS.md)。

signalchain/ 特有约束：
- 新增字段类型需同步 4 处：models.py 信号码、knowledge.py 知识、operations/ 新模块、operations/registry.py 注册（见 [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) 定制开发指南）
- 所有操作必须继承 operations/base.py 的 Operation 接口（1:1 返回 Series；1:N 设 splits_column）
- 文件职责与关键导出归 [README.md](README.md)，设计理由归架构文档
- 改 stage0–stage5 任一段后必跑对应 tests/test_stage*.py（见 [../tests/README.md](../tests/README.md)）