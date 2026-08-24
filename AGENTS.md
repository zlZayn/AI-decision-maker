# SignalChain — 维护索引（仪表盘 + 变更路由）

## 全局规则
- 架构设计：见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 核心模块手册：见 [signalchain/README.md](signalchain/README.md)
- 测试手册：见 [tests/README.md](tests/README.md)
- 决策记录：见 [.agents/notes/](.agents/notes/)
- 用户文档：见 [README.md](README.md) 与 [PROJECT_INTRO.md](PROJECT_INTRO.md)

## 常用命令（可执行规范）
- `uv run pytest` — 单元测试
- `uv run python run_clean.py [名称] [--no-cache]` — 清洗 data/dirty/ → data/clean/
- `uv run python run_categorical.py [--no-cache]` — 分类变量分析（需 R 环境）

## 验证快照（2026-08-24 实测）
- pytest: 137 passed / 0 failed（1 个环境级 .pytest_cache 写入警告）

## 待办
- [ ] .python-version 未纳入 git（git status ??），确认是否跟踪
- [ ] 梳理 docs/ 早期说明（unified_framework_design / 分类变量有序判断）与 ARCHITECTURE 边界

## 活跃坑
- 分类链路依赖 Rscript，未装 R 时 run_categorical.py 失败（见 [README.md](README.md)）
- signalchain 是根目录包（非 src-layout），导入路径以 signalchain.* 开头（见 [signalchain/README.md](signalchain/README.md)）