# SignalChain — 维护索引（仪表盘 + 变更路由）

## 全局规则
- 架构设计：见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 核心模块手册：见 [signalchain/README.md](signalchain/README.md)
- 测试手册：见 [tests/README.md](tests/README.md)
- 决策记录：见 [.agents/notes/](.agents/notes/)
- 用户文档：见 [README.md](README.md) 与 [PROJECT_INTRO.md](PROJECT_INTRO.md)

## 常用命令（可执行规范）
- `uv run pytest` — 单元测试
- `uv run python run_clean.py [名称] [--no-cache]` — 清洗 data/dirty/ → data/clean/（纯系统二）
- `uv run python run_clean.py [名称] --system1` — 纯系统一（Jev），不需要 DeepSeek Key
- `uv run python run_clean.py [名称] --system1 --escalate` — 串联：系统一低置信时交系统二
- `uv run python run_categorical.py [--no-cache] [--system1] [--escalate]` — 分类变量分析（需 R 环境）
- `uv run python run_smoke_jev.py [--offline|--all]` — 系统一（Jev）冒烟：中文样本 / 有序性 / 延迟 / token
- `uv sync --extra system1` — 装系统一依赖（typesafe-sdk）；不装则系统一不可用

## 验证快照（2026-08-24 实测）
- pytest: **250 passed / 0 failed**（基线 137 → 系统一接入 +97 → 解耦 +9 → 缓存分区 +7）
- Jev 在线实测（jev-1.13.0）: 场景 3/3、字段码 16/16、有序性 5/5（见 [docs/SYSTEM1_JEV.md](docs/SYSTEM1_JEV.md)）
- 纯系统一 vs 纯系统二 对照（2026-09-21）: 清洗链路 3 文件 SHA256 逐字节一致、操作链 18 行一致；分类链路 5 个 JSON 一致
- 定位: demo，不上升生产级（见 [.agents/notes/decision-system1-jev-integration-2026-09-21.md](.agents/notes/decision-system1-jev-integration-2026-09-21.md)）

## 待办
- [ ] docs/ 归属待厘清：早期说明（unified_framework_design / 分类变量有序判断）与新增 SYSTEM1_JEV 均非 ARCHITECTURE.md，按文档档位应进 .agents/notes/ 或模块手册
- [ ] [README.md](README.md) 文件结构清单与实际不符（列了不存在的 signalchain/run_categorical_analysis.R）

## 活跃坑
- 分类链路依赖 Rscript，未装 R 时 run_categorical.py 失败（见 [README.md](README.md)）
- signalchain 是根目录包（非 src-layout），导入路径以 signalchain.* 开头（见 [signalchain/README.md](signalchain/README.md)）
- config.py 的 SYSTEM2_MODEL 是 deepseek-flash，与 README/客户端默认的 deepseek-v4-flash 不一致（未擅自改）
- 两套系统默认解耦：不传 escalate_to_system2 时 decider 拿不到系统二客户端；纯系统一下低置信字段落 X（不猜不改）
- 决策记录的 verdict 与 escalated 是两件事：verdict=escalate 且 escalated=False 表示"该升级但没升级"
- PyPI 上的 typesafe 是无关库；官方 SDK 是 typesafe-sdk（jev 包要求 Python>=3.14，本项目 3.12 装不了）
- provisional 缓存按"全部写入"处理（demo 要可重复性，不要长期正确性），与 [docs/SYSTEM1_JEV.md](docs/SYSTEM1_JEV.md) 早期表述不一致处以实现为准
- 缓存按引擎分区（schema 2）：切引擎只失效对应命名空间；旧格式文件会在首次加载时整体作废一次