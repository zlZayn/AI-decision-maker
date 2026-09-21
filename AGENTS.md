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
- `uv run python run_smoke_jev.py [--offline|--all]` — 系统一（Jev）冒烟：中文样本 / 有序性 / 延迟 / token
- `uv sync --extra system1` — 装系统一依赖（typesafe-sdk）；不装则自动降级为纯系统二

## 验证快照（2026-08-24 实测）
- pytest: **234 passed / 0 failed**（2026-08-24 基线 137 项 → 系统一接入新增 97 项）
- Jev 在线实测（jev-1.13.0）: 场景 3/3、字段码 16/16、有序性 5/5（见 [docs/SYSTEM1_JEV.md](docs/SYSTEM1_JEV.md)）

## 待办
- [ ] docs/ 归属待厘清：早期说明（unified_framework_design / 分类变量有序判断）与新增 SYSTEM1_JEV 均非 ARCHITECTURE.md，按文档档位应进 .agents/notes/ 或模块手册
- [ ] ARCHITECTURE.md 尚未描述系统一链路（内容在 [docs/SYSTEM1_JEV.md](docs/SYSTEM1_JEV.md)，边界待划）
- [ ] 配置契约已变（API_KEY/API_URL/MODEL → SYSTEM2_*，新增 SYSTEM1_*），ARCHITECTURE.md 未同步
- [ ] [README.md](README.md) 文件结构清单与实际不符（列了不存在的 signalchain/run_categorical_analysis.R）

## 活跃坑
- 分类链路依赖 Rscript，未装 R 时 run_categorical.py 失败（见 [README.md](README.md)）
- signalchain 是根目录包（非 src-layout），导入路径以 signalchain.* 开头（见 [signalchain/README.md](signalchain/README.md)）
- config.py 的 SYSTEM2_MODEL 是 deepseek-flash，与 README/客户端默认的 deepseek-v4-flash 不一致（未擅自改）
- 系统一接入后 signal_cache.json 会失效一次（引擎标识进了缓存哈希），首次运行重新决策是预期行为
- PyPI 上的 typesafe 是无关库；官方 SDK 是 typesafe-sdk（jev 包要求 Python>=3.14，本项目 3.12 装不了）