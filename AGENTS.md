# SignalChain — 维护索引（仪表盘 + 变更路由）

## 全局规则
- 架构设计：见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 核心模块手册：见 [signalchain/README.md](signalchain/README.md)
- 字段操作手册：见 [signalchain/operations/README.md](signalchain/operations/README.md)
- 系统一设计与实测：见 [signalchain/SYSTEM1.md](signalchain/SYSTEM1.md)
- 测试手册：见 [tests/README.md](tests/README.md)
- 决策记录：见 [.agents/notes/](.agents/notes/)
- 用户文档：见 [README.md](README.md)（根目录唯一门面）
- 示例代码：见 [examples/README.md](examples/README.md)

## 常用命令（可执行规范）
- `uv run pytest` — 单元测试
- `uv run python run_clean.py [名称] [--no-cache]` — 清洗 data/dirty/ → data/clean/（纯系统二）
- `uv run python run_clean.py [名称] --system1` — 纯系统一（Jev），不需要 DeepSeek Key
- `uv run python run_clean.py [名称] --system1 --escalate` — 串联：系统一低置信时交系统二
- `uv run python run_categorical.py [--no-cache] [--system1] [--escalate]` — 分类变量分析（需 R 环境）
- `uv run python run_smoke_jev.py [--offline|--all]` — 系统一（Jev）冒烟：中文样本 / 有序性 / 延迟 / token
- `uv sync --extra system1` — 装系统一依赖（typesafe-sdk）；不装则系统一不可用

## 验证快照（最近一次：2026-09-21）
- pytest: **250 passed / 0 failed**（基线 137 → 系统一接入 +97 → 解耦 +9 → 缓存分区 +7）
- Jev 在线实测（jev-1.13.0）: 场景 3/3、字段码 16/16、有序性 5/5（见 [signalchain/SYSTEM1.md](signalchain/SYSTEM1.md)）
- 纯系统一 vs 纯系统二 对照（2026-09-21）: 清洗链路 3 文件 SHA256 逐字节一致、操作链 18 行一致；分类链路 5/6 一致（data_B_type.json 系统二多判了 id，系统一更合理；report.json 不受影响）；两引擎各自可复现
- token 口径（2026-09-21）: 计费口径（API 自报）清洗 6.6× / 分类 12.7×；"同口径 4.0× / 13.3×"因本地 tokenizer 中文计 0 已作废
- 定位: demo，不上升生产级（见 [.agents/notes/decision-system1-jev-integration-2026-09-21.md](.agents/notes/decision-system1-jev-integration-2026-09-21.md)）
- 缓存实测（2026-09-21，清空缓存后跑两轮）: 清洗链路纯系统一 cold 2.79s / 3 次 Jev → hit 0.31s / **0 次**；纯系统二 cold 6.69s / 6 次 → hit 1.53s / **0 次**；两个命名空间并存且互相切换后仍命中（分区未互相冲掉）；分类链路走文件级缓存（`*_type.json` 按 mtime），4/4 命中

## 待办
- （暂无）

## 活跃坑
- 分类链路依赖 Rscript，未装 R 时 run_categorical.py 失败（见 [README.md](README.md)）
- signalchain 是根目录包（非 src-layout），导入路径以 signalchain.* 开头（见 [signalchain/README.md](signalchain/README.md)）
- config.py 的 SYSTEM2_MODEL 是 deepseek-flash，与 README/客户端默认的 deepseek-v4-flash 不一致（未擅自改）
- 两套系统默认解耦：不传 escalate_to_system2 时 decider 拿不到系统二客户端；纯系统一下低置信字段落 X（不猜不改）
- 决策记录的 verdict 与 escalated 是两件事：verdict=escalate 且 escalated=False 表示"该升级但没升级"
- PyPI 上的 typesafe 是无关库；官方 SDK 是 typesafe-sdk（jev 包要求 Python>=3.14，本项目 3.12 装不了）
- provisional 缓存按"全部写入"处理（demo 要可重复性，不要长期正确性），与 [signalchain/SYSTEM1.md](signalchain/SYSTEM1.md) 早期表述不一致处以实现为准
- 缓存按引擎分区（schema 2）：切引擎只失效对应命名空间；旧格式文件会在首次加载时整体作废一次
- 已知边界（官方明示，未修改）：certainty() 把 noul 与 choice 归一到同一尺度并共用阈值，官方明确两者不可比；大 state 会降准确度，但本项目场景判断依赖字段清单（见 [signalchain/SYSTEM1.md](signalchain/SYSTEM1.md)）
- Jev 单价 $0.042/M 未能在公开文档核实，以 console.typesafe.ai 为准
- ⚠️ signalchain/tokenizer.py 的本地 tokenizer **对中文返回 0 token**（CJK 词条为 0、unk_id 为 None、byte_fallback 为 false）；官方 `deepseek_v4_tokenizer/` 目录行为相同。**不可用于跨引擎 token 比较**，跨引擎一律用 API 自报数（详见 [signalchain/SYSTEM1.md](signalchain/SYSTEM1.md) §13.4.1）