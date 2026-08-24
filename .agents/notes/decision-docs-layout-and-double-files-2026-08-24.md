# 决策：架构文档统一位置与目录双件（2026-08-24）

已实施：docs 结构标准化改造

## 问题
- 根目录有独立 ARCHITECTURE.md，与统一约定（架构文档放 docs/ARCHITECTURE.md）不符
- signalchain/、tests/ 无 AGENTS.md + README.md 双件，Agent 进目录无规则注入、改动无手册兜底
- 无决策记录目录（.agents/notes/）

## 决策
- ARCHITECTURE.md 移到 docs/ARCHITECTURE.md，一级标题改为「SignalChain 架构说明」
- signalchain/、tests/ 建立双件：AGENTS.md 只写规则，README.md 只写职责/导出/变更路由
- 根 AGENTS.md 建立仪表盘（全局规则、常用命令、验证快照、待办、活跃坑、文档地图）
- .agents/notes/ 建立决策记录，README.md 文件树同步修正（docs/ARCHITECTURE.md、.python-version、uv.lock、deepseek_tokenizer/）

## 替代方案（强制）
- 原地保留 ARCHITECTURE.md：根目录不放置独立架构文档是统一约定，指针应由 AGENTS 文档地图承载，故否决
- 只补文档不移动文件：根 ARCHITECTURE.md 与 docs/ 并存造成双 home，链接网会出现两跳歧义，故否决
- 批量重命名违规文件（如 docs/分类变量有序判断.md）：任务边界是只报告不改，重命名会改动引用与历史，故否决（疑问另见交付报告）
- 给 operations/ 也建双件：本次按最小范围只覆盖 signalchain/ 与 tests/，operations/ 留待下次（见交付报告疑问）

## 影响
- 收益：文档统一位置、双件职责分离、链接可校验、无任何功能代码改动
- 代价：移动后旧外部引用需指向 docs/ARCHITECTURE.md（当前全项目 markdown 无此引用，零修复量）