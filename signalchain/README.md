# signalchain/ — 数据清洗与分类分析核心

- 职责：两条链路的核心实现（清洗 pipeline + 分类变量分析）
- __init__.py：对外导出 SignalChainPipeline 与核心数据类
- pipeline.py：SignalChainPipeline，清洗编排（含 cache_file / evaluator / escalate_to_system2 参数），被根脚本与 e2e 测试依赖
- ai_client.py：DeepSeekV4Client（**系统二**），AI 调用封装（model/api_key/base_url/thinking）
- system1.py：**系统一**接入层 —— Evaluator 协议、JevEvaluator、MockEvaluator、certainty 数学、GatePolicy
- fastpath.py：清洗链路系统一快通道（一次请求定场景+字段，低置信批量升级系统二）
- categorical_system1.py：分类链路系统一通道（noul 筛分类变量 + noul 判有序 + score 定顺序）
- categorical.py：CategoricalClassifier，分类变量两层 AI 识别
- cache.py：指纹缓存（signal_cache.json），被 pipeline 依赖
- models.py：信号码/场景码常量，被 stage* 依赖
- knowledge.py：字段语义知识库，被 operations/* 依赖
- stage0_profile.py → stage5_execute.py：五阶段清洗链（profile→scene→router→semantic→assemble→execute）
- operations/：base.py 基类 + registry.py 注册表 + 12 个字段操作（文件清单与信号码对应见 [operations/README.md](operations/README.md)）
- run_categorical_analysis.R：分类链路统计脚本，需 R 环境（见 [../README.md](../README.md)）
- [SYSTEM1.md](SYSTEM1.md)：系统一的完整说明（为何用 Jev、契约、门控、成本口径、实测与已知边界），
  按 §0–§13 编号，外部文档按此引用（如 §13.4）
- 变更影响路由：改这里 → 同步根 [../AGENTS.md](../AGENTS.md) 快照/坑 + 契约变更写 [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- 规则与约束 → 见 [AGENTS.md](AGENTS.md)