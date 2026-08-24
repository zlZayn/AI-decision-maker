# signalchain/ — 数据清洗与分类分析核心

- 职责：两条链路的核心实现（清洗 pipeline + 分类变量分析）
- pipeline.py：SignalChainPipeline，清洗编排（含 cache_file 参数），被根脚本与 e2e 测试依赖
- ai_client.py：DeepSeekV4Client，AI 调用封装（model/api_key/base_url/thinking）
- categorical.py：CategoricalClassifier，分类变量两层 AI 识别
- cache.py：指纹缓存（signal_cache.json），被 pipeline 依赖
- models.py：信号码/场景码常量，被 stage* 依赖
- knowledge.py：字段语义知识库，被 operations/* 依赖
- tokenizer.py：count_tokens 离线 Token 估算，被 run_token_benchmark 依赖
- stage0_profile.py → stage5_execute.py：五阶段清洗链（profile→scene→router→semantic→assemble→execute）
- operations/：base.py 基类 + registry.py 注册表 + 12 个字段操作
- run_categorical_analysis.R：分类链路统计脚本，需 R 环境（见 [../README.md](../README.md)）
- 变更影响路由：改这里 → 同步根 [../AGENTS.md](../AGENTS.md) 快照/坑 + 契约变更写 [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- 规则与约束 → 见 [AGENTS.md](AGENTS.md)