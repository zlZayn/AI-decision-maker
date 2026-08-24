# tests/ — 测试手册

- 运行：`uv run pytest`（项目根执行）
- 映射：test_stage0.py–test_stage5.py → signalchain/stage0_profile.py–stage5_execute.py
- test_operations.py → signalchain/operations/*
- test_pipeline.py / test_cache.py / test_categorical.py → pipeline.py / cache.py / categorical.py
- run_all.py：三段聚合入口（单元 + 两个 e2e，后两者消耗 Token）
- run_e2e_pipeline.py / run_e2e_categorical.py：端到端，需真实 API Key，不默认运行
- run_token_benchmark.py：Token 基准，需 API Key，使用 signalchain.tokenizer
- deepseek_tokenizer/：内嵌 DeepSeek tokenizer 资产，被 signalchain/tokenizer.py 引用，不可删
- 变更影响路由：改用例数 → 同步根 [../AGENTS.md](../AGENTS.md) 验证快照数字
- 规则与约束 → 见 [AGENTS.md](AGENTS.md)