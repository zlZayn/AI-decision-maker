# tests/ — 测试手册

- 运行：`uv run pytest`（项目根执行）
- 映射：test_stage0.py–test_stage5.py → signalchain/stage0_profile.py–stage5_execute.py
- test_operations.py → signalchain/operations/*
- test_pipeline.py / test_cache.py / test_categorical.py → pipeline.py / cache.py / categorical.py
- test_system1.py → signalchain/system1.py（置信度数学、条件化、异常归类、问题原语）
- test_fastpath.py → signalchain/fastpath.py（门控、批量升级、与系统二路径结果逐帧一致）
- test_categorical_system1.py → signalchain/categorical_system1.py（noul 筛选、有序性门、假阳性回归）
- run_unit.py：只跑单元测试（不消耗 Token）
- run_all.py：三段聚合入口（单元 + 两个 e2e，后两者消耗 Token）
- run_e2e_pipeline.py / run_e2e_categorical.py：端到端，需真实 API Key，不默认运行
- run_token_benchmark.py：Token 基准，需 API Key，使用 signalchain.tokenizer
- ../run_smoke_jev.py：系统一冒烟（在线需 SYSTEM1_API_KEY；--offline 用 MockEvaluator 不联网）
- deepseek_tokenizer/：内嵌 DeepSeek tokenizer 资产，被 signalchain/tokenizer.py 引用，不可删
  - ⚠️ 该词表**对中文返回 0 token**（CJK 词条为 0、unk_id 为 None）；
    `deepseek_v4_tokenizer/` 子目录行为相同。**不可用于跨引擎 token 比较**，
    详见 signalchain/tokenizer.py 的模块说明与 [../signalchain/SYSTEM1.md](../signalchain/SYSTEM1.md) §13.4.1
- 变更影响路由：改用例数 → 同步根 [../AGENTS.md](../AGENTS.md) 验证快照数字
- 规则与约束 → 见 [AGENTS.md](AGENTS.md)