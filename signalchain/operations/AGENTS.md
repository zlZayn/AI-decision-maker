# operations/ — 规则层

继承根规则，见 [../../AGENTS.md](../../AGENTS.md)。

operations/ 特有约束：
- 所有操作必须继承 [base.py](base.py) 的 Operation：1:1 返回 Series，1:N 设 `splits_column = True` 并返回 DataFrame
- 新增操作必须在 [registry.py](registry.py) 注册；漏注册时 stage4 查不到会静默回落 pass_through，不报错
- 操作只做确定性执行，不做语义推断；语义知识（取值表、映射、正则）一律放 [../knowledge.py](../knowledge.py)
- 文件清单、操作名与信号码的对应关系 → 见 [README.md](README.md)
- 设计理由（为什么 AI 只传码、本地执行）→ 见 [../../docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)
