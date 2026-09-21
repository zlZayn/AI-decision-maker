# operations/ — 字段操作手册

- 职责：12 个本地字段操作 + 抽象基类 + 注册表。每个操作对应一个字段信号码
- 契约：一律继承 [base.py](base.py) 的 `Operation`；1:1 返回 `Series`，1:N 设 `splits_column = True` 并返回 `DataFrame`
- [registry.py](registry.py)：操作名 → 实例。stage4 按信号码查表取实例执行

| 模块 | 操作名 | 信号码 | 返回 |
| --- | --- | --- | --- |
| age.py | extract_age | A | 1:1 |
| coordinates.py | validate_coordinates | R | 1:1 |
| currency.py | split_currency | M | 1:N（amount_value / amount_currency） |
| datetime.py | parse_datetime | T | 1:1 |
| department.py | normalize_department | D | 1:1 |
| drug_name.py | normalize_drug_name | N | 1:1 |
| email.py | validate_email | E | 1:1 |
| gender.py | normalize_gender | G | 1:1 |
| icd10.py | validate_icd10 | C | 1:1 |
| log_level.py | normalize_log_level | L | 1:1 |
| pass_through.py | pass_through | I / X | 1:1 |
| phone.py | validate_phone | P | 1:1 |
| base.py | — | — | `Operation` 抽象基类 |

- 语义数据不在本目录：[../knowledge.py](../knowledge.py) 提供性别取值表、科室映射、邮箱/手机正则、日志级别映射等，操作只负责执行
- 新增字段类型要同步 4 处：`models.py` 加信号码 → `knowledge.py` 加语义 → 本目录加操作模块 → `registry.py` 注册；另外可在 `stage2_router.FIELD_NAME_HINTS` 加字段名预判（可选）
- 变更影响路由：改操作 → 跑 [../../tests/test_operations.py](../../tests/test_operations.py)；改 Operation 契约 → 同步 [../../docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)
- 使用约束与工作偏好 → 见 [AGENTS.md](AGENTS.md)
