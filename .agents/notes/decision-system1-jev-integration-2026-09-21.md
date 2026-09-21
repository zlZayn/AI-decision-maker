# 决策：系统一（Jev）接入（2026-09-21）

已实施：清洗链路与分类链路的闭集判断改由 TypeSafe System One（Jev）承担，系统二负责升级裁决

## 问题
- 五处判断用系统二模拟系统一：场景码（[stage1_scene.py](../../signalchain/stage1_scene.py)）、字段码定长字符串（[stage3_semantic.py](../../signalchain/stage3_semantic.py)）、分类变量筛选与有序性判断（[categorical.py](../../signalchain/categorical.py)）
- 代价有三：字段错位（位置即语义，多一个字符全体后移）、静默失败（非法场景码静默回落 S0、非法信号码静默替换 X）、零置信度（只能整体接受或整体回退）
- 有序性判断写成文本协议（`字段名:值1>值2`），正确性取决于模型是否记得用 > 与中文冒号
- 官方明示 Jev 中文能力较弱，而本项目字段名与样本值重度中文，接入前风险未知

## 决策
- 新增 `Evaluator` 协议作为系统一/系统二分界线；`JevEvaluator`（官方 typesafe-sdk）与 `MockEvaluator`（离线）两种实现
- 清洗链路合并为一次请求：1 个场景 choice + N 个字段 choice，本地按场景 valid_codes 条件化（[fastpath.py](../../signalchain/fastpath.py)）
- 分类链路：noul 筛分类变量 + noul 判有序性 + score 定顺序，同一次请求发出（[categorical_system1.py](../../signalchain/categorical_system1.py)）
- 门控两档：certainty ≥ 0.80 采用，< 0.55 升级系统二，中间带采用但标记 provisional；低置信字段批量升级（一次调用处理全部）
- 置信度统一入口 `certainty()`：choice/score 用官方公式 (n×peak−1)/(n−1) 现算（条件化后选项数变化，引擎返回值失效），noul 用 |2p−1|
- criteria 默认只给标签；缓存哈希纳入引擎标识，条目记录 certainty 与 engine
- typesafe-sdk 放可选依赖组，惰性导入；未安装/无 Key/网络故障一律降级系统二

## 替代方案（强制）
- 继续用 LLM 输出约束字符串再解析修复：错位与静默回落是协议固有缺陷，无法靠调 prompt 消除，否决
- 手写 httpx 调 /v1/systemone 而不上官方 SDK：SDK 自带 RetryPolicy（tenacity + Retry-After），自己写重试是重复实现，否决
- PyPI 的 typesafe 包：是无关的装饰器库（formal type asserting decorators），装错即灾难，否决
- PyPI 的 jev 包：官方装饰器层，但 requires_python >= 3.14，本项目 3.12.10 装不了，否决
- 两套系统各写一套 prompt 与校验：升级路径直接复用 stage2 模板与 stage3 校验器（把不确定字段装成子 DataProfile），另写一套即双份维护，否决
- 只靠 score 离散度判有序：实测把性别（男 0.72 / 女 3.61，离散度 2.89）误判为有序，否决
- 有序性 noul 与逐值 score 分两次请求：官方 fan-out 明确同请求并行更省，分开只多一次往返，否决
- typesafe-sdk 进主 dependencies：未装时项目整体不可用，与"永不因 Jev 失败"冲突，否决
- 配置只加 SYSTEM1_* 而保留 API_KEY/API_URL/MODEL：两套系统命名一明确一笼统，无法从名字看出归属，否决
- 用 jieba 等本地词表预判中文语义替代系统一：需为 13 个信号码各写规则，等于把 AI 判断硬编码，维护成本高于收益，否决

## 影响
- 收益：清洗链路网络往返 2 次 → 1 次，实测单次 0.24–0.43s；幻觉码不再静默降级而是触发升级；每个字段产出概率分布与一条 DecisionRecord
- 中文风险实测未出现：场景 3/3、字段码 16/16（含 `三十`、`约25`、`Male）`、`" ¥ 1，000.50 "`）
- 有序性实测 5/5：真有序 P(有序)=0.980，性别/血型 P(有序)=0.040
- 代价：输入 token 约 2335（同一数据集原系统二约 75），已通过短判据降到 2335，进一步压缩空间有限
- 代价：引擎标识进缓存哈希，升级后 `signal_cache.json`（gitignored）失效一次，首次运行重新决策
- 代价：配置项改名（API_KEY/API_URL/MODEL → SYSTEM2_*）对既有本地脚本是破坏性的，5 个调用方已在同一次改动内同步
- 未验证：升级分支与 two_pass 严格模式未能在线触发（真实引擎一直给出高置信答案），仅由离线用例覆盖

## demo 范围（2026-09-21 定）

- 定位：demo，不上升生产级，不追加工程化投入
- provisional 缓存：不修，全部写入。测试对照要的是可重复性，不是长期正确性
- 串联与升级分支：不验证。本轮只做纯系统一 vs 纯系统二的对照
- 已知但不修：两套引擎交替运行会互相冲掉对方缓存（`_code_hash` 全局比对，不匹配即整体丢弃整个 entries）

## 对照结论（2026-09-21 实测）

同一份 data/dirty/（finance / medical / user 三文件），先清空 data/clean/ 与缓存，两个模式各跑一次：

| 项 | 纯系统二 | 纯系统一 |
| --- | --- | --- |
| 调用 | 6 次 LLM（每文件 2 次） | 3 次 Jev（每文件 1 次），系统二 0 次 |
| 耗时 | 7.63s | 4.80s |
| 输入 token | 999 | 6506 |
| 输出 token | 20 | 1598（TypeSafe 不计费） |
| 改动行数 | 114 | 114 |

- 三个 `*_clean.csv` **SHA256 逐字节一致**
- 操作链（字段 × 操作 × 改动数 × 错误数，18 行）完全一致
- 分类链路同法对照：`report.json` 与 4 个 `*_type.json` 一致，仅 `report.xlsx` 差 1 字节（xlsx 内嵌时间戳）

两条链路互为对照，结论一致：**纯系统一在本次数据上复现了纯系统二的清洗结果**。
