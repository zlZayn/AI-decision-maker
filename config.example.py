"""SignalChain 配置模板

用法：复制本文件为 config.py，填入真实值。

  copy config.example.py config.py

注意：
- config.py 已加入 .gitignore，真实密钥不会进版本库；本文件只放占位符。
- 系统一的 Key 也可走环境变量 TYPESAFE_API_KEY。
"""

# ============================================================
# 系统二 —— 通用大语言模型（慢、审慎、可生成文本）
#
# 用途：开放式判断，以及系统一"置信度不足"时的升级裁决。
# 代价：输入+输出都计费；单次 0.5~2s；输出需要格式校验。
# ============================================================
SYSTEM2_API_KEY = "your-deepseek-api-key-here"
SYSTEM2_BASE_URL = "https://api.deepseek.com"
SYSTEM2_MODEL = "deepseek-v4-flash"

# ============================================================
# 系统一 —— Jev / TypeSafe System One（快、直觉、只出概率分布）
#
# 用途：闭集分类判断的主路径（场景识别 / 字段语义 / 分类变量定序）。
# 特点：输出 token 免费；不生成自由文本；返回概率分布 + 置信度。
# 留空 SYSTEM1_API_KEY 则自动降级为纯系统二，功能不受影响。
# 需要：uv sync --extra system1
# ============================================================
SYSTEM1_API_KEY = ""
SYSTEM1_BASE_URL = "https://api.typesafe.ai"
SYSTEM1_MODEL = "jev-latest"

# ---- 门控阈值：系统一有多大把握才允许直接执行（certainty ∈ [0,1]）----
SYSTEM1_ACCEPT = 0.80        # >= 0.80 直接采用
SYSTEM1_ESCALATE = 0.55      # <  0.55 升级给系统二；中间带采用但标记 provisional
