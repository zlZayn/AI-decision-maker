# 决策：uv 生态统一（2026-08-28）

已实施：dev 依赖组、锁文件源、Python 版本三处统一

## 问题
- dev 依赖（pytest 等）原在 [project.optional-dependencies]，uv run 默认不装 extra，pytest 不在项目环境
- 实测 uv run pytest 回落系统 Python312（警告路径暴露），uv run python -c "import pytest" 报 ModuleNotFoundError
- 锁文件全部包源为 pypi.org，本机为清华镜像环境，下载慢
- .python-version 未入库，Python 版本无强约束（pyproject 仅 >=3.10 宽松）

## 决策
- dev 依赖迁移到 PEP 735 [dependency-groups] dev（pytest>=8.0, pytest-cov），uv run 默认加载 dev 组
- 锁文件默认源切换为清华镜像（uv lock --default-index https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple）
- .python-version 已入库，锁定 3.12.10（符合项目统一标准）

## 替代方案（强制）
- dev 留在 [project.optional-dependencies]：uv run 不自动安装 extra，pytest 缺失导致回落系统解释器，环境不自洽，否决
- 锁文件保留官方 pypi.org：本机为清华镜像生态，官方源下载慢，否决
- .python-version 不入库：只靠 pyproject 宽松约束无法固定解释器版本，协作与复现环境不可控，否决
- 不引入 PEP 735、依赖 uv run 对命令的透明回退：行为依赖系统环境的意外，无确定性，否决

## 影响
- 收益：uv run pytest 开箱即用（实测 137 passed / 0 failed，pytest 9.1.1 入 .venv）；锁文件仅 URL 行变化（969+/969-，hash 与内容零变化）；Python 3.12.10 全仓库统一
- 代价：uv.lock 换源后解析需联网（清华镜像可达）；[project.optional-dependencies] 移除后 pip install .[dev] 不再适用（本项目工具链为 uv，基础安装不受影响，见根 README 指引）