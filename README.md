# 湖仓一体任务编排与数据治理平台

面向数据湖和数据仓库的ETL任务编排、分区管理、数据血缘和质量治理的工程平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8018/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5198/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8018`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| ETL流水线 | `etl_pipeline` | 流水线 | 流水线名称、源端类型、目标端类型 |
| 分区管理 | `partition_manager` | 分区配置 | 配置名称、目标表、分区字段 |
| 数据质量规则 | `data_quality_rule` | 质量规则 | 规则名称、校验维度、目标表 |
| Schema变更管理 | `schema_change` | 变更记录 | 目标表、变更类型、变更内容 |
| 数据血缘 | `lineage_graph` | 血缘关系 | 目标表、来源表列表、转换逻辑 |
| 补数任务 | `backfill_task` | 补数任务 | 目标表、补数时间范围、补数状态 |
| 表归档管理 | `table_archive` | 归档策略 | 策略名称、目标表、归档条件 |
| 聚合表构建器 | `agg_builder` | 聚合配置 | 配置名称、源事实表、聚合维度 |
| 流水线监控 | `pipeline_monitor` | 监控记录 | 流水线名称、运行时间、执行状态 |
| 冷数据恢复 | `cold_restore` | 恢复任务 | 归档来源、恢复目标路径、恢复时间范围 |
| 维度表生成器 | `dimension_builder` | 维度配置 | 配置名称、来源表、维度键 |
| 数据保留策略 | `data_retention` | 保留规则 | 规则名称、目标数据集、保留天数 |
| 转换方案 | `transform_plan` | 转换步骤 | 方案名称、步骤序列、输入表 |
| 数据迁移任务 | `data_migration` | 迁移任务 | 任务名称、源存储、目标存储 |
| 增量合并引擎 | `delta_merge` | 合并任务 | 目标表、增量来源、合并键 |
| 快照生成器 | `snapshot_creator` | 快照任务 | 目标数据集、快照时间点、快照类型 |
| 湖仓审计 | `data_lake_audit` | 审计报告 | 审计范围、审计时间、表总数 |
| SLA策略 | `sla_policy` | SLA规则 | 规则名称、关联流水线、产出截止时间 |
| 清理作业 | `cleanup_job` | 清理作业 | 作业名称、目标路径、清理条件 |
| 目录同步 | `catalog_sync` | 同步任务 | 任务名称、源Catalog、目标Catalog |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 湖仓审计：孤儿文件巡检闭环

湖仓审计在通用模块之外提供一条 Parquet 数据路径核查专用链路
（`backend/app/services/orphan_review.py` + `routers/orphan_review.py`，
前端页面 `/data_lake_audit/orphan-review`），覆盖：

1. **扫描批次与核查单**：登记扫描批次及扫描凭据（manifest、SHA256 校验和、
   基线快照），核查单逐行列明「扫描批次、文件全路径、判作孤儿的依据」，
   `GET /api/orphan-review/checklist/export` 导出 CSV。
2. **分批逐行回执**：回执可分几次回传（JSON `POST /receipts/import` 或
   CSV `POST /receipts/import-csv`），按原批次与路径归档；坏行逐行拒收、
   重复回传按修订版本覆盖。没收到回执的项目继续列在 `GET /pending` 待核清单。
3. **快照引用约束**：`POST /snapshot-references` 登记当前快照引用。回执确认
   孤儿的路径若被当前快照引用，仍被拦截在清理范围外（快照约束优先于反馈约束）。
4. **清理计划**：`POST /cleanup-plans` 从孤儿回执提取拟清理路径，可执行范围
   同时受当前快照引用与回执反馈约束；计划原样保留扫描凭据，路径可回溯到批次。
5. **审计留痕**：全部动作写入 `GET /audit-trail`，`GET /audit-narrative`
   生成审计说明，交代新快照引用如何改变原孤儿意见及本轮处置（拦截项转下一轮
   巡检待核清单）。

`POST /api/orphan-review/demo/init` 可一键构造标准演示场景：同一张核查单两个
Parquet 文件，其中一个在回执回传前被新快照引用。后端验证脚本：

```bash
cd backend
.venv/bin/python -m tests.test_orphan_review        # 领域逻辑（53 项断言）
.venv/bin/python -m tests.test_orphan_review_http   # HTTP 端到端（需先启动后端）
```

领域：数据工程
子方向：数仓与数据湖
