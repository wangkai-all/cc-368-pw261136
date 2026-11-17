# 共享清洗结果的多报表重算系统

多份报表重算时复用同一段清洗结果；取消某份报表不会删除其他报表仍在使用的
中间数据。系统由三个**独立进程**组成，全部状态落盘、可崩溃恢复：

| 进程 | 入口 | 职责 |
|---|---|---|
| 提交服务 | `src/run_submit_service.py` | HTTP API + 网页；提交时固化输入快照、计算复用键、建立引用 |
| 共享计算进程 | `src/run_worker.py` | 真实清洗/聚合，内容寻址产物 + 引用计数 + 构建租约 + 失败重试 + GC |
| 报表发布端 | `src/run_publisher.py` | 原子发布最终报表与血缘清单 |

## 复用判定（三段式，与任务名称无关）

```
clean_key     = 输入版哈希 + 清洗规则哈希 + 字段权限哈希
partition_key = clean_key + 完整规则哈希（含分组/指标）
```

- 同输入、同字段权限、同清洗规则（即使分组不同）→ **共享清洗后的输入**；
- 再同完整规则 → **共享整个中间分区**；
- 输入文件变化 → 快照内容哈希变化，绝不复用旧结果；
- 字段权限不同 → 清洗产物物理隔离（输出只含授权列），连清洗结果都不共享。

## 引用计数、失败重试与清理（均可恢复）

- 每个产物在 `artifacts/<id>.json` 注册表中记录 `refs`（引用它的作业）、
  `status`（creating/ready/failed）、构建租约与 `attempts`。
- **取消**只移除本作业的引用；仅当 refs 归零且无有效租约时，GC 才删除文件，
  因此一份报表取消不会连带删掉别人在用的中间数据。
- **共享计算失败**由计算进程按产物级 `attempts` 重试（上限 3 次，指数退避）；
  重试耗尽才让依赖它的作业 FAILED 并释放引用。
- 计算进程崩溃后，新进程启动时 `reap_stale_leases` 接管上一世代的全部
  creating 租约（单 worker 架构下旧 owner 必已死亡），依据磁盘状态续算。
- 发布端在 rename 前崩溃只留下 staging 临时目录，作业仍是 PUBLISHING，
  重启后按作业记录幂等重发。

## 可追溯性

每个作业目录 `jobs/<job_id>/` 含不可变 `job.json`（快照哈希、三段复用键、
规则、权限）与只增的 `events.log`。发布产物 `published/<report>/<job>/` 含
`report.csv` 和 `lineage.json`，记录输入快照、复用键、规则与权限；输入快照
在发布后保留，随时可回溯当时的输入版。

## 运行

```bash
# 依赖：仅 Python 3.11 标准库，无需安装第三方包
scripts/start-all.sh     # 启动三进程，控制台 http://127.0.0.1:8637/
scripts/stop-all.sh      # 优雅停止
python3 -I tests/test_e2e.py   # 端到端测试（11 个场景，独立临时目录）
```

环境变量：`RECALC_HOME`（状态根目录，默认 `/workspace/.recalc`）、
`RECALC_CONFIG`（报表配置，默认 `config/reports.json`）、`RECALC_PORT`。

网页支持：并行发起多份重算、取消单个作业、修改规则再提交、故障注入
（阶段延迟 / 瞬时失败 / 发布前硬退出），实时展示作业状态机、产物引用计数
与输入-输出血缘。

## 目录结构

```
config/reports.json   报表、处理规则、字段权限
data/raw/*.csv        原始输入（含脏数据）
src/recalc/           业务包（见各模块 docstring）
src/run_*.py          三进程入口（兼容 python3 -I 隔离模式）
tests/test_e2e.py     端到端测试
scripts/              启停脚本
```
