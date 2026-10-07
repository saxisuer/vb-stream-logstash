# vb-stream-wal-source — WAL 直解源

不创建备库、不占主库逻辑解码 CPU：pgjdbc **物理复制流**接收原始 WAL 字节，纯 Java
解析记录布局，自维护 pg_attribute/pg_class 的 ctid 重放 as-of 字典（"伪装备库"）。
v1 交付 catalog 同步 + WAL 接收两块；**v2 交付用户表 DML 解码与事务组装**（TOAST
external 重组 / 2PC / engine `ConsoleRenderer` 格式复刻输出），并经**双路对拍 IT**
（engine 逻辑解码路 vs wal 直解路，六场景 + 干扰矩阵）证明两路 CDC 输出逐字节等价。

## 快速开始

前置 PG（仓库自带 Docker 环境：catalog 同步面只需 `wal_level≥replica`；**DML 面需
`wal_level=logical`**——UPDATE 旧元组记录与省略门同逻辑日志绑定，replica 档 DML 面有
系统性限制，见"已知限制"；src/docker 镜像已是 logical，另含 `max_slot_wal_keep_size=2GB`
兜底）：

```bash
cd src/docker && docker compose up -d && cd ../..     # PG 18 @ localhost:55432
mvn -q -pl vb-stream-wal-source compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "vb-stream-wal-source/target/classes:$(cat vb-stream-wal-source/target/cp.txt)" \
     -Dvb.wal.host=localhost -Dvb.wal.port=55432 \
     -Dvb.wal.state.dir=/tmp/wal-source-state \
     org.vastdata.vbstream.walsource.Main
```

（macOS/Linux classpath 分隔符 `:`，Windows 为 `;`。无需 `--add-opens`——本模块零
Chronicle 依赖。运行期每 10s 打一行 `smoke:` 周期统计 INFO——消费前沿 LSN + 计数器 +
census 前 3 形态，state 启用时附 `ckpt=`/`adv=` 检查点与槽推进观测，DML 面启用时附
`dml=[buckets=N rows=M]` 发射计数；事务块（TXN-BEGIN/逐行/TXN-END）走 CDC 专用 logger
`org.vastdata.vbstream.walsource.cdc`；Ctrl-C 优雅退出含最终 best-effort 检查点 + 槽
推进；启动失败 exit 1。）

配置键全表（`-Dvb.wal.*` 十一键，默认值即下表；单一来源 `WalSource`/`StateConfig`）：

| 键 | 默认 | 语义 |
|---|---|---|
| `vb.wal.host` | `localhost` | PG 主机 |
| `vb.wal.port` | `5432` | PG 端口（非数字启动期 fail-fast） |
| `vb.wal.db` | `postgres` | 库名 |
| `vb.wal.user` | `postgres` | 用户（须有 REPLICATION 权限） |
| `vb.wal.pass` | `postgres` | 密码 |
| `vb.wal.slot` | `wal_source` | 物理复制槽名（自动建/复用，`immediately_reserve=true`） |
| `vb.wal.state.dir` | 缺省 = 禁用 | 检查点目录（启用后：续传/周期落盘/槽推进整个生命周期面；一目录一活实例） |
| `vb.wal.state.interval.ms` | `30000` | 周期检查点间隔（毫秒） |
| `vb.wal.state.events` | `1000` | catalog 行事件数阈值（与间隔先到为准） |
| `vb.wal.tables` | 缺省/空 = 全放行 | DML 输出面表白名单（逗号分隔 `schema.table`，relkind 'r'/'p' 持久用户表） |
| `vb.wal.dml` | `true` | DML 输出开关（false 回纯 v1 catalog 形态；非 true/false 启动期 fail-fast） |

## 架构

设计全文见 [docs/superpowers/specs/2026-10-05-wal-source-module-design.md](../docs/superpowers/specs/2026-10-05-wal-source-module-design.md)
（v1）与 [docs/superpowers/specs/2026-10-06-wal-source-v2-design.md](../docs/superpowers/specs/2026-10-06-wal-source-v2-design.md)
（v2 DML 面；先导实验 [docs/wal-direct-decode-spike.md](../docs/wal-direct-decode-spike.md)，
9 场景 27 条发现）。一句话：五个面向接口的 v1 组件——版本化布局描述符 `WalLayout`
（V17/V18，逐常量标注 REL_STABLE 源码出处，未注册版本启动即拒）、物理流接收
`WalStreamReceiver`/`WalStreamWalker`（pageaddr 锚定协议 + 断流自动重连）、记录解析
`WalRecordParser`（heap 家族 → 强类型 record，FPW 页镜像重建）、ctid 重放
`CatalogSynchronizer`（JDBC 一致性引导 + 竞速三修正：引导一致性 / ctid 寻址精确采纳
自愈（末态回填）/ pageaddr + 自维护数据锚）、检查点持久化 `StateStore`（单文件 VBWS
格式，CRC 双验 + 原子 rename，损坏回落全新引导）——由 `WalSource` 门面装配为一次
`start()`，单线程直通（接收→解析→重放/解码→周期落盘）。**v2 增 `changes` 包 DML 面**：
`ChangeStream` 门面 → `XactGrouper` 事务组装（提交时批量发射）+ `TableFilter` 白名单 +
`ToastAssembler` TOAST 三形态重组 + `DiskValueRenderer`/`PgFloatFormat` 值渲染 +
`OutputRenderer` 复刻 engine `ConsoleRenderer` 事务块格式。运行依赖 pgjdbc +
slf4j-api + lz4-java（TOAST lz4 压缩值解压），组件细节与坑位见模块 `CLAUDE.md`。

## 验收

- **catalog 面**：对抗性对拍 IT——autovacuum 拉满 / 并发 DDL / 随机 CHECKPOINT 干扰下
  执行 DDL 场景序列，快照与 JDBC REPEATABLE READ 实查**逐行全等**（ctid / relfilenode /
  toast 映射 / dropped / 列序），PG 17/18 双版本矩阵。
- **DML 面（v2）**：双路对拍——同一容器同一批 DML，engine 逻辑解码路（in-process 复刻
  engine Main 装配）与 wal 直解路各自独立槽位捕获输出，按 xid 交集**逐行 diff 为空**；
  六场景（基础 DML 六形态 / 17 类型边界值 / TOAST 三形态 + 重启 unchanged / DDL-in-txn
  as-of / 2PC 四形态 / 中途停续）+ CREATE SCHEMA + 干扰矩阵。
- `mvn test -pl vb-stream-wal-source` 单命令全跑（253 用例 = 离线 213 + Testcontainers
  IT 40，后者需本机 Docker）。

## 场景支持速览（详版矩阵见模块 CLAUDE.md）

**✅ 支持（双路对拍验收过）**：INSERT/UPDATE/DELETE（`wal_level=logical` + REPLICA IDENTITY
FULL）、事务组装（交错/SAVEPOINT/回滚零输出/2PC 四形态）、TOAST 全形态（external 重组/
pglz 与 lz4 解压/行内压缩/未变列 `<toast-unchanged>`/重启回查）、17 类型矩阵、事务内 DDL
as-of、RENAME/TRUNCATE/CREATE SCHEMA、检查点续传/损坏回落/at-least-once 重发、PG 17+18
双版本。

**❌ 不支持（触发时的行为）**：
- **replica 形态 UPDATE**（`wal_level=replica`）→ 行级跳过 + WARN + 计数——系统性缺行，
  **DML 面需 `wal_level=logical`**
- **压缩 FPW** → ISE fail-fast——运维前提 `wal_compression=off`
- **矩阵外类型**（enum/jsonb/域等）→ `0x` 十六进制降级 + WARN，流不断
- **VACUUM FULL/CLUSTER** → 字典跟踪但 DML 语义未承诺
- **dropped 列** → wal `∅` vs engine 不发——已知分叉面

## 已知限制（详版见模块 CLAUDE.md）

- **DML 面需 `wal_level=logical`**：replica 档下 UPDATE 前缀/后缀省略 + DEFAULT 身份无
  旧元组——跨记录行态重建不可达，受影响的行**有痕跳过**（WARN + skipped 计数）而非
  错值；正解排 v3 或声明非目标
- **接收器 5 次重连失败/解析 ISE 后进程 exit 1**——接收线程自行退出后冒烟 `Main`
  周期行检测终态 → ERROR + `System.exit(1)`（fail-fast，对齐引擎约定）
- **压缩 FPW 不支持**——运维前提 `wal_compression=off`（CHECKPOINT 后首写必带页镜像）；
  external/行内压缩 varlena 的值面已支持 pglz 与 lz4 双方法
- **首发 17 类型集外的列**（enum/域/jsonb/组合类型等）`hex:` 降级 + WARN——双路对拍
  仅在首发集内逐字节一致；大事务输出缓冲 O(事务)（与 engine block 模式同级，落盘化
  是 v3 路径）；engine 流式驱逐形态 kind=STREAMED 与 wal 侧恒 NORMAL 是已知分叉
  （对拍环境禁驱逐规避）
- **末态回填语义**：自愈只承诺 catalog **末态**正确（对拍面即末态全等），丢页/断链窗口
  内的中间代际不可恢复
- 56KB 服务端侧流形态跳变未根因（pageaddr 重锚自愈、对拍未破，`resyncs ≤ 2` 容差 +
  lossyResyncs 观测面）；PG 17 干扰风暴与 DDL 并发交织偶发丢 INSERT 事件（矩阵已次序
  规避；引导一致性 C1 修复后根因面已收窄，专项复验待重跑）
- VACUUM FULL/CLUSTER 全表重发的 CDC 语义未承诺（v3 议）
