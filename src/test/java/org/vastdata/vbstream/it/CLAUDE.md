# it/ 集成测试——Testcontainers 真 PG 18 端到端

全部测试类跑真库：共享单例容器 `PgTestEnv.PG`（postgres:18，`wal_level=logical`、`logical_decoding_work_mem=64kB`、`max_prepared_transactions=16`、`max_slot_wal_keep_size=1GB`），**需要本机 Docker**；`mvn test` 单命令即含本包（与单测同轮）。

## 基建与通用模式

- **`PgTestEnv`**：类加载即启动的单例容器 + 静态工具（`newSqlConnection` / `newConfig(slot, pub)`——固定 proto 4 + PARALLEL + two_phase + 反馈 2s / `execSql` / `dropSlotQuietly`）。容器**跨测试类共享**，故各测试类用独立槽名。残留槽危害：上次异常退出留同名槽会从旧 confirmed_flush_lsn 续传、静默吞掉先于建流写入的事务——组装类（TransactionAssemblyTest/DecoupledPipelineTest）与解耦/frontier 类（ReaderUnblockedTest/FrontierCapTest）因此 `@BeforeEach` 清残留槽；全部类 `@AfterEach` drop（先杀 walsender 再删；`BenchCorpusRecordTest` 例外，槽清理内联在录制路径 finally）
- **`SessionHarness`**：会话包装 + 双轨录制（raw 字节与解码消息两列表同序一一对应）。停止条件是 countDown latch——列表在 close 前仍会增长，**确定性全量断言必须先 `close()` 再读**。两种生命周期习语：多数用例（以停止条件收尾、块内断言）走 try-with-resources；需 close 后断言/回放的用例（RawSessionContractTest / DecoupledPipelineTest / BenchCorpusRecordTest）用显式 try/finally——harness 出块后仍可引用。机制细节见 replication 包 CLAUDE.md 的 SessionHarness 节
- **录制→离线回放模式**（组装器类测试通用）：真库录制 raw 字节后把 `rawMessages()` 回放给 `TransactionAssembler`（确定性纯状态机，离线回放与在线组装一致）——随机数据只进录制侧，不进双配置对照断言。回放时点两种：`TransactionAssemblyTest` 五场景在 harness 块内（close 前）回放——停止条件恰在最后一条预期消息触发、尾部无多余流量才安全；`DecoupledPipelineTest`/`BenchCorpusRecordTest` close 后回放（DecoupledPipelineTest 用**异步**组装器 + close 毒丸排干后断言——排干承诺使输出成为确定性终态）——新场景若录制尾部含预期外消息必须用 close-first 形态

## 11 组测试各自验证什么

| 测试类 | 验证场景 |
|---|---|
| `NormalTransactionTest` | 普通事务消息子序列（首个 Begin..Commit 精确四元 + 三事务计数）；LSN 反馈两段式断言：`pg_stat_replication.flush_lsn` 先采纳 → 槽 `confirmed_flush_lsn` 在解码推进时跳位 |
| `StreamedTransactionTest` | 500 行×8KB 单事务触发流式分段（StreamStart firstSegment、分段结构）；parallel 模式 StreamAbort 携带附加字段且后续无错位 |
| `TwoPhaseTransactionTest` | PREPARE→COMMIT PREPARED（b/变更/P/K 按 gid 匹配）；PREPARE→ROLLBACK PREPARED（r）；大事务 PREPARE 以 StreamPrepare 分段收尾 |
| `DataTypeTest` | 19 列常见类型（时间/数字/字符串/bool/uuid/jsonb/bytea）文本协议解码端到端一致性——以 PG 自身 JDBC getString 输出为 oracle（同一套类型输出函数），不硬编码期望值 |
| `TruncateTest` | TRUNCATE 选项位（CASCADE/RESTART_IDENTITY）与多表 oid 列表解码 |
| `RawSessionContractTest` | raw 接缝契约三角：raw 与解码消息逐条等长、每条 raw 首字节是 19 种合法类型字符之一、全新 `DecodedMessageBridge` 重放 raw 流得 record 值相等序列 |
| `TransactionAssemblyTest` | 组装器五场景：普通多语句事务 / 流式+子事务回滚剔除 / 2PC 提交与回滚 / 双连接并发大事务多桶交错 / 多类型值 round-trip |
| `DecoupledPipelineTest` | 解耦管道三场景（原 AssemblySpillTest 升级，回放侧换**异步**组装器真实双线程）：①双连接并发流式大事务多桶交错 + StreamAbort 子事务剔除，双回放输出全等 ②大事务内同事务 DDL，前后段按 asOf 版本渲染 ③流式大事务回滚后低水位推进 + 注入陈旧滚动文件验证 releaseBelow 实际删档；场景明细见根 CLAUDE.md"集成测试"条 |
| `ReaderUnblockedTest` | **解耦头名验收**（1.7 设计 §9.3）：consumer 阻塞在输出回调期间 reader 持续从复制流接收（接收计数严格增长）；放行后排干输出 == 提交数（不丢不重） |
| `FrontierCapTest` | **反馈语义验收**（1.7 设计 §5/§9.3）：consumer 阻塞期间未输出事务钉住槽 `confirmed_flush_lsn`（≤ before 锚点，两段式 + WAL 锚点策略），放行并补 WAL 活动后越过封顶 |
| `BenchCorpusRecordTest` | JMH 语料生成器：6 场景脚本真库录制 → `corpus.bin` + SHA-256 指纹边车；指纹一致时**只做健康断言、不启容器**（PgTestEnv 引用收缩在录制分支内，常规 mvn test 秒级过）。改 `src/main/resources/sql/` 脚本或 DDL 即触发重录并**改写源码树**（产物须提交回库；无 Docker 环境下重录失败属预期） |

## 领域注意（不在本包重复）

构造流式数据的载荷规则（TOAST 压缩后记账、不可压缩载荷写法）与双连接交错原理见根 CLAUDE.md"运行 Main"节末条（流式驱逐的内存记账）。
