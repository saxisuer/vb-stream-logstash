# protocol/ 模块——pgoutput 协议解码（纯函数，无 IO）

输入是"一条 pgoutput 消息体"的 `ByteBuffer`（已由 pgjdbc `PGReplicationStream.read()` 剥去 CopyBoth 消息头），输出是强类型 record。所有整数 big-endian；字符串为 null 结尾 UTF-8（CString）；时间戳为距 2000-01-01 UTC 的微秒数。字节格式总表见 `docs/superpowers/specs/2026-08-26-pgoutput-stream-decoder-design.md` 附录 A，PG 源码摘录见同文档附录 B。

## PgOutputDecoder（唯一入口）

`decode(ByteBuffer)`：读 1 字节类型 → `dispatch` 到对应 parser → **解析后若剩余字节 ≠ 0 立即抛 `ProtocolMisalignmentException`**（防错位扩散）。逐消息打 DEBUG 日志（类型字符 + record toString，默认关闭）。

内建**流块状态机** `inStream`：收到 `'S'`(StreamStart) 置位、`'E'`(StreamStop) 复位；`inStream` 期间 M/R/Y/I/U/D/T 七类消息在类型字节后**前置 Int32 xid**（由 `streamXid()` 消费并放入消息的 `streamXid` 字段），顶层消息无此前缀。构造时传入 `StreamingMode` 仅供 `StreamParsers.abort` 判断是否读取 parallel 附加字段。**非线程安全**，每次 `run()` 循环新建一个实例。

## PgOutputMessage（19 种消息模型，sealed interface）

所有消息 record 的公共约定：`streamXid` 为 `OptionalLong`，非空表示该消息处于流式块内（即协议前置了 xid）。数组组件的 record（`Truncate.relationOids`、`LogicalMsg.content`）显式 override equals/hashCode 为值相等语义——record 默认对数组退化为引用相等。各 record 字段语义：

- `Begin(finalLsn, commitTimestamp, xid)`：事务开始；`finalLsn` 即该事务 commit 记录将出现的 LSN
- `Commit(commitLsn, endLsn, commitTimestamp)`：事务提交结束位
- `Origin(originCommitLsn, originName)`：逻辑复制源节点位点（级联复制场景）
- `Relation(streamXid, relationOid, schema, table, replicaIdentity, List<Column>)`：表元数据，**先于同表的 DML 到达**；`replicaIdentity` 为单个字符（'d'/'i'/'f'/'n'）
- `Type(streamXid, typeOid, schema, name)`：复合类型定义（仅在 publication 含复合类型列时出现）
- `Insert(streamXid, relationOid, newTuple)` / `Delete(streamXid, relationOid, oldTuple)`：DML，元组见 TupleData
- `Update(streamXid, relationOid, Optional<TupleData> oldTuple, newTuple)`：`oldTuple` 是否存在取决于表 replica identity 与 REPLICA IDENTITY FULL（'K' 键元组 / 'O' 旧整行 / 无）
- `Truncate(streamXid, EnumSet<TruncateOption> options, int[] relationOids)`：一条语句截断多张表，oids 数组
- `LogicalMsg(streamXid, transactional, lsn, prefix, byte[] content)`：`pg_logical_emit_message` 自定义消息
- 流式四件套：`StreamStart(xid, firstSegment)`、`StreamStop()`、`StreamCommit(xid, commitLsn, endLsn, commitTimestamp)`、`StreamAbort(xid, subxid, OptionalLong abortLsn, OptionalLong abortTimestamp)`——**abort 两个附加字段仅 streaming=parallel 时由服务端附加**（否则 empty）；注意 `abortTimestamp` 是微秒原值（未转 Instant）
- 两阶段五件套：`BeginPrepare`/`Prepare`/`StreamPrepare` 为 (prepareLsn, endLsn, prepareTimestamp, xid, gid)；`CommitPrepared` 为 (commitLsn, endLsn, commitTimestamp, xid, gid)；`RollbackPrepared` 为 (prepareEndLsn, rollbackEndLsn, prepareTimestamp, rollbackTimestamp, xid, gid)——唯一**双时间戳**消息

## 四个 parser 族（包私有 final 类，静态方法，一消息一方法）

### NormalParsers（B/C/O/R/Y——事务边界与元数据）
- `commit`：首字节是 currently-unused flags，**读掉但不建模**（漏读 1 字节即后续全错位）
- `relation`：列循环按 [flags I8, name CString, typeId I32, typmod I32] 顺序；flags **bit0 = partOfKey**（其余位未定义）；列数 I16
- `type`：oid/ schema/ name 三字段

### DmlParsers（I/U/D/T/M——DML 与 TupleData）
- `insert`：oid + 元组标记 `'N'`（expectTupleTag 不符即抛 `UnknownMessageTypeException`）
- `update`：oid 后可选 `'K'`（主键元组）或 `'O'`（旧整行）→ 读出 oldTuple 后必跟 `'N'`；也可能直接 `'N'`（无旧元组）；出现其他标记抛 `UnknownMessageTypeException`
- `delete`：oid 后**必须** `'K'` 或 `'O'`
- `truncate`：I32 表数 + I8 选项位（bit0=CASCADE、bit1=RESTART_IDENTITY）+ N 个 I32 oid
- `logicalMsg`：I8 flags（bit0=transactional）+ I64 lsn + prefix + I32 长度 + 字节内容
- `tupleData`（包级工具，Update/Delete/Insert 共用）：I16 列数；每列种类字节 `'n'` NULL / `'u'` TOAST 未变 / `'t'` 文本（I32 长度 + UTF-8 字节）/ `'b'` 二进制（I32 长度 + 原始字节）

### StreamParsers（S/E/c/A——流式大事务控制）
- `start`：I32 xid + I8 firstSegment（1 = 首个流段）
- `stop`：无字段
- `commit`：I32 xid + I8 flags（消费不建模）+ I64/I64 LSN + I64 微秒时间戳
- `abort`：I32 xid + I32 subxid；**仅构造时传入的 mode == PARALLEL 才继续读** I64 abortLsn + I64 abortTime（微秒原值），否则两字段为 empty——错读/漏读都会导致后续消息错位

### TwoPhaseParsers（b/P/K/r/p——两阶段提交）
- 所有含 flags 的消息（P/K/r/p）首字节 I8(0) 消费不建模
- `prepare` 与 `streamPrepare` **消息体同构**（I64 prepareLsn + I64 endLsn + I64 时间戳 + I32 xid + gid），经 `readPreparedTxn` 复用读取，仅构造的 record 类型不同
- `rollbackPrepared`：两个 I64 LSN（prepare 结束/rollback 结束）+ **两个** I64 微秒时间戳

## 值类型与工具

- `ByteBufferReader`：big-endian 逐字段读取，**非线程安全、每消息新建**。`readUnsignedInt` 返回 long（xid 无符号语义，避免负数）；`readString` 读到 `\0`；静态 `pgMicrosToInstant` 用 floorDiv/floorMod 换算（正确处理负微秒），纪元偏移 946684800 秒
- `TupleData(List<TupleValue>)`：一行的列值序列（record，不可变 List）
- `TupleValue`（sealed interface）：`Null` / `UnchangedToast`（TOAST 列未变时服务端不发送值——流式大事务高频出现，**值不可得**而非 NULL）/ `Text(String)` / `Binary(byte[])`（Binary 显式值相等）
- `Column(name, typeId, typeModifier, partOfKey)`：Relation 的单列元数据
- `TruncateOption`：枚举 CASCADE（bit0）/ RESTART_IDENTITY（bit1）
- `StreamingMode`：OFF/ON/PARALLEL，映射 START_REPLICATION 的 streaming 参数

## 异常（两个都是 fail-fast，RuntimeException）

- `ProtocolMisalignmentException`：解析完成后仍有剩余字节——字段布局与协议不符
- `UnknownMessageTypeException`：未知类型字节 / 未预期的元组标记 / 未知列种类；消息含"剩余 N 字节"提示可能已错位

## 线程与日志

decoder 及四个 parser 族均为无状态或单流状态（inStream），**由读取该复制流的单一线程调用**；record 全部不可变可跨线程传递。日志仅 decoder 出口一条 DEBUG。
