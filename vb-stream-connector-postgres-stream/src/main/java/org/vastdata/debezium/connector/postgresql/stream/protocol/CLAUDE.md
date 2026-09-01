# protocol/ 包——pgoutput 协议解码（纯函数，无 IO）

输入是"一条 pgoutput 消息体"的 `ByteBuffer`（已剥去复制协议封装），输出是强类型 record。本包是引擎 `vb-stream-engine` 的 `org.vastdata.vbstream.protocol` 包按设计决策 **D2 的 1:1 重写**：分发表、字段序列、fail-fast 异常语义逐行一致，但**零引擎 import**（connector 模块不依赖 vb-stream-engine，pom 结构性保证；javadoc 中的 `org.vastdata.vbstream...` 均为 `{@code}` 文字参照，不是依赖）。包边界与引擎侧一致：除 `java.*` 与 `org.slf4j` 外零依赖，**不 import 任何 Debezium 类**——连接器三件套在父包 `...postgresql.stream`，本包保持纯函数可独立移植。

所有整数 big-endian；字符串为 null 结尾 UTF-8（CString）；时间戳为距 2000-01-01 UTC 的微秒数（parser 侧换算 `Instant`，唯一例外见 StreamAbort）。字节格式第一手总表见引擎 spec `docs/superpowers/specs/2026-08-26-pgoutput-stream-decoder-design.md` 附录 A，与本包源码互证。

## 19 种消息（sealed interface `PgOutputMessage`，一条消息一个 record）

| 类型字节 | 消息 | 一句话 |
|---|---|---|
| `B` | `Begin(finalLsn, commitTimestamp, xid)` | 事务开始；finalLsn 即 commit 记录将出现的 LSN |
| `C` | `Commit(commitLsn, endLsn, commitTimestamp)` | 事务提交结束位；首字节 flags 读掉不建模 |
| `O` | `Origin(originCommitLsn, originName)` | 级联复制的源节点位点 |
| `R` | `Relation(streamXid, relationOid, schema, table, replicaIdentity, List<RelationColumn>)` | 表元数据，先于同表 DML 到达；replicaIdentity 单字符 d/i/f/n |
| `Y` | `Type(streamXid, typeOid, schema, name)` | 复合类型定义（publication 含复合类型列时才出现） |
| `I` | `Insert(streamXid, relationOid, newTuple)` | 插入，`'N'` 标记 + 新元组 |
| `U` | `Update(streamXid, relationOid, Optional oldTuple, newTuple)` | 更新三形态：`'K'` 键元组 / `'O'` 旧整行 / 无旧元组 |
| `D` | `Delete(streamXid, relationOid, oldTuple)` | 删除，必带 `'K'` 或 `'O'` 旧元组 |
| `T` | `Truncate(streamXid, EnumSet<TruncateOption>, int[] relationOids)` | 一条语句截断多表 |
| `M` | `LogicalMsg(streamXid, transactional, lsn, prefix, byte[] content)` | `pg_logical_emit_message` 自定义消息 |
| `S` | `StreamStart(xid, firstSegment)` | 流式块开始；其后消息前置 xid |
| `E` | `StreamStop()` | 流式块结束，无字段 |
| `c` | `StreamCommit(xid, commitLsn, endLsn, commitTimestamp)` | 流式事务提交（**小写 c**） |
| `A` | `StreamAbort(xid, subxid, OptionalLong abortLsn, OptionalLong abortTimestamp)` | 子事务回滚剔除（**大写 A**，见下勘误） |
| `b` | `BeginPrepare(prepareLsn, endLsn, prepareTimestamp, xid, gid)` | 两阶段 prepare 开始（**无** flags 字节） |
| `P` | `Prepare(同上五字段)` | prepare 完成；首字节 flags(0) 读掉不建模 |
| `K` | `CommitPrepared(commitLsn, endLsn, commitTimestamp, xid, gid)` | 两阶段提交；首字节 flags(0) 读掉不建模 |
| `r` | `RollbackPrepared(双 LSN + 双时间戳 + xid, gid)` | 两阶段回滚，唯一双时间戳消息 |
| `p` | `StreamPrepare(与 P 同构)` | 流式事务的两阶段 prepare；首字节 flags(0) 读掉不建模 |

公共约定：`streamXid` 为 `OptionalLong`，非空表示消息处于流式块内（协议前置了 xid）；数组组件（`Truncate.relationOids`、`LogicalMsg.content`）显式 override equals/hashCode 为值相等。

## 双入口契约：`PgOutputStreamDecoder`

- `decode(ByteBuffer)`——顶层消息入口：读 1 字节类型 → `dispatch` 分发 → **剩余字节 ≠ 0 立即抛 `ProtocolMisalignmentException`**（防错位扩散）→ 逐消息 DEBUG（默认关闭）。内建流块状态机 `inStream`：收到 `'S'` 置位、`'E'` 复位；`inStream` 期间 M/R/Y/I/U/D/T 七类在类型字节后**前置 Int32 xid**，顶层消息无此前缀。构造时传入 `StreamingMode` 仅供 `StreamParsers.abort` 判断是否读取 parallel 附加字段
- `decodeSingle(ByteBuffer, boolean inStream)`——回放场景单条入口：**白名单只接受 M/R/Y/I/U/D/T**（只有这七类有前缀语义），其余类型（含 'S'/'E'/'c'/'A' 与两阶段控制）一律 `IllegalArgumentException`；用**入参**而非实例字段作 inStream，完全不读写实例流块状态——回放侧本来就知道每条消息在不在流式块内，免 'S'/'E' 包裹重建上下文（MS2 组装器回放路径的解码接缝）

## 四个 parser 族与值类型（包私有 final 类，静态方法，一消息一方法）

- `NormalParsers`（B/C/O/R/Y）：commit 首字节 flags 读掉不建模；relation 列循环 [I8 flags(bit0=partOfKey) + CString name + I32 typeId + I32 typmod]，列数 I16，出口 `List.copyOf`
- `DmlParsers`（I/U/D/T/M + tupleData）：update 三形态标记分支；`tupleData` I16 列数 + 每列种类字节 `'n'` NULL / `'u'` TOAST 未变（值不可得而非 NULL，流式大事务高频） / `'t'` 文本 / `'b'` 二进制；未知标记/种类 fail-fast
- `StreamParsers`（S/E/c/A）：见下速查表，abort 按 `StreamingMode` 分支
- `TwoPhaseParsers`（b/P/K/r/p）：P/K/r/p 首字节 I8(0) flags 消费不建模；`prepare` 与 `streamPrepare` 消息体同构，经 `readPreparedTxn` 复用
- `WireReader`：big-endian 逐字段读取（引擎 `ByteBufferReader` 更名）；`readUnsignedInt` 返回 long（xid 无符号语义）；静态 `pgMicrosToInstant` 用 floorDiv/floorMod（负微秒正确），纪元偏移 946684800 秒
- `TupleData(List<TupleValue>)` / `TupleValue`（Null / UnchangedToast / Text / Binary）/ `RelationColumn(name, typeId, typeModifier, partOfKey)` / `TruncateOption`（CASCADE bit0、RESTART_IDENTITY bit1）/ `StreamingMode`（OFF/ON/PARALLEL）
- 异常两个均 RuntimeException fail-fast：`ProtocolMisalignmentException`（剩余字节 ≠ 0）、`UnknownMessageTypeException`（未知类型字节/元组标记/列种类，消息含"剩余 N 字节"提示）

## 字节格式速查（类型字节之后的字段序列；`[xid]` = 仅流式块内前置 I32）

`B`: I64 finalLsn, I64 commitTs(µs), I32 xid ｜ `C`: I8 flags(0), I64 commitLsn, I64 endLsn, I64 commitTs ｜ `O`: I64 lsn, CString name ｜ `R`: [xid], I32 oid, CString schema, CString table, I8 replident, I16 ncols, N×(I8 flags, CString name, I32 typeId, I32 typmod) ｜ `Y`: [xid], I32 oid, CString schema, CString name ｜ `I`: [xid], I32 oid, `'N'`, TupleData ｜ `U`: [xid], I32 oid, (`'K'`|`'O'`)?TupleData, `'N'`, TupleData ｜ `D`: [xid], I32 oid, (`'K'`|`'O'`), TupleData ｜ `T`: [xid], I32 nrel, I8 options, nrel×I32 oid ｜ `M`: [xid], I8 flags(bit0=transactional), I64 lsn, CString prefix, I32 len, len bytes ｜ `S`: I32 xid, I8 firstSegment ｜ `E`: 空 ｜ `c`: I32 xid, **I8 flags(0)**, I64 commitLsn, I64 endLsn, I64 commitTs ｜ `A`: I32 xid, I32 subxid, (仅 PARALLEL: I64 abortLsn, I64 abortTime[µs 原值]) ｜ `b`: I64 prepareLsn, I64 endLsn, I64 prepareTs, I32 xid, CString gid ｜ `P`/`p`: I8 flags(0), I64 prepareLsn, I64 endLsn, I64 prepareTs, I32 xid, CString gid ｜ `K`: I8 flags(0), I64 commitLsn, I64 endLsn, I64 commitTs, I32 xid, CString gid ｜ `r`: I8 flags(0), I64 prepareEndLsn, I64 rollbackEndLsn, I64 prepareTs, I64 rollbackTs, I32 xid, CString gid

**两处字节格式勘误结论（实测钉死，防后人按错误记忆重写）**：
1. **StreamAbort 的类型字节是 `'A'`（大写）**——勿按小写 `'a'` 记忆（PG 协议里流式控制四个类型字节为 S/E/c/A，仅 StreamCommit 是小写）
2. **StreamCommit（`'c'`）在 I32 xid 之后有一个被消费的 I8 flags(0)**——解析读掉不建模，漏读这 1 字节即消息流全部错位

## 与引擎包的类映射（重写对照；引擎类 = `org.vastdata.vbstream.protocol.*`）

| 本包类 | 引擎参照 | 差异 |
|---|---|---|
| `WireReader` | `ByteBufferReader` | 更名避免与直觉冲突；API 逐方法一致 |
| `PgOutputStreamDecoder` | `PgOutputDecoder` | 同名换 Stream 后缀（与父包连接器名呼应）；双入口契约（`decode` 状态机 + `decodeSingle` 白名单）逐行一致 |
| `PgOutputMessage` | `PgOutputMessage` | sealed + 19 record，组件名一致 |
| `RelationColumn` | `Column` | 更名避免与 `io.debezium.relational.Column` 撞名 |
| `TupleData` / `TupleValue` / `TruncateOption` / `StreamingMode` | 同名 | 一致 |
| `NormalParsers` / `DmlParsers` / `StreamParsers` / `TwoPhaseParsers` | 同名 | 包私有 static，读取序列逐行一致 |
| `ProtocolMisalignmentException` / `UnknownMessageTypeException` | 同名 | 一致 |
| 测试 `MsgBuilder` | 引擎测试 `MsgBuilder` | 手造字节辅助，同法 |

## 线程与日志

decoder 及四个 parser 族为无状态或单流状态（inStream），**非线程安全，由读取该复制流的单一线程调用**；record 全部不可变可跨线程传递。日志仅 decoder 出口一条 DEBUG（默认关闭）。
