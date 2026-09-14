# vb-stream-logstash 与 PostgreSQL 内核的交互协议和数据格式

> 版本锚定:PostgreSQL 18(REL_18_STABLE)。文档日期 2026-09-14。
>
> 全文约定:多字节整数一律 **big-endian**;I8/I16/I32/I64 = 对应宽度整数,U16/U32 = 无符号;字符串为 **null 结尾 UTF-8**(下称 CString);LSN 一律 I64;xid 为 I32(按无符号解释);"流式块"指一对 Stream Start('S')/Stream Stop('E')之间的消息区间;时间戳为距 **PG epoch(2000-01-01 00:00:00 UTC = Unix epoch + 946684800 秒)的微秒数**。

## 1. 复制连接与数据读取(PGReplicationStream)

### 1.1 连接建立

复制连接是普通 JDBC 连接 + 启动参数差异:

| 项 | 值 | 语义 |
|---|---|---|
| URL 参数 `replication` | `database` | 连接进入 walsender 模式,且允许在复制连接上跑 SQL |
| URL 参数 `assumeMinServerVersion` | `>= 9.4` | **pgjdbc 硬性要求**:不带此项时 pgjdbc 会把 `replication` 参数从启动包静默丢弃,后续 START_REPLICATION 被服务端当普通 SQL 解析报语法错 |

内核侧前置(任一缺失则建流失败):`wal_level=logical`、`max_replication_slots >= 1`、`max_wal_senders >= 1`;two_phase 场景另需 `max_prepared_transactions > 0`(默认 0,此时 `PREPARE TRANSACTION` 在源库直接报错)。

本项目实际持有两条连接:复制连接(承载 START_REPLICATION 之后的一切数据面流量)+ 普通 SQL 连接(建槽/元数据查询;解码数据流完全不走这条连接)。

### 1.2 复制槽

建槽走普通 SQL 连接(不使用 walsender 协议的 CREATE_REPLICATION_SLOT 命令),幂等形式:

```sql
SELECT pg_create_logical_replication_slot(:slot_name, 'pgoutput', false, :two_phase)
-- 第三参 temporary=false(持久槽);第四参 two_phase 随配置(本项目默认 true)
```

- 槽已存在时服务端抛 SQLState `42710`(duplicate_object),本项目捕获后 WARN 并复用既有槽
- **`two_phase` 槽属性建槽后不可变**:与建流参数不符时由服务端在 START_REPLICATION 时报错
- 槽持久保留:客户端失联期间 WAL 按槽保留(依赖 `max_slot_wal_keep_size` 兜底磁盘)
- **重启续传**:START_REPLICATION 起始位传 `0/0`(INVALID_LSN)——语义是"从槽当前确认位点开始"

### 1.3 START_REPLICATION SLOT LOGICAL 参数

本项目经 pgjdbc `withSlotOption` 传参,内核侧 pgoutput startup 解析:

| 参数 | 本项目取值 | 语义与门槛 |
|---|---|---|
| `proto_version` | 恒 `4` | 协商版本。v4 = streaming=parallel 形态(需要 PG 15+)。本项目不降级协商:服务端不支持 v4 时 START_REPLICATION 报错,进程 fail-fast 退出(槽保留) |
| `publication_names` | 逗号分隔列表,默认单 publication | **协议硬性要求至少一个**,不传即报错。不在 publication 中的表不产生事件;publication 与槽相互独立,仅是 START_REPLICATION 的过滤参数 |
| `streaming` | `off` / `on` / `parallel`(本项目默认 `parallel`) | 流式大事务档位。`parallel` 档下 Stream Abort 消息带附加字段(§2.4 'A' 行);parallel 要求 two_phase 同开(服务端建流时校验) |
| `two_phase` | `on` / `off`(默认 `on`) | 须与**槽属性**一致,不一致由服务端建流时报错。`on` 时流中出现两阶段消息族(§2.5) |
| `binary` | **仅配置为 true 时传参**;默认(等效 false)不传 | PG 16+ 选项。开启后数据列值走 `'b'` 种类(各类型 typsend 线格式,§2.6)。**条件传参是刻意设计**:低版本内核不识别该选项,传参即报错;false 是服务端默认,不传零兼容风险 |
| `messages` | 仅连接器 `slot.messages=true` 时传 `true`(默认 false 不传) | PG 14+ 选项。开启后 `pg_logical_emit_message` 产生的 'M' 消息才下发;不传时内核不发 'M' |

各能力的内核引入版本:`streaming` 流式族/`messages` = PG 14;`two_phase` 族 + `streaming=parallel` = PG 15;`binary` = PG 16。本项目恒定 `proto_version=4` 全开(默认 parallel + two_phase)。

### 1.4 数据读取:帧封装与消息循环

walsender 把每条 pgoutput 消息封装在 CopyBoth 协议的 CopyData 帧里;**pgjdbc 负责剥帧**,本项目从 `PGReplicationStream.readPending()` 拿到的是"一条 pgoutput 消息的完整 payload"(首字节 = 消息类型字节)。帧层布局:

| 帧类型 | 方向 | 布局(pgjdbc 消费) |
|---|---|---|
| CopyData `'w'`(XLogData) | 内核 → 客户端 | I64 walStart; I64 walEnd; I64 sendTime(PG epoch µs);之后为 pgoutput 消息 payload |
| CopyData `'k'`(keepalive) | 内核 → 客户端 | I64 walEnd; I64 sendTime; I8 replyRequested(1 = 要求客户端立即回 status) |
| CopyData `'r'`(standby status update) | 客户端 → 内核 | I64 walEnd; I64 receivedLSN; I64 flushedLSN; I64 appliedLSN; I64 sendTime; I8 replyRequested |

**读取形态:非阻塞 drain 轮询。** 本项目用 `readPending()` 非阻塞取尽当前缓冲的全部消息,空轮才 sleep 100ms。实测依据(2026-08-31):阻塞 `read()` 空闲期**不按 statusInterval 醒来**,status 上报依赖服务端 keepalive(~`wal_sender_timeout/2`)才触发;轮询形态使反馈周期独立于消息到达,断连感知也更快(每轮查 `stream.isClosed()`)。

**LSN 反馈**:每轮 `setAppliedLSN(lsn)` + `setFlushedLSN(lsn)`(received/flushed/applied 三位点本项目设同值),每满反馈周期(默认 10s)`forceUpdateStatus()` 立即上报。**服务端行为(Diag 实证)**:standby status 先被采纳进 `pg_stat_replication.flush_lsn`;槽的 `confirmed_flush_lsn` 由 walsender 在**解码推进时**(candidate 机制)落库——空闲期不推进但确认不丢失,下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点。另外,`wal_sender_timeout` 只要求 status 消息到达、不要求 LSN 前进——消费慢的客户端不会因位点停滞被断连。

## 2. pgoutput 数据格式(PG 18):19 种消息字节格式

### 2.1 流式块规则(影响所有消息的解析形态)

- **块界定**:内核在流式发送一批进行中大事务的变更时以 `'S'` 开块、`'E'` 闭块;块间可穿插其他事务的消息(parallel 档多事务交错)。**流块不嵌套**
- **块内 xid 前缀**:`'M'/'R'/'Y'/'I'/'U'/'D'/'T'` 七类消息在流式块内时,类型字节后**前置 I32 xid**;流式控制消息(S/E/c/A)与两阶段消息(b/P/K/r/p)恒不前置。客户端必须跟踪块状态(单布尔状态机)才能正确解析。内核扩展消息 `'X'`(§3.1)亦遵守同款前缀规则——前缀集由此多一类(第八类)
- **first_segment**:同一事务的多个流段中仅首个 `'S'` 置 1
- **块内元数据重发**:同一事务跨流段时,'R' 等元数据消息在需要的段内重新下发(带流内 xid 前缀),客户端不能假设只出现一次

字节格式总表来源:PG 18 官方文档 *Logical Replication Message Formats* + `src/include/replication/logicalproto.h` 双源核对。

### 2.2 族 1:事务边界与 DML(v1 起)

| 类型字节 | 消息 | 字段序列(类型字节之后) |
|---|---|---|
| `'B'` | Begin | I64 final_lsn(该事务 commit 记录将出现的 LSN); I64 commit_ts; I32 xid |
| `'C'` | Commit | I8 flags(0,currently-unused); I64 commit_lsn; I64 end_lsn; I64 commit_ts |
| `'O'` | Origin | I64 origin_commit_lsn; CString origin_name(级联复制源节点信息) |
| `'I'` | Insert | [流内: I32 xid]; I32 relation_oid; `'N'`; TupleData(§2.6) |
| `'U'` | Update | [流内: I32 xid]; I32 relation_oid; 可选 `'K'` TupleData 或 `'O'` TupleData(有无与取形见 §2.6 表); `'N'`; TupleData |
| `'D'` | Delete | [流内: I32 xid]; I32 relation_oid; (`'K'` \| `'O'`); TupleData |
| `'T'` | Truncate | [流内: I32 xid]; I32 nrel; I8 opts(bit0=CASCADE, bit1=RESTART_IDENTITY); nrel × I32 relation_oid |
| `'M'` | Message | [流内: I32 xid]; I8 flags(bit0=transactional); I64 lsn; CString prefix; I32 content_len; 字节内容 |

### 2.3 族 2:元数据(消息流中随时插入,v1 起)

| 类型字节 | 消息 | 字段序列 |
|---|---|---|
| `'R'` | Relation | [流内: I32 xid]; I32 relation_oid; CString schema; CString table; I8 replident('d'/'i'/'f'/'n'); I16 ncols; ncols × { I8 flags(bit0=partOfKey,其余位未定义); CString name; I32 type_oid; I32 typmod } |
| `'Y'` | Type | [流内: I32 xid]; I32 type_oid; CString schema; CString name |

注:typmod 下发并非 PG 18 新增——REL_17/18 源码审计确认自 PG 10 即有。

### 2.4 族 3:流式大事务(v2 = PG 14 起)

| 类型字节 | 消息 | 字段序列 | 备注 |
|---|---|---|---|
| `'S'` | Stream Start | I32 xid; I8 first_segment(1 = 首个流段) | |
| `'E'` | Stream Stop | (无字段) | |
| `'c'` | Stream Commit | I32 xid; I8 flags(0); I64 commit_lsn; I64 end_lsn; I64 commit_ts | 流式事务的提交收尾,此后不再有该事务的 `'C'` |
| `'A'` | Stream Abort | I32 xid; I32 subxid; **[仅 streaming=parallel: I64 abort_lsn; I64 abort_time]** | 附加字段门槛是协议形态分叉点,客户端按建流档位决定是否读这两个 I64;错读/漏读导致后续消息整体错位 |

### 2.5 族 4:两阶段(v3 = PG 15 起)

| 类型字节 | 消息 | 字段序列 |
|---|---|---|
| `'b'` | Begin Prepare | I64 prepare_lsn; I64 end_lsn; I64 prepare_ts; I32 xid; CString gid |
| `'P'` | Prepare | I8 flags(0); I64 prepare_lsn; I64 end_lsn; I64 prepare_ts; I32 xid; CString gid |
| `'K'` | Commit Prepared | I8 flags(0); I64 commit_lsn; I64 end_lsn; I64 commit_ts; I32 xid; CString gid |
| `'r'` | Rollback Prepared | I8 flags(0); I64 prepare_end_lsn; I64 rollback_end_lsn; I64 prepare_ts; I64 rollback_ts; I32 xid; CString gid(唯一双时间戳消息) |
| `'p'` | Stream Prepare | I8 flags(0); I64 prepare_lsn; I64 end_lsn; I64 prepare_ts; I32 xid; CString gid(消息体与 `'P'` 同构,内核共用 `logicalrep_write_prepare_common`,仅类型字节不同) |

### 2.6 TupleData 与 replica identity

TupleData(Insert/Update/Delete 的元组载荷):

```
I16 ncols
每列:
  'n'          NULL(无负载)
  'u'          TOAST 未变(无负载)——该列值不发送,是"值不可得"而非 NULL;
               流式大事务高频出现,客户端必须显式建模
  't' I32 len  text 值(len 字节 UTF-8,内容 = 该类型 *_out 输出函数文本)
  'b' I32 len  binary 值(len 字节,内容 = 该类型的 typsend 线格式;仅 binary 选项开启时)
```

binary 选项不改变 'n'/'u' 种类与 TupleData 外壳,只把数据列(含 K/O 旧元组)的 `'t'` 换成 `'b'`。binary 载荷逐类型的解码矩阵见 `vb-stream-engine/src/main/java/org/vastdata/vbstream/protocol/CLAUDE.md`(BinaryValueDecoder 节)。

元组前缀标记 `'N'`(新值)/`'K'`(复制键)/`'O'`(旧整行)的取形由表 replica identity 决定('R' 消息的 replident 字符即由此而来):

| REPLICA IDENTITY | replident | UPDATE 旧元组 | DELETE 旧元组 |
|---|---|---|---|
| DEFAULT(主键) | `'d'` | 仅当键列被修改时发 `'K'`(键元组);否则直接 `'N'` | 恒发 `'K'` |
| INDEX(候选唯一索引) | `'i'` | 同 DEFAULT(按该索引列) | 恒发 `'K'` |
| FULL | `'f'` | 恒发 `'O'`(变更前整行) | 恒发 `'O'` |
| NOTHING | `'n'` | 不发(直接 `'N'`) | 旧元组不可用(空元组,ncols=0) |

## 3. DDL 数据格式(内核扩展——设计约定,尚未实测)

上游原版 PG(vanilla,未经扩展的 community 版)的 pgoutput 不发射 DDL;本节记录 **Vastbase 内核扩展** DDL 事件的数据格式**设计约定**。载体为**新增消息类型字节 `'X'`**——§2 的 19 种之外(原版 PG 未占用该字节)。**本节全部内容均为约定/预期形态,尚未经实际运行验证**(CDC 侧本地原版 PG 18 无该扩展无法复现;内核侧联调验证后在此销账,演进中的字段集与门控亦在此同步增补)。修订记录:2026-09-14 由"复用 'M' Message + prefix 区分"方案修订为独立 `'X'` 类型(决策理由见 §3.1 注记)。

### 3.1 载体:新增 'X' 消息类型

线格式 = 'M' 线格式(§2.2)在消息体插入 **I32 xid**(flags 之后、lsn 之前),并遵守 §2.1 流内前缀规则:

```
'X' | [流内: I32 xid] | I8 flags | I32 xid | I64 lsn | CString prefix | I32 content_len | content 字节
```

| 字段 | DDL 事件约定取值 |
|---|---|
| flags | bit0 = transactional **恒置 1**——'X' 仅事务性发射(§3.3);客户端校验 bit0=1,不符即协议错位 fail-fast;其余位预留 0(为将来"非事务性 DDL 通知"形态保留扩展位) |
| xid | I32,产生该 DDL 语句的**(子)事务 xid**——取值规则与 DML 流内前缀对齐:DDL 处于 SAVEPOINT 子事务时给 subxid、否则给顶层 xid。两个作用:①子事务回滚的剔除过滤由此生效(与 DML 的 StreamAbort 过滤同构);②消息自包含——顶层路径无流内前缀时,下游凭单条消息即可定位事务归属 |
| lsn | I64,DDL deparse 记录写入 WAL 的 **start LSN**(消息自身位点,非事务 commit lsn)——下游定位/幂等锚点,与 'M' 的 lsn 字段同源语义 |
| prefix | 约定恒为 **`deparse`**——CDC 侧按此识别 DDL 事件;命名源自内核侧 DDL 反解析(deparse)基建。内核未来其他扩展用途可换 prefix,客户端遇未知 prefix 按 content_len 整条跳过(self-delimiting,流不断) |
| content | UTF-8 文本,k=v 结构(§3.2) |

**流内前缀(传输封装层,非消息字段)**:'X' 遵守 §2.1 块内前缀规则——流式块内类型字节后前置 I32 xid,取值 = **顶层事务 xid**(与 'M' 的前缀取值规则一致)。前缀与消息体 xid **互补而非冗余**:前缀 = 顶层归属(封装层),体 xid = 子事务归属(消息内容;顶层路径无前缀时兜底自包含)。

**决策注记(2026-09-14,为何不复用 'M')**:'M' 的语义已被 `pg_logical_emit_message` 占据,生态客户端(Debezium 等)按既有语义处理;DDL 事件用独立类型字节使解码端明确路由,且不与原版 PG 的 `messages` 选项(仅门控 'M')互相牵连。'X' 与 'M' 线格式同构(仅多消息体 I32 xid),内核发射/客户端解码均按 'M' 模板平移。

**门控(待内核侧定)**:建议新增 START_REPLICATION 选项如 `ddl_messages=on`(**仅 true 时传参**,条件传参模式同 §1.3 `binary` 选项),默认不发射 'X'——否则任何按 §2 十九种消息实现的原版 PG 客户端(未感知本扩展的消费者)收到 'X' 即以未知类型字节报错死亡。'X' 字节存在上游占用风险(未来原版 PG 版本可能引入新类型字节,PG 17 曾给 'A' 附加字段即为例证),该开关同时是撞车缓解。

### 3.2 content 载荷:k=v 文本

约定字段(键集不完整,演进中):

| 键 | 语义 | 示例 |
|---|---|---|
| `ddlSql` | DDL 语句原文(含结尾分号) | `ddlSql=CREATE TABLE t_ddl_test_2(id int);` |

- **解析约定:第一个 `=` 之后全部为值**——SQL 值内再出现 `=`、引号、换行均不歧义;内核侧约束 key 区不得再引入 `=`
- **全部 DDL 形态(CREATE/ALTER/DROP、索引、序列等)同格式发射**(约定)
- 其余键位(目标表/对象类型/操作类别等)待内核侧定型后在此补充
- **渲染层注记**:2026-09-14 CDC/内核设计对齐——content 原文即 `ddlSql=<SQL>` 形态(此前依据预期渲染行反推,曾疑 `ddlSql=` 为渲染器标签而非线格式原文;同源样例中的 `public.t_ddl_test` 表名已确认属渲染层信息);内核发射侧拼 content 的代码或原始 'X' 帧 payload 核对后在此销注

### 3.3 交付语义(预期形态)

以下交付形态与语义按设计约定推演,均未经 DDL 场景实际验证。**CDC 引擎当前尚未内建 'X' 解码**——下方接入改造点完成后,交付语义由既有桶记账/回放/确认路径天然覆盖('X' 与 DML/'M' 同走一条路径):

- **事务性交付(约定)**:'X' 计为所属事务的一个变更单元(`changes=1`),随事务边界完整交付(TXN-BEGIN 头行 → 逐变更行 → TXN-END 尾行);流式路径下消息处于流式块内(带 I32 xid 前缀)
- **与 DML 同等语义(约定,接入后由引擎机制覆盖)**:at-least-once、按事务 End 确认、事务回滚不交付、aborted 子事务剔除(按消息体 xid 过滤)
- **CDC 侧接入改造点(设计清单,尚未实现)**:protocol 层 `PgOutputMessage` 增 DdlMsg record、`PgOutputDecoder` dispatch 加 `'X'` 分支、`decodeSingle` 前缀白名单 7 类扩 8 类(M/R/Y/I/U/D/T/X)、未知 prefix 按长度跳过;组装器路由 `case 'X'` 轻窥入桶(同 'M' 路径,无 relation oid);回放器可回放白名单 I/U/D/T/M 扩 'X'、消息体 xid 参与子事务过滤;`ConsoleRenderer` DDL 变更行 INFO;connector 模块协议层 1:1 同步

预期输出样例(流式路径——NORMAL/STREAMED 两条路径均为正常交付路径,此处仅演示流式形态;变更行为 CDC 渲染层预期形态,实现时定型,**线格式以 §3.1/§3.2 为准**):

```
STREAM-STOP
STREAM-COMMIT     xid=10 commitLsn=0/1B9B490
TXN-BEGIN xid=10 kind=STREAMED gid=null commitLsn=0/1B9B490 commitTs=2026-09-14T03:37:23Z changes=1
  [1] DDL        xid=10 lsn=0/1B9B488 sql=CREATE TABLE t_ddl_test_2(id int); [streamed xid=10]
TXN-END   xid=10
```

- **流式形态不特殊化(2026-09-14 内核侧已明确)**:'X' 是否处于流式块由所属事务的整体驱逐形态决定,与其他消息一致——遵守原版 PG 的流式驱逐记账(全局 `rb->size` 越过 `logical_decoding_work_mem` 才驱逐):纯 DDL 小事务走 NORMAL 路径(Begin..Commit 包裹,'X' 无流内前缀);巨量 DML + DDL 的混合大事务走 STREAMED 路径('X' 带流内前缀)。**不存在"DDL 事务强制流式/提前交付"的设计意图**——早期样例按 STREAMED 形态呈现系特定构造下的推演,非设计约定
- **待验证清单(全部未验证)**:①流式路径交付形态(样例本身)②普通事务路径(kind=NORMAL)交付形态——含纯 DDL 小事务按驱逐记账不触发流式的核对 ③DDL 事务回滚不交付 ④crash/重启后 DDL 事务按 at-least-once 重发 ⑤非事务性发射是否存在(当前仅按事务性理解,flags bit0 恒 1)⑥发射门控开关形态(§3.1 建议 `ddl_messages` 独立选项,待内核定)⑦全部 DDL 形态同格式(CREATE/ALTER/DROP、索引、序列)⑧content 键集经原始 'X' 帧确认(§3.2 渲染层注记)⑨流内前缀取值(顶层 xid)与消息体 xid(子事务场景取 subxid)经实测核对 ⑩上游 'X' 字节占用巡检(原版 PG 基线升级时核对 §2 类型字节清单)

---

延伸阅读:时序不变量与排障对照、PG 17/18 逐文件差异审计、binary typsend 解码矩阵等实现级细节,见根 `README.md` 与 `vb-stream-engine` 各包内 `CLAUDE.md`(protocol / replication)。
