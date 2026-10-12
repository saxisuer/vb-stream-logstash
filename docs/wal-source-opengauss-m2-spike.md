# openGauss M2 逻辑解码路可用性探测（spike 结论）

PARITY=LOGICAL（选型：**路② opengauss-jdbc 5.0.0-og `logical()` in-process 对拍器**；路① pg_recvlogical 备选 sidecar；路③ vanilla pgjdbc 不可用）

- 日期：2026-10-12；容器 `og-plan`（opengauss/opengauss:5.0.0，端口 15700）；探针代码 throwaway 在 /tmp/ogprobe（不入库）
- 裁定含义：M2 验收采用**双路对拍**（wal 直解路 vs 逻辑解码基准路），与 v2 在 PG 17/18 上的 `DualPathParityIT` 同形态；基准路 = opengauss-jdbc 逻辑流收 OG pgoutput 二进制帧 + 归一化器对成 v2 TXN 块

## 0. 基础面（Step 1）

| 项 | 结果 |
|---|---|
| wal_level | logical（镜像默认） |
| 内核版本 | **9.2.4**（`getMetaData().getDatabaseProductVersion()`——OG 内核基线 PG 9.2.4，proto v2 流式/two_phase 天然不存在，M2 对拍不需要） |
| 槽函数 | `pg_create_logical_replication_slot` / `pg_drop_replication_slot` / `pg_logical_slot_{get,peek}{,_binary}_changes` 全在（签名同 vanilla；返回 record 列名与 vanilla 不同，`SELECT * FROM` 取值） |
| 插件 .so | `pgoutput.so`、`test_decoding.so`、`mppdb_decoding.so`（`/usr/local/opengauss/lib/` 下，**不在** vanilla 的 lib/postgresql/ 路径） |
| pgoutput 建槽 | `pg_create_logical_replication_slot('m2spike','pgoutput')` 成功 |
| publication | `CREATE PUBLICATION ... FOR ALL TABLES` 可用且 pgoutput 解码期真实消费（见坑 2/3） |
| SQL 层 peek | pgoutput：**失败**（`ERROR: output plugin cannot produce binary output`——SQL 接口不可用，须走 START_REPLICATION）；test_decoding / mppdb_decoding：可用（文本/JSON，见 §备选） |

测试库：`CREATE DATABASE m2pg DBCOMPATIBILITY 'PG'`，表 `t1(id int primary key, name text, val numeric)`。

## 1. 三路探测记录（Step 2）

### 路① 容器内 pg_recvlogical —— **可行（备选）**

`pg_recvlogical -d m2pg -S m2spike2 --start -f - -o proto_version=1 -o publication_names=m2pub -n`

- 起流：成功（`initiated streaming`）；建槽可由 gsql 预先完成或 `-P pgoutput` 自建（该二进制默认插件是 **mppdb_decoding**，OG 改动）
- 收变更：完整事务帧序列 `B → R → I/U/D → C` 逐字节收到（hex 证据见 §2，与路② 完全同构）
- **坑 A：客户端在每帧后追加 `0a`（换行）**——路② JDBC 的 CopyData 载荷无此字节，判定为 OG pg_recvlogical 自己按文本行写文件的行为；sidecar 形态解析时须剥尾部 `\n`
- **坑 B：stderr 状态反馈行会与 stdout 数据流交叠**（`written_lsn = ...`），采样要分离两个流
- 断开行为：容器内 `kill -9` 客户端进程 → 槽 4s 内释放（active=f）、**实例存活**（逻辑路无物理路的实例雷，实证）
- 注意：`docker exec` 宿主侧 kill 杀不死容器内进程（无 TTY 转发），模拟猝死要 `docker exec ... kill -9 <容器内pid>`

### 路② opengauss-jdbc 5.0.0-og `logical()` —— **可行（选型）**

`jdbc:opengauss://host:15700/m2pg?replication=database&assumeMinServerVersion=9.4`（驱动 `org.opengauss.*` 命名空间，API 为 pgjdbc 镜像：`PgConnection.getReplicationAPI().replicationStream().logical().withSlotName(..).withSlotOption("proto_version","1").withSlotOption("publication_names",..).start()`；`readPending()` 非阻塞 drain、`setAppliedLSN/setFlushedLSN + forceUpdateStatus` 回传——与 engine 现有会话模式同构）

- 建槽/起流：成功；服务端日志 `received wal replication command: START_REPLICATION SLOT m2jdbc LOGICAL 0/0 ("proto_version" '1', "publication_names" 'm2pub')`——**walsender.cpp:471 的 `replication=database` 硬路由对逻辑流恰是正确路由，实证**
- 收变更：消息级干净交付（一条 CopyData = 一个完整 pgoutput 帧，ByteBuffer），样例见 §2
- 45s 浸润（DML@5s/25s + 20s 空闲段，statusInterval 10s）：7 帧全数正常收达；1s statusInterval 的 45s 纯空闲对照亦全程存活（服务端日志 walsender 生命周期与客户端会话严格重合）
- 断开/close：实例存活、槽释放；**`stream.close()` 恒抛 `Database connection failed when ending copy`（EOF 竞态）**——服务端在 close 时刻正常收尾（日志核对 shutdown 时间=close 时刻），异常属驱动侧收尾竞态，T6 吞掉即可
- **`readPending()` 不暴露服务端断流**（连接死后持续返回 null 而非抛错）——T6 对拍器须沿用 M1 的重连护栏模式（周期性活性探测/超时判死）

### 路③ vanilla pgjdbc 42.7.13 `logical()`（对照组）—— **不可用**

- 连接/建槽/START_REPLICATION：成功（md5 认证、v3 协议兼容）
- **`readPending()` 首读即 `FATAL: insufficient data left in message`**（服务端报错）——OG 与 vanilla 的复制消息帧长不一致（见 §2：B 帧 xid 宽度等差异），vanilla 驱动的解析/应答与 OG walsender 互不兼容。实例存活，无副作用

## 2. 归一化器设计输入（Step 3）

### OG pgoutput 帧格式（实测锚定，proto_version=1）

样例事务（INSERT + UPDATE + DELETE，xid 13853）：

```
MSG#1 len=25 type=B  hex: 42 00 00 00 00 02 90 c8 e8 | 00 03 00 9b 9d b6 a4 bc | 00 00 00 00 00 00 36 1d
MSG#2 len=57 type=R  hex: 52 00 00 60 06 70 75 62 6c 69 63 00 74 31 00 64 00 03
                        01 69 64 00 00 00 00 17 ff ff ff ff 00 6e 61 6d 65 00 00 00 00 19 ff ff ff ff
                        00 76 61 6c 00 00 00 06 a4 ff ff ff ff
MSG#3 len=31 type=I  hex: 49 00 00 60 06 4e 00 03 74 00 00 00 01 37 74 00 00 00 03 65 74 61 74 00 00 00 04 37 30 2e 37
MSG#4 len=45 type=U  hex: 55 00 00 60 06 4b 00 03 74 00 00 00 01 31 6e 6e 4e 00 03 74 00 00 00 01 31 74 00 00 00 06 61 6c 70 68 61 32 74 00 00 00 04 32 2e 33 34
MSG#5 len=16 type=D  hex: 44 00 00 60 06 4b 00 03 74 00 00 00 01 36 6e 6e
MSG#6 len=26 type=C  hex: 43 00 00 00 00 00 02 90 c8 e8 00 00 00 00 02 90 c9 48 00 03 00 9b 9d b6 a4 bc
```

逐帧布局（与 vanilla pgoutput 的差异加粗）：

| 帧 | 布局 |
|---|---|
| B (25B) | `'B'` + finalLsn Int64 + commitTs Int64 + **xid Int64**（vanilla 为 Int32，B 帧 21B——**两处帧级差异之一**；高 4 字节恒 0，`txid_current()` 对锚验证 xid=末 8 字节） |
| R | `'R'` + relId Int32 + ns cstring + relname cstring + replica-identity 字节（'d'）+ Int16 ncols + 每列 {flags Int8（1=复制身份列）+ name cstring + typeId Int32 + typmod Int32}——**与 vanilla 逐字段等同**（int4=23/text=25/numeric=1700 实测） |
| I | `'I'` + relId + `'N'` + TupleData |
| U | `'U'` + relId + `'K'` + TupleData(old-key) + `'N'` + TupleData(new-tuple)（replica identity default 下 old-key 只含 PK 列，其余列以 'n' 占位） |
| D | `'D'` + relId + `'K'` + TupleData(old-key) |
| C (26B) | `'C'` + flags Int8(0) + commitLsn Int64 + endLsn Int64 + ts Int64——与 vanilla 等同 |
| **TupleData（I/U/D 内）** | **Int16 ncols 前缀** + 每列 kind：`'t'`+Int32 len+文本字节 / `'n'`=NULL / `'u'`=unchanged-TOAST 哨兵。**vanilla 无 ncols 前缀**（列读到消息尾）——两处帧级差异之二 |

值一律 text 形态（length-prefixed UTF-8），与 engine text 模式可直接对拍。

### 映射表：OG pgoutput 帧 → v2 TXN 块

| OG 帧 | 归一化输出 |
|---|---|
| `B`（xid） | `TXN-BEGIN` 行（xid 取末 8 字节 Int64；expectedChanges 不在 B 帧中，按桶内实收条数计） |
| `R`（relId/表名/列定义） | 进对拍器本地 relId→表字典缓存，供后续 I/U/D 行渲染表名/列名（对应 wal 直解路的 as-of 字典角色） |
| `I`/`U`/`D` + TupleData | 逐行变更行：值取 `'t'` 文本、`'n'`→NULL、`'u'`→unchanged-TOAST 哨兵（与 engine pgoutput 'u' 同形；UPDATE 行 = old-key 列（可仅 PK）+ new-tuple 全列） |
| `C`（endLsn） | `TXN-END` 行（endLsn 取第二 LSN 字段，即事务提交终点） |
| （无帧） | abort 事务不发帧（proto v1 语义），与 v2 过滤 aborted 一致；DDL/TRUNCATE 面 M2 不对拍 |

### 备选格式（记录，不采用）

- `mppdb_decoding`（OG 自家 JSON 插件，pg_recvlogical 的默认插件）：`{"table_name":"public.t1","op_type":"INSERT","columns_name":[...],"columns_val":[...],"old_keys_val":[...]}` + `BEGIN xid` / `COMMIT xid (at ...) CSN n` 文本行——事务边界/行/键形态齐全，若路② 后续发现硬伤可降级用它走 sidecar（多一个 JSON 归一层）
- `test_decoding`：vanilla 文本形态（`table public.t1: INSERT: id[integer]:3 ...`），同上属备选

## 3. 坑清单（T6/T7 实施必读）

1. **pg_hba 本地 replication 行**：镜像默认 `local all all trust` 不覆盖复制连接——容器内客户端（pg_recvlogical/gsql 本地槽操作）需追加 `local replication all trust` + reload；JDBC 远程走 M1 已配的 `host replication gaussdb 0.0.0.0/0 md5`
2. **publication 必须先于被解码事务存在**（历史快照语义，vanilla 同）：槽/流建在 publication 之前 → 解码旧事务时 `FATAL: publication "m2pub" does not exist` 杀流。IT 时序固定：建 publication → 建槽 → 写 DML → 起流
3. **`publication_names` 必传**：缺省即 `FATAL: publication_names parameter missing`（startup callback 拒绝）——与 vanilla 一致，`proto_version` 仅验过 `"1"`
4. **B 帧 xid 为 Int64、TupleData 带 Int16 ncols 前缀**：不能直接复用 engine 的 `PgOutputDecoder`（帧长两处差异）；归一化器按 §2 布局新写，勿共享解码路径
5. `stream.close()` 恒抛 EOF 竞态（吞掉）；`readPending()` 不暴露断流（需活性护栏）——**实证依据含一次 1.7s 空闲流被服务端关闭的孤例**（无错误日志、现象仅见 walsender shutdown；两次对照组——45s 10s-interval 含 20s 空闲段 / 45s 1s-interval 纯空闲——均未复现，未定因）：T6 活性护栏以防御位处理，不以"流恒存活"为前提
6. pg_recvlogical 逐帧追加 `\n` + stderr 污染（仅路①；选型路② 无此问题）
7. gsql 输出 "total time" 页脚会污染 hex 采样（grep 纯行，沿用调研期经验）
8. vanilla pgjdbc 对 OG 逻辑流首读即 FATAL（勿在 OG 面上尝试 vanilla 驱动）
