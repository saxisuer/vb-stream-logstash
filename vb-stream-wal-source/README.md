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
cp src/docker/walconfig-pg.properties ./walconfig.properties   # 配置文件形态（也可走下方 -D 形态）
java -cp "vb-stream-wal-source/target/classes:$(cat vb-stream-wal-source/target/cp.txt)" \
     org.vastdata.vbstream.walsource.Main
```

**配置三层合并**（2026-10-12 起）：工作目录 `walconfig.properties` 基础值（键名与
`-D` 参数完全同形——本 README 键表直接照抄；白名单外键 WARN 忽略）→ `-Dvb.wal.*`
系统属性同键覆盖 → 门面默认值兜底。默认文件不存在即纯 `-D` 形态（与文件支持前行为
一致）；`-Dvb.config=<路径>` 整份指定外部文件（指向不存在 fail-fast）。模板：
`src/docker/walconfig-pg.properties`（本档）/ `walconfig-og.properties`（openGauss 档）。

（macOS/Linux classpath 分隔符 `:`，Windows 为 `;`。无需 `--add-opens`——本模块零
Chronicle 依赖。运行期每 10s 打一行 `smoke:` 周期统计 INFO——消费前沿 LSN + 计数器 +
census 前 3 形态，state 启用时附 `ckpt=`/`adv=` 检查点与槽推进观测，DML 面启用时附
`dml=[buckets=N rows=M partMiss=K]` 发射计数与 OG 分区链断跳过计数（vanilla 恒 0，
M3）；事务块（TXN-BEGIN/逐行/TXN-END）走 CDC 专用 logger
`org.vastdata.vbstream.walsource.cdc`；Ctrl-C 优雅退出含最终 best-effort 检查点 + 槽
推进；启动失败 exit 1。）

## openGauss 5.0.0 快速开始（M1 catalog + M2 DML 面 + M3 A 档/分区捕获）

catalog 同步 + WAL 接收（M1 面）+ **DML 输出面（M2 落地——`vb.wal.dml` 缺省 true 即装配，
兼容模式护栏先探测；v2 六场景经双路对拍全量验收，能力与限制见下文"M2 DML 面能力与
限制"）** + **M3：A 兼容模式库（Oracle 风格）开门 + 分区表 DML 正常捕获**（分区行以
母表身份输出；详见下文 M3 条目）；存储引擎只做 ASTORE（UStore/列存/段页式/bucket 表
识别即启动期 ISE），仅单机/无 replconninfo 形态。**DML 面支持 PG/A 兼容模式库**
（`DBCOMPATIBILITY 'PG'` 或 `'A'`——M3 起 A 档放行）：Main 冒烟默认注入 DML 输出面，
在白名单外档位库（B 档/'C' 档）上启动期 ISE 附档位与 M4 指引；仅需 catalog 同步面可显式
`-Dvb.wal.dml=false`（catalog 面与兼容模式无关，任何档位可用）。
起容器与运行（配置文件形态——零 `-D`；Windows 宿主机无 pg_receivexlog，模板已选
`direct` 通道）：

```bash
docker run -d --name og-m1 -e GS_PASSWORD=openGauss@123 -p 15700:5432 opengauss/opengauss:5.0.0   # 等就绪 ~60s
docker exec og-m1 su - opengauss -c '/usr/local/opengauss/bin/gsql -d postgres -p 5432 \
     -c "CREATE DATABASE vbpg DBCOMPATIBILITY '\''PG'\''"'   # PG 兼容档库（DML 面前提；M3 起 'A' 档亦可）
mvn -q -pl vb-stream-wal-source compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
cp src/docker/walconfig-og.properties ./walconfig.properties
java -cp "vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" \
     org.vastdata.vbstream.walsource.Main
# （Windows classpath 分隔符 ';'，macOS/Linux 为 ':'；-Dvb.wal.* 同键可临时覆盖文件值）
```

等价的纯 `-D` 形态：

```bash
java -cp "vb-stream-wal-source/target/classes:$(cat vb-stream-wal-source/target/cp.txt)" \
     -Dvb.wal.port=15700 -Dvb.wal.db=vbpg -Dvb.wal.user=gaussdb -Dvb.wal.pass=openGauss@123 \
     -Dvb.wal.og.jdbc.prefix=jdbc:opengauss: \
     -Dvb.wal.state.dir=/tmp/wal-source-state \
     org.vastdata.vbstream.walsource.Main
```

**认证二选一**（`vb.wal.og.jdbc.prefix` 键，缺省 `jdbc:postgresql:`——是否 OG 档在建连
后才知道，缺省换前缀会反噬 PG 档）：`jdbc:opengauss:` = opengauss-jdbc 走 sha256（镜像
默认 `password_encryption_type=2`，**OG 部署推荐、零服务端改动**）；`jdbc:postgresql:` =
vanilla pgjdbc 走 md5，须服务端预埋 `password_encryption_type=1` + 重设密码 + pg_hba
md5 行（运维自担）。

**pg_hba replication 行是硬前提**：openGauss pg_hba 的 `all` 不匹配 replication 伪库，
replication 用户必须显式一行（缺省镜像恰已含同形行；自建部署须自查）：

```
host replication gaussdb 0.0.0.0/0 md5
```

槽自动建/复用（OG 变体 `pg_create_physical_replication_slot(slot, false)`——第二参是
dummy_standby 非 immediately_reserve）；收流通道 `-Dvb.wal.og.source=file|direct`
（缺省 file=sidecar pg_receivexlog 落目录 tail；direct=裸 v3 协议直连，两通道 IT 对拍
记录序列全等）。OG 五键全表见模块 `CLAUDE.md` 配置面节。

**M2 DML 面能力与限制 + M3 增量**（详版见模块 `CLAUDE.md`"openGauss 5.0.0 档 M2/M3"节）：

- ✅ **v2 六场景全量**：基础 DML 六形态（含 COPY 批量 MULTI_INSERT 真几何）/17 类型边界值/
  TOAST 三形态 + 重启 unchanged/DDL-in-txn as-of/2PC 四形态/中途停续——经**双路对拍**
  验收（基准路 = opengauss-jdbc 逻辑流 proto v1 + 独立归一化器；wal 路 = 本模块 DML 面；
  按 xid 交集逐行 diff 空）；**M3 起上述场景在 PG/A 双兼容档库参数化同套复验**。
- ✅ **M3：A 兼容模式库（Oracle 风格）全量开门**——v2 全场景在 A 档库复验全绿（双路
  对拍同套参数化）；A 档 `date` 关键字底层即 timestamp（存时段零损失），其余 16 类型
  双档逐字面同形；`''` 空串入库即 NULL（服务端语义，两路一致）。
- ✅ **M3：分区表 DML 正常捕获**（M2 的"有痕跳过"语义退役）——pg_partition 第四 watched
  字典（StateStore v4），分区行以**母表身份**输出（`schema.母表名` + 母表列序，与 OG
  pgoutput 逻辑流同形）；**白名单双匹配**（有意与 vanilla 分叉）：写**母表名覆盖全部
  分区**（基准形态），写分区名只命中该分区路由的行（兼容增强）；流内 ADD/DROP/MERGE/
  SPLIT/EXCHANGE PARTITION 竞速 as-of 捕获；跨分区 UPDATE 迁移 = DELETE+INSERT 两行
  （vanilla 语义自然）；仅链断/DROP 后迟到行保留 fail-safe 有痕跳过（WARN 一次 +
  冒烟行 `partMiss=` 计数）。
- ⚠️ **attcmprmode 列压缩在行存面 DDL 天然不可达**：服务端三路 DDL 语法全拒（普通表
  attcmprmode∈{0,127}）——DELTA/DICT/PREFIX/NUMSTR 四算法解压纯函数已备（零接线）；
  列存/段页式/bucket/UStore 长期非目标（ASTORE 限定——A 档列存表 ORIENTATION=COLUMN
  启动期 ISE）。
- ❌ **B/C 兼容档库整库不支持**（启动期 ISE 附档位与 M4 指引——DML 面仅
  DBCOMPATIBILITY 'PG'/'A' 库）；MultiXactId 变体未锚（到达即错位 ISE）；页字典压缩表
  （MULTI_INSERT isCompressed≠0）ISE。

## VastBase G100 快速开始（V3.0 / OG 6.0.0 内核——第四布局档）

VastBase G100 V3.0 报 `server_version_num=90204` 且 `version()` **无 openGauss/VastBase
branding**——判别走第二探测面（`pg_class.relname` attlen=128，`NAMEDATALEN 64→128` 是
VastBase 词典几何的签名）。**前置**：①`wal_level=logical`（DML 面前提，catalog 面可
`-Dvb.wal.dml=false` 放宽到 replica 档）；②连接用户有 REPLICATION 权限（rolreplication=t）
且 pg_hba 含 replication 放行行；③**`vb.wal.og.jdbc.prefix=jdbc:opengauss:`**（VastBase 档
装配走 OG 分支，opengauss-jdbc 前缀对 md5/sha256 认证兼容——真库实测 md5 码 5 一次过，无
OG 容器的码 11 死角）；④收流推荐 `-Dvb.wal.og.source=direct`（裸 v3 直连，无 sidecar 依赖
——真库全链验证形态）。

```bash
mvn -q -pl vb-stream-wal-source compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" \
     -Dvb.wal.host=<vastbase-host> -Dvb.wal.port=<port> -Dvb.wal.db=test_a \
     -Dvb.wal.user=test -Dvb.wal.pass=<password> \
     -Dvb.wal.slot=vb_wal_source \
     -Dvb.wal.og.source=direct -Dvb.wal.og.jdbc.prefix=jdbc:opengauss: \
     -Dvb.wal.state.dir=./vast-wal-state \
     org.vastdata.vbstream.walsource.Main
# （Windows classpath 分隔符 ';'，macOS/Linux 为 ':'；能力/限制与词典几何差异详见模块
#  CLAUDE.md "VastBase G100 档"节——A 档 date→oradate 已归一渲染，uuid/numeric 输出
#  方言是记档差异面）
```

手动档真库 IT（`VastCatalogSyncIT`/`VastDmlParityIT`/`VastLifecycleIT`，常规 `mvn test`
全 skip；连接参数 `-Dvb.it.vastbase.{host,port,db,user,pass}` 可覆盖）：

```bash
mvn test -pl vb-stream-wal-source -Dtest='Vast*IT' -Dvb.it.vastbase=true
```

## 离线回放模式（VastBase 档，2026-10-15）

库不可访问（宕机/网络隔离）时凭**本地检查点 + WAL 段快照目录**离线续传解析 DML 直至
WAL 耗尽。**前提：须先在线运行产生检查点**——`vb.wal.state.dir` 已有 VastBase 档
（pgVersion=6）检查点；冷启动离线不支持（检查点缺失/损坏/档位不符启动期 ISE 附"先在线
运行"或档位指引）。段目录 = 从数据库侧拿到的 WAL 段文件（拷 pg_xlog 或复制流按 LSN
落段），启动期审计三项（起点段在场/段号连续/tli 一致）任一不过即 ISE fail-fast。三键
用法（`vb.wal.mode` 是 `-D` 专属分派键，不进 walconfig.properties 白名单）：

```bash
java -cp "vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" \
     -Dvb.wal.mode=offline \
     -Dvb.wal.offline.dir=/path/to/wal-segments \
     -Dvb.wal.state.dir=/path/to/wal-state \
     org.vastdata.vbstream.walsource.Main
# 可选 -Dvb.wal.offline.endLsn=2/071BF288（交付上界——到达即提前停，缺省清单读尽即 EOF）
# EOF 读尽即优雅退出（exit 0，最终检查点同目录落盘）；state 三键与 DML 两键
# （tables/dml）照常生效，在线专用键（host/port/db/user/pass/slot/og.*）忽略；
# 库修复后同 state 目录直接在线运行即续传——检查点同目录续写，离线/在线双向互通
```

能力/限制（仅 VastBase 档；TOAST INSERT 空面窄缝渲哨兵等已知限制）详见模块
`CLAUDE.md`"离线回放档"节。

配置键全表（`-Dvb.wal.*` 十一基础键 + openGauss 档五键 + 离线回放两键，默认值即下表；
单一来源 `WalSource`/`StateConfig`；`vb.wal.mode` 是 Main 层分派键不入本表）：

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
| `vb.wal.og.source` | `file` | **OG 档**收流通道：file=sidecar 落目录 tail / direct=裸 v3 直连（仅 OG 档读本键） |
| `vb.wal.og.rx.mode` | `managed` | OG file 通道 sidecar：managed=自拉起 pg_receivexlog / external=只读既有目录 |
| `vb.wal.og.rx.dir` | `wal-og-queue` | OG file 通道落盘目录（managed 亦作 `-D` 参数） |
| `vb.wal.og.rx.bin` | `pg_receivexlog` | OG managed 模式可执行文件（走 PATH） |
| `vb.wal.og.jdbc.prefix` | `jdbc:postgresql:` | OG 档 SQL 会话驱动前缀（`jdbc:opengauss:`=sha256 推荐；缺省保 pgjdbc 防 PG 档反噬，见上文认证二选一） |
| `vb.wal.offline.dir` | 缺省 = 在线档 | **离线回放档**段快照目录（`vb.wal.mode=offline` 时必填，缺失启动期 ISE；见上文"离线回放模式"） |
| `vb.wal.offline.endLsn` | 缺省 = 无上界 | 离线回放可选交付上界（十六进制 LSN 文本如 `2/071BF288`，到达即提前 EOF；非法文本启动期 fail-fast） |

## 架构

设计全文见 [docs/superpowers/specs/2026-10-05-wal-source-module-design.md](../docs/superpowers/specs/2026-10-05-wal-source-module-design.md)
（v1）与 [docs/superpowers/specs/2026-10-06-wal-source-v2-design.md](../docs/superpowers/specs/2026-10-06-wal-source-v2-design.md)
（v2 DML 面；先导实验 [docs/wal-direct-decode-spike.md](../docs/wal-direct-decode-spike.md)，
9 场景 27 条发现）；openGauss 档 M1/M2/M3 设计见
[2026-10-11](../docs/superpowers/specs/2026-10-11-wal-source-opengauss-m1-design.md) /
[2026-10-12](../docs/superpowers/specs/2026-10-12-wal-source-opengauss-m2-design.md) /
[2026-10-13](../docs/superpowers/specs/2026-10-13-wal-source-opengauss-m3-design.md)
spec（M3 实测锚定 [docs/wal-source-opengauss-m3-spike.md](../docs/wal-source-opengauss-m3-spike.md)）。一句话：五个面向接口的 v1 组件——版本化布局描述符 `WalLayout`
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
slf4j-api + lz4-java（TOAST lz4 压缩值解压）+ logback-classic（runtime 绑定——
直接 `java -cp` 跑冒烟 Main 即有日志；宿主自带绑定时以其配置为准），组件细节与
坑位见模块 `CLAUDE.md`。

## 验收

- **catalog 面**：对抗性对拍 IT——autovacuum 拉满 / 并发 DDL / 随机 CHECKPOINT 干扰下
  执行 DDL 场景序列，快照与 JDBC REPEATABLE READ 实查**逐行全等**（ctid / relfilenode /
  toast 映射 / dropped / 列序），PG 17/18 双版本矩阵。
- **DML 面（v2）**：双路对拍——同一容器同一批 DML，engine 逻辑解码路（in-process 复刻
  engine Main 装配）与 wal 直解路各自独立槽位捕获输出，按 xid 交集**逐行 diff 为空**；
  六场景（基础 DML 六形态 / 17 类型边界值 / TOAST 三形态 + 重启 unchanged / DDL-in-txn
  as-of / 2PC 四形态 / 中途停续）+ CREATE SCHEMA + 干扰矩阵。
- **openGauss 档（M1 + M2 + M3）**：`opengauss/opengauss:5.0.0` Testcontainers 十组
  IT——M1 五组（catalog 末态对拍[65 轮干扰 + 宽 DDL，断言面 vanilla 同源逐行全等] +
  生命周期[检查点续传/损坏回落/门面全装配] + 实例存活红线[5 轮双 kill 向量：服务端
  walsender 终止 + 客户端裸 socket 关闭，每轮 SELECT 1 + 日志无 gaussdb exit] + 黄金
  wire 对拍[真流上 'k' 恰 33B / 65B 'r' 反馈 peer_role=3/peer_state=7 私有帧形] + 双通道
  对拍[file vs direct 同窗口记录序列全等]）+ **M2 四组（对拍冒烟 / v2 六场景 OG 档全量
  对拍 / attcmprmode 护栏 + 分区捕获 / B 档兼容护栏）——基准路 = opengauss-jdbc 逻辑流
  + 独立归一化器，认证走专用角色 m2par_base + pg_hba sha256 行（gaussdb 走复制流撞
  码 11 的认证死角绕开）** + **M3（分区专项十六用例[range/list 捕获对拍/白名单四形态/
  流内 DDL 竞速/EXCHANGE 失效/跨分区迁移] + A 档全量复验[13 场景 × PG/A 参数化 + date
  专形] + A 档列存 ISE，2026-10-13）**。
- **VastBase G100 档（vastbase 2026-10-13）**：手动档真库 IT 三组（`-Dvb.it.vastbase=true`
  ——非容器长活真库，常规 `mvn test` 全 skip）——catalog 四字典干扰对拍逐行全等 + 门面
  判别链（relname 探测 → `WalLayoutVVastbase` → v6 检查点排他锚）/ DML 双路对拍 15 场景
  （v2 六场景 + A 档 date 专形 + COPY 批量 MULTI_INSERT 真几何——基准路 = opengauss-jdbc
  逻辑流 + `OgParityBaseline` 复用，认证 md5 码 5 一次过无 OG 码 11 死角）/ 生命周期 3
  （检查点续传两轮启停对拍 / state 截断回落全新引导 / 门面全装配 direct 通道两轮启停）。
- **离线回放档（2026-10-15，仅 VastBase 档）**：离线单测 +14（StateStore pgVersion 回读 /
  FileWalSource 静态目录档六用例——确定性 EOF·三审计 fail-fast·endLsn 截停·partial 前沿 /
  CatalogOfflineTest 无库装配三用例 / 配置面 +4）+ `VastOfflineIT` 真库手动档 4 用例
  （在线/离线按 xid 交集逐块全等 + 离线→在线双向回切 + 崩溃再离线 + endLsn 截停）。
- `mvn test -pl vb-stream-wal-source` 单命令全跑（535 用例 = 离线 412 + Testcontainers
  IT 99——PG 41 + openGauss 58（需本机 Docker）+ VastBase 手动档真库 24（全 skip，
  `-Dvb.it.vastbase=true` 才跑——catalog 对拍 2 / DML 对拍 15 / 生命周期 3 / 离线回放
  4[VastOfflineIT，2026-10-15]）；Windows 开发机另有环境性存量 7 例已知记档——
  pg_receivexlog 文件通道族[FileWalSourceTest 6 + OgDualChannelParityIT 1]，非回归）。

## 场景支持速览（详版矩阵见模块 CLAUDE.md）

**✅ 支持（双路对拍验收过）**：INSERT/UPDATE/DELETE（`wal_level=logical` + REPLICA IDENTITY
FULL）、事务组装（交错/SAVEPOINT/回滚零输出/2PC 四形态）、TOAST 全形态（external 重组/
pglz 与 lz4 解压/行内压缩/未变列 `<toast-unchanged>`/重启回查）、17 类型矩阵、事务内 DDL
as-of、RENAME/TRUNCATE/CREATE SCHEMA、检查点续传/损坏回落/at-least-once 重发、PG 17+18
双版本。

**✅ 支持（openGauss 5.0.0 档，M1 catalog + M2 DML[PG/A 兼容库]）**：catalog 同步（引导 +
ctid 重放 + 末态对拍全等）、双通道收流（file/direct 对拍一致）、检查点续传与槽推进、
实例存活红线；**DML 面 v2 六场景全量**（基础 DML 六形态含 COPY MULTI_INSERT/17 类型/
TOAST/DDL-in-txn as-of/2PC/中途停续——双路对拍验收，见上文"M2 DML 面能力与限制"）；
**分区表 DML 捕获**（母表身份输出、白名单母表名/分区名双匹配、流内 DDL 竞速 as-of——
M3）；**A 兼容档库**（Oracle 风格——v2 全场景双档参数化复验，M3）。

**✅ 支持（VastBase G100 V3.0 档，第四布局档）**：与 OG 档同构的 catalog 同步（四字典
[含 pg_partition 分区捕获]）+ direct 通道收流 + 检查点续传/损坏回落 + DML 面（六场景 +
COPY 批量 + A 档 date 专形经 oradate→timestamp 归一——双路对拍逐行全等，真库验收）；
判别 = relname 探测（version() 无 branding 的三方撞号第二面）。已知差异面：uuid/numeric
输出函数方言（对拍矩阵避开 + 记档）、file 通道未专项验证。

**✅ 支持（离线回放档，2026-10-15——仅 VastBase 档）**：库不可访问时凭本地检查点 +
WAL 段快照目录离线续传 DML（`-Dvb.wal.mode=offline`——启动期目录审计三项 fail-fast +
读尽确定性 EOF + 可选 endLsn 上界；检查点同目录续写，离线/在线双向互通——含崩溃再离线
与回切，真库手动档 IT 钉）。冷启动离线与 OG/vanilla 档离线非目标（ISE 附指引）。

**❌ 不支持（触发时的行为）**：
- **replica 形态 UPDATE**（`wal_level=replica`）→ 行级跳过 + WARN + 计数——系统性缺行，
  **DML 面需 `wal_level=logical`**
- **压缩 FPW** → ISE fail-fast——运维前提 `wal_compression=off`
- **矩阵外类型**（enum/jsonb/域等）→ `0x` 十六进制降级 + WARN，流不断
- **VACUUM FULL/CLUSTER** → 字典跟踪但 DML 语义未承诺
- **dropped 列** → wal `∅` vs engine 不发——已知分叉面
- **openGauss 档**：UStore/列存/段页式/bucket 表 → 启动期/流内 ISE（只做 ASTORE，
  含 A 档列存 ORIENTATION=COLUMN）；B/C 兼容档库 → 启动期 ISE 附 M4 指引（DML 面仅
  PG/A 档库）；分区链断/DROP 后迟到行 → fail-safe 有痕跳过（WARN 一次 + `partMiss=`
  计数，不砖流）；attcmprmode 列压缩 → 行存 DDL 天然不可达（服务端
  拒绝）；页字典压缩表（MULTI_INSERT isCompressed≠0）与 MultiXactId 变体 → ISE/未锚；
  挂 replconninfo 的 HA 形态 → 不在支持面

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
