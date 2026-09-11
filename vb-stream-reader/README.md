# vb-stream-reader — debezium-embedded 宿主冒烟应用

## 定位与架构

不经 Kafka Connect runtime 的最小 embedded 宿主：`DebeziumEngine.create(io.debezium.embedded.Connect.class)` 工厂建 async 引擎（3.x 唯一实现；Connect 格式直通——`event.value()` 即原始 `SourceRecord`，`event.key()` 恒 null 不用）加载同仓连接器 `PostgresStreamConnector`（模块依赖 `vb-stream-connector-postgres-stream`；`AsyncEngineBuilder` 构造器包私有，必走工厂），`ChangeConsumer` 批回调消费，`markProcessed`/`markBatchFinished` 手动 offset 记账。**输出形态二选一**（`vb.sink.mode`，2026-09-11 起）：`log`（默认，逐条渲染 INFO）或 `file`（CDC 记录落地 VBFG 二进制文件——与 vb-cdc-file-transform 仓 cdc-sink 的消费格式互通，见下节）。依赖方向 reader → connector（引擎模块不受牵连）。定位是**冒烟/联调宿主**，不是生产形态——生产部署走 Kafka Connect（连接器打包安装见 [vb-stream-connector-postgres-stream/README.md](../vb-stream-connector-postgres-stream/README.md)）。

组件六件（细节见模块 `CLAUDE.md`）：

| 组件 | 职责 |
|---|---|
| `ReaderProperties` | **三层合并**配置面（下节），必填校验与 password 打码；`resolveSink` 组装输出形态配置 |
| `SinkConfig` | 输出形态配置（log/file + file 形态的目录/task/滚动参数） |
| `LogChangeConsumer` | 批回调逐条渲染 INFO 到专用 logger + offset 手动记账 |
| `FileChangeConsumer`（file 形态） | 批回调落地 VBFG 文件 + offset 与文件 publish 严格联动（`file` 包） |
| `EngineLifecycle` | `DebeziumEngine.create` 装配 + `engine-run` 线程 + latch/hook 收敛闸门（Main 与 IT 共用） |
| `Main` | 编排主线：配置解析 → 校验（缺失 exit 2）→ 按 sink 形态建 consumer → 起引擎 → await 停机 → 收敛（失败 exit 1 / 正常 0） |

## 配置面：三层合并

```
classpath 的 dbconfig.properties（基础值，随包分发的 src/docker 本地 PG 模板）
  → 系统属性 -Dvb.<debezium 键>=<值>（剥 vb. 前缀覆盖同名项；值空串视为未设）
  → reader 默认值（putIfAbsent 兜底）
```

- **文件键即 Debezium 裸键**（不带 `vb.` 前缀——文件本身是 reader 专属命名空间；**例外是 `sink.*` 前缀键**——归输出形态命名空间，不透传 Debezium）；同键多来源以 `-D` 为准
- **整体换文件**：复制模板为外部文件，`-Dvb.config=<绝对路径>` 指定（免重编译改配置；该键是保留键不进 Debezium props；路径不存在启动期 fail-fast，拒绝静默回落）
- **透传红利**：连接器六个专属项（`slot.streaming`/`slot.two.phase`/`pipe.dir`/`pipe.roll.cycle`/`slot.feedback.interval.ms`/`slot.messages`）与 engine 高级项（`record.processing.order`/`offset.commit.policy` 等）无论写在文件还是以 `-D` 传入均零代码直达——语义真源见连接器 README 配置表

reader 自身注入四项默认：

| 配置项 | 默认 | 语义 |
|---|---|---|
| `connector.class` | 固定 `...stream.PostgresStreamConnector` | 本宿主只服务自研连接器，任何来源的同名项被强制覆盖 |
| `name` | `vb-stream-reader` | 引擎实例名 |
| `offset.storage.file.filename` | `data/reader-offsets.dat`（已 gitignore） | 引擎 offset 文件存储；相对路径按启动工作目录解析，跨 CWD 启动建议配绝对路径；**父目录与空文件由启动期自动创建**（`FileOffsetBackingStore` 自身不建目录，缺失时首次 offset flush 直接 `NoSuchFileException` 且报错深埋引擎线程栈难归因） |
| `offset.flush.interval.ms` | `1000` | offset 落盘周期（`markBatchFinished` 后按此节流） |

必填五项：`database.hostname`/`database.dbname`/`database.user`/`database.password`/`topic.prefix`——缺失打印用法 exit 2（写在文件或以 `-Dvb.*` 传入均可）。`database.password` 在启动日志中打码为 `********`。

## 运行

前置同引擎 Main：`cd src/docker && docker compose up -d` 起 PG 18（localhost:55432），随后**零 `-D` 参数即可起**（默认读 classpath 的 `dbconfig.properties`）：

```bash
mvn -q -pl vb-stream-reader compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED \
     --add-opens java.base/sun.nio.fs=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "vb-stream-reader/target/classes;$(cat vb-stream-reader/target/cp.txt)" \
     org.vastdata.vbstream.reader.Main
# 临时覆盖单项: -Dvb.slot.name=another_slot  -Dvb.topic.prefix=other  -Dvb.slot.streaming=parallel
# 输出形态切文件落地: -Dvb.sink.mode=file  (可加 -Dvb.sink.data-dir=... -Dvb.sink.roll.max-records=...)
# 整体换文件:   -Dvb.config=/abs/path/my.properties  (文件内裸键 sink.mode/sink.data-dir/...)
```

## file 输出形态（VBFG 落地）

`-Dvb.sink.mode=file`（或配置文件内 `sink.mode=file`）后，CDC 记录落地为 VBFG 二进制文件——
学习自 vb-cdc-file-transform 仓 cdc-capture 的落地链路（源码移植，格式与该项目 cdc-sink
消费端**逐字节互通**，跨网闸场景可直接对接）：

- **不丢数据契约**：文件 publish = `finish()`（FOOTER + CRC32）+ fsync + **原子 rename** 到
  数据目录；`markBatchFinished()`（推进 offset/slot）只在本批有文件 publish 之后调用——
  数据完整落地之前 offset 绝不推进，写文件 IO 失败直接停引擎（offset 不动，重启重放）
- **COMMIT 边界切分**：文件只在事务提交后切分（每份文件自包含完整事务）；条数
  （`sink.roll.max-records`，默认 1000）或时长（`sink.roll.interval-ms`，默认 10s）先到先触发
- **文件命名**：`<task>-<seq 16位零填充>-<yyyyMMddHHmmss>.bin`（task 默认取 `topic.prefix`），
  **文件名字典序 = 消费顺序**；seq 重启后扫数据目录恢复，不依赖状态文件；tmp 目录残留
  （未 publish 半成品）启动即清——删除安全（offset 未推进，源端重发）
- **格式**：`[Magic "VBFG"][version][seq][sourceDb]` 文件头 + 记录流（TABLE_DEF/BEGIN/EVENT/
  COMMIT/TRUNCATE + FOOTER 校验和，LEB128 变长整数，null 位图），时间/decimal 统一字符串
  落地（目标端按列类型转换）——布局细目与跨仓同步契约见模块 CLAUDE.md 的 file 形态节

| `sink.*` 键 | 默认 | 语义 |
|---|---|---|
| `sink.mode` | `log` | `log`=逐条 INFO 渲染；`file`=VBFG 落地（非法值启动期报错） |
| `sink.data-dir` | `data/cdc-files` | 已发布落地文件目录（gitignore 已覆盖） |
| `sink.tmp-dir` | `data/cdc-tmp` | 写入中的 `.part` 半成品目录（启动清空） |
| `sink.task` | `topic.prefix` 值 | 文件名前缀（任务标识） |
| `sink.roll.max-records` | `1000` | 条数切分阈值（COMMIT 处评估） |
| `sink.roll.interval-ms` | `10000` | 时长切分阈值（距上次 publish） |

file 形态下 CDC logger 仍有 INFO 摘要（每事务一行 TXN-END + 落地文件 publish 行），不黑盒。

（命令为 Windows 形态——classpath 分隔符 `;`；macOS/Linux 为 `:`。`--add-opens` 清单必带：连接器内 Chronicle Queue 的 mmap 在 Java 17 需开放内部包，与根 pom surefire argLine 同源。）

## 输出与 offset 语义

- **CDC 记录行**走专用 logger `org.vastdata.vbstream.reader.cdc`（INFO，与系统日志分离、可独立调级），一行一条 `SourceRecord`（渲染示意）：

  ```
  INFO  o.v.v.reader.cdc - topic=vbread.t_stream_test key=Struct{id=1404} op=c txId=769 lsn=38096224 lsn_commit=38096224 value=Struct{after={id=1404, payload=...}, op=c, source=...}
  ```

  取数口径：`op` 取数据记录 value 顶层（c/u/d/t）、事务元数据记录取 `status`（BEGIN/END，`provide.transaction.metadata` 默认开）；`txId` 取 source 块（与事务元数据记录同源的纯数字顶层 xid）；`lsn`/`lsn_commit` 取 sourceOffset map（连接器事务边界 offset 双写）；value 预览超 512 字符截断并以 `...(N chars)` 标注原始长度。字段缺失以 `-` 占位不抛。INFO 关闭时整批短路渲染（`isInfoEnabled` 批头守卫——`Struct.toString()` 是消费端最贵的单条操作之一）。
- **重启续传锚复制槽确认位**（连接器 LSN 反馈按输出前沿封顶），engine offset 文件是框架层记录；两者位点不一致时重复段取并集、不丢不静默吞——语义细目见连接器 README 的 at-least-once 节
- **停机**：Ctrl+C 走 shutdown hook（latch countDown + join 60s，JVM halt 前排干 final `commitOffsets`）；正常 exit 0 / 用法错 exit 2 / 失败 exit 1

## 已知限制与坑位

- **冒烟定位**：渲染consumer 把 INFO 打印成功即视为该条已消费（`markProcessed` 随后推进 offset），无下游缓冲与重试——生产形态请走 Kafka Connect
- **file 形态的类型面**（VBFG 精简格式的固有约束，与 cdc-capture 的 binary 路径同款）：复合 Connect 类型（ARRAY/MAP/STRUCT——如数组列的默认类型化表示）在 `EnvelopeParser` 抛 IAE 停引擎；连接器开 `values.as.string=true`（全串模式，数组原文透传）可消解，代价是全部列按 VARCHAR 下游同形
- **slf4j-api 必须显式 2.0.17**：debezium-embedded 传递 slf4j-api 1.7.36，与 logback 1.5 绑定不兼容（NOP 静默无日志），pom 显式声明压掉，不可省
- 引擎 offset 存储默认 connect-runtime 的 `FileOffsetBackingStore`（debezium-embedded 传递），只需给文件路径；`key.converter`/`value.converter` 免传（EmbeddedWorkerConfig 自动注入 + Connect 直通不经过）

## 开发与测试

`mvn test` 单命令全跑（surefire 显式补 `**/*IT.java`，与 connector 模块同款）：离线单测 `ReaderPropertiesTest`（三层合并次序 / classpath 模板 / 外部文件替换与 fail-fast / 必填校验与打码 / sink 键合并与剥离，零 PG）+ `format` 包 `VarintTest`/`ChangeFileIOTest`（VBFG 编解码 RoundTrip、null 位图、defId 去重、CRC/截断破坏检测——移植正确性锚定）+ `file` 包 `FileChangeConsumerTest`（offset 与 publish 联动、tombstone、落地文件读回）/`FileRollingWriterTest`（时间切分、seq 恢复、tmp 清理）+ `it` 包（Testcontainers postgres:18，需本机 Docker）`ReaderEndToEndIT` 两场景——①端到端：INSERT 断言数据记录 op=c + 事务元数据 BEGIN/END 对 + 零 op=r（snapshot 钉死 no_data）；②重启无重复：同槽同 offset 文件两轮运行零重发、新写入恰收新记录；`ReaderFileSinkIT` file 形态端到端——六类型列落地 VBFG 文件 → 移植 Reader 读回断言记录序/表定义/值/txid/CRC 完整。每方法独立槽名/publication/表名。
