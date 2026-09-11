# vb-stream-reader — debezium-embedded 宿主冒烟应用

最小 embedded 宿主：`DebeziumEngine.create(io.debezium.embedded.Connect.class)` 工厂建 async
引擎（3.x 唯一实现，Connect 格式直通——`event.value()` 即原始 `SourceRecord`，`event.key()`
恒 null 不用）加载同仓连接器 `PostgresStreamConnector`（模块依赖
vb-stream-connector-postgres-stream，`AsyncEngineBuilder` 构造器包私有故必走工厂）。
输出形态二选一（`vb.sink.mode`，2026-09-11 起）：**log**（默认，`ChangeConsumer` 批回调逐条
渲染 INFO 到专用 logger `org.vastdata.vbstream.reader.cdc`，`markProcessed`/
`markBatchFinished` 手动 offset 记账）或 **file**（CDC 记录落地 VBFG 二进制文件，与
vb-cdc-file-transform 仓 cdc-sink 的消费格式互通）。依赖方向 reader → connector + file-format（引擎零依赖
不受 D2 约束）。

## 组件

- `ReaderProperties`：**三层合并**配置面（classpath 的 `dbconfig.properties` 基础值 →
  `-Dvb.*` 系统属性剥前缀覆盖 → reader 默认兜底；`-Dvb.config=<路径>` 可整份替换为外部
  文件——路径不存在 fail-fast 拒绝静默回落，且该键是保留键不进 Debezium props）。文件键即
  Debezium 裸键（文件本身是 reader 专属命名空间），**例外是 `sink.*` 前缀键**——归输出形态
  命名空间（与 `-Dvb.sink.*` 合并后交 `resolveSink`，不透传 Debezium）；连接器 6 个专属项与
  engine 高级项无论写在文件还是 -D 传入均零代码可用（如 `slot.streaming=parallel`）。注入默认
  `connector.class`（固定不可覆盖）/`name`/`offset.storage.file.filename`（默认
  `data/reader-offsets.dat`，已 gitignore）/`offset.flush.interval.ms`（默认 1000）；必填五项
  hostname/dbname/user/password/topic.prefix 缺失 exit 2；`masked` 打码 password。
- `SinkConfig`：输出形态配置（record）——`mode`（log|file，非法值启动期 IAE）+ file 形态参数
  （dataDir 默认 `data/cdc-files`、tmpDir 默认 `data/cdc-tmp`、task 缺省取 topic.prefix、
  roll.max-records 默认 1000、roll.interval-ms 默认 10000——滚动默认对齐 cdc-capture）。
  解析入口 `ReaderProperties.resolveSink(debeziumProps)`：文件 `sink.*` 基础值 → `-Dvb.sink.*`
  覆盖 → 默认兜底。
- `LogChangeConsumer`：一条记录一行（topic/key/op/txId/lsn/lsn_commit/value 预览截 512 字符）；
  op/txId 取 value Struct（数据记录 op 顶层 + source 块 txId，事务元数据记录 status/id），
  lsn 取 sourceOffset map（连接器事务边界 offset 双写）。
- `EngineLifecycle`：装配 + `engine-run` 线程跑阻塞的 `run()`（async 引擎 run 从不抛，失败走
  CompletionCallback，回调恰在 run 返回前触发一次）+ latch/hook 收敛：hook 只 countDown + join
  60s（JVM halt 前排干 final commitOffsets），close 归 main 的 `shutdown()`——completed 守卫
  （引擎已自行结束则跳过 close，再调抛 ISE）+ ISE 兜底重试（撞 STARTING_TASKS 启动窗口 1s×5）
  + IOException WARN 吸收。Main 与 IT 共用。
- `Main`：vb-stream-engine Main 同款模式（latch + hook + 退出码 2/1/0）；按 `SinkConfig.Mode`
  装配 consumer——file 形态的 `FileChangeConsumer` 构造（建目录/清 tmp/恢复 seq）与关闭
  （停机收敛后，未 publish 的 tmp 丢弃）都归 main 编排。

## file 输出形态（VBFG 落地，2026-09-11 起）

学习自 vb-cdc-file-transform 仓 cdc-capture 的落地链路，一个模块依赖 + 一个子包：

- **`vb-stream-file-format` 模块（VBFG 契约层，compile 依赖）**：cdc-file-format 的源码
  移植（写侧+读侧），包 `org.vastdata.vbstream.format`——文件布局、跨仓同步契约、关键
  不变量见该模块 CLAUDE.md。reader 的 file 包是其首个使用方（EnvelopeParser 解析结果经
  VbfgEventWriter 写入；ReaderFileSinkIT 经传递依赖读回验收跨模块复用）。
- **`file` 包（落地链路）**：`EnvelopeParser`（SourceRecord → 精简事件——事务元数据按结构
  特征识别 [status/id 有、op 无]；时间 `io.debezium.time.*` 字符串化、decimal 靠 connector
  `decimal.handling.mode=string`、DELETE 取 before 镜像、TRUNCATE 走独立记录；**移植偏离**：
  无时区 timestamp 按 UTC 读墙钟 + MicroTimestamp 微秒保真——修正 cdc-capture doc §2.6 第 6
  条记档的 8h 偏差与毫秒截断，javadoc 记档）、`VbfgEventWriter`（解析结果写
  ChangeFileWriter，事务内跟踪 lastLsn 供 BEGIN/COMMIT；txid 纯数字直接 parseLong、复合
  "a:b" 打包、其余哈希兜底）、`FileRollingWriter`（tmp `.part` → COMMIT 边界评估切分
  [条数/时长] → finish+fsync+**原子 rename** publish；启动清 tmp 残留、seq 扫数据目录恢复、
  close 丢弃未 publish tmp）、`FileChangeConsumer`（批回调分流 Begin/Event/Commit；
  **markBatchFinished 只在本批有 publish 后调用**——数据完整落地前 offset 绝不推进，IO 失败
  抛出让引擎停 [offset 不动重启重放]；每事务 COMMIT 后 INFO 摘要一行到 CDC logger）。

运行 file 形态：`-Dvb.sink.mode=file`（可加 `-Dvb.sink.data-dir=...` 等）；产物可直接喂
vb-cdc-file-transform 的 cdc-sink 消费（二进制格式互通）。

## 运行

配置默认来自 classpath 的 `dbconfig.properties`（src/docker 本地 PG 模板，**零 `-D` 参数即可起**）；
临时覆盖单项 `-Dvb.<键>=<值>`，整体换文件 `-Dvb.config=<外部文件绝对路径>`（免重编译改配置）：

```bash
cd src/docker && docker compose up -d && cd ../..     # 前置 PG(55432)
mvn -q -pl vb-stream-reader compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED \
     --add-opens java.base/sun.nio.fs=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "vb-stream-reader/target/classes;$(cat vb-stream-reader/target/cp.txt)" \
     org.vastdata.vbstream.reader.Main
# 临时覆盖示例: -Dvb.slot.name=another_slot  -Dvb.topic.prefix=other
# 整体换文件:   -Dvb.config=/abs/path/my.properties
```

（Windows classpath 分隔符 `;`，macOS/Linux 为 `:`；`--add-opens` 必带——连接器内 Chronicle
Queue 的 mmap 需要，与根 pom surefire argLine 同源。）

## 坑位记档

- **slf4j-api 必须显式 2.0.17**：debezium-embedded 传递 slf4j-api 1.7.36，与 logback 1.5
  绑定不兼容（1.7.x 找 StaticLoggerBinder，logback 1.3+ 无 → NOP **静默无日志**，connector
  模块 pom 记过同坑）；pom 显式声明压掉，不可省。
- **ChangeConsumer/RecordCommitter 是 `DebeziumEngine` 的嵌套接口**（3.x 布局）：import
  `io.debezium.engine.DebeziumEngine.ChangeConsumer`，非顶层 `io.debezium.engine.ChangeConsumer`
  （2.x 旧布局，3.x 不存在）。
- **引擎 offset 存储默认文件**：`offset.storage` 默认 connect-runtime 的
  FileOffsetBackingStore（debezium-embedded 传递），只需给
  `offset.storage.file.filename` 路径；`key.converter`/`value.converter` 免传
  （EmbeddedWorkerConfig 自动注入 + Connect 直通不经过）。
- **FileOffsetBackingStore 不创建父目录**（本地首跑实测）：`data/` 目录不存在时首次
  offset flush（markBatchFinished → commitOffsets）直接 NoSuchFileException，报错深埋
  引擎线程栈难归因——Main 启动期 `prepareOffsetStorage` 自动建父目录与空文件（仅文件
  存储形态处理）；路径已显式进 dbconfig.properties。
- **重启语义**：续传锚槽确认位（连接器 LSN 反馈按输出前沿封顶），engine offset 文件是
  框架层记录；IT 场景二验证同槽同 offset 文件两轮运行无重发——安全停机前置等
  `pg_stat_replication.flush_lsn` **覆盖已收记录的最大 lsn**（standby status 采纳面；槽目录
  confirmed_flush 空闲库不落库——引擎 Diag 实证记档）。

## 测试形态

`mvn test` 单命令全跑（surefire 显式补 `**/*IT.java`，与 connector 模块同款）。
`it` 包：`ReaderPgEnv` 单例 postgres:18 容器（连接器 IT 同配方翻译裁剪）+
`ReaderEndToEndIT` 两场景——①端到端：3 行 INSERT 断言数据记录（op=c、id 齐）+
事务元数据 BEGIN/END 对 + 零 op=r（snapshot 钉死 no_data）；②重启无重复：offset 文件接线
验收（等 standby flush 覆盖已收 endLsn 再停机 → 同槽重启静默窗口零重发 → 新写入恰收新记录）。
`ReaderFileSinkIT` file 形态端到端——六类型列（INT/TEXT/BOOL/FLOAT64/DATE/TIMESTAMP，锚定
时间字符串化路径）经 `FileChangeConsumer` 落地 VBFG 文件（roll=1 首事务即 publish）→ 移植
`ChangeFileReader` 读回断言记录序 BEGIN/TABLE_DEF/EVENT/COMMIT、表定义与列类型对齐、值墙钟
原值往返、txid 同源、FOOTER CRC 完整。
每方法独立槽名/publication/表名；`offset.flush.interval.ms=0`（每批落盘）+
`slot.feedback.interval.ms=1000`（亚秒反馈）。需本机 Docker。
