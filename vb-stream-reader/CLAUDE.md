# vb-stream-reader — debezium-embedded 宿主冒烟应用

最小 embedded 宿主：`DebeziumEngine.create(io.debezium.embedded.Connect.class)` 工厂建 async
引擎（3.x 唯一实现，Connect 格式直通——`event.value()` 即原始 `SourceRecord`，`event.key()`
恒 null 不用）加载同仓连接器 `PostgresStreamConnector`（模块依赖
vb-stream-connector-postgres-stream，`AsyncEngineBuilder` 构造器包私有故必走工厂），
`ChangeConsumer` 批回调逐条渲染 INFO 到专用 logger `org.vastdata.vbstream.reader.cdc`，
`markProcessed`/`markBatchFinished` 手动 offset 记账。依赖方向 reader → connector（引擎零依赖
不受 D2 约束）。

## 组件

- `ReaderProperties`：**三层合并**配置面（classpath 的 `dbconfig.properties` 基础值 →
  `-Dvb.*` 系统属性剥前缀覆盖 → reader 默认兜底；`-Dvb.config=<路径>` 可整份替换为外部
  文件——路径不存在 fail-fast 拒绝静默回落，且该键是保留键不进 Debezium props）。文件键即
  Debezium 裸键（文件本身是 reader 专属命名空间）；连接器 6 个专属项与 engine 高级项无论
  写在文件还是 -D 传入均零代码可用（如 `slot.streaming=parallel`）。注入默认
  `connector.class`（固定不可覆盖）/`name`/`offset.storage.file.filename`（默认
  `data/reader-offsets.dat`，已 gitignore）/`offset.flush.interval.ms`（默认 1000）；必填五项
  hostname/dbname/user/password/topic.prefix 缺失 exit 2；`masked` 打码 password。
- `LogChangeConsumer`：一条记录一行（topic/key/op/txId/lsn/lsn_commit/value 预览截 512 字符）；
  op/txId 取 value Struct（数据记录 op 顶层 + source 块 txId，事务元数据记录 status/id），
  lsn 取 sourceOffset map（连接器事务边界 offset 双写）。
- `EngineLifecycle`：装配 + `engine-run` 线程跑阻塞的 `run()`（async 引擎 run 从不抛，失败走
  CompletionCallback，回调恰在 run 返回前触发一次）+ latch/hook 收敛：hook 只 countDown + join
  60s（JVM halt 前排干 final commitOffsets），close 归 main 的 `shutdown()`——completed 守卫
  （引擎已自行结束则跳过 close，再调抛 ISE）+ ISE 兜底重试（撞 STARTING_TASKS 启动窗口 1s×5）
  + IOException WARN 吸收。Main 与 IT 共用。
- `Main`：vb-stream-engine Main 同款模式（latch + hook + 退出码 2/1/0）。

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
每方法独立槽名/publication/表名；`offset.flush.interval.ms=0`（每批落盘）+
`slot.feedback.interval.ms=1000`（亚秒反馈）。需本机 Docker。
