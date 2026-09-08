# vb-stream-reader — debezium-embedded 宿主冒烟应用

## 定位与架构

不经 Kafka Connect runtime 的最小 embedded 宿主：`DebeziumEngine.create(io.debezium.embedded.Connect.class)` 工厂建 async 引擎（3.x 唯一实现；Connect 格式直通——`event.value()` 即原始 `SourceRecord`，`event.key()` 恒 null 不用）加载同仓连接器 `PostgresStreamConnector`（模块依赖 `vb-stream-connector-postgres-stream`；`AsyncEngineBuilder` 构造器包私有，必走工厂），`ChangeConsumer` 批回调逐条渲染 INFO，`markProcessed`/`markBatchFinished` 手动 offset 记账。依赖方向 reader → connector（引擎模块不受牵连）。定位是**冒烟/联调宿主**，不是生产形态——生产部署走 Kafka Connect（连接器打包安装见 [vb-stream-connector-postgres-stream/README.md](../vb-stream-connector-postgres-stream/README.md)）。

组件四件（细节见模块 `CLAUDE.md`）：

| 组件 | 职责 |
|---|---|
| `ReaderProperties` | **三层合并**配置面（下节），必填校验与 password 打码 |
| `LogChangeConsumer` | 批回调逐条渲染 INFO 到专用 logger + offset 手动记账 |
| `EngineLifecycle` | `DebeziumEngine.create` 装配 + `engine-run` 线程 + latch/hook 收敛闸门（Main 与 IT 共用） |
| `Main` | 编排主线：配置解析 → 校验（缺失 exit 2）→ 起引擎 → await 停机 → 收敛（失败 exit 1 / 正常 0） |

## 配置面：三层合并

```
classpath 的 dbconfig.properties（基础值，随包分发的 src/docker 本地 PG 模板）
  → 系统属性 -Dvb.<debezium 键>=<值>（剥 vb. 前缀覆盖同名项；值空串视为未设）
  → reader 默认值（putIfAbsent 兜底）
```

- **文件键即 Debezium 裸键**（不带 `vb.` 前缀——文件本身是 reader 专属命名空间）；同键多来源以 `-D` 为准
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
# 整体换文件:   -Dvb.config=/abs/path/my.properties
```

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
- **slf4j-api 必须显式 2.0.17**：debezium-embedded 传递 slf4j-api 1.7.36，与 logback 1.5 绑定不兼容（NOP 静默无日志），pom 显式声明压掉，不可省
- 引擎 offset 存储默认 connect-runtime 的 `FileOffsetBackingStore`（debezium-embedded 传递），只需给文件路径；`key.converter`/`value.converter` 免传（EmbeddedWorkerConfig 自动注入 + Connect 直通不经过）

## 开发与测试

`mvn test` 单命令全跑（surefire 显式补 `**/*IT.java`，与 connector 模块同款）：离线单测 `ReaderPropertiesTest`（三层合并次序 / classpath 模板 / 外部文件替换与 fail-fast / 必填校验与打码，零 PG）+ `it` 包 `ReaderEndToEndIT` 两场景（Testcontainers postgres:18，需本机 Docker）——①端到端：INSERT 断言数据记录 op=c + 事务元数据 BEGIN/END 对 + 零 op=r（snapshot 钉死 no_data）；②重启无重复：同槽同 offset 文件两轮运行零重发、新写入恰收新记录。每方法独立槽名/publication/表名。
