# MS6 设计：插件打包（R4）+ Connect 容器验收 IT + 模块 README + R2 延期记档

日期：2026-09-05
状态：已批准（用户拍板：R2 增量快照不接记档延期、打包形态 plugin 目录清单、验收走真 Kafka Connect 容器 IT、文档落模块 README）

## 1. 背景与范围

原设计（`2026-09-01-debezium-connector-postgres-stream-design.md` §10）MS6 = 插件打包（R4）、配置文档、at-least-once/停机语义文档，验收"插件可装进 Connect 运行"。实施期裁定：

1. **R2 决策**：不接 signal-based 增量快照，v1 文档记 known limitation——R2 审计（`2026-09-05-ms5-r2-incremental-snapshot-audit.md`）已给完整接入路线图（前置条件五项），后人需要时按图实施
2. **打包形态**（原设计 §11 待定项）：plugin 目录清单（assembly），非 fat-jar
3. **验收方式**：Testcontainers 起真 Kafka Connect 容器装插件跑通（R4 类加载面全覆盖）

**MS6 = 四件**：maven-assembly 打包、ConnectPluginIT、模块 README、R2 延期记档 + CLAUDE.md 收官记档。

## 2. 打包——maven-assembly plugin 目录清单

- 连接器模块加 `maven-assembly-plugin` + 自定义 descriptor（`assembly-plugin.xml`，模块根 `src/main/assembly/`）：产物 = `vb-stream-connector-postgres-stream-plugin/` 目录 + 同名 zip 分发物——**根放连接器自身 jar，`lib/` 放全部运行时依赖 jar**（Debezium 官方插件 tarball 同构布局）
- **排除 Connect runtime 已提供的坐标**：`connect-api`、`kafka-clients`、`slf4j-api`（安装进 `plugin.path` 后插件类加载器自包含——R4 两连接器并存不互扰的关键；排除名单以 Debezium 官方 3.6 插件包布局为准，实施时对照核定）
- 绑 `package` 阶段；测试依赖（JUnit/Testcontainers/debezium-embedded tests classifier）不入清单
- 既有 `-Pjmh` 档与默认构建不受影响

## 3. ConnectPluginIT——真 Kafka Connect 容器验收

- 新 IT `vb-stream-connector-postgres-stream/src/test/java/.../it/ConnectPluginIT.java`：**不挂 StreamITBase**（它是真 Connect runtime，非 embedded engine）——独立起容器组
- 容器组：单容器 Kafka（KRaft）+ Connect 镜像（`confluentinc/cp-kafka-connect` 类，`plugin.path` 指向挂载/复制的插件目录）+ 复用 `StreamPgTestEnv.PG` 单例
- 流程：`mvn package` 产物（或 IT 内直接引用已构建的 plugin 目录——取 Maven 属性/相对路径）复制进容器 → REST `PUT /connectors` 建连接器（指向 Testcontainers PG）→ `INSERT` → 轮询 Kafka topic 收到记录 → 断言 op/值/topic → 收敛清理
- 验收面：类加载（R4）、REST 配置暴露（configDef/validate 三层防线在真 Connect 的生效）、序列化、连接器生命周期
- 风险注记：cp 镜像体积大/拉取慢——IT 放独立类可单跑；需本机 Docker

## 4. 模块 README

`vb-stream-connector-postgres-stream/README.md`：

1. **定位与架构**：流式专用 PG 逻辑解码连接器（一段话 + 引擎/连接器关系）
2. **配置面全档**：六自定项（`slot.streaming`/`slot.two.phase`/`pipe.dir`/`pipe.roll.cycle`/`slot.feedback.interval.ms`/`slot.messages`）逐项语义/默认值/约束 + 两项默认覆盖（`snapshot.mode` 仅 no_data、`provide.transaction.metadata` 默认 true）
3. **打包与安装**：`mvn -pl vb-stream-connector-postgres-stream package` → 产物结构 → `plugin.path` 安装步骤 → REST 建连接器示例 JSON
4. **at-least-once/停机语义**：End 锚定输出前沿、LSN 确认封顶、crash 重启整事务重发不去重（下游按事务元数据幂等收敛）、D7 不排干
5. **Known limitations**：数组列 fail-fast 不静默 null、未知类型静默 null、LogicalMsg 解析不发射、`snapshot.mode` 仅 no_data（无快照数据抽取）、**增量快照不接**（R2 审计路线图指针）

## 5. R2 延期记档与收官

- R2 审计文档（`2026-09-05-ms5-r2-incremental-snapshot-audit.md`）结论节补一行：MS6 裁定 v1 不接 signal-based 增量快照，后续需要时按前置条件五项路线图实施
- 根 CLAUDE.md connector 源码结构段追加 MS6 句（打包形态/IT/README 指针）+ 用例计数更新；模块 `stream/CLAUDE.md` 同步
- 根 `README.md` 若无连接器入口指针则补一行

## 6. 测试与验收

- 单测：assembly descriptor 产物结构断言（目录布局/连接器 jar 在根/lib 依赖齐全/排除项不在）——轻量集成于 `ConnectPluginIT` 前置或独立测试类
- IT：`ConnectPluginIT` 全绿（需 Docker）
- 全量：`mvn test` 全绿；`mvn package` 产物结构核对
- 里程碑验收 = §3 IT 绿 + README/审计记档入库 + CLAUDE.md 记档
