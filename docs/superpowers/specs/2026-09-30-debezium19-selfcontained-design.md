# debezium19 模块自含化设计——移除 debezium-connector-postgres 依赖（裁剪复刻进自有命名空间）

- 日期：2026-09-30
- 状态：已获批（brainstorming 对话逐项裁定）
- 影响模块：`vb-stream-connector-postgres-stream-debezium19`（桶 B 接缝层 + 新增 vendored 子系统；桶 A 零改动）
- 前置 spec：`2026-09-30-debezium19-connector-design.md`（D1-D4 裁定沿用，本 spec 修订其 D3 的依赖面）

## 1. 背景与动机

模块当前依赖 `io.debezium:debezium-connector-postgres:1.9.7.Final`（pom 注释原裁定"宿主产品亦为同版本，类重复无害"）。用户澄清推翻该前提：**宿主产品不带 debezium-connector-postgres jar**——插件必须自含运行所需的全部 PG 连接器类；且长期方向是**完全自研演进**（可逐步偏离 vanilla 实现形态）。

两项动机共同指向：把所用 vanilla 类搬进自有命名空间，与 vanilla 源码形态解耦。

## 2. 裁定（对话逐项确认）

| # | 裁定 |
|---|---|
| S1 | 依赖边界：**只移除 debezium-connector-postgres**；debezium-core 保留（框架层——`EventDispatcher` 基类、`ChangeEventQueue`、relational 体系、`RelationalBaseSourceConnector` 等） |
| S2 | 包名策略：**自有命名空间**（`org.vastdata.debezium.connector.postgresql.*`，与现有 `.stream` 子包平级镜像 vanilla 结构）；类名原样保留 vanilla 名，便于复刻期逐文件对照与后续上游比对 |
| S3 | 忠实度：**裁剪复刻** vanilla 1.9.7 源码（以本地 sources jar 为基准），行为与现有 275 用例锚定；后续再逐步偏离演进 |
| S4 | 裁剪深度：**需求驱动**——只搬本连接器运行面真实可达的类；decoder/snapshotter/复制流子树不搬 |

## 3. 目标与非目标

**目标**

- 模块 pom 不再依赖 `debezium-connector-postgres`；编译、275 用例（含 27 IT + ConnectPluginIT）、assembly 打包全部自含
- 复刻类落自有命名空间，保留 Apache 2.0 版权头 + 来源/裁剪标注
- 新增零 Docker 边界单测：断言模块产物无任何类引用 `io.debezium.connector.postgresql`（防将来手滑混入）

**非目标**

- 不动桶 A（协议/组装/管道/会话内核）——双线同步约定不受影响（本次全在桶 B 与新增 vendored 子系统）
- 不 vendored debezium-core / connect-api / debezium-embedded（宿主与 Connect runtime 各自提供；test 面 embedded 基座照旧）
- 不偏离 1.9.7 行为面（行为演进是后续独立任务）

## 4. 包布局与文件清单

### 4.1 布局

```
org.vastdata.debezium.connector.postgresql          ← 新（与 .stream 平级，镜像 vanilla 结构）
├── (根)   类型矩阵/Schema/offset/dispatcher/入口配置
├── connection/  JDBC 连接(裁)/默认值转换/TOAST 哨兵链/Lsn
├── connection/pgoutput/  PgOutputReplicationMessage
└── data/  Ltree
```

### 4.2 保留集（vendored，21 文件；另有 Connector/Config 两文件重写方向见 §5.1）

依据：主源码 17 个直接引用类型的传递闭包（48 顶层文件，2026-09-30 从 sources jar 实测）剔除 S4 排除项后的运行面。

**根包（13）**：`TypeRegistry`、`PostgresType`、`PgOid`、`PostgresSchema`、`PostgresValueConverter`、`PostgresOffsetContext`、`PostgresPartition`、`SourceInfo`、`PostgresTopicSelector`、`PostgresErrorHandler`、`PostgresEventDispatcher`、`PostgresEventMetadataProvider`、`UnchangedToastedReplicationMessageColumn`

**connection（6）**：`PostgresConnection`（裁成 JDBC 查询包装，见 §5）、`PostgresDefaultValueConverter`、`Lsn`、`AbstractReplicationMessageColumn`、`ReplicationMessage`（接口 + 嵌套 Column/ColumnValue/Operation/NoopMessage——TOAST 哨兵链的类型面）、`ReplicationMessageColumnValueResolver`（闭包内被拉入，随 DefaultValueConverter 保留）

**connection/pgoutput（1）**：`PgOutputReplicationMessage`——**保留原因**：`TypeRegistryColumnValueMapper` 运行期真用其 static `getValue(...)`（typed 模式列值转换主力）；随保留需其实现 `ReplicationMessage` 面的嵌套 Column

**data（1）**：`Ltree`（PostgresValueConverter 的 ltree 类型支持）

**重写不复制（1）**：`PostgresConnector` 不整搬——`PostgresStreamConnector` 改直接 extends debezium-core `RelationalBaseSourceConnector`，继承面用到的行为（`validateAllFields` 驱动集、`taskConfigs` 注入、version、taskClass）收进自有实现。`PostgresConnectorConfig` 裁剪复刻（见 §5）。

### 4.3 排除集（不搬，运行期不可达）

- **三解码器族**：`connection/pgoutput/PgOutputMessageDecoder`、`connection/pgproto/*`（PgProto/Op/RowMessage/decoder）、`connection/wal2json/*`（两个 decoder）、`AbstractMessageDecoder`/`MessageDecoder`/`MessageDecoderContext`——本连接器协议层自建（桶 A），不走 vanilla 解码路径
- **snapshotter 族**：`snapshot/*`（Always/Initial/InitialOnly/Never）——snapshot 恒 never（MS5 钉死），Config 面从"替换父 Field"改为自有 Field 后无实例化路径
- **复制流体系**：`ReplicationConnection`、`ReplicationStream`、`WalPositionLocator`、`ServerInfo`、`TransactionMessage`、`LogicalDecodingMessage`——自建 `ReplicationSession` 已覆盖（如个别类型被保留集签名牵连，按最小面内联/裁剪并在文件头记档）
- `PostgresStreamingChangeEventSource`——只需其嵌套 `PgConnectionSupplier` 接口语义，自有监督壳已自建等价供给，接口随接缝收编

## 5. 子系统改写要点

### 5.1 入口与配置（Connector/Config）

- `PostgresStreamConnector extends RelationalBaseSourceConnector`（debezium-core）；`validate`/`validateAllFields` 离线门槛（连接相关五字段）与 `taskConfigs`（no_data→never 注入、事务元数据默认 true）行为照旧，断言面不动
- `PostgresConnectorConfig` 裁剪复刻：父类改 `RelationalDatabaseConnectorConfig`（debezium-core）；**剔除** snapshotter 实例化面与 `plugin.name` 的 wal2json/pgproto 分支（插件恒 pgoutput、snapshot 恒 never——与 MS5 裁定同语义，实现面从"替换父 Field"变自有 Field）；自建七配置面逐字保留；`SnapshotMode` 枚举只留 never 语义所需
- 测试面 `snapshot.custom.class=...NeverSnapshotter` 字符串值改为自有命名空间等价或移除该用例的该维（断言语义不变：非法 snapshot.mode 仍启动期拒绝）

### 5.2 PostgresConnection 裁成 JDBC 查询包装

实测使用面（2026-09-30 grep）：`commit`、`readPrimaryKeyNames`、`readTableUniqueIndices`、`getTableColumnsFromDatabase` 同构循环（RelationMetadataSource 的 enrich 真源）、TypeRegistry 构造入口。裁掉：复制流建立/decoder 选择/ServerInfo 探测/槽管理 SQL（自建 ReplicationSession 幂等建槽）。构造器形态保持接缝兼容（`PostgresStreamStreamingChangeEventSource`/`RelationMetadataSource` 的调用点仅改 import。

### 5.3 其余保留集

- `PostgresEventDispatcher`/`PostgresEventMetadataProvider`/`SourceInfo`/`PostgresTopicSelector`/`PostgresErrorHandler`/offset 体系/类型矩阵/Schema 链/TOAST 哨兵链：复刻时仅做（a）import 迁移到自有包、（b）排除集类型引用的最小内联/裁剪、（c）Apache 2.0 头 + 来源标注；逻辑零改动
- `PostgresValueConverter` 构造参 11 `byte[]` 等 1.9.7 形态原样（既有接缝已按此对齐，无二次适配）

## 6. pom / assembly / 边界防护

- pom：删 `debezium-connector-postgres` 依赖；显式加 `io.debezium:debezium-core:${debezium19.version}`（compile）；其余（pgjdbc pin、chronicle 排除、test 面 embedded/connect-runtime/testcontainers/jackson 钉版）零改动
- assembly `plugin.xml`：lib/ 集随依赖树自动瘦身（connector-postgres 及其独占传递件消失）；排除清单不动——connector-postgres 原经 compile 传递混入的 connect-api/kafka-clients 排除项失效为无害冗余，保留（防御性）
- **边界单测**（零 Docker，`VanillaBoundaryTest`）：扫 `target/classes` 字节码，断言无 `io.debezium.connector.postgresql` 常量池引用（覆盖主源码与资源）

## 7. 测试策略

- 275 用例断言面零改动；测试代码 import 从 `io.debezium.connector.postgresql.*` 迁 `org.vastdata.debezium.connector.postgresql.*`（8 个类型引用 + NeverSnapshotter 字符串一处）
- 全模块 `mvn test` 为行为对齐最终裁决（含 27 IT；需本机 Docker）
- **ConnectPluginIT 是本次硬需求的验收锚**：plugin.path 只挂 assembly 产物（不含 vanilla jar）下真 Connect 端到端跑通 = 自含性实证
- 新增边界单测见 §6

## 8. 风险与已知限制

| 风险 | 缓解 |
|---|---|
| 裁剪边界误伤（排除集类型被保留集签名牵连） | 编译期即刻暴露；最小内联并在文件头记档，不扩保留集 |
| `PgOutputReplicationMessage.getValue` 复刻走样（typed 模式列值转换） | `TypeRegistryColumnValueMapperTest`/`StringValuesIT` 双锚；文件仅做 import 迁移零逻辑改动 |
| Config 裁剪后 REST validate 面收窄 | `DefaultsAndMetricsIT` 场景 2（snapshot.mode=initial 拒绝）+ `PostgresStreamConnectorConfigTest` 锚定 |
| vanilla 上游修复无法逐文件 diff 同步（包名不同） | 类名原样保留 + 文件头标注来源 FQN；接受（S2/S3 裁定：自研演进优先） |

## 9. 许可与溯源

复刻文件保留 vanilla 原 Apache 2.0 版权头；javadoc 首段标注：来源（`io.debezium.connector.postgresql.*` 1.9.7.Final sources）、复刻日期、裁剪说明（如有）。模块 README/CLAUDE.md 差异表同步（"依赖 vanilla 连接器 1.9.7"→"自含裁剪复刻，无 vanilla 依赖"）；根 CLAUDE.md 模块行同步。

## 10. 文档同步清单

- 本模块 `CLAUDE.md`：定位节（依赖面修订）+ 源码三桶（桶 B 增补 vendored 子系统）+ 1.9.7 API 差异清单（加"已自含"前言）
- 本模块 `README.md`：定位/打包安装节（lib/ 清单变化）+ 与 3.6.1 模块差异表（新增"vanilla 依赖自含化"行）
- 根 `CLAUDE.md`：模块行一句话同步
