# MS5 设计：snapshot.mode 校验 + 事务元数据默认开 + R2 审计 + 指标（日志行 + MBean）

日期：2026-09-04
状态：已批准（用户拍板：快照面收敛为仅支持 no_data——不做数据抽取，与 vanilla no_data 语义一致；指标从原拆分方案并回本期）

## 1. 背景与范围

原设计（`2026-09-01-debezium-connector-postgres-stream-design.md` §10）MS5 = 快照衔接 + 事务元数据默认开 + 指标 + R2 专项。本期实施中用户三项裁定收敛范围：

1. **快照**：只支持 `snapshot.mode=no_data`，不做快照数据抽取（那些逻辑属 vanilla postgresql-connector）；只做参数校验
2. **R2 增量快照**：只审计不接（接不接留 MS6 决策）
3. **指标**（原拟拆 MS5.5）：并回本期一期做完

**MS5 = 四件**：snapshot.mode 校验、事务元数据默认开、R2 审计文档、指标（ThroughputMetrics 同构复刻日志行 + MBean 暴露）。

YAGNI 排除项：快照数据抽取（initial/when_needed 等全模式）、增量快照接入、导出快照零重复机制。

## 2. `snapshot.mode` 校验（快照面全部收敛于此）

- `PostgresStreamConnectorConfig.validate` 增一条：`snapshot.mode` 仅接受 `no_data`，其余值（initial/always/when_needed/initial_only/custom 等）向 problems 记一条报错——文案说明本连接器定位流式专用、数据抽取请用 vanilla postgresql-connector
- **默认值覆盖为 `no_data`**（vanilla 默认 initial 会让开箱即用直接报错；本连接器现行实际行为就恒 skip，覆盖默认与事实一致）
- `SkippedSnapshotSource` 保留现状不动——它就是 no_data 的忠实实现（跳过快照直接进流式，见 `StreamChangeEventSourceFactory`）

## 3. 事务元数据默认开启

- `provide.transaction.metadata` 默认值覆盖为 `true`（本连接器的事务 id/边界语义是核心交付面，vanilla 默认 false 不合理）
- 存量 IT 全部显式设 true，不受影响

## 4. R2 增量快照交错——只审计不接

- 产出：`docs/superpowers/specs/2026-09-05-ms5-r2-incremental-snapshot-audit.md`
- 审计面（若未来接入 vanilla 增量快照，SignalProcessor + IncrementalSnapshot 线程）：
  1. 增量快照会引入**第二个 dispatch 调用线程**——R1 结论"dispatch 仅 consumer 线程"被打破，`PostgresEventDispatcher` 共享可变态需重审
  2. 信号表读取/chunk 读占 main 连接——R3 串行化约束（reader 独占）需扩展分析
  3. chunk 记录的事务边界 offset 与 End 锚定输出前沿的交互（chunk 发射期间前沿是否被钉住/超前）
- 结论形态：接入前置条件/改造点清单 + 是否建议 MS6 接入的建议

## 5. 指标（本期最大块）

### 5.1 日志行：ThroughputMetrics 同构复刻

- 模块边界 D2：文字参照重写、零 `org.vastdata.vbstream` import（同 `PgOutputStreamDecoder` 先例），落连接器包
- 全套语义与引擎同构：三段速率（slot 读取/组装/输出，窗口差分观察面）+ 回放耗时/事务大小分位数（HdrHistogram 区间隔离）+ 八项会话峰值行（峰值速率为会话最高单秒速率——秒桶）+ 10s 周期 INFO 日志行
- 记账分工与引擎同构：reader 线程记 slot 读取/组装，consumer 线程记输出/分布/峰值，报告 tick 挂 consumer 统计
- 装配点：`PostgresStreamStreamingChangeEventSource` 建会话/组装器时构造并注入

### 5.2 MBean：自建 metrics 工厂

- `PostgresStreamConnectorTask.start` 里 `DefaultChangeEventSourceMetricsFactory<>` 换自建工厂；streaming metrics 类实现 `StreamingChangeEventSourceMetrics<PostgresPartition>`
- 暴露面（getter 命名对齐 vanilla `PostgresStreamingMetrics` 风格，实施时核对依赖源码定名）：
  - 三段速率（读取/组装/输出）
  - `lastReceived − frontier` 滞后
  - 挂起 prepared 数（`StreamedTransactionAssembler.preparedByGid.size()` 需加只读访问器）
  - CQ 目录磁盘占用（`MessagePipe` 目录遍历求和，周期采样）
- MBean 注册走 Debezium 框架既有 `metrics.register` 配置面（JmxUtils），不自己碰 JMX API
- 堆有界性承诺不变：metrics 常驻有界（LongAdder + 2 位精度 HDR Recorder，KB 量级）

## 6. 测试与验收

- 单测：snapshot.mode 校验三分支（no_data 过 / initial 拒 / 缺省 no_data）+ metadata 缺省开 + metrics 类 getter 从注入计数器读值
- IT（挂 `StreamITBase`）：
  1. 缺省配置（不设 snapshot.mode / provide.transaction.metadata）连接器可跑、事务元数据 topic 有记录
  2. `snapshot.mode=initial` 启动失败（fail-fast 文案断言）
  3. 跑一段流量后 metrics 实例各 getter 非零/滞后可读（直接持有 metrics 对象断言，不经 JMX——嵌入式环境 JMX 注册不可靠）
- 验收：`mvn test` 全绿 + R2 审计入档 + 根/模块 CLAUDE.md 记档
