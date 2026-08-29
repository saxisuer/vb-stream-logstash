# 事务流式输出设计（里程碑 2.0）

## 1. 背景与目标

1.7 完成读取与组装输出解耦后，回放期的堆峰值仍是 **O(事务大小)** 的无界角落：`BucketReplayer.replay` 虽逐条回读 CQ（原始字节任何时刻只有一条在堆），但解码产物累积进 `List<TxChange>`，攒齐整事务才封箱 `Transaction` 一次性回调 `onTransaction`——解码形态比原始字节膨胀 2~4×，1GB 流式事务的回放瞬间堆峰 2~4GB。原因不在 CQ 读法，在**输出契约**：整块不可变事务 + "不允许输出半截事务"的构造保证。

**目标：把输出契约改为流式事件交付（事务头 → 逐变更 → 事务尾），回放期堆峰值从 O(事务) 降到 O(单条)。** 交付机制变、输出格式不变（`TXN-BEGIN`/逐行/`TXN-END` 逐字节一致）；reader 侧唯一改动是桶单元计数一个 long。

**完成判据**：既有全部用例经 `TransactionCollector` 断言零改动通过；同步/异步**完整事件流**全等；流式时序用例证明输出先于回放完成；JMH `-prof gc` 前后对照（replayBucket 口径分配率显著下降）入档 baseline；`mvn clean test` 全绿。

### 1.1 已定决策

| 决策点 | 结论 | 备注 |
|---|---|---|
| 契约形状 | **sealed 事件单回调**（`TransactionEvent` permits Begin/End/TxChange，`onEvent(TransactionEvent)`） | 与 `TxChange` 既有 sealed 习惯同构；单回调=单一背压点；未来 Logstash 下游拿一种事件类型 |
| 兼容策略 | 破坏性替换 + 测试收集器 | 仓库内一次迁移；`Transaction` record 保留换角色为"重组值对象" |
| 中途失败 | **fail-fast 截断** | 已输出条数进 ERROR 日志；frontier 不推进（End 未达）→ 重启整事务重发（at-least-once，下游可能见重复头行，文档化）；为未来非 fail-fast 下游预留 onAbort 属后续里程碑（YAGNI） |
| 事务条数 | reader 桶记账恢复单元计数 | `TxBuffer.unitCount`（每单元 long 自增）→ `Begin.expectedChanges`——`TXN-BEGIN` 保持 `changes=N` 格式 |

## 2. 契约

```java
/** 事务输出事件族（2.0）：单回调流式交付，替代 1.7 的 onTransaction(Transaction) 整块契约。 */
public sealed interface TransactionEvent permits TransactionEvent.Begin, TransactionEvent.End, TxChange {

    /** 事务头（回放前发出；expectedChanges 来自 reader 记账的桶单元计数，aborted 过滤前的值）。 */
    record Begin(long xid, TransactionKind kind, String gid, long commitLsn, long endLsn,
                 Instant commitTimestamp, long expectedChanges) implements TransactionEvent { }

    /** 事务尾（全部变更交付完发出；emittedChanges 为实际交付数——aborted 过滤后）。 */
    record End(long xid, long emittedChanges) implements TransactionEvent { }
}
```

- **`TxChange` 直接 `implements TransactionEvent`**（permits 列它，不包 Change 壳）——逐条零额外分配；`TxChange` 自身仍是 sealed（RowChange/TruncateChange/MsgChange），两层 sealed 叠加合法且同构于既有习惯
- `TransactionListener` 重定义：`void onEvent(TransactionEvent event)`（仍 @FunctionalInterface）；旧 `onTransaction(Transaction)` 删除
- **`Transaction` record 保留、换角色**：不再由 consumer 产出；javadoc 改述为"事件流的重组值对象（测试等价币/未来需要整块的下游用）"
- 新增公开 **`TransactionCollector implements TransactionListener`**（replication 包）：Begin 开桶、逐 TxChange 攒入、End 封箱 `Transaction` 并 expose `List<Transaction> transactions()`；流合法性校验 fail-fast——End 无 Begin、Begin 内嵌 Begin、End 的 xid 与开启的 Begin 不匹配、`emitted > expected` 抛 ISE（**emitted < expected 合法**——aborted 子事务过滤的正常结果；每个用它的测试免费获得事件流形状检查）

## 3. 组件与数据流

```
processBucket（consumer 线程）：
  listener.onEvent(new Begin(bucket 元数据 + unitCount))        ← 头
  long emitted = replayer.replay(bucket, pipe, sink)            ← sink 逐条 listener.onEvent(change)
  listener.onEvent(new End(bucket.xid, emitted))                ← 尾
  frontier.accumulateAndGet(endLsn, max)                        ← End 返回后
  bucket.state = DONE
```

- **`TxBuffer`** 增 `long unitCount`（reader 追加期 `appendUnit` 内自增；非冻结面——LIVE 期写、交接后只读）
- **`BucketReplayer.replay`**：签名改 `(TxBuffer, MessagePipe, Consumer<TxChange> sink) → long emitted`——不再构造 List；aborted 过滤、decodeSingle、快照 asOf 渲染逐条原样；返回值为过滤后交付数
- **`TransactionConsumer`**：如上伪码；sink 包装为 `c -> { emitted++; listener.onEvent(c); }` 使已输出计数在异常路径存活（ERROR 日志标注 xid 与已输出条数）；空桶产出 Begin + End(0)（对应现在的空 changes 事务，合法）
- **`ConsoleListener`**：实现 `onEvent` 三段渲染——Begin 打现格式 `TXN-BEGIN xid= kind= gid= commitLsn= commitTs= changes=N`（N=expected）；TxChange 打 `  [i] ...`（行号 `i` 为实例字段，Begin 时清零——ConsoleListener 从无状态变轻状态，线程限定 consumer 线程，javadoc 注明）；End 打现格式 `TXN-END   xid=`。**输出格式与 1.7 逐字节一致**
- **frontier 契约注记**：End 返回 = 下游确认完整消费（下游必须在 End 返回前完成落盘/投递）；End 未达（异常/阻塞）则该事务不推进前沿
- reader 侧唯一改动 = `unitCount`；MessagePipe/桶状态机/两个低水位/节流/交接协议零触碰

## 4. 错误语义（fail-fast 截断）

回放中（解码或 onEvent）抛异常 → 异常穿出 processBucket → consumer 循环 catch Throwable → ERROR（xid/已输出条数/firstIndex）→ onFailure → 不排干退出 → 进程停机（与 1.7 失败语义一致）。下游已见的半截输出（TXN-BEGIN + 部分行）以 ERROR 日志收尾，无显式中止回调——进程即将终止，标记意义有限（未来 Logstash 集成时再议 onAbort）。frontier 未推进 → 重启从 confirmed_flush 重发整个事务 → 下游可能看到重复头行（at-least-once 既定语义，文档化）。

## 5. 测试与验收

1. **等价面**：既有全部组装器/回放器用例经 `TransactionCollector` 断言零改动（夹具内部 `out::add` 换收集器）；`DecoupledEquivalenceTest` 升级为完整事件流全等（同步 vs 异步，头尾进断言——比 List\<Transaction\> 更严）
2. **流式时序证明**（新用例）：listener 在第一条 TxChange 后 countDown latch，主线程断言此刻 `processBucket` 尚未返回（Future 未完成）——直接证明"边回放边输出"
3. **堆峰验收**：结构论证（消费路径无累积容器）+ JMH `-prof gc` 前后对照（replayBucket 口径分配率下降）入档 baseline 2.0 段
4. **IT**：DecoupledPipelineTest 三场景经收集器全等断言；ReaderUnblockedTest/FrontierCapTest 的阻塞 listener 迁移到 `onEvent`（阻塞点选 Begin 后首条 TxChange 或 End，语义等价——frontier 在 End 后，FrontierCap 阻塞 End 更贴切）
5. **基准**：BenchPipeBridge.replay 改计数 sink 返回条数；AssembleMemoryBenchmark 断言输出计数；PipePathBenchmark.replayBucket 口径重跑入档

## 6. 非目标

- Logstash 集成（事件契约为其铺路，pipeline.send 映射属后续里程碑）
- onAbort 显式中止事件（fail-fast 下游下无真实消费者，YAGNI）
- 拉模式/多消费者/事务重排不做
- `Transaction` record 不删除（重组值对象）；reader 记账路径性能不追改（1.7.1 已归因收窄）

## 7. 交付物

- `replication` 包：`TransactionEvent`（新）、`TransactionCollector`（新）、`TransactionListener` 重定义、`TxChange implements TransactionEvent`、`TxBuffer.unitCount`、`BucketReplayer.replay` 签名改造、`TransactionConsumer` 流式化、`Transaction` javadoc 换角色
- `ConsoleListener.onEvent` 三段渲染（格式不变）；`Main` 装配（listener 直传 console）
- 测试迁移 + 新增（流式时序/事件流等价/收集器校验）+ 基准迁移 + baseline 2.0 段
- 文档：根/replication CLAUDE.md、jmh/CLAUDE.md、README 同步
