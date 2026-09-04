# 2026-09-04 · LogicalMsg 缺口补齐设计(MS3.5:记录 + 安全推进)

## 1. 背景与目标

MS3 记档了 LogicalMsg('M')延期(见 `2026-09-02-ms2-r1-r3-audit.md` "已知限制与延期"节):非事务消息需要"即时前沿推进点",动 End 锚定模型,单独设计。本设计按用户裁定的缩小范围补齐缺口:

- **不发射到下游**(不进 Kafka topic——发射仍延期,设计要点保留)
- **解析日志记录**(每条 'M' 留痕)
- **位置推进带安全护栏**(非事务消息即时推进前沿,严格防"错误推进导致重启丢数据")

三个已确认的设计决策:日志 INFO 逐条+预览;记录分时点(非事务即时/事务性回放期);独立小里程碑 MS3.5(MS3 合并后从新 main 拉分支)。

## 2. 机制现状(不动部分)

- 协议层 'M' 解码已就绪(MS1):`LogicalMsg(streamXid, transactional, lsn, prefix, content)`,字节级测试在档
- 前沿唯一推进点 = consumer 的 `processBucket` 中 **End 返回之后**写 `bucket.endLsn`(`outputFrontier.accumulateAndGet(endLsn, max)`)——"推进顺序 == 输出顺序"是 at-least-once 安全证明的全部
- reader 每轮循环读前沿封顶回传:`capFeedback(min(已收到, 前沿))` → setAppliedLSN/setFlushedLSN → 周期 forceUpdateStatus;PG 侧 confirmed_flush 落库经 candidate 机制(重启锚点 = 槽 confirmed_flush,MS3 Task 4 考古)
- 组装器对 'M' 现状:事务性/有桶 → appendUnit 入桶(回放期 listener DEBUG 跳过);无桶非事务性 → WARN 丢弃;槽选项未含 `messages=true`(PG 根本不下发)

## 3. 设计

### 3.1 槽选项(1 行)

`ReplicationSession` 槽选项 4→5 项:加 `messages=true`(PG 14+;既有 streaming/two_phase 选项已隐含更高版本假设,无条件加)。

### 3.2 日志两时点

| 形态 | 时点 | 线程 | 位置 |
|---|---|---|---|
| 非事务 'M' | 收到即记 | reader | `routeLogicalMsg` 无桶非事务分支(原 WARN 丢弃处改造):INFO 一行 `逻辑消息: prefix={}, lsn={}, 事务性=false, content={}`,content 预览 text 截 64 字符 / bytea 十六进制前 32 字节(照项目渲染惯例),经 RawPeeks 窥取(不整条解码) |
| 事务性 'M' | 回放期记 | consumer | `DispatcherTransactionListener` 的 MsgChange 分支(原 DEBUG 跳过):INFO 同款格式,`事务性=true`——天然跳过 aborted 子事务的消息(回滚的不记),与 CDC 数据语义对齐 |

两处均**不入下游**。消息本身仍 `pipe.append`(不动"先 append 再路由"红线);无桶非事务 'M' 无桶引用,落盘后随 wipe-on-open 清除(javadoc 记)。

### 3.3 护栏(核心新件)

```java
// StreamedTransactionAssembler 包私有静态纯函数
static long safeMessageAdvance(long msgLsn, Deque<TxBuffer> handedOff) {
    // pending = state != DONE 的交接桶;返回 min(msgLsn, min(pending.commitLsn))
    // 无 pending → msgLsn
}
```

调用点:非事务分支记日志后 `outputFrontier.accumulateAndGet(safeMessageAdvance(...), max)`。`outputFrontier` 组装器构造已有(同步自持/异步穿参),零新增接线。

**为何用 commitLsn 而非 endLsn**(设计评审中修正的 off-by-one):confirmed_flush 语义 = "commit 结束位 ≤ 确认值的事务视为已送达,重启跳过"。若护栏在某未输出桶的 **endLsn** 上取 min,确认值 == 其 endLsn → 该桶被跳过 → 已输出头部还在、未输出尾部**永久丢失**。`commitLsn`(commit 记录自身 LSN,恒 < endLsn)保证该桶 commit 结束位 > 确认值 → 重启**整桶重发**(头部重复允许,尾部补齐)。桶封箱元数据同时存 commitLsn/endLsn(引擎同款),零额外成本。

**两条推进路径各用各的值**:End 路径输出完成后写 endLsn(跳过 = 正确,已完成);护栏路径输出完成前只到 commitLsn(留出整事务重发空间)。同写一个 AtomicLong、同一 max 语义,证明自洽。

### 3.4 安全性论证(状态枚举)

reader 单线程按 WAL 序处理,消息 X 到达时每个事务必居其一:

| 状态 | 重启后 | 下游视角 |
|---|---|---|
| ① 已输出(DONE,前沿 ≥ 其 endLsn) | 跳过 | 不重复不丢失(已在下游) |
| ② 已提交未输出完(pending 桶) | 整事务重发(护栏 < 其 commitLsn) | 头部重复(允许,下游靠事务元数据过滤)+ 尾部补齐 |
| ③ 在途未提交(live 桶,endLsn 未知) | PG restart_lsn 回放整体重发(其 commit 在 WAL 序上必然 > X) | 完整到达(可能重复) |

- **状态②不可能漏账**:Commit 记录 C 与 X 有 WAL 序——C < X 则 reader 先处理 C 先入账(handedOff 是 reader 维护的);C > X 则属状态③。同线程同顺序,无竞态窗口
- **"全发完了"场景(无 pending)→ safe = X 是安全上限本身**:已输出事务前沿已覆盖;在途属状态③;X 之后的新事务 LSN > X 重放覆盖——IT 场景 1 钉它
- **可见性方向单调**:guard 读 `state`(volatile)与 `commitLsn`(handoff 时写定)。consumer 次序"先写前沿后标 DONE"——看到 DONE:前沿已覆盖,排除正确;看到旧值:护栏保守压低,只多重复不丢。两方向安全,无需加锁
- **在途桶刻意不进护栏参数**:其安全性由 WAL 序论证承担;firstIndex 非 LSN,塞进 min 反而引入类型/语义混乱(javadoc 与单测注释写死)
- **状态③的 restart_lsn 断言必须实证**:crash 注入 IT 是硬验收(见 4.2 场景 2),不做纯机制断言

### 3.5 不动的面

offset/Connect 链路(log-only 无记录无 offset,确认走既有前沿→reader 回传路径)、事件族、发射路径、配置面(日志级别固定 INFO,messages=true 无条件,均不加配置项)。

## 4. 测试面

### 4.1 单测

- 护栏纯函数全分支:无 pending→msgLsn / 单 pending→其 commitLsn / 多 pending→最小 / msgLsn 更小→msgLsn / DONE 桶被排除
- `routeLogicalMsg` 改造:日志被调 + 推进值正确(经 outputFrontier 观察)+ 不 appendUnit
- listener MsgChange 分支:INFO 形态(事务性=true)
- 日志预览截断形态(text 64 / bytea hex 32)

### 4.2 IT(真 PG,硬验收)

1. **心跳推进**:纯非事务消息流(无事务)→ `confirmed_flush` 越过消息 LSN——钉"空闲库不钉死"的原始动机
2. **crash 注入主验收**:阻塞 listener 使已提交事务滞留 HANDED_OFF → 发心跳 → 断言 `confirmed_flush < 该桶 commitLsn`(护栏可见)→ 停 engine(确认已落库)→ 重启 → 完整事务全达(**尾部不丢**;若对 restart_lsn/confirmed 语义理解有误此 IT 必红)
3. **状态②重复语义**:部分输出后停机重启 → 整事务重发 → 并集断言(重复允许,下游靠事务元数据过滤)

## 5. 里程碑与交付

MS3.5,独立分支(**MS3 PR 合并后**从新 main 拉),约 5 任务:

| 任务 | 内容 |
|---|---|
| T1 | 槽选项 messages=true + 日志两时点 + 护栏纯函数(TDD 全分支单测) |
| T2 | 组装器接线(routeLogicalMsg 分支改造)+ listener MsgChange 分支 |
| T3 | IT 心跳推进场景 |
| T4 | IT crash 注入主验收 + 状态②重复语义 |
| T5 | 记档更新(audit 文档 LogicalMsg 条目"延期"→"部分实现:记录+安全推进已落地;发射仍延期")+ 收官 |

## 6. 决策记录

| # | 决策 | 依据 |
|---|---|---|
| L1 | 不发射下游,仅日志记录 | 用户裁定(缩小范围) |
| L2 | 日志 INFO 逐条+预览(text 64/bytea hex 32) | 用户裁定 |
| L3 | 记录分时点:非事务 reader 即时 / 事务性回放期(回滚不记) | 用户裁定 |
| L4 | 护栏 = min(msgLsn, min(未输出桶 commitLsn));方案 A(reader 即时推进+纯函数),否决微型事务包装(过重/心跳被积压阻塞)与不推进(WAL 兜底反而制造丢失) | 设计评审;用户确认 |
| L5 | off-by-one 修正:commitLsn 而非 endLsn(确认 AT endLsn = 跳过未输出事务 = 尾部丢失) | 用户质询暴露,评审确认 |
| L6 | 独立里程碑 MS3.5,MS3 合并后开工 | 用户裁定 |
