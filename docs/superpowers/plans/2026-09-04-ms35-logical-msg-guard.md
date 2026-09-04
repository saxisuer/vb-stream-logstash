# MS3.5 LogicalMsg 记录与安全推进 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 'M' 逻辑消息按配置门控解析记录(INFO 日志,不发射下游)+ 非事务消息经护栏即时推进前沿(`min(msgLsn, min(未输出桶 commitLsn))`),crash 注入 IT 硬验收不丢数据。

**Architecture:** 槽选项经新 Field `slot.messages`(默认 false)门控;日志两时点(非事务 reader 即时/事务性 consumer 回放期);护栏为组装器包私有纯函数,reader 调用后写共享 outputFrontier(max 语义);End 路径不动。

**Tech Stack:** 既有栈(Debezium 3.6.1.Final / connector IT 基建 / Testcontainers postgres:18)。

**Spec:** `docs/superpowers/specs/2026-09-04-logical-msg-guard-design.md`(全部设计依据;安全性论证 §3.4 必读)。

## Global Constraints

- **护栏公式(含 off-by-one 修正)**:`safeMessageAdvance(msgLsn, handedOff) = min(msgLsn, min(state != DONE 桶的 commitLsn))`,无 pending → msgLsn;**必须用 commitLsn 不是 endLsn**(确认 AT endLsn = 跳过未输出事务 = 尾部丢失)
- 两条推进路径各用各的值:End 路径输出完成后写 endLsn(不动);护栏路径只到 commitLsn;同写一个 AtomicLong、max 语义
- **配置门控**:`slot.messages`(BOOLEAN 默认 false)四件套(Field/ALL_FIELDS.with/ConfigDef.define 取 Field.defaultValue/getter `messagesEnabled()`);false 时槽选项维持 4 项,行为与 MS3 完全一致
- **日志两时点**:非事务 reader 即时(routeLogicalMsg 无桶非事务分支,RawPeeks 窥 prefix/lsn/content,不整条解码);事务性 consumer 回放期(listener MsgChange 分支);INFO 一行 `逻辑消息: prefix={}, lsn={}, 事务性={}, content={}`;预览 text 截 64 字符 / bytea 十六进制前 32 字节
- 消息仍 `pipe.append`(不动"先 append 再路由"红线);无桶非事务 'M' 无桶引用、随 wipe-on-open 清除(javadoc 记);**在途桶刻意不进护栏参数**(javadoc/单测注释写死)
- 不动:offset/Connect 链路、事件族、发射路径;零 `org.vastdata.vbstream` import;javadoc 全覆盖;slf4j 禁 System.out;commit 末尾 `Co-Authored-By: Claude <noreply@anthropic.com>`;验证 `clean`;IT 需 Docker
- 发 'M' 的 SQL:`SELECT pg_logical_emit_message(false|true, '<prefix>', '<content>')`

**路径速查**:**NEW** = `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream`,**NEW-T** = 对应 test,**CONN-IT** = NEW-T/it。

---

### Task 1: 配置项 + 门控槽选项 + 护栏纯函数 + 日志预览件(TDD)

**Files:**
- Modify: `NEW/PostgresStreamConnectorConfig.java`(Field 四件套)
- Modify: `NEW/ReplicationSession.java`(槽选项条件第 5 项 `messages=true`;Parameters/构造携带开关——沿 Task 1(MS2)的 Parameters 形态加一个分量)
- Modify: `NEW/StreamedTransactionAssembler.java`(新增包私有静态 `safeMessageAdvance(long msgLsn, Deque<TxBuffer> handedOff)`)
- Create: `NEW/MessagePreview.java`(预览纯函数:text 截 64 / bytea hex 前 32——供两时点共用)
- Test: `PostgresStreamConnectorConfigTest` 补 2(默认 false/true 解析+ALL_FIELDS 含新名);`ReplicationSessionTest` 补 2(开关 false 恰 4 项/true 恰 5 项含 messages=true);`StreamedTransactionAssemblerTest` 补护栏全分支 5(无 pending→msgLsn/单 pending→其 commitLsn/多 pending→最小/msgLsn 更小→msgLsn/DONE 桶被排除);`MessagePreviewTest`(text/bytea/边界长度)

**Interfaces:**
- Produces(Task 2/3 消费):`safeMessageAdvance` 与 `MessagePreview.preview(byte[] content)`(签名以实现为准,javadoc 记截断规则);`config.messagesEnabled()`

- [ ] TDD 五步(红:各测试先行)→ commit `feat(ms35-t1): slot.messages 配置门控 + 护栏纯函数 + 消息预览件(TDD 全分支)`

---

### Task 2: 组装器接线 + listener MsgChange 分支

**Files:**
- Modify: `NEW/StreamedTransactionAssembler.java`(`routeLogicalMsg` 无桶非事务分支:WARN 丢弃 → INFO 日志(RawPeeks 窥 prefix/lsn/content + MessagePreview)+ `outputFrontier.accumulateAndGet(safeMessageAdvance(msgLsn, handedOff), max)`;组装器需能拿到 outputFrontier——同步构造本就自持,异步构造已有穿参,核对即可)
- Modify: `NEW/DispatcherTransactionListener.java`(MsgChange 分支:DEBUG 跳过 → INFO 日志(事务性=true,用 MsgChange 组件直取 prefix/content;仍不 dispatch))
- Test: `StreamedTransactionAssemblerTest` 补(非事务 'M' 经 PgWire.logicalMsg 造字节:日志断言可用 logback ListAppender 或以推进值断言为主——推进值 = min(msgLsn, pending) 经自持 outputFrontier 观察;有 pending 桶时推进被压到其 commitLsn 下;事务性 'M' 入桶不受影响);`DispatcherTransactionListenerTest` 补(MsgChange → INFO 形态、零 dispatch)

- [ ] TDD 五步 → commit `feat(ms35-t2): 组装器接线——非事务 'M' 即时记录+护栏推进;listener 事务性 'M' 回放期记录`

---

### Task 3: IT 心跳推进场景

**Files:**
- Create: `CONN-IT/LogicalMsgIT.java`(场景 1)

**场景**:config 开 `slot.messages=true`;纯非事务消息流(若干 `pg_logical_emit_message(false,'heartbeat',...)`,间隔 sleep 产生独立 WAL 段与反馈周期;无任何表事务)→ 断言:`confirmed_flush` 轮询**越过**首条消息 LSN(暖场边界锚点,照 FrontierCapIT 的 awaitPredicate 形态)——钉"空闲库不钉死"。Javadoc 写机制依据(spec §3.4"全发完"场景)。

- [ ] IT 先红后绿 → commit `feat(ms35-t3): IT 心跳推进——纯消息流 confirmed_flush 越过消息位`

---

### Task 4: IT crash 注入主验收 + 状态②重复语义

**Files:**
- Modify: `CONN-IT/LogicalMsgIT.java`(场景 2、3)

**场景 2(crash 注入主验收,硬验收)**:阻塞 listener(照 ReaderUnblockedIT 手法)使已提交事务滞留 HANDED_OFF → 发非事务心跳 → 断言 `confirmed_flush < 该桶 commitLsn`(护栏可见——轮询到稳态)→ 停 engine(确认已落库)→ **重启**(同 offset 文件)→ 放行/等待 → 断言滞留事务完整全达(BEGIN+全行+END,**尾部不丢**)。Javadoc:若对 restart_lsn/confirmed 语义理解有误此 IT 必红。
**场景 3(状态②重复)**:部分输出后停机重启 → 整事务重发 → 并集断言(Set 口径,重复允许)。

- [ ] IT 先红后绿 → commit `feat(ms35-t4): IT crash 注入主验收——护栏钉住未输出事务,重启尾部不丢 + 状态②整事务重发`

---

### Task 5: 记档更新 + 收官

**Files:**
- Modify: `docs/superpowers/specs/2026-09-02-ms2-r1-r3-audit.md`(LogicalMsg 条目"延期"→"部分实现:记录(slot.messages 门控)+ 安全推进(护栏 commitLsn)已落地;**发射仍延期**(专属 topic/prefix 过滤/发射载体设计要点保留)")
- Modify: 根 `CLAUDE.md` connector 行(一句话)+ 计划验收复选框
- 全量验收:`mvn clean test` 三段 SUCCESS;零引擎 import

- [ ] commit `docs: MS3.5 收官——LogicalMsg 记档转部分实现 + CLAUDE.md 更新`

---

## 验收汇总

- [ ] slot.messages=false 行为与 MS3 完全一致(4 槽选项,零 'M')
- [ ] 护栏纯函数全分支单测;推进值经 IT 场景 2 实证(confirmed_flush < pending commitLsn)
- [ ] 心跳推进 IT(纯消息流不钉死)
- [ ] crash 注入 IT 尾部不丢(硬验收)
- [ ] 状态②整事务重发(重复允许)
- [ ] 全量绿;零引擎 import;记档更新
