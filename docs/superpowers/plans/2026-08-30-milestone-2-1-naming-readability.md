# 里程碑 2.1 实施计划：命名辨识度改造

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 纯改名里程碑——输出契约族与 Console 实现类按形态对称方案重命名，主代码 + 活文档同步，行为零变化。

**Architecture:** 5 个主类 + 3 个测试类经 IDE rename refactoring 重命名（引用/`{@link}`/文件名随动），三份活文档（根 CLAUDE.md、replication/CLAUDE.md、README.md）词边界替换，历史 spec/plans/baseline 文档不动。设计依据：`docs/superpowers/specs/2026-08-30-milestone-2-1-naming-readability-design.md`。

**Tech Stack:** Java 17 + Maven；IDEA MCP `rename_refactoring`（主）；perl 词边界替换（辅/兜底）。

## Global Constraints（每个任务隐含遵守）

- **纯命名，零行为变化**：不新增/删除任何逻辑、不改任何 lambda/回调/stream 形态；diff 只允许出现标识符与 javadoc 措辞变化
- **外部面不动**：配置键 `vb.output.mode`/`vb.pg.*`/`vb.pipe.*`、线程名 `transaction-consumer`/`pgoutput-reader`、logger 名 `org.vastdata.vbstream.cdc`、日志文本、控制台输出格式（TXN-BEGIN/逐行/TXN-END）逐字节不变
- **`BlockTransactionListener` 保留不改**——其文件名/类名含 `TransactionListener` 子串，一切文本替换必须用词边界（perl `\b`，禁用无边界 sed），替换后必须验证 `BlockTransactionListener` 原样存在
- **历史文档不追改**：`docs/superpowers/specs/**`、`docs/superpowers/plans/**`、`docs/benchmarks/baseline` 类（`docs/benchmarks-baseline.md`）出现旧名属预期，严禁修改
- **验证一律 `mvn clean test`**（增量编译假绿是已知陷阱）；158 用例全绿是每个代码任务的硬门
- 注释/javadoc 中文风格、slf4j 规约照旧；无新依赖
- 每任务结束 commit + push（跨机开发规约）；commit message 中文，`refactor`/`docs` 前缀

**改名对照表（全计划唯一事实源）**

| 旧名 | 新名 |
|---|---|
| `TransactionListener` | `StreamingTransactionListener` |
| `BlockOutputAdapter` | `StreamingToBlockAdapter` |
| `TransactionCollector` | `TransactionRecorder` |
| `ConsoleListener` | `ConsoleRenderer` |
| `BlockOutputAdapterTest` | `StreamingToBlockAdapterTest` |
| `TransactionCollectorTest` | `TransactionRecorderTest` |
| `ConsoleListenerTest` | `ConsoleRendererTest` |

**旧名引用基线（改动前实测，供核对消减）**：`Main.java` 9、`TransactionAssembler.java` 7、`DecoupledEquivalenceTest.java` 8、`TransactionListener.java` 2、`BlockOutputAdapter.java` 3、`TransactionCollector.java` 1、`ConsoleListener.java` 5、`TransactionConsumer.java` 2、`BlockTransactionListener.java` 2（javadoc 提及，随改名更新）、`TransactionEvent.java`/`Transaction.java`/`RelationLookup.java`/`OutputMode.java` 各 1、`TransactionCollectorTest.java` 6、`TransactionAssemblerTest.java` 6、`ConsoleListenerTest.java` 6、`BlockOutputAdapterTest.java` 4、`StreamingDeliveryTest.java` 1、it/ 四测试各 3。

**说明（对 TDD 的偏离）**：本里程碑是纯改名重构，不写新测试——每个任务的"测试环"= 既有 158 用例在改名后保持全绿 + grep 旧名零残留。

---

### Task 1: 输出契约族改名（原子提交）

**Files:**
- Modify/Rename: `src/main/java/org/vastdata/vbstream/replication/TransactionListener.java` → `StreamingTransactionListener.java`
- Modify/Rename: `src/main/java/org/vastdata/vbstream/replication/BlockOutputAdapter.java` → `StreamingToBlockAdapter.java`
- Modify/Rename: `src/main/java/org/vastdata/vbstream/replication/TransactionCollector.java` → `TransactionRecorder.java`
- Modify/Rename: `src/test/java/org/vastdata/vbstream/replication/BlockOutputAdapterTest.java` → `StreamingToBlockAdapterTest.java`
- Modify/Rename: `src/test/java/org/vastdata/vbstream/replication/TransactionCollectorTest.java` → `TransactionRecorderTest.java`
- Modify（引用随动）: `src/main/java` 下 `Main.java`、`TransactionAssembler.java`、`TransactionConsumer.java`、`BlockTransactionListener.java`、`TransactionEvent.java`、`Transaction.java`、`RelationLookup.java`、`OutputMode.java`；`src/test/java` 下 `DecoupledEquivalenceTest.java`、`TransactionAssemblerTest.java`、`StreamingDeliveryTest.java`、`it/TransactionAssemblyTest.java`、`it/ReaderUnblockedTest.java`、`it/FrontierCapTest.java`、`it/DecoupledPipelineTest.java`

**Interfaces:**
- Consumes: 无（首任务）
- Produces: `StreamingTransactionListener`（接口，方法 `onEvent(TransactionEvent)` 签名不变）、`StreamingToBlockAdapter`、`TransactionRecorder`（类名，后续任务与文档同步依赖）

- [ ] **Step 1: IDE 重命名 `TransactionListener` → `StreamingTransactionListener`**

用 IDEA MCP `rename_refactoring`：`pathInProject=src/main/java/org/vastdata/vbstream/replication/TransactionListener.java`，`symbolName=TransactionListener`，`newName=StreamingTransactionListener`（文件随类名自动改）。

兜底（MCP 不可用/失败时，后续各步同）：
```bash
git mv src/main/java/org/vastdata/vbstream/replication/TransactionListener.java \
       src/main/java/org/vastdata/vbstream/replication/StreamingTransactionListener.java
perl -pi -e 's/\bTransactionListener\b/StreamingTransactionListener/g' $(grep -rl '\bTransactionListener\b' src/)
```

- [ ] **Step 2: 清点残留并修复**

```bash
grep -rn '\bTransactionListener\b' src/ | grep -v '\bBlockTransactionListener\b'
```
预期：仅剩 IDE 未改到的注释纯文本行（如有）。逐文件修复：
```bash
perl -pi -e 's/\bTransactionListener\b/StreamingTransactionListener/g' <残留文件列表>
```
复跑 grep 至零输出；同时验证保留名未损：
```bash
grep -rc '\bBlockTransactionListener\b' src/main/java/org/vastdata/vbstream/replication/BlockTransactionListener.java   # 预期 ≥1
```

- [ ] **Step 3: 快速编译门**

```bash
mvn -q clean compile
```
预期：BUILD SUCCESS。

- [ ] **Step 4: 重命名 `BlockOutputAdapter` → `StreamingToBlockAdapter`（含测试类）**

IDE `rename_refactoring` 两连：`BlockOutputAdapter`（主类）与 `BlockOutputAdapterTest`（测试类）。兜底同 Step 1 模式（两个类各一次 git mv + perl，测试类 perl 词 `\bBlockOutputAdapter\b` 会同时更新 `BlockOutputAdapterTest` 内的 `@Test` 类名引用与类声明——注意 perl 替换 `\bBlockOutputAdapter\b` 不匹配 `BlockOutputAdapterTest`（`r` 与 `T` 之间无边界），类声明行由 git mv 前对该文件单独执行 `s/\bBlockOutputAdapterTest\b/StreamingToBlockAdapterTest/g` 覆盖）。

- [ ] **Step 5: 重命名 `TransactionCollector` → `TransactionRecorder`（含测试类）**

同 Step 4 模式：主类 + `TransactionCollectorTest` 两连 rename。

- [ ] **Step 6: 三旧名全量残留清点**

```bash
grep -rnE '\b(TransactionListener|BlockOutputAdapter|TransactionCollector)\b' src/ | grep -v '\bBlockTransactionListener\b'
```
预期：零输出（有则 perl 修复后复跑）。再验证编译：
```bash
mvn -q clean test-compile
```

- [ ] **Step 7: 全量测试门**

```bash
mvn clean test
```
预期：BUILD SUCCESS，Tests run: 158, Failures: 0, Errors: 0。

- [ ] **Step 8: commit + push**

```bash
git add -A src/
git commit -m "refactor(replication): 输出契约族形态对称改名——StreamingTransactionListener/StreamingToBlockAdapter/TransactionRecorder

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

---

### Task 2: ConsoleListener → ConsoleRenderer（含测试类）

**Files:**
- Modify/Rename: `src/main/java/org/vastdata/vbstream/ConsoleListener.java` → `ConsoleRenderer.java`
- Modify/Rename: `src/test/java/org/vastdata/vbstream/ConsoleListenerTest.java` → `ConsoleRendererTest.java`
- Modify（引用随动）: `src/main/java/org/vastdata/vbstream/Main.java`

**Interfaces:**
- Consumes: Task 1 的 `StreamingTransactionListener`（本类 implements 它）
- Produces: `ConsoleRenderer`（Task 3 文档同步依赖的类名）

- [ ] **Step 1: IDE 重命名两连**

`rename_refactoring`：`ConsoleListener` → `ConsoleRenderer`（`pathInProject=src/main/java/org/vastdata/vbstream/ConsoleListener.java`）；随后 `ConsoleListenerTest` → `ConsoleRendererTest`（`pathInProject=src/test/java/org/vastdata/vbstream/ConsoleListenerTest.java`）。兜底：两个类各一次 git mv + perl（词 `\bConsoleListener\b` 与 `\bConsoleListenerTest\b`）。

- [ ] **Step 2: 残留清点 + 保留名验证**

```bash
grep -rn '\bConsoleListener\b' src/        # 预期零输出
grep -rc '\bStreamingTransactionListener\b' src/main/java/org/vastdata/vbstream/ConsoleRenderer.java   # 预期 ≥1（implements 关系在）
mvn -q clean test-compile                  # 预期 BUILD SUCCESS
```

- [ ] **Step 3: 全量测试门**

```bash
mvn clean test
```
预期：BUILD SUCCESS，158 用例全绿。

- [ ] **Step 4: commit + push**

```bash
git add -A src/
git commit -m "refactor: ConsoleListener→ConsoleRenderer——实现类与契约接口命名分层

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

---

### Task 3: 活文档同步

**Files:**
- Modify: `CLAUDE.md`（根，10 处旧名）
- Modify: `src/main/java/org/vastdata/vbstream/replication/CLAUDE.md`（10 处旧名）
- Modify: `README.md`（1 处旧名）
- 验证不动（零命中）: `src/main/java/org/vastdata/vbstream/protocol/CLAUDE.md`、`src/jmh/CLAUDE.md`、`src/test/java/org/vastdata/vbstream/it/CLAUDE.md`

**Interfaces:**
- Consumes: Task 1/2 的全部新类名
- Produces: 无（终文档状态）

- [ ] **Step 1: 词边界批量替换三份活文档**

```bash
perl -pi -e 's/\bTransactionListener\b/StreamingTransactionListener/g; s/\bBlockOutputAdapter\b/StreamingToBlockAdapter/g; s/\bTransactionCollector\b/TransactionRecorder/g; s/\bConsoleListener\b/ConsoleRenderer/g' \
  CLAUDE.md src/main/java/org/vastdata/vbstream/replication/CLAUDE.md README.md
```
（`\b` 保证 `BlockTransactionListener` 不被误伤。）

- [ ] **Step 2: 活文档残留与误伤双查**

```bash
grep -rnE '\b(TransactionListener|BlockOutputAdapter|TransactionCollector|ConsoleListener)\b' \
  CLAUDE.md README.md $(find src -name CLAUDE.md) | grep -v '\bBlockTransactionListener\b'   # 预期零输出
grep -rc '\bBlockTransactionListener\b' src/main/java/org/vastdata/vbstream/replication/CLAUDE.md   # 预期 ≥1（保留名原样）
grep -c 'StreamingTransactionListener' src/main/java/org/vastdata/vbstream/replication/CLAUDE.md    # 预期 ≥5（新名已就位）
```
逐处人工抽读三份文档中被替换行上下文（`git diff`），确认语句通顺——尤其根 CLAUDE.md 架构图里的 `TransactionListener.onEvent(TransactionEvent)` 行应变为 `StreamingTransactionListener.onEvent(TransactionEvent)`。

- [ ] **Step 3: 全库终态 grep（排除面留在历史文档属预期）**

```bash
grep -rnE '\b(TransactionListener|BlockOutputAdapter|TransactionCollector|ConsoleListener)\b' \
  --include='*.java' --include='*.md' src CLAUDE.md README.md docs 2>/dev/null | grep -v '\bBlockTransactionListener\b' \
  | grep -v '^docs/superpowers/' | grep -v '^docs/benchmarks-baseline.md'
```
预期：零输出。

- [ ] **Step 4: commit + push**

```bash
git add CLAUDE.md README.md src/main/java/org/vastdata/vbstream/replication/CLAUDE.md
git commit -m "docs: CLAUDE.md×2+README 同步 2.1 新名——契约族与 ConsoleRenderer

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

---

### Task 4: 终验收（spec §7 四条标准）

**Files:** 无改动（纯验证；若发现问题回上游任务修复后重跑）

**Interfaces:**
- Consumes: Task 1-3 全部产物
- Produces: 验收结论（四条全过 = 里程碑完成）

- [ ] **Step 1: 标准一——全量测试**

```bash
mvn clean test
```
预期：BUILD SUCCESS，Tests run: 158, Failures: 0, Errors: 0。

- [ ] **Step 2: 标准二——行为零变化（diff 形态审查）**

```bash
BASE=09450f4   # 里程碑前提交 = 2.1 spec 修订提交（Task 1 前的 HEAD；可经 git log --oneline -5 核对，为其后第一条 refactor 提交之前）
git diff --stat $BASE..HEAD -- src/ | tail -5
git diff $BASE..HEAD -- src/ | grep -E '^[+-]' | grep -vE '^[+-]{3}' | grep -vE 'Streaming|Recorder|Renderer' | head -50
```
第二条命令输出 = diff 中与改名无关的增删行，逐行确认全部属 javadoc 措辞/上下文行（如整行含旧名被替换后的新行会带新名关键词被滤掉；真正意外出现的逻辑改动——包括线程名/配置键/日志文本的变动——必须回查修复）。

- [ ] **Step 3: 标准三——旧名零残留**（复跑 Task 3 Step 3 命令，预期零输出）

- [ ] **Step 4: 标准四——javadoc 规约抽检**

抽读五个改名后文件的类级与方法级 javadoc：`StreamingTransactionListener.java`、`StreamingToBlockAdapter.java`、`TransactionRecorder.java`、`ConsoleRenderer.java`、`StreamingToBlockAdapterTest.java`。确认：每函数仍含职责/步骤/边界描述（项目规约），`{@link}` 指向有效新名（IDE 无红色引用，或 `mvn -q clean test-compile` 已证）。

- [ ] **Step 5: 收尾**

四条全过 → 里程碑完成，报告用户（含改名对照表与验证数字）。发现任何不过项 → 定位回对应任务修复、commit、重跑本任务。
