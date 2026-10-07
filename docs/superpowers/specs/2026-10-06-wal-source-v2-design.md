# vb-stream-wal-source v2 设计（用户表 DML 解码与输出）— 2026-10-06

> 状态：已评审定稿（brainstorming 五问 + 依赖方向用户纠正 + 流式语义修正 + 六节设计确认）
> 依据：v1 spec `2026-10-05-wal-source-module-design.md` 及其已交付实现（main 分支）；
> spike 九场景语义验证（`docs/wal-direct-decode-spike.md`）

## 1. 背景与目标

v1 交付了"伪备库"的两块地基：catalog 同步（pg_attribute/pg_class ctid 重放）与 WAL
接收（物理流/锚定协议/检查点持久化）。v2 在其上完成**用户表 DML 解码与事务组装**，
并以**双路对拍**证明与 engine（pgoutput 逻辑槽路径）的语义等价。

**模块定位原则（用户裁定，纠正初版方案的依赖设想）**：`vb-stream-engine` 与
`vb-stream-wal-source` 是**平级的解析工具**——前者解析逻辑复制槽输出，后者解析
物理复制槽/WAL；两者各自内含事务组装逻辑是领域固有，**不构成相互依赖的理由**。
输出格式对齐靠测试（双路对拍）钉死，不靠代码共享。

**v2 目标**
- 用户表 DML 解码：INSERT/UPDATE（新旧值）/DELETE（前像，replica identity 语义）
- TOAST 值面：chunk 采集、external 重组、pglz 解压、窗口前指针 JDBC 回查
- 事务组装：toplevel 归并、abort 过滤（BatchAborted 通知）、2PC 挂起/发射/弃桶
- 输出：TXN-BEGIN/逐行/TXN-END 文本输出，格式以 engine `ConsoleRenderer` 为
  复刻契约；**双路对拍 IT 断言两路输出逐字节一致**

**非目标**
- engine 模块的任何改动（两模块永零依赖，双路对拍中 engine 以黑盒跑）
- lz4 external 值解压（检测 + ISE fail-fast，文档前提；升级路径平滑）
- 矩阵外类型（超出 §5 首发集）的 text 渲染——降级 `hex:` + WARN，双路对拍在
  该集外会 diff，文档化为已知差异面
- 压缩 FPW（维持 v1 的 `wal_compression=off` 运维前提）
- VACUUM FULL/CLUSTER 全表重发的 CDC 语义（检测 + WARN 记档，v3 议）
- Debezium 连接器形态、file 落地（格式层三模块依赖是无争议后门，v3 议）

## 2. 决策记录

| 决策点 | 结论 | 备注 |
|---|---|---|
| 验收形态 | 引擎输出格式端到端对齐（双路对拍逐字节一致） | 输出格式 = engine streaming 模式输出的复刻契约 |
| 依赖关系 | **零相互依赖**（用户纠正） | 格式对齐靠双路对拍 IT，同 debezium19 双线同步约定模式 |
| TOAST 压缩面 | 仅 pglz（Java 内建移植） | lz4 列字典面提前 WARN + 运行期 ISE fail-fast |
| two_phase | 含 | XACT 分组天然等 commit 类记录；边际成本 ≈ 专项 IT |
| 表过滤 | `vb.wal.tables` 白名单，空 = 全部持久用户表（relkind 'r'/'p'） | 空 = `FOR ALL TABLES` 等价语义 |
| 事务发射模型 | **即时解码即时发射 + BatchAborted 通知**（流式修正） | 见 §4；输出侧 pending 缓冲保证"回滚零输出" |

## 3. 模块结构（changes 包新增）

```
wal-source（依赖面不变：pgjdbc + slf4j）
  changes/
    ChangeStream          门面：消费 WalRecord，TableFilter 过滤后喂 XactGrouper；
                          WalSource 门面把接收器 sink 单 pass 分发给
                          CatalogSynchronizer 与 ChangeStream（顺序：catalog 先应用，
                          DML 后解码——事务内 DDL 的 as-of 语义由顺序天然成立，
                          spike S5 已验证）
    XactGrouper           事务组装核心（§4）
    TableFilter           relkind + vb.wal.tables 白名单
    ToastAssembler        chunk 采集（valueid→seq→bytes）+ external 重组 +
                          pglz 解压 + 窗口前指针 JDBC 回查兜底
    Pglz                  PG pglz 解压（src/common/pg_lzcompress.c 移植）
    DiskValueRenderer     磁盘格式值 → PG text 表示（§5 渲染矩阵）
    OutputRenderer        pending 缓冲 + TXN-BEGIN/逐行/TXN-END 输出
  api/
    ChangeOutputListener  中立事件接口：BatchBegin(xid, lsn) /
                          RowChange(TableMeta, op, oldRow, newRow) /
                          BatchCommit(lsn) / BatchAborted(xid)
```

## 4. XactGrouper（事务组装核心）

**状态机**：`Map<xid, Bucket>` 待决桶；heap I/U/D 记录 → 即时解码（字典 as-of，
含 TOAST 重组）→ **即时发射** RowChange；XACT 记录驱动事务终态：

| XACT 记录 | 动作 |
|---|---|
| COMMIT | BatchCommit → 弃桶 |
| ABORT | BatchAborted → 弃桶 |
| PREPARE | 桶标记挂起（事件已发射，终态悬置） |
| COMMIT_PREPARED | BatchCommit；ABORT_PREPARED → BatchAborted |
| ASSIGNMENT(0x50) | 子 xid→toplevel 映射兜底（TOPLEVEL_XID 标记优先，spike 发现 25） |

**流式语义（设计修正的依据）**：pgoutput streaming 的价值是"大事务不占 server
内存、由下游落盘缓冲承接"——engine 的流式输出也是提交后才回放。WAL 本身就是
天然的落盘缓冲（进行中事务记录实时流经、不占内存、显式 ABORT 记录完备），
故 WAL 直解与 pgoutput streaming **架构同构**：源即时发射，"回滚零输出"由
输出侧 pending 缓冲保证。内存账本 O(事务) 在输出缓冲（与 engine block 模式
同级）；输出缓冲将来换落盘形态（格式层/file）即全链 O(1)——升级路径与
engine 从 block 到 MessagePipe 的演进平行。

**跨重启安全**：挂起桶终态悬置无碍——检查点 LSN 必然落后 PREPARE 位点，
重放重建桶并补发终态事件。

## 5. 值面（DiskValueRenderer + ToastAssembler）

**渲染矩阵**（磁盘格式 → PG text，对齐 pgoutput text 输出）：
- 首发类型集：bool/int2/int4/int8/float4/float8/numeric/text/varchar/bytea/
  date/time/timestamp/timestamptz/uuid——逐类型钉死渲染规则（numeric 精度、
  timestamptz 时区、负值与边界）
- 矩阵外类型：`hex:<十六进制>` + WARN 一次；dropped 列：`∅`
- 方法论复用 engine `BinaryValueDecoder` 的类型矩阵经验（同为"二进制 → text
  对齐"问题族，但布局是磁盘格式而非 typsend 格式）

**TOAST 重组**：
- chunk 采集：toast 关系 heap 记录（三列 chunk_id/chunk_seq/chunk_data）经
  字典 as-of 解码；toast 关系经 pg_class reltoastrelid 链定位（v1 字典已备）
- external 指针（18B 短 varlena，spike 发现 20）：valueid 拼装 + extsize 校验；
  `extsize < rawsize-4` → pglz 解压
- **窗口前指针 JDBC 回查**（默认开启）：重启续传后 UPDATE 未变 TOAST 列的
  指针指向重放窗口之前的 chunk（WAL 中不存在）——经 SQL 查 toast 表拼装
  （toast 表是普通可查表）；回查失败 → 该值降级 `toast-unavailable` + WARN，
  不 fail 整条流
- **lz4 检测**：字典面 attcompression=='l' 列启动期 WARN；运行期撞 lz4
  external → ISE fail-fast

## 6. 双路对拍验收（核心验收面）

```
DualPathParityIT（干扰容器变体，每场景两路同构跑 → 输出逐字节 diff 为空）：
  pgoutput 路：engine Main（零改动，黑盒）→ 输出 A
  wal 直解路：wal-source Main（v2 DML 输出面）→ 输出 B
  断言 A == B
```

场景矩阵：
1. 基础 DML：单行/多行/交错事务/回滚零输出/子事务 SAVEPOINT
2. 类型矩阵：首发集全类型 + 边界值（负数/NULL/微秒/UTF-8）
3. TOAST：不可压缩宽值（重组）+ 可压缩宽值（pglz 解压）+ 重启后未变列指针
   （JDBC 回查）
4. DDL-in-txn：事务内 ADD COLUMN 前后段 as-of 渲染
5. 2PC 四场景：挂起期输出为零（事件已发射、终态悬置于 pending 缓冲）/弃桶
   （BatchAborted）/COMMIT PREPARED 发射/挂起期重启续传
6. 生命周期：wal-source 检查点续传后输出与 pgoutput 全程输出对齐（跨重启的
   at-least-once 重复两侧一致）

**格式对齐契约**：OutputRenderer 输出格式以 engine `ConsoleRenderer` 为契约源，
变更需双端同步（debezium19 双线同步约定模式）；双路对拍 IT 是契约的执行机制。

## 7. 里程碑

| MS | 内容 | 验收 |
|---|---|---|
| MS1 值面 | DiskValueRenderer 矩阵 + Pglz 移植 + ToastAssembler（采集/重组/回查/lz4 检测） | 离线单测（手造磁盘字节 + pglz 样本往返）；渲染规则与 engine BinaryOutputTest 同族对拍 |
| MS2 事务组装 | XactGrouper（即时发射 + BatchAborted + 归并 + 2PC）+ 事件接口 + TableFilter | 离线单测：WalBytes 合成序列（交错/回滚/子事务/2PC/挂起重放） |
| MS3 输出与双路基座 | OutputRenderer + ChangeStream 接线 + 双路对拍场景 1/2/4 + Main DML 面 | 双路 diff 为空（基础/类型矩阵/DDL-in-txn）——风险区，+25% 缓冲 |
| MS4 对抗收官 | 场景 3/5/6 + 干扰容器全矩阵 + 模块文档 + 根索引 | 双路全矩阵 diff 为空；文档归档 |

## 8. 风险与开放问题

- **MS3 双路对拍基建**：两路输出的捕获与归一化（engine Main 输出捕获要干净——
  计划经 logger 配置隔离捕获 CDC 行）；MS1 类型矩阵逐类型对齐是最细的活
- **大事务输出缓冲内存**：O(事务) 文档化（与 engine block 模式同级），落盘化
  是 v3 升级路径
- **矩阵外类型差异面**：双路对拍仅在首发类型集内逐字节一致——文档化
- **VACUUM FULL 重写**：整表重发为 bulk INSERT 形态——v2 检测（新 relfilenode
  首批高频 multi-insert）+ WARN 记档，语义决策留 v3
- v1 遗留开放项顺延（56KB 跳变根因/17 丢 INSERT 复验——Task 13 收官注记：该丢失形态
  与引导一致性 C1 修复前的窗口同形（风暴推高跨窗口在途事务概率），C1 + Task 11
  pendingFloorLsn 后两个吞记录通道均已闭合，17 风暴专项复验待重跑后可勾销/
  跨多页长事务窗口）

## 9. 开发规约遵循

同 v1（方法名英文 camelCase、import 简名、slf4j、每函数 javadoc、TDD、
每任务 commit + push）。
