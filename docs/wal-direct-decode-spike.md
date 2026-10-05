# WAL 直解先导实验（spike）结论 — 2026-10-05

> 代码：`spike/wal-parse/WalParseSpike.java`（**throwaway**，不进 Maven 模块树、不参与构建；
> 布局转录自 REL_18_STABLE 头文件，fetch 日期 2026-10-05）

## 动机与问题

在不创建备库、不占用主库解码 CPU 的前提下，验证"伪装备库"路线：pgjdbc **物理复制流**收
原始 WAL 字节，纯 Java 手抄 PG 18 记录布局，把**单表 INSERT** 的行值按事务分组解出来。
主风险点：字节序/对齐、WAL 页头导航、FPW 页镜像提取、pgjdbc 物理流 API 完整度。

## 结论：主干成立（PASS，连续多次稳定复现）

14 个事务 24 个变更全部解出且与写入值**逐字节一致**（INSERT 行值、UPDATE 新旧整行、
DELETE 前像；含 UTF-8 文本 π、float8、bool、timestamp 微秒精度）；事务分组与提交序
全对；**catalog 重放器最小闭环打通**——字典不再硬编码，由 JDBC 引导 + WAL 重放驱动。
验证覆盖：

| 场景 | 结果 |
|---|---|
| S1：3 条 autocommit 单 INSERT + 5 行 multi-VALUES（显式事务） | ✓ 值全等、分组正确 |
| S1c：COPY 3 行 → 真 multi-insert 记录（HEAP2 0x50，3 tuple 单记录） | ✓ 逐 tuple 解码正确 |
| S2：CHECKPOINT 后 INSERT → FPW 记录（跨页 contrecord + 页镜像） | ✓ 记录拼接成功；页镜像重建（hole 拼接）→ ItemId 走读 → 页内 tuple 解码交叉验证 1/1 通过 |
| S3：两连接交错事务（A1,B1,A2,B2；先 commit A 后 B） | ✓ 按 xid 分组不串、提交序正确 |
| S4：默认 replica identity 的 UPDATE（无前像）→ `REPLICA IDENTITY FULL` 后 UPDATE（新旧整行）/ DELETE（前像整行） | ✓ 默认 RI 无 old tuple（与逻辑解码同约束，实证）；FULL 下 `U(old)>(new)`、`D(old)` 逐字节正确；HOT 更新（0x40）与普通更新同构解码 |
| S5：catalog 重放器——流中 `ALTER TABLE ADD COLUMN`，字典从 pg_attribute 自身的 WAL 记录按 ctid 重放（JDBC 引导含 ctid 种子） | ✓ ADD COLUMN 被捕捉（`extra attnum=6 typOid=25`）；ALTER 后的行用重放出的 6 列字典解码、第六列值正确，ALTER 前的行仍按当时 5 列字典——as-of 语义成立 |

## 过程中钉死的关键事实（正式模块的资产）

1. **pgjdbc 物理流 API 完整可用**（42.7.13）：`replicationStream().physical()
   .withStartPosition(lsn).start()`；`readPending()` 返回 ByteBuffer；块边界**不按页对齐**，
   但 `getLastReceiveLSN() - len` 可精确锚定每块起始 LSN——解析器须做成"绝对 LSN 游标 +
   跨块拼接"的字节流状态机，不能假设块=页。
2. **无 main data 的记录没有 DATA_SHORT/LONG 终止头**（如 XLOG_FPI）：块头区的终止判据是
   "剩余字节数 == 各块已声明的 image+data 总长"（xlogreader 同款判据）。
3. **C 结构体对齐填充会进 WAL**：`xl_heap_multi_insert` 的 `uint8 flags` 后有 1 字节 padding，
   `ntuples` 在 offset 2；`XLogRegisterData` 注册的是整块 scratch（含填充垃圾）。
   转录 C struct 到 Java 时必须按编译器规则补 padding，不能只看逻辑字段。
4. **ItemIdData 位段序**：`lp_off:15`（低位）→ `lp_flags:2`（bit 15-16）→ `lp_len:15`；
   **LP_NORMAL = 1**（不是 2，不是最高位）。
5. **FPW 页镜像的语义**：镜像是变更后状态（见发现 15 勘误）；既可用于按 offnum 解
   新行，也可读旧行（UPDATE/DELETE 前像、既有行回读）。
6. **块内载荷序 = [页镜像][block data]**，其后才是 main data；`mainOff+mainLen==totLen`
   可作布局自检不变量。
7. **tuple 列对齐相对 tuple 起点**（t_hoff + 偏移满足 typalign），WAL 载荷里 tuple 起点
   不在 8 字节边界——不能按外层缓冲区绝对位置对齐。
8. **multi-VALUES INSERT 走逐行 heap_insert**（12+1 条单条记录）；触发 heap_multi_insert
   需要 COPY / CTAS 类 bulk 路径。spike 用 COPY 覆盖了该记录形态。
9. **wal_level=logical 是 tuple 数据全量在 WAL 的硬前提**：`RelationIsLogicallyLogged`
   = XLogLogicalInfoActive && NeedsWAL && 非外播非 catalog——insert 设
   REGBUF_KEEP_DATA（FPW 时也保留数据）、multi-insert 仅在此时注册 tuple 数据。
   `wal_level=replica` 下 FPW 记录**不含** block data，必须走页镜像提取（可行，镜像即页）。
10. 记录形态普查（本次窗口）：heap 0x00/0x10/0x20/0x40/0x70/0x80、heap2 0x50/0x70、
    XACT 0x00~0x80（含 INVALIDATIONS 0x60）、XLOG_FPI 0xB0、btree、standby、smgr
    create——正式模块的过滤白名单以外的形态只需识别即可跳过。
11. 流起始的**孤立续体**（contrecord 头在窗口之前）需静默跳过；起始 LSN 向下对齐到页边界
    后首个页可能是续体页。记录跨页拼接要按"当前页剩余空间"取数，不能把累计消耗与页内
    偏移混在同一坐标系（多页记录必踩）。
12. **UPDATE/DELETE 的前像位置**：old tuple（replica identity 产出）拼在 **main data**
    尾部——`xl_heap_update`（14B：old_xmax u32/old_offnum u16/old_infobits u8/flags u8/
    new_xmax u32/new_offnum u16）之后、`xl_heap_delete`（8B）之后，格式同 insert 的
    `[xl_heap_header 5B][从 tuple offset 23 起的字节]`；new tuple 在 block 0 data（同
    insert）。HOT 更新（0x40）与普通更新（0x20）记录同构。
13. **prefix/suffix 截断**（`XLH_UPDATE_PREFIX/SUFFIX_FROM_OLD`）：更新未变更的首/尾列
    字节不落盘，需用 old tuple 重建——spike 以断言拦截（S4 通过改全列规避），正式模块
    必须实现（old tuple 可得时重建平凡）。
14. **catalog 变更不走 KEEP_DATA 且单行也走 multi-insert**（pg_waldump 实证）：catalog
    被 `RelationIsLogicallyLogged` 排除（IsCatalogRelation），FPW 记录**不含 tuple
    data**——数据只在页镜像里；且 `ALTER ADD COLUMN` 对 pg_attribute 的单行插入走
    HEAP2 MULTI_INSERT。catalog 重放器因此是双路径：非 FPW 记录用 block data、FPW
    记录从页镜像按 offsets 提取。
15. **FPW 页镜像是变更后状态**（勘误：S2 时一度误判为变更前）——redo 对
    XLADR_RESTORED 直接跳过应用即为证据；镜像里新行已就位（LP_NORMAL、lp_len 完整），
    直接按 offnum 从镜像解新行即可。
16. **ctid 重放模型**：pg_attribute 行集以 `(block, offnum)` 物理位置索引——insert
    upsert、delete 按位删、update 先删后插（与 heap 自身的 MVCC 移动同构）；JDBC 引导
    快照带 ctid 种子、重放 upsert 语义使重叠窗口幂等。pg_attribute 列布局按版本转录
    （PG 18 为 25 列，`attstattarget` 在 17+ 挪到尾部 varlen 区——版本漂移活例）。
    另：流起点/终点必须用 `pg_current_wal_flush_lsn()`——write 位点可能超前于 flush，
    START_REPLICATION 会拒绝超 flush 的起点。

## 与既有架构的衔接（若立项正式模块）

- 下游全复用：事务组装按 XACT commit/abort 记录分桶 = 现有 assembler 语义；输出管线
  （MessagePipe / 回放 / 格式层）不变。
- pgjdbc 物理流不建逻辑槽，槽管理退化为物理槽（或纯 LSN 记账）+ 自管 checkpoint。
- 新增机器三件：WAL 记录解析器（已验证主干）、磁盘格式 tuple 解码器（对齐/null
  bitmap/varlena 已验证，TOAST/pglz 压缩未做）、catalog 重放器（最小闭环已验证：
  pg_attribute/ctid/双路径；pg_class 等其余 catalog 未做）。

## 正式模块第二梯队验证清单（按风险排序）

1. ~~UPDATE / DELETE 记录 + replica identity 前像提取~~ **已验证**（S4，2026-10-05 补强）：
   RI FULL 下前像整行在 main data 尾、HOT 同构、默认 RI 无前像；遗留仅 prefix/suffix
   截断重建（见发现 13）
2. ~~catalog 重放器最小闭环~~ **已验证**（S5，2026-10-05 补强）：pg_attribute 按 ctid
   重放 + JDBC 引导幂等 + as-of 字典驱动解码 + FPW 镜像路径。遗留：catalog 的
   UPDATE/DELETE 镜像路径（本次窗口只有 insert）、压缩 FPW（pglz/lz4/zstd）、
   DROP COLUMN 的 attisdropped 标记流中验证
3. relfilenode 生命周期：TRUNCATE/VACUUM FULL/rewrite 的 SMGR 记录映射 + pg_filenode.map
   （XLOG_RELMAP）
4. TOAST：toast chunk 记录重组 + 未变列指针早于捕获起点的 JDBC 回查兜底
5. 子事务（XLOG_XACT_ASSIGNMENT）与 two_phase（commit/abort prepared）
6. ADD COLUMN DEFAULT 的 attmissingval 语义、DROP COLUMN 的 attisdropped 空洞
7. 每大版本布局差异的 descriptor 化（PG 17 对照样转录一组做 diff 验证）

## 复现

```bash
cd src/docker && docker compose up -d && cd ../..          # PG 18（wal_level=logical）
java -cp <pgjdbc-42.7.13.jar> spike/wal-parse/WalParseSpike.java
# 期望输出末行：>>> SPIKE RESULT: PASS
# DEBUG=true 可开逐记录轨迹（walker/块头/tuple 解码探针）
```
