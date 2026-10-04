# WAL 直解先导实验（spike）结论 — 2026-10-05

> 代码：`spike/wal-parse/WalParseSpike.java`（**throwaway**，不进 Maven 模块树、不参与构建；
> 布局转录自 REL_18_STABLE 头文件，fetch 日期 2026-10-05）

## 动机与问题

在不创建备库、不占用主库解码 CPU 的前提下，验证"伪装备库"路线：pgjdbc **物理复制流**收
原始 WAL 字节，纯 Java 手抄 PG 18 记录布局，把**单表 INSERT** 的行值按事务分组解出来。
主风险点：字节序/对齐、WAL 页头导航、FPW 页镜像提取、pgjdbc 物理流 API 完整度。

## 结论：主干成立（PASS，连续多次稳定复现）

8 个事务 16 行全部解出且与写入值**逐字节一致**（含 UTF-8 文本 π、float8、bool、
timestamp 微秒精度）；事务分组与提交序全对。验证覆盖：

| 场景 | 结果 |
|---|---|
| S1：3 条 autocommit 单 INSERT + 5 行 multi-VALUES（显式事务） | ✓ 值全等、分组正确 |
| S1c：COPY 3 行 → 真 multi-insert 记录（HEAP2 0x50，3 tuple 单记录） | ✓ 逐 tuple 解码正确 |
| S2：CHECKPOINT 后 INSERT → FPW 记录（跨页 contrecord + 页镜像） | ✓ 记录拼接成功；页镜像重建（hole 拼接）→ ItemId 走读 → 页内 tuple 解码交叉验证 1/1 通过 |
| S3：两连接交错事务（A1,B1,A2,B2；先 commit A 后 B） | ✓ 按 xid 分组不串、提交序正确 |

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
5. **FPW 页镜像是变更前状态**：新插入行的 offnum 在镜像里是 LP_UNUSED（lp_off 指向
   pd_upper 空闲位）；镜像的正确用途是读**旧行**（UPDATE/DELETE 前像、既有行回读）。
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
10. 记录形态普查（本次窗口）：heap 0x00/0x40/0x70/0x80、heap2 0x50/0x70、XACT 0x00~0x80
    （含 INVALIDATIONS 0x60）、XLOG_FPI 0xB0、btree、standby、smgr create——正式模块的
    过滤白名单以外的形态只需识别即可跳过。
11. 流起始的**孤立续体**（contrecord 头在窗口之前）需静默跳过；起始 LSN 向下对齐到页边界
    后首个页可能是续体页。记录跨页拼接要按"当前页剩余空间"取数，不能把累计消耗与页内
    偏移混在同一坐标系（多页记录必踩）。

## 与既有架构的衔接（若立项正式模块）

- 下游全复用：事务组装按 XACT commit/abort 记录分桶 = 现有 assembler 语义；输出管线
  （MessagePipe / 回放 / 格式层）不变。
- pgjdbc 物理流不建逻辑槽，槽管理退化为物理槽（或纯 LSN 记账）+ 自管 checkpoint。
- 新增机器三件：WAL 记录解析器（本 spike 已验证主干）、磁盘格式 tuple 解码器（对齐/
  null bitmap/varlena 已验证，TOAST/pglz 压缩未做）、catalog 重放器（未做）。

## 正式模块第二梯队验证清单（按风险排序）

1. UPDATE / DELETE 记录 + replica identity 前像提取（delete 前像仅 replica identity 非
   default 时在 WAL）
2. catalog 重放器：pg_attribute/pg_class 的 heap 记录按 ctid 索引重建行集 + JDBC 引导快照
   的重叠窗口幂等性
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
