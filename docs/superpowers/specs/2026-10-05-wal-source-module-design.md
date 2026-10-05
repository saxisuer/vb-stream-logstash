# vb-stream-wal-source 模块设计（v1）— 2026-10-05

> 状态：已评审定稿（brainstorming 五问 + 执行模型选型 + 六节设计均经用户确认）
> 依据：先导实验 `spike/wal-parse/WalParseSpike.java`（throwaway）与
> `docs/wal-direct-decode-spike.md`（9 场景 / 27 条发现，全部 PASS 可复现）

## 1. 背景与动机

在不创建备库、不占用主库逻辑解码 CPU 的前提下，以"伪装备库"方式获取行级变更流：
pgjdbc **物理复制流**接收原始 WAL 字节，纯 Java 解析记录布局，自维护 catalog 字典
（pg_attribute/pg_class 按 ctid 重放），后续版本在此之上解码用户表 DML 并汇入
`vb-stream-engine` 既有管线。

先导实验已证明：该路线的全部基础语义（事务组装、前像、catalog 重放、relfilenode
生命周期、TOAST、子事务、列增删）均可在纯 Java 中完整解出，27 条布局/语义发现入档。
v1 把其中最硬的两块——**catalog 同步**与 **WAL 接收**——做成生产可用的正式模块。

## 2. 目标与非目标

**v1 目标（仅两件事）**
- A. 伪备库 catalog 同步：JDBC 一致性引导 + WAL 重放维护 pg_attribute/pg_class
  的 as-of 快照（含 ctid、relfilenode、toast 映射、dropped 语义）
- B. WAL 接收：物理复制流接收、解析、位点管理、状态持久化

**非目标（明确不做）**
- 用户表 DML 解码与 engine 汇入（v2；输出契约已为其预留）
- TOAST chunk 采集与 external 重组（v2；v1 的两张 catalog 表行内无超界值）
- two_phase（PREPARE/COMMIT PREPARED）记录消费
- 压缩 external 值（pglz/lz4/zstd）与压缩 FPW
- Debezium 连接器形态、VACUUM FULL 全表重发的 CDC 语义

## 3. 决策记录

| 决策点 | 结论 | 备注 |
|---|---|---|
| 挂接点 | 产出喂 `vb-stream-engine` 解析；本模块为独立纯库 | 引擎侧"多 source 抽象"留待 v2 接入时做 |
| 持久化 | v1 包含（catalog 快照 + LSN 检查点原子落盘） | 重启安全是 v1 核心价值 |
| 版本面 | **PG 17 + 18 双版本** descriptor，双版本 IT 矩阵 | Pg17TestEnv 复用；每大版本一笔转录税认了 |
| 验收强度 | **对抗性 IT**：autovacuum 拉满/并发 DDL/CHECKPOINT 注入下对拍全等 | 与"catalog 同步生产可用"的卖点同构 |
| catalog 集合 | pg_attribute + pg_class 两张 | 重放器通用（注册制），pg_type 等留 v2 |
| 执行模型 | **单线程直通**（接收→解析→重放→周期落盘） | v1 数据量下游刃有余；组件接口化留解耦位 |

## 4. 模块边界与依赖

- 新 Maven 模块 `vb-stream-wal-source`，聚合 parent 加一行
- 运行依赖仅：`org.postgresql:postgresql`（与 parent 同版本）+ `slf4j-api`
- 零 engine/Chronicle/logback 依赖——依赖方向是将来 engine（或连接器）依赖本模块
- 包结构：`org.vastdata.vbstream.walsource` 下
  `layout`（版本 descriptor）/ `receive`（流接收）/ `replay`（catalog 重放）/
  `state`(持久化) / `api`（输出契约）+ 冒烟 `Main`
- 模块 CLAUDE.md 与 README 随 MS4 归档（仓库惯例）

## 5. 组件与接口

五个组件，全部面向接口（实现可独立替换/测试）：

| 组件 | 接口要点 | 职责 |
|---|---|---|
| `WalLayout` | `pageMagic()` / `pageSize()` / 记录布局查询 / `supports(pgVersionNum)` | 版本化布局常量：`WalLayoutV17`/`WalLayoutV18`，全部从 REL_STABLE 源码头文件转录、javadoc 标注出处；对端版本不识别 → 启动即拒（fail-fast） |
| `WalStreamReceiver` | `start(listener)` / `consumedLsn()` / `stop()` | pgjdbc 物理流 + 物理槽管理（建/复用/按消费位点确认）；绝对 LSN 游标状态机：跨块拼接、contrecord 拼接、流首孤立续体跳过；**页头 pageaddr 连续性锚定** |
| `WalRecordParser` | `WalRecord parse(bytes, lsn)` / `isWatched(rmid)` | 记录头/block 头/heap 家族（INS/UPD/DEL/HOT/INPLACE/MULTI_INSERT/PRUNE）→ 强类型 record；布局差异全部委托 `WalLayout` |
| `CatalogSynchronizer` | `apply(WalRecord)` / `CatalogSnapshot snapshot()` | 通用 ctid 重放引擎（spike `heapEvents` 正式化）；watched 表注册制（relfilenode + 行提取器）；relfilenode/toast 跟踪；JDBC 一致性引导；竞速三修正（§6） |
| `StateStore` | `checkpoint(snapshot, lsn)` / `StoredState load()` | 原子持久化（§7） |

**输出契约（`api` 包）——v2 engine 的接入点**
- `CatalogSnapshot`：as-of 查询——`columnsOf(relOid)`（含 dropped 占位语义）、
  `currentRelfilenode(relOid)`、`toastOf(relOid)`、快照 LSN
- `WalSource` 门面：`start()/stop()`、`consumedLsn()`、`snapshot()`、指标只读面
  （记录普查计数、再同步计数、检查点时延）
- 冒烟 `Main`：周期 INFO 打印 catalog 变更事件、位点推进、记录 census

## 6. 竞速鲁棒性三修正（核心质量设计）

spike 实测通过率 ~1/3 的三根刺及 v1 解法：

1. **引导一致性**：顺序固定为——建/复用物理槽记录 consistent_point P₀ →
   引导查询在 REPEATABLE READ 单事务内执行（两表全行含 ctid）→
   流从 max(P₀ 页对齐, 引导后) 起收，窗口重叠由 ctid upsert 幂等消化。
   种子与流起点从此有确定边界。
2. **自愈校验**：截断更新遇未知 oldCtid 的候选重建，以**中段新值对 JDBC 末态
   校验**（relfilenode/relpages/reltuples 至少一项与该 oid 实查一致才采纳），
   废弃 spike 的 OID 自校验（prefix 来自候选、恒真无鉴别力）；采纳即修 tracked
   ctid 至记录 new 位；校验失败 → WARN + 标记 stale 待下轮引导。
3. **锚定协议**：以页头 `xlp_pageaddr` 为唯一权威——状态机维护期望页地址逐页校验；
   失配时块内扫描下一个合法页头（magic + pageaddr 容差内）再同步；失配/再同步
   计数进指标，超容差 WARN + IT 失败。废除 `getLastReceiveLSN - len` 裸算。

另：PRUNE/INPLACE 作为 `WalRecord` 一等公民全量重放；
**`pg_read_binary_file` 页读兜底整体删除**，由值编码重建（spike 发现 24）替代。

## 7. 持久化格式（StateStore）

单文件 `wal-source-state.bin`（目录 `vb.wal.state.dir` 可配），VBFG 风格：

```
[header] magic 'VBWS' + formatVersion(u16=1) + pgVersion(u16) + lsn(u64) + CRC32
[pg_attribute 表] rowCount(u32) + 行 { ctid(block u32, off u32) + 解码后模型字段（数值定长、
                 字符串以前缀长度存） }
[pg_class 表]    同构
[footer] lsn(u64) 复述 + CRC32（整文件校验）
```

- 写：全量序列化 `.part` → fsync → 原子 rename（reader 模块既有范式）
- 读：header/footer CRC + lsn 双验；损坏或版本不符 → 拒载 + ERROR，**回落全新引导**
  （安全侧：宁可重引导，不可错位窗口重放）
- 检查点：每 30s（`vb.wal.state.interval.ms` 可配）或每 1000 条 catalog 事件
  （`vb.wal.state.events` 可配），先到为准；取当前已消费 LSN——单线程下天然
  一致点，无需快照冻结

## 8. 测试与验收矩阵

**离线单测（零 Docker，秒级）**
- `layout`：手造字节（spike `MsgBuilder` 范式）——页头/记录头/块头走读、contrecord
  拼接、无 main data 终止判据、C padding、ItemId 位段、varlena 双头、external 18B
- `replay`：合成记录序列——ctid 重定位/删、PRUNE 三段、INPLACE 值重建、截断更新
  值编码重建（含 prefix 越界拒绝路径）、dropped 占位
- `state`：往返、CRC 损坏拒载、版本不符拒载

**对抗性 IT（Testcontainers，postgres:18 + Pg17TestEnv 双矩阵）**
- 对拍：DDL 场景序列（建表/加列/默认值列/删列/TRUNCATE/改名/重建 toast/并发交错）
  在干扰下执行，`snapshot()` 与 JDBC 实查（REPEATABLE READ 点）逐行全等——ctid、
  relfilenode、toast 映射、dropped、列序
- 干扰注入：autovacuum 拉满（cost_delay=0/阈值调低）、独立连接循环造死元组驱动
  PRUNE、随机 CHECKPOINT、统计风暴
- 生命周期：断流重连续传（位点不回退不跳段）、重启加载检查点续传（对拍仍全等）、
  状态损坏回落重引导、跨版本启动拒绝
- 验收含**再同步计数为 0**（容差内静默）

## 9. 里程碑

| MS | 内容 | 验收 |
|---|---|---|
| MS1 | 模块骨架 + `WalLayoutV18` + `WalRecordParser` + 离线单测 | 手造字节全家族解析绿 |
| MS2 | `WalStreamReceiver`（物理槽 + 锚定协议）+ 冒烟 Main + 基础 IT | census 与 pg_waldump 一致 |
| MS3 | `CatalogSynchronizer`（引导一致性 + 全形态重放 + 三修正）+ 对抗性对拍 IT | 干扰下 18 对拍全等（风险区，+25% 缓冲） |
| MS4 | `StateStore` + 生命周期 IT + `WalLayoutV17` 转录 + 17 全矩阵 + 文档归档 | 双版本全绿 |

## 10. 风险与开放问题

- **MS3 是风险集中区**：对抗性场景的失败可诊断性（IT 需能在失败时 dump 快照 diff）
- **PG 17 转录**：以"与 V18 descriptor 的 diff IT"钉死；17 的 prune/catalog 差异
  （如 pg_attribute attstattarget 位置）已在 spike 记档
- **WAL 保留**：物理槽 + `max_slot_wal_keep_size` 兜底；consumer 停摆场景与 engine
  同语义（磁盘增长告警），文档化即可
- **pgjdbc 物理流 API 面**：spike 已验证核心路径；若时间线处理遇缺口，回落自组
  CopyData（复制协议很薄），MS2 内消化

## 11. 开发规约遵循

方法名英文 camelCase、import 简名、slf4j 日志、每函数 javadoc（含布局出处的源码
行号引用）、TDD（superpowers:test-driven-development）、每任务完成 commit + push。
