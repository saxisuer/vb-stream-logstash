# CDC 数据生成场景脚本

对 `src/docker` 的 PostgreSQL（compose 环境）执行的数据构造脚本，用于在 `Main` 挂槽状态下观察
pgoutput 逻辑解码的各类行为。表 `t_assembly_types` 与 publication `vb_pub` 由 `initdb.d/02-init.sql`
创建（重建环境：`docker compose down && rm -rf pgdata && docker compose up -d`）。

## 用法

```bash
docker exec -i vb-stream-pg psql -U postgres -d postgres -v ON_ERROR_STOP=1 \
  < src/main/resources/sql/01-insert-types.sql
```

**前置顺序**：先启动 `Main`（槽在启动时创建，槽创建点之前的 WAL 不会被解码），再执行脚本。

## 场景一览

| 脚本 | 场景 | 预期观察（Main 控制台） |
|---|---|---|
| `01-insert-types.sql` | 8 类型边界值插入 10 行 | 单事务 10 个 Insert：空串 vs NULL（`t`/`n` 标志）、多行文本、unicode/emoji、bigint/double 极值 |
| `02-update-cases.sql` | UPDATE 形态集（5 个独立事务） | TOAST 化长文本；new tuple 未动大字段为 `u`（unchanged TOAST）；置 NULL（`n`）；NULL 恢复；单语句批量改 3 行 = 单事务 3 个 Update |
| `03-delete-cases.sql` | DELETE 形态集 | 同事务先 UPDATE 后 DELETE（保序）；单行删；单语句删 2 行 = 单事务 2 个 Delete；old tuple 默认仅含主键 |
| `04-streaming-large-txn.sql` | 流式大事务（触发驱逐） | 12 行 × ~16KB 不可压缩载荷、分批间隔 0.7s；提交前 DEBUG 级可见 StreamStart/StreamInsert/StreamStop 分段陆续到达，提交时 StreamCommit → INFO 组装 12-Insert 事务块 |
| `05-stream-abort.sql` | 流式大事务回滚 | 流段已下发后 StreamAbort；组装器丢弃半组装事务，INFO 级无事务块输出 |
| `06-replica-identity-full.sql` | REPLICA IDENTITY FULL | ALTER 触发 Relation 元数据重发；UPDATE/DELETE 的 old tuple 携带全 8 列旧值；末尾恢复 DEFAULT（再次触发 Relation 重发） |

## 说明

- **可重跑**：脚本自带清理或 `ON CONFLICT DO NOTHING` 重播种；注意清理用的 DELETE 本身也是一个 CDC 事务。
- **流式触发原理**（`04`/`05`）：reorder buffer 的 `rb->size` 按变更元组 **TOAST 压缩后**大小记账，
  阈值 `logical_decoding_work_mem=64kB` 是全局值；随机 md5 拼接（~16KB/行）不可压缩，4~5 行即越过，
  且事务内分批 + 间隔保证 walsender 在提交前完成解码驱逐。规则图案（`repeat('x',N)`）会被 pglz
  压到百字节级，少量行永远触发不了。
- **逐消息观察**：Stream\* 分段等原始消息走 DEBUG 渲染，默认关闭；临时调
  `src/main/resources/logback.xml` 中对应 logger 为 `DEBUG` 可见。
- **极值注意**：id=4 的 `v_float=-1e+308`，对其做乘法会 double 溢出（语句级回滚，不产生 CDC 事件）。
