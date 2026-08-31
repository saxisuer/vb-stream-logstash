-- 场景 7：吞吐冒烟——三段负载喂 ThroughputMetrics（六速率 + 回放耗时/事务大小分布）
-- 负载 1 大批量普通事务：单语句 INSERT..SELECT generate_series 造 20 万行 × 200B（~42MB 消息流）。
--        领域要点：walsender 已追平时单语句批量写入的大事务**不触发流式**（整段于提交后回放）——
--        本段正是该形态：观察 slot 字节吞吐量级、事务大小分布的 max 极值、单桶整段回放耗时长尾。
-- 负载 2 流式大事务：16 批 × 32 行 × ~16KB 不可压缩载荷 + 0.3s 间隔（流式触发原理同场景 04——
--        rb->size 按 TOAST 压缩后记账，随机 md5 拼接不可压缩，数行即越过 64kB 阈值，间隔给
--        walsender 解码驱逐窗口）——观察 STREAMED 路径的分段到达与 512 行大事务回放。
-- 负载 3 高频小事务：DO 块内 2000 次 INSERT+COMMIT（PG 11+ 过程内 COMMIT）——观察组装/输出 tx/s
--        与 slot msg/s 的陡增窗口、小事务在分布段的形态。
-- 预期观察（Main 控制台每 10s 的"吞吐:/分布:"两行）：约 5~8 个统计窗口依次呈现三种负载形态。
-- 重跑语义：开头 TRUNCATE 自愈（TRUNCATE 本身也是一个 CDC 事务，会多一个 0 变更事务块）。
-- 时长：负载 1 数秒 + 负载 2 约 8s + 负载 3 数秒 ≈ 20s。
-- 注意：本脚本**不进** BenchCorpusRecordTest 的语料指纹（该测试按显式 6 脚本列表计摘要，
--        本文件是手工冒烟用途，百万级字节量也不适合做离线语料）。

-- 表与 publication：vb_pub 是单表白名单（initdb.d/02-init.sql），新表必须显式挂入；
-- ADD TABLE 无 IF NOT EXISTS 形态，DO 块捕 duplicate_object 幂等（重复执行静默通过）。
CREATE TABLE IF NOT EXISTS t_throughput
(
    id      int PRIMARY KEY,
    payload text
);

DO $do$
BEGIN
    ALTER PUBLICATION vb_pub ADD TABLE t_throughput;
EXCEPTION
    WHEN duplicate_object THEN NULL;   -- 已在 publication 中，重复执行幂等
END
$do$;

TRUNCATE t_throughput;

-- ---- 负载 1：大批量普通事务（单语句，generate_series，不触发流式） ----
BEGIN;
INSERT INTO t_throughput (id, payload)
SELECT g, repeat('x', 200)
FROM generate_series(1, 200000) AS g;
COMMIT;

-- ---- 负载 2：流式大事务（16 批 × 32 行 × ~16KB 不可压缩，触发 64kB 驱逐） ----
BEGIN;
DO $do$
BEGIN
    FOR batch IN 1..16 LOOP
        INSERT INTO t_throughput (id, payload)
        SELECT 300000 + (batch - 1) * 32 + g,
               (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512))
        FROM generate_series(1, 32) AS g;
        PERFORM pg_sleep(0.3);
    END LOOP;
END
$do$;
COMMIT;

-- ---- 负载 3：高频小事务（2000 个 autocommit 形态的独立事务） ----
DO $do$
BEGIN
    FOR i IN 1..2000 LOOP
        INSERT INTO t_throughput (id, payload) VALUES (900000 + i, 'small-tx');
        COMMIT;   -- 过程内提交：每次 INSERT 落为一个独立事务
    END LOOP;
END
$do$;
