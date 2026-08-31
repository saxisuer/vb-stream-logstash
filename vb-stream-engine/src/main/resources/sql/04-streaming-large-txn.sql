-- 场景 4：流式大事务——进行中事务越过 logical_decoding_work_mem(64kB) 触发流式驱逐
-- 原理：rb->size 按变更元组 TOAST 压缩后大小记账；随机 md5 拼接（~16KB/行）不可压缩，
--       4~5 行即越过阈值；事务内分批 + 0.7s 间隔保证 walsender 在提交前完成解码驱逐
-- 预期 CDC：提交前 DEBUG 级可见 StreamStart/StreamInsert/StreamStop 分段陆续到达；
--           提交时 StreamCommit → INFO 级组装出 12-Insert 事务块
-- 重跑语义：先删 id 101..112（该 DELETE 是独立小事务）
-- 总时长约 8.4s（12 批 × 0.7s）

DELETE FROM t_assembly_types WHERE id BETWEEN 101 AND 112;

DO $do$
DECLARE
    i int;
    p text;
BEGIN
    FOR i IN 101..112 LOOP
        p := (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512));
        INSERT INTO t_assembly_types (id, v_text, v_bigint) VALUES (i, p, i);
        PERFORM pg_sleep(0.7);
    END LOOP;
END
$do$;
