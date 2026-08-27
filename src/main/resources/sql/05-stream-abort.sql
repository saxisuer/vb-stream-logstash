-- 场景 5：流式大事务回滚——流段已下发后以 StreamAbort 收场，组装器应丢弃半组装事务
-- 预期 CDC：DEBUG 级可见 StreamStart/StreamInsert/StreamStop 分段陆续到达，随后 StreamAbort；
--           INFO 级无事务块输出（回滚事务不回放）
-- 显式 BEGIN ... ROLLBACK：DO 块是单语句，必须显式包裹才能回滚
-- 重跑语义：回滚不落库，天然幂等，无需清理

BEGIN;

DO $do$
DECLARE
    i int;
    p text;
BEGIN
    FOR i IN 201..212 LOOP
        p := (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512));
        INSERT INTO t_assembly_types (id, v_text, v_bigint) VALUES (i, p, i);
        PERFORM pg_sleep(0.7);
    END LOOP;
END
$do$;

ROLLBACK;
