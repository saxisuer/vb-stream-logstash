-- 场景 6：REPLICA IDENTITY FULL——UPDATE/DELETE 的 old tuple 携带全 8 列旧值（默认仅主键）
-- 预期 CDC：ALTER 后服务端重发 Relation 元数据（replica identity 标志变更）；
--           随后 UPDATE/DELETE 的 old tuple 含全部列旧值；末尾恢复 DEFAULT 再次触发 Relation 重发
-- 前置：场景 1 已执行（用到 id 1 与 id 10）；id 10 若已被删则重播种（已存在则跳过）

INSERT INTO t_assembly_types (id, v_text, v_bigint, v_float, v_numeric, v_date, v_time, v_ts)
VALUES (10, 'row-ten 中文🙂', -1, -2.5, -3.75, '2026-08-27', '13:45:30', '2026-08-27 13:45:30.123456')
ON CONFLICT (id) DO NOTHING;

-- 事务 1：切换 FULL（DDL，触发 Relation 重发）
ALTER TABLE t_assembly_types REPLICA IDENTITY FULL;

-- 事务 2：UPDATE——old tuple 含全列旧值（对照场景 2 只有 id）
UPDATE t_assembly_types SET v_text = 'ri-full-update' WHERE id = 1;

-- 事务 3：DELETE——old tuple 同样含全列旧值
DELETE FROM t_assembly_types WHERE id = 10;

-- 事务 4：恢复默认（再次触发 Relation 重发；后续场景回到仅主键的 old tuple）
ALTER TABLE t_assembly_types REPLICA IDENTITY DEFAULT;
