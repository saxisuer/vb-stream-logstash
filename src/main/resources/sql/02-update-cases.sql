-- 场景 2：UPDATE 形态集——5 个独立事务（psql 逐语句自动提交）
-- 前置：场景 1 已执行（操作 id 1..5）
-- 重跑语义：可重复——事务 1/2 重设载荷，事务 3/4 为置空/恢复循环，事务 5 用 CASE 写死目标值
-- 注意：id=4 的 v_float 为 -1e+308 极值，勿对其做算术（double 溢出 → 语句回滚，无 CDC 事件）

-- 事务 1：text 换成 ~16KB 不可压缩随机载荷（TOAST 化；随机 md5 拼接压缩后仍存满 16384）
UPDATE t_assembly_types
SET v_text = (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512))
WHERE id = 2;

-- 事务 2：同一行只改数值列——new tuple 中 v_text 列预期 'u'（unchanged TOAST，未动的大字段不重传）
UPDATE t_assembly_types SET v_bigint = 1234567890123, v_float = 9.87654321 WHERE id = 2;

-- 事务 3：置 NULL——对应列的 'n' 标志
UPDATE t_assembly_types SET v_numeric = NULL, v_time = NULL WHERE id = 1;

-- 事务 4：NULL 恢复为值——与事务 3 对照
UPDATE t_assembly_types SET v_numeric = 99.99, v_time = '07:07:07' WHERE id = 1;

-- 事务 5：一条语句批量改 3 行——单事务 3 个 Update 变更（值写死保证重跑结果一致）
UPDATE t_assembly_types
SET v_float = CASE id WHEN 3 THEN 0.5 WHEN 4 THEN -5e+307 WHEN 5 THEN 5e-16 END
WHERE id IN (3, 4, 5);
