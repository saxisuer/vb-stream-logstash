-- 场景 3：DELETE 形态集
-- 前置：场景 1 已执行；本脚本开头重播种 id 6..9（已存在则跳过），可重复执行
-- 观察：old tuple 默认仅含主键列（replica identity = DEFAULT）

-- 重播种（自身构成一个 Insert 事务；行已存在时为 0 行变更、无事件）
INSERT INTO t_assembly_types (id, v_text, v_bigint, v_float, v_numeric, v_date, v_time, v_ts) VALUES
 (6, E'multi\nline\ntext',   10000000000, -0.5,   1000000000000.00, '2100-12-31', '08:08:08', '2100-12-31 08:08:08'),
 (7, '_PAYLOAD_8192_x',      777,          123.456789, 42.42,       '2026-01-01', '11:11:11', '2026-01-01 11:11:11'),
 (8, 'null big parts',       NULL,         NULL,   NULL,             '2025-06-15', NULL,       '2025-06-15 14:00:00'),
 (9, 'null time parts',      9,            9.9,    9.99,             NULL,         '10:20:30', NULL)
ON CONFLICT (id) DO NOTHING;

-- 事务 1：同事务内先 UPDATE 后 DELETE——观察组装器对事务内变更顺序的保序
BEGIN;
UPDATE t_assembly_types SET v_text = 'shortened' WHERE id = 6;
DELETE FROM t_assembly_types WHERE id = 6;
COMMIT;

-- 事务 2：单行删除
DELETE FROM t_assembly_types WHERE id = 7;

-- 事务 3：一条语句删 2 行——单事务 2 个 Delete
DELETE FROM t_assembly_types WHERE id IN (8, 9);
