-- 第二批（备份检查点之后——模拟"作业暂停/库死亡窗口"的数据，只能靠离线 WAL 重放补齐）

-- txn: 多行 INSERT + UPDATE + DELETE
BEGIN;
INSERT INTO public.t_spike (id, s, f) VALUES (100, 'hundred', 100.5), (101, 'hundred-one', 101.25);
UPDATE public.t_spike SET s = 'hundred-upd', f = 42.42 WHERE id = 100;
DELETE FROM public.t_spike WHERE id = 101;
COMMIT;

-- txn: TOAST external 大值（不可压缩 md5 链 ~6.4KB → external 存储，chunk 采集重组验证）
INSERT INTO public.t_spike (id, s, f, pad) VALUES (200, 'toasted', 2.5,
    repeat(md5(random()::text) || md5(random()::text), 100));

-- txn: DDL-in-txn as-of（事务内 ADD COLUMN 前后段渲染）
BEGIN;
INSERT INTO public.t_spike (id, s, f) VALUES (300, 'pre-ddl', 3.0);
ALTER TABLE public.t_spike ADD COLUMN extra int;
INSERT INTO public.t_spike (id, s, f, extra) VALUES (301, 'post-ddl', 3.1, 777);
COMMIT;

-- txn: 回滚零输出
BEGIN;
INSERT INTO public.t_spike (id, s) VALUES (999, 'rolled-back');
ROLLBACK;

-- txn: 2PC 挂起后提交（验证挂起桶跨检查点/floor 面——本批提交前不等待）
BEGIN;
INSERT INTO public.t_spike (id, s, f) VALUES (400, 'twophase', 4.0);
PREPARE TRANSACTION 'spike2pc';
COMMIT PREPARED 'spike2pc';
