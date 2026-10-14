-- VastBase 真库第二批（离线重放窗口数据）
BEGIN;
INSERT INTO public.t_vbspike (id, s, f) VALUES (100, 'hundred', 100.5), (101, 'hundred-one', 101.25);
UPDATE public.t_vbspike SET s = 'hundred-upd', f = 42.42 WHERE id = 100;
DELETE FROM public.t_vbspike WHERE id = 101;
COMMIT;

INSERT INTO public.t_vbspike (id, s, f, pad) VALUES (200, 'toasted', 2.5,
    repeat(md5(random()::text) || md5(random()::text), 100));

BEGIN;
INSERT INTO public.t_vbspike (id, s, f) VALUES (300, 'pre-ddl', 3.0);
ALTER TABLE public.t_vbspike ADD COLUMN extra int;
INSERT INTO public.t_vbspike (id, s, f, extra) VALUES (301, 'post-ddl', 3.1, 777);
COMMIT;

BEGIN;
INSERT INTO public.t_vbspike (id, s) VALUES (999, 'rolled-back');
ROLLBACK;

BEGIN;
INSERT INTO public.t_vbspike (id, s, f) VALUES (400, 'twophase', 4.0);
PREPARE TRANSACTION 'vbspike2pc';
COMMIT PREPARED 'vbspike2pc';
