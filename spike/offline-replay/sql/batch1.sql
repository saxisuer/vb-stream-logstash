-- 第一批（在线检查点前——数据将进入备份时点的检查点）
CREATE TABLE public.t_spike (id int PRIMARY KEY, s varchar(100), f float8, pad text);
ALTER TABLE public.t_spike REPLICA IDENTITY FULL;

BEGIN;
INSERT INTO public.t_spike (id, s, f) VALUES (1, 'alpha', 1.5), (2, 'beta', -0.0001), (3, 'gamma', 1e+20);
INSERT INTO public.t_spike (id, s, f) VALUES (4, 'delta', 3.141592653589793);
COMMIT;

-- autocommit 单行（独立事务）
INSERT INTO public.t_spike (id, s, f) VALUES (5, 'epsilon', 0);
