-- VastBase 真库第一批（t_vbspike，验证后清理）
CREATE TABLE public.t_vbspike (id int PRIMARY KEY, s varchar(100), f float8, pad text);
ALTER TABLE public.t_vbspike REPLICA IDENTITY FULL;

BEGIN;
INSERT INTO public.t_vbspike (id, s, f) VALUES (1, 'alpha', 1.5), (2, 'beta', -0.0001), (3, 'gamma', 1e+20);
INSERT INTO public.t_vbspike (id, s, f) VALUES (4, 'delta', 3.141592653589793);
COMMIT;

INSERT INTO public.t_vbspike (id, s, f) VALUES (5, 'epsilon', 0);
