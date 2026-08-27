-- 集成测试初始数据（仅首次 initdb 时执行，之后随 pgdata 持久化）
-- pgoutput 逻辑解码要求表在 publication 中才会产生变更事件。


CREATE TABLE IF NOT EXISTS t_assembly_types
(
    id        int PRIMARY KEY,  -- 整数（主键）
    v_text    text,             -- 字符串
    v_bigint  bigint,           -- 大整数
    v_float   double precision, -- 浮点
    v_numeric numeric,          -- 精确小数
    v_date    date,             -- 日期
    v_time    time,             -- 时间
    v_ts      timestamp  -- 时间戳
);

CREATE PUBLICATION vb_pub FOR TABLE t_assembly_types;
