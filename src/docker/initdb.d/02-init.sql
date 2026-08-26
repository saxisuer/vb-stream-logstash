-- 集成测试初始数据（仅首次 initdb 时执行，之后随 pgdata 持久化）
-- pgoutput 逻辑解码要求表在 publication 中才会产生变更事件。

CREATE TABLE IF NOT EXISTS t_stream_test (
    id         bigserial PRIMARY KEY,
    payload    text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE PUBLICATION vb_pub FOR TABLE t_stream_test;
