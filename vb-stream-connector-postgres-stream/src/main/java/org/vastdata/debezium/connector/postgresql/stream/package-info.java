/**
 * PG 逻辑解码 stream 模式的 Debezium 连接器（debezium-connector-postgres-stream）。
 *
 * <p>双自建线程（vb-pgoutput-reader / vb-transaction-consumer）+ Chronicle Queue 管道 +
 * End 锚定 LSN 的流式 CDC 管道：reader 从复制槽 drain 轮询 raw 字节落管道、桶记账，
 * consumer 回放桶逐条经 Debezium dispatcher 发射 Kafka Connect 记录；快照/offset/
 * 事务元数据体系复用 io.debezium 的 debezium-connector-postgres。
 *
 * <p>包名刻意与 {@code io.debezium.*} 隔离：两连接器插件在同一 Kafka Connect 集群
 * 并存时零类冲突。设计见
 * {@code docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md}。
 */
package org.vastdata.debezium.connector.postgresql.stream;
