package org.vastdata.vbstream.replication;

/**
 * raw 字节消费者契约：{@link PgReplicationSession#run(RawMessageListener)} 的交付接缝。
 *
 * <p>分量语义：{@code raw} 是**完整单条** pgoutput 消息的字节序列——含首个类型字节与其后全部
 * 字段（流式块内消息含可选的 Int32 xid 前缀，见 spec 附录 A），即解码器可直接消费的整体。
 * 每次回调的数组为该条消息独占新建，调用方可无复制地长期持有（spill 落盘、延迟重放皆安全）。
 *
 * <p>线程约束：回调在执行 run() 的复制读取线程内**同步**调用——回调耗时直接拖慢消息循环与
 * LSN 反馈，实现方不应阻塞；需要旧解码契约（消息对象 + Relation 缓存）的上层用
 * {@link DecodedMessageBridge} 适配。
 */
@FunctionalInterface
public interface RawMessageListener {

    /** 消费一条 raw 消息。抛出的异常经 run 循环原样上抛，终止会话线程。 */
    void onRaw(byte[] raw);
}
