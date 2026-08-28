package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.ByteBuffer;

/**
 * raw 契约 → 旧解码契约的桥：把 {@link RawMessageListener} 的原始字节交付适配为
 * {@link PgOutputListener} 的消息对象交付，行为与接缝改造前 run 循环内置的
 * "decode → registry.accept → 回调" 链路逐字节等价。
 *
 * <p>自持一套 {@link PgOutputDecoder}（含流块状态机 inStream）与 {@link RelationRegistry}，
 * 两者随桥实例生命周期存续——同一桥只能重放**一条**按序消息流；对同一 raw 流从头新建桥
 * 重放，可得到与在线解码完全一致的消息序列（record 值相等，RawSessionContractTest 验证）。
 *
 * <p>线程约束：非线程安全。decoder 的 inStream 状态要求本桥只被单一读取线程调用；
 * registry 跨线程查询安全（ConcurrentHashMap），与既有约定一致。
 */
public final class DecodedMessageBridge implements RawMessageListener {

    private final PgOutputListener target;
    private final PgOutputDecoder decoder;
    private final RelationRegistry registry;

    /**
     * 绑定解码交付目标与流式模式。
     *
     * @param target 解码后的消息回调（旧契约，原样透传 registry）
     * @param mode   流式模式，仅影响 decoder 对 StreamAbort 附加字段的解析（与
     *               {@code PgReplicationSession.start} 传给服务端的 streaming 参数须一致，
     *               不一致时 abort 消息解析错位 fail-fast）
     */
    public DecodedMessageBridge(PgOutputListener target, StreamingMode mode) {
        this.target = target;
        this.decoder = new PgOutputDecoder(mode);
        this.registry = new RelationRegistry();
    }

    /**
     * 单条 raw 消息的解码交付：wrap → decode（维护流块状态）→ Relation 入缓存 → 回调 target。
     * 边界与异常语义：字节与协议不符时由 decoder fail-fast 抛
     * {@link org.vastdata.vbstream.protocol.ProtocolMisalignmentException} /
     * {@link org.vastdata.vbstream.protocol.UnknownMessageTypeException}，
     * 经 run 循环上抛终止会话线程——与改造前 session 内联解码的行为一致。
     */
    @Override
    public void onRaw(byte[] raw) {
        PgOutputMessage message = decoder.decode(ByteBuffer.wrap(raw));
        registry.accept(message);
        target.onMessage(message, registry);
    }

    /** 暴露桥持有的 Relation 缓存（测试断言/上层组装器查询用）。 */
    public RelationRegistry registry() {
        return registry;
    }
}
