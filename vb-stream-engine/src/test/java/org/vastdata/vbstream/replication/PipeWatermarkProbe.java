package org.vastdata.vbstream.replication;

/**
 * 跨包测试桥：把 {@link TransactionAssembler} 的包私有 {@code pipeWatermark()}（CQ 删除低水位）
 * 以公开静态方法透出给 {@code org.vastdata.vbstream.it} 包的集成测试。仅测试代码可用，不属于
 * 主代码契约。
 */
public final class PipeWatermarkProbe {

    private PipeWatermarkProbe() {
    }

    /**
     * 责任：读取组装器当前 CQ 删除低水位（语义同 {@code TransactionAssembler.pipeWatermark()}）。
     * 纯读无副作用，可在离线回放的任意两条消息之间调用做过程快照。
     *
     * @param assembler 被观测的组装器（非 null）
     * @return 低水位 CQ index（≥0；未 append 过时为 0，无 -1 哨兵——管道恒存在）
     */
    public static long of(TransactionAssembler assembler) {
        return assembler.pipeWatermark();
    }
}
