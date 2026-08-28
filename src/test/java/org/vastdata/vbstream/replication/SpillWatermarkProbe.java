package org.vastdata.vbstream.replication;

/**
 * 跨包测试桥：把 {@link TransactionAssembler} 的包私有 {@code spillWatermark()}（溢写低水位，
 * -1 = 溢写池从未建立）以公开静态方法透出给 {@code org.vastdata.vbstream.it} 包的集成测试。
 * 存在动机：溢写低水位与"spill 是否真的发生"是溢写路径集成测试的核心观测点
 * （防止"阈值配小但 spill 被短路"的假绿等价断言），但不宜为测试把内部状态机观测点升为公开 API；
 * 在测试源码目录以同包桥接类最小化解决。仅测试代码可用，不属于主代码契约。
 */
public final class SpillWatermarkProbe {

    private SpillWatermarkProbe() {
    }

    /**
     * 责任：读取组装器当前溢写低水位（语义同 {@code TransactionAssembler.spillWatermark()}）。
     * 边界：组装器溢写池从未建立（spill 被禁用或全程未越限）返回哨兵 -1；纯读无副作用，
     * 可在离线回放的任意两条消息之间调用做过程快照。
     *
     * @param assembler 被观测的组装器（非 null）
     * @return 低水位 CQ index；溢写池未建立时 -1
     */
    public static long of(TransactionAssembler assembler) {
        return assembler.spillWatermark();
    }
}
