package org.vastdata.vbstream.walsource.receive;

/**
 * walker 解析边界确定性失败（2026-10-15 离线回放真库 pg_xlog 拷段实证引入的分流类型）：
 * 字节不再构成当前 WAL 流的合法延续——页链锚定丢失（pageaddr 与期望页址断链，容差窗口
 * 扫尽）或记录头损坏（totLen &lt; 记录头尺寸，撕裂写的典型形态）。
 *
 * <p><strong>为什么需要专用类型</strong>：pg_xlog 段文件循环复用——写前沿之后的尾区不是
 * 零而是上一生命周期残留的旧 WAL（真库实证：残留页 pageaddr 的低 24 位恰等于文件内偏移，
 * 高位是旧地址）。裸拷 pg_xlog 活跃段喂静态目录档时，无 {@code endLsn} 上界的读尽会撞进
 * 残留区，本异常即"撞上写前沿"的指纹。{@code FileWalSource} 静态档凭本类型把<strong>清单
 * 末段</strong>的该失败降级为软 EOF（pg_waldump 语义——读到第一条不合法记录即停，已解
 * 出数据照常交付）；非末段撞到仍终态失败（段间断链 = 真缺口/真损坏）。
 * <strong>在线路径行为零变化</strong>：本类 extends {@link IllegalStateException}，
 * 接收器/直连源对 ISE 的 fail-fast 契约原样成立。</p>
 *
 * <p>边界语义：本类型只覆盖"字节流走读的解析边界"失败；sink 侧（catalog 重放/变更组装）
 * 抛出的其他 RuntimeException 不属于本类型，静态档一律终态失败——避免把真 bug 软化成
 * 静默截停。线程约束：单写者（walker 所在流线程）抛出，无并发面。</p>
 */
public class WalStreamParseException extends IllegalStateException {

    /**
     * 带诊断消息构造（消息语义与原裸 ISE 完全一致——期望页址/失配游标/实得值或损坏
     * 记录头的具体数值，供软 EOF 的 WARN 与终态失败的 ERROR 同面可诊断）。
     *
     * @param message 诊断消息
     */
    public WalStreamParseException(String message) {
        super(message);
    }
}
