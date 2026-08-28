package org.vastdata.vbstream.replication;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * 组装桶（1.7 设计 §4.1）：纯 CQ index 段记账——桶内不持有任何 payload 字节，堆占用只有元数据
 * （段数 × long[2] + oid/aborted 集合）。数据消息字节只在 reader 追加时写一次 CQ、consumer 回放时读一次。
 *
 * <p>字段语义：firstIndex/lastIndex 是桶内数据单元的 CQ index 全局端点（firstIndex &lt; 0 = 空桶）；
 * firstIndex 兼任 1.6 的 minSeq（seq ≡ CQ index，见组装器 javadoc），参与 registry 剪枝低水位。
 * segments 是 [first,last] 闭区间连续段列表（追加顺序）；连续段规则=上一次全局 append（含控制消息）
 * 是本桶数据消息才顺延，否则新开段——一段内全部是同桶数据单元（构造保证）。
 *
 * <p>hasPrefix 是桶级不变量：流式桶的单元恒在流块内收到（带 4 字节 xid 前缀）、普通/两阶段桶恒在
 * 块外（无前缀）；追加期校验，混现即 ISE fail-fast（协议不允许，防御）。回放据此决定 decodeSingle
 * 的 inStream 实参并重窥前缀值作 streamXid（子事务过滤用）。
 *
 * <p>线程约束：LIVE 期间仅 reader 线程触碰（单写者）。
 */
final class TxBuffer {

    final long xid;
    String gid;
    long firstIndex = -1L;
    long lastIndex = -1L;
    final ArrayDeque<long[]> segments = new ArrayDeque<>();
    /** 追加期窥出的 relation oid 集合（I/U/D 单 oid、T 多 oid、M 无）——交接快照圈定范围用。 */
    final Set<Integer> oidSet = new HashSet<>();
    boolean hasPrefix;
    boolean prefixKnown = false;
    final Set<Long> abortedSubxids = new HashSet<>();

    TxBuffer(long xid) {
        this.xid = xid;
    }
}
