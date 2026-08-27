package org.vastdata.vbstream.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * pgoutput 消息 → 原子事务的组装状态机（纯内存、无 IO）。
 *
 * <p>职责：按消息驱动规则（spec §4.1）缓冲同事务变更，收到提交信号（Commit/StreamCommit/
 * CommitPrepared）后封箱为不可变 {@link Transaction} 回调；回滚路径（RollbackPrepared/
 * StreamAbort）剔除或丢弃缓冲后不回调。
 *
 * <p>桶模型（spec §4.2/§4.3，含对 spec 的实现细化）：
 * <ul>
 *   <li>普通事务：单指针 {@code currentNormalTx}——Commit 消息无 xid 字段，且 walsender 按
 *       LSN 序串行输出 Begin..Commit，同时至多一个活动普通事务</li>
 *   <li>流式事务：{@code streamedByXid} 多桶（key=StreamStart 的顶层 xid）+ {@code currentStream}
 *       流块上下文指针——多个并发大事务的流段会交错（spec §4.2 已源码验证），流块本身不嵌套</li>
 *   <li>两阶段：活动期单指针 {@code currentPrepareTx}，PREPARE 后转 {@code preparedByGid}
 *       挂起池等待 COMMIT PREPARED / ROLLBACK PREPARED</li>
 * </ul>
 *
 * <p>线程约束：非线程安全。设计为在单一 run 循环线程内被调用（与 PgOutputDecoder 同约束）；
 * 输出的 Transaction 不可变，可跨线程传递。
 */
public final class TransactionAssembler {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionAssembler.class);

    private final TransactionListener listener;

    /** 活动普通事务桶（Begin 置位，Commit 封箱清空；协议保证 Begin..Commit 串行不嵌套）。 */
    private TxBuffer currentNormalTx;
    /** 活动两阶段事务桶（BeginPrepare 置位，Prepare 转挂起池）。Task 5 使用。 */
    private TxBuffer currentPrepareTx;
    /** 当前流块上下文：stream_start..stream_stop 之间非 null，指向 streamedByXid 中某桶。 */
    private TxBuffer currentStream;
    /** 流式事务桶，key=顶层 xid（多桶并存，段间交错——spec §4.2）。 */
    private final Map<Long, TxBuffer> streamedByXid = new HashMap<>();
    /** 两阶段挂起池，key=gid（PREPARE 至 COMMIT/ROLLBACK PREPARED 之间，可能长期挂起）。Task 5 使用。 */
    private final Map<String, TxBuffer> preparedByGid = new HashMap<>();

    /** 组装缓冲：xid 与变更序列；gid 仅两阶段桶非 null。非线程安全（仅 run 线程触碰）。 */
    private static final class TxBuffer {
        final long xid;
        String gid;
        final List<TxChange> changes = new ArrayList<>();

        TxBuffer(long xid) {
            this.xid = xid;
        }
    }

    /**
     * 构造组装器。
     *
     * @param listener 完整事务到达时的回调（同步调用，调用线程与本组装器的调用线程一致）
     */
    public TransactionAssembler(TransactionListener listener) {
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    /**
     * 喂入一条已解析消息（调用方需先让 registry 消化 Relation 元数据，参照 Main 的装配顺序）。
     *
     * <p>关键步骤：按消息类型分发到对应规则（spec §4.1 全路径表）；任何桶缺失/重复/流块状态
     * 异常均抛 {@link IllegalStateException}（fail-fast，协议流不应出现）。
     *
     * @param message  协议消息（19 种之一）
     * @param registry 关系元数据缓存，用于把变更的 relationOid 解析为快照
     */
    public void accept(PgOutputMessage message, RelationRegistry registry) {
        // 注：record pattern switch 是 Java 21 正式特性，本项目约束 Java 17，故用 instanceof 链
        if (message instanceof PgOutputMessage.Begin m) {
            begin(m);
        } else if (message instanceof PgOutputMessage.Commit m) {
            commit(m);
        } else if (message instanceof PgOutputMessage.Insert m) {
            activeBucket(m).changes.add(new RowChange(DmlKind.INSERT, registry.require(m.relationOid()),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Update m) {
            activeBucket(m).changes.add(new RowChange(DmlKind.UPDATE, registry.require(m.relationOid()),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Delete m) {
            activeBucket(m).changes.add(new RowChange(DmlKind.DELETE, registry.require(m.relationOid()),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(registry::require)
                    .toList();
            activeBucket(m).changes.add(new TruncateChange(snapshots, m.options(), m.streamXid()));
        } else if (message instanceof PgOutputMessage.LogicalMsg m) {
            logicalMsg(m);
        } else if (message instanceof PgOutputMessage.StreamStart m) {
            streamStart(m);
        } else if (message instanceof PgOutputMessage.StreamStop m) {
            streamStop();
        } else if (message instanceof PgOutputMessage.StreamCommit m) {
            streamCommit(m);
        } else if (message instanceof PgOutputMessage.StreamAbort m) {
            streamAbort(m);
        } else if (message instanceof PgOutputMessage.BeginPrepare m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.Prepare m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.CommitPrepared m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.RollbackPrepared m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.StreamPrepare m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.Relation || message instanceof PgOutputMessage.Type) {
            // 元数据消息：registry 职责（调用方已转发），组装器不处理
        } else if (message instanceof PgOutputMessage.Origin) {
            // 级联复制源位点：本里程碑非目标，透传忽略
        } else {
            throw new IllegalStateException("未知消息类型: " + message.getClass());
        }
    }

    /**
     * 取当前应接收变更的活动桶。
     *
     * <p>查找顺序（spec §4.3）：流块上下文（最高优先）→ 活动两阶段桶 → 活动普通桶；
     * 三者皆空说明变更消息游离在任何事务外，协议流异常，fail-fast 异常携带触发消息的
     * 类型与 relationOid/prefix 上下文（{@link #describeTrigger}）以便定位。
     *
     * @param trigger 触发本次查找的变更消息（Insert/Update/Delete/Truncate/LogicalMsg）
     */
    private TxBuffer activeBucket(PgOutputMessage trigger) {
        if (currentStream != null) {
            return currentStream;
        }
        if (currentPrepareTx != null) {
            return currentPrepareTx;
        }
        if (currentNormalTx != null) {
            return currentNormalTx;
        }
        throw new IllegalStateException("变更消息到达但无任何活动事务桶: " + describeTrigger(trigger));
    }

    /** 是否存在任一活动桶（流块/两阶段/普通三指针）——桶集合不变性的唯一判定入口，logicalMsg 丢弃路径等使用。 */
    private boolean hasActiveBucket() {
        return currentStream != null || currentPrepareTx != null || currentNormalTx != null;
    }

    /** 描述触发 fail-fast 的消息：类型 + relationOid(s)（LogicalMsg 用 prefix），供异常消息定位协议流断点。 */
    private static String describeTrigger(PgOutputMessage trigger) {
        if (trigger instanceof PgOutputMessage.Insert m) {
            return "Insert relationOid=" + m.relationOid();
        }
        if (trigger instanceof PgOutputMessage.Update m) {
            return "Update relationOid=" + m.relationOid();
        }
        if (trigger instanceof PgOutputMessage.Delete m) {
            return "Delete relationOid=" + m.relationOid();
        }
        if (trigger instanceof PgOutputMessage.Truncate m) {
            return "Truncate relationOids=" + Arrays.toString(m.relationOids());
        }
        if (trigger instanceof PgOutputMessage.LogicalMsg m) {
            return "LogicalMsg prefix=" + m.prefix();
        }
        return trigger.getClass().getSimpleName();
    }

    /** Begin：开新普通事务桶；已有未闭合普通事务即 fail-fast（协议上 Begin..Commit 不嵌套）。 */
    private void begin(PgOutputMessage.Begin m) {
        if (currentNormalTx != null) {
            throw new IllegalStateException("Begin 到达但普通事务未闭合: xid=" + currentNormalTx.xid);
        }
        currentNormalTx = new TxBuffer(m.xid());
    }

    /** Commit（无 xid 字段）：封箱当前普通事务桶为 NORMAL Transaction 回调并清空指针；无桶即 fail-fast（异常带 commitLsn 定位）。 */
    private void commit(PgOutputMessage.Commit m) {
        if (currentNormalTx == null) {
            throw new IllegalStateException("Commit 到达但无活动普通事务: commitLsn=0x"
                    + Long.toHexString(m.commitLsn()));
        }
        TxBuffer bucket = currentNormalTx;
        currentNormalTx = null;
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.NORMAL, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), bucket.changes));
    }

    /**
     * StreamStart(xid, firstSegment)：xid 恒为顶层 xid（spec B.3——ReorderBufferStreamTXN 断言 toptxn，
     * firstSegment=!rbtxn_is_streamed(txn)）。
     *
     * <p>firstSegment=true（该顶层事务首段）→ 新建桶入 streamedByXid（已存在同 xid → fail-fast）；
     * false（后续段）→ 桶必须已存在（miss → fail-fast）。两种情况都切换 currentStream 到该桶。
     */
    private void streamStart(PgOutputMessage.StreamStart m) {
        TxBuffer bucket;
        if (m.firstSegment()) {
            bucket = new TxBuffer(m.xid());
            if (streamedByXid.putIfAbsent(m.xid(), bucket) != null) {
                throw new IllegalStateException("流式事务桶已存在: xid=" + m.xid());
            }
        } else {
            bucket = streamedByXid.get(m.xid());
            if (bucket == null) {
                throw new IllegalStateException("StreamStart(first=false) 但顶层事务无桶: xid=" + m.xid());
            }
        }
        currentStream = bucket;
    }

    /**
     * StreamStop：流块边界（消息不携带 xid——spec B.3）。currentStream 必须非 null（否则 fail-fast），
     * 置 null。流桶保留在 streamedByXid 中等待后续段或 StreamCommit/StreamAbort/StreamPrepare。
     */
    private void streamStop() {
        if (currentStream == null) {
            throw new IllegalStateException("StreamStop 到达但无进行中的流块");
        }
        currentStream = null;
    }

    /**
     * StreamCommit(xid)：顶层事务全部流段已收齐，封箱 STREAMED Transaction 回调并移除桶；
     * 桶 miss 或仍有未闭合流块均 fail-fast（协议保证 stream_commit 必在流块外，spec B.3）。
     */
    private void streamCommit(PgOutputMessage.StreamCommit m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamCommit 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamCommit 对应流式事务桶不存在: xid=" + m.xid());
        }
        listener.onTransaction(new Transaction(m.xid(), TransactionKind.STREAMED, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), bucket.changes));
    }

    /**
     * StreamAbort(top, sub)：已流式事务的（子）事务回滚，剔除其已下发的变更（spec B.4）。
     *
     * <p>top==sub（整顶层回滚，decode 层"先子后顶"的最后一条）→ 移除整个桶；
     * 否则从桶中剔除所有 streamXid==sub 的变更（Message 的 streamXid=顶层 xid，不会误删）。
     * 桶 miss 或流块未闭合均 fail-fast（abort 必在流块外）。
     */
    private void streamAbort(PgOutputMessage.StreamAbort m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamAbort 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.get(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamAbort 对应流式事务桶不存在: xid=" + m.xid());
        }
        if (m.xid() == m.subxid()) {
            streamedByXid.remove(m.xid());
        } else {
            bucket.changes.removeIf(c -> c.streamXid().isPresent() && c.streamXid().getAsLong() == m.subxid());
        }
    }

    /**
     * LogicalMsg：事务性消息必须落在活动桶内（无桶即 fail-fast）；
     * 非事务性消息有活动桶则随桶走（abort 剔除按 streamXid，语义安全），无桶则 WARN 丢弃
     * （协议允许其游离于任何事务之外，非协议流异常）。
     */
    private void logicalMsg(PgOutputMessage.LogicalMsg m) {
        if (m.transactional()) {
            activeBucket(m).changes.add(new MsgChange(true, m.prefix(), m.content(), m.streamXid()));
            return;
        }
        if (hasActiveBucket()) {
            activeBucket(m).changes.add(new MsgChange(false, m.prefix(), m.content(), m.streamXid()));
            return;
        }
        LOG.warn("非事务性消息游离于任何事务之外，丢弃: prefix={} lsn=0x{}",
                m.prefix(), Long.toHexString(m.lsn()));
    }
}
