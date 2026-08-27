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
    /** 当前流块上下文：stream_start..stream_stop 之间非 null，指向 streamedByXid 中某桶。Task 3 使用。 */
    private TxBuffer currentStream;
    /** 流式事务桶，key=顶层 xid（多桶并存，段间交错——spec §4.2）。Task 3 使用。 */
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
            activeBucket().changes.add(new RowChange(DmlKind.INSERT, registry.require(m.relationOid()),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Update m) {
            activeBucket().changes.add(new RowChange(DmlKind.UPDATE, registry.require(m.relationOid()),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Delete m) {
            activeBucket().changes.add(new RowChange(DmlKind.DELETE, registry.require(m.relationOid()),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(registry::require)
                    .toList();
            activeBucket().changes.add(new TruncateChange(snapshots, m.options(), m.streamXid()));
        } else if (message instanceof PgOutputMessage.LogicalMsg m) {
            logicalMsg(m);
        } else if (message instanceof PgOutputMessage.StreamStart m) {
            throw new IllegalStateException("流式路径尚未实现（Task 3）: " + m);
        } else if (message instanceof PgOutputMessage.StreamStop m) {
            throw new IllegalStateException("流式路径尚未实现（Task 3）: " + m);
        } else if (message instanceof PgOutputMessage.StreamCommit m) {
            throw new IllegalStateException("流式路径尚未实现（Task 3）: " + m);
        } else if (message instanceof PgOutputMessage.StreamAbort m) {
            throw new IllegalStateException("StreamAbort 尚未实现（Task 4）: " + m);
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
     * 三者皆空说明变更消息游离在任何事务外，协议流异常。
     */
    private TxBuffer activeBucket() {
        if (currentStream != null) {
            return currentStream;
        }
        if (currentPrepareTx != null) {
            return currentPrepareTx;
        }
        if (currentNormalTx != null) {
            return currentNormalTx;
        }
        throw new IllegalStateException("变更消息到达但无任何活动事务桶");
    }

    /** Begin：开新普通事务桶；已有未闭合普通事务即 fail-fast（协议上 Begin..Commit 不嵌套）。 */
    private void begin(PgOutputMessage.Begin m) {
        if (currentNormalTx != null) {
            throw new IllegalStateException("Begin 到达但普通事务未闭合: xid=" + currentNormalTx.xid);
        }
        currentNormalTx = new TxBuffer(m.xid());
    }

    /** Commit（无 xid 字段）：封箱当前普通事务桶为 NORMAL Transaction 回调并清空指针；无桶即 fail-fast。 */
    private void commit(PgOutputMessage.Commit m) {
        if (currentNormalTx == null) {
            throw new IllegalStateException("Commit 到达但无活动普通事务");
        }
        TxBuffer bucket = currentNormalTx;
        currentNormalTx = null;
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.NORMAL, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), bucket.changes));
    }

    /**
     * LogicalMsg：事务性消息必须落在活动桶内（无桶即 fail-fast）；
     * 非事务性消息有活动桶则随桶走（abort 剔除按 streamXid，语义安全），无桶则 WARN 丢弃
     * （协议允许其游离于任何事务之外，非协议流异常）。
     */
    private void logicalMsg(PgOutputMessage.LogicalMsg m) {
        if (m.transactional()) {
            activeBucket().changes.add(new MsgChange(true, m.prefix(), m.content(), m.streamXid()));
            return;
        }
        if (currentStream != null || currentPrepareTx != null || currentNormalTx != null) {
            activeBucket().changes.add(new MsgChange(false, m.prefix(), m.content(), m.streamXid()));
            return;
        }
        LOG.warn("非事务性消息游离于任何事务之外，丢弃: prefix={} lsn=0x{}",
                m.prefix(), Long.toHexString(m.lsn()));
    }
}
