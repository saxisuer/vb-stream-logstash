package org.vastdata.vbstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.MsgChange;
import org.vastdata.vbstream.replication.PgOutputListener;
import org.vastdata.vbstream.replication.RelationLookup;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionListener;
import org.vastdata.vbstream.replication.TruncateChange;
import org.vastdata.vbstream.replication.TxChange;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;

/**
 * 控制台打印 listener（Main 默认事务形态，spec §5）：事务块（TXN-BEGIN/TXN-END 头尾 + 逐变更行）与
 * 事务生命周期控制消息（流式分段边界 + 两阶段信号）走 CDC logger INFO；行级数据与元数据等逐消息细节
 * 降为 DEBUG（默认关闭），仅供排障时开启。INFO 级保证任何事务形态至少留一行痕迹，不吞噬事务信息。
 *
 * <p>同一实例承担双角色：事务回调（{@link #onTransaction}，组装器提交路径）与逐消息渲染
 * （{@link #onMessage}，Main 装配下挂在组装器的解码点 observer——控制消息/Relation live 解码与
 * 提交回放期 payload 解码，见其 javadoc）。
 */
public final class ConsoleListener implements PgOutputListener, TransactionListener {

    /** CDC 数据通道专用 logger 名：生产可单独调整级别或重定向到独立 appender，与诊断日志区分流。 */
    private static final Logger CDC = LoggerFactory.getLogger("org.vastdata.vbstream.cdc");

    /**
     * 逐消息渲染出口，按消息类别分流级别（spec §5）：事务生命周期控制消息升 INFO
     * （低量高信号，且 StreamAbort/RollbackPrepared 不产生组装后事务块，降 DEBUG 会吞掉唯一事务级线索），
     * 其余（行级数据/元数据）维持 DEBUG 默认不发射。级别守卫避免关闭时的无谓渲染开销——render 是实参、急切求值。
     *
     * <p>调用时点（Main 装配）= {@code TransactionAssembler} 的解码点 observer，组装器是唯一解码者：
     * 控制消息（Begin/Commit/流式与两阶段信号）与 Relation('R') 到达即 live 解码、按到达序回调；
     * 数据消息（I/U/D/T/M）live 期以原始字节入桶不解码，在提交回放期解码回调——每条恰好一次
     * （被 StreamAbort 过滤的子事务单元不回调；'Y'/'O' 组装器直接丢弃，不产生回调）。
     * 渲染用的 Relation 视图参型为 {@link RelationLookup}（1.7 设计 §4.3）：Main 装配下闭包持
     * 版本日志 registry（最新版视图，随 'R' 到达演进——逐消息渲染按最新 schema 展示即可）；提交块的
     * asOf 精确渲染由组装器回放出的 TxChange 内嵌快照承担，见 {@link #onTransaction}，不经本方法。
     */
    @Override
    public void onMessage(PgOutputMessage message, RelationLookup registry) {
        if (isTxLifecycle(message)) {
            if (CDC.isInfoEnabled()) {
                CDC.info("{}", render(message, registry));
            }
            return;
        }
        if (CDC.isDebugEnabled()) {
            CDC.debug("{}", render(message, registry));
        }
    }

    /**
     * 事务生命周期控制消息判定：流式（StreamStart/StreamStop/StreamCommit/StreamAbort/StreamPrepare）
     * 与两阶段（BeginPrepare/Prepare/CommitPrepared/RollbackPrepared）共 9 种——它们标记事务状态的迁移
     * 而非数据内容。行级（Insert/Update/Delete/Truncate/LogicalMsg）与元数据（Relation/Type/Origin）
     * 以及普通路径 Begin/Commit 不在此列：提交路径已由组装后事务块在 INFO 覆盖，保持 DEBUG 防刷屏。
     */
    private static boolean isTxLifecycle(PgOutputMessage message) {
        return message instanceof PgOutputMessage.StreamStart
                || message instanceof PgOutputMessage.StreamStop
                || message instanceof PgOutputMessage.StreamCommit
                || message instanceof PgOutputMessage.StreamAbort
                || message instanceof PgOutputMessage.StreamPrepare
                || message instanceof PgOutputMessage.BeginPrepare
                || message instanceof PgOutputMessage.Prepare
                || message instanceof PgOutputMessage.CommitPrepared
                || message instanceof PgOutputMessage.RollbackPrepared;
    }

    /**
     * 事务块输出：头/尾各一行 INFO（CDC logger），变更行逐条基于内嵌 Relation 快照渲染（不依赖 registry）。
     * 调用线程 = run 循环线程（与 onMessage 同约束，同步执行应快速返回）。
     */
    @Override
    public void onTransaction(Transaction transaction) {
        CDC.info("TXN-BEGIN xid={} kind={} gid={} commitLsn=0x{} commitTs={} changes={}",
                transaction.xid(), transaction.kind(), transaction.gid(),
                Long.toHexString(transaction.commitLsn()), transaction.commitTimestamp(),
                transaction.changes().size());
        int seq = 1;
        for (TxChange change : transaction.changes()) {
            CDC.info("  [{}] {}", seq++, renderChange(change));
        }
        CDC.info("TXN-END   xid={}", transaction.xid());
    }

    /** 单条变更渲染：列名取自嵌入的 Relation 快照（下游自包含，无需 registry）。 */
    private static String renderChange(TxChange change) {
        if (change instanceof RowChange rc) {
            return "%s %s BEFORE=%s AFTER=%s%s".formatted(rc.dml(), tableOf(rc.relation()),
                    rc.before().map(t -> tupleOf(t, rc.relation())).orElse("-"),
                    rc.after().map(t -> tupleOf(t, rc.relation())).orElse("-"),
                    suffix(rc.streamXid()));
        }
        if (change instanceof TruncateChange tc) {
            return "TRUNCATE %s options=%s%s".formatted(
                    tc.relations().stream().map(ConsoleListener::tableOf).toList(),
                    tc.options(), suffix(tc.streamXid()));
        }
        if (change instanceof MsgChange mc) {
            return "MESSAGE prefix=%s bytes=%d%s".formatted(mc.prefix(), mc.content().length, suffix(mc.streamXid()));
        }
        throw new IllegalStateException("未知变更类型: " + change.getClass());
    }

    /** 表名渲染（基于内嵌快照，schema.table 形式）。 */
    private static String tableOf(PgOutputMessage.Relation relation) {
        return relation.schema() + "." + relation.table();
    }

    /** 列名=值 打印（基于内嵌快照；与逐消息版 {@link #tupleOf(int, TupleData, RelationLookup)} 同规则：列名取快照、越界退化为 "#i"，值渲染规则见 {@link #renderValue}）。 */
    private static String tupleOf(TupleData tuple, PgOutputMessage.Relation relation) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < tuple.columns().size(); i++) {
            String column = i < relation.columns().size() ? relation.columns().get(i).name() : "#" + i;
            parts.add(column + "=" + renderValue(tuple.columns().get(i)));
        }
        return parts.toString();
    }

    /** 单列值渲染规则：NULL / TOAST 未变显式标注，text 截断到 64 字符（超出附原长字节数），binary 走十六进制；未知列值类型抛 {@link IllegalStateException}（fail-fast）。 */
    private static String renderValue(TupleValue value) {
        if (value instanceof TupleValue.Null) {
            return "NULL";
        }
        if (value instanceof TupleValue.UnchangedToast) {
            return "<toast-unchanged>";
        }
        if (value instanceof TupleValue.Text t) {
            String s = t.value();
            return s.length() > 64 ? s.substring(0, 64) + "...(" + s.length() + "B)" : s;
        }
        if (value instanceof TupleValue.Binary b) {
            return "0x" + HexFormat.of().formatHex(b.value());
        }
        throw new IllegalStateException("未知列值类型: " + value.getClass());
    }

    /**
     * 逐消息可读渲染：19 种 pgoutput 消息各一行（BEGIN/COMMIT/DML/TRUNCATE/流式与两阶段控制消息等），
     * DML 的表名/列名经 registry 解析。未知消息类型抛 {@link IllegalStateException}（fail-fast）。
     */
    // 注：record pattern switch 是 Java 21 正式特性，本项目约束 Java 17，故用 instanceof 链
    private String render(PgOutputMessage msg, RelationLookup registry) {
        if (msg instanceof PgOutputMessage.Begin m) {
            return "BEGIN             xid=%d finalLsn=0x%s".formatted(m.xid(), Long.toHexString(m.finalLsn()));
        }
        if (msg instanceof PgOutputMessage.Commit m) {
            return "COMMIT            commitLsn=0x%s endLsn=0x%s"
                    .formatted(Long.toHexString(m.commitLsn()), Long.toHexString(m.endLsn()));
        }
        if (msg instanceof PgOutputMessage.Origin m) {
            return "ORIGIN            lsn=0x%s name=%s".formatted(Long.toHexString(m.originCommitLsn()), m.originName());
        }
        if (msg instanceof PgOutputMessage.Relation m) {
            return "RELATION          %s.%s oid=%d cols=%d%s"
                    .formatted(m.schema(), m.table(), m.relationOid(), m.columns().size(), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Type m) {
            return "TYPE              %s.%s oid=%d%s".formatted(m.schema(), m.name(), m.typeOid(), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Insert m) {
            return "INSERT            %s BEFORE=- AFTER=%s%s".formatted(tableOf(m.relationOid(), registry),
                    tupleOf(m.relationOid(), m.newTuple(), registry), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Update m) {
            // BEFORE 镜像取决于 replica identity：默认（'d'）仅在键列被修改时携带键元组，
            // REPLICA IDENTITY FULL 恒携带整行；无旧镜像时打 "-"
            return "UPDATE            %s BEFORE=%s AFTER=%s%s".formatted(tableOf(m.relationOid(), registry),
                    m.oldTuple().map(t -> tupleOf(m.relationOid(), t, registry)).orElse("-"),
                    tupleOf(m.relationOid(), m.newTuple(), registry), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Delete m) {
            return "DELETE            %s BEFORE=%s AFTER=-%s".formatted(tableOf(m.relationOid(), registry),
                    tupleOf(m.relationOid(), m.oldTuple(), registry), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Truncate m) {
            return "TRUNCATE          oids=%s options=%s%s"
                    .formatted(java.util.Arrays.toString(m.relationOids()), m.options(), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.LogicalMsg m) {
            return "MESSAGE           prefix=%s bytes=%d%s".formatted(m.prefix(), m.content().length, suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.StreamStart m) {
            return "STREAM-START      xid=%d firstSegment=%s".formatted(m.xid(), m.firstSegment());
        }
        if (msg instanceof PgOutputMessage.StreamStop) {
            return "STREAM-STOP";
        }
        if (msg instanceof PgOutputMessage.StreamCommit m) {
            return "STREAM-COMMIT     xid=%d commitLsn=0x%s".formatted(m.xid(), Long.toHexString(m.commitLsn()));
        }
        if (msg instanceof PgOutputMessage.StreamAbort m) {
            return "STREAM-ABORT      xid=%d subxid=%d%s".formatted(m.xid(), m.subxid(),
                    m.abortLsn().isPresent() ? " abortLsn=0x" + Long.toHexString(m.abortLsn().getAsLong()) : "");
        }
        if (msg instanceof PgOutputMessage.BeginPrepare m) {
            return "BEGIN-PREPARE     gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.Prepare m) {
            return "PREPARE           gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.CommitPrepared m) {
            return "COMMIT-PREPARED   gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.RollbackPrepared m) {
            return "ROLLBACK-PREPARED gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.StreamPrepare m) {
            return "STREAM-PREPARE    gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        throw new IllegalStateException("未知消息类型: " + msg.getClass());
    }

    /** 流式块内消息的尾缀标注：携带 xid 时追加 " [streamed xid=N]"，顶层消息返回空串。 */
    private static String suffix(OptionalLong streamXid) {
        return streamXid.isPresent() ? " [streamed xid=" + streamXid.getAsLong() + "]" : "";
    }

    /** 表名渲染（registry 版）：命中返回 schema.table，miss（DML 先于 Relation，协议流异常）退化为 "oid:N"。 */
    private static String tableOf(int oid, RelationLookup registry) {
        return registry.find(oid)
                .map(rel -> rel.schema() + "." + rel.table())
                .orElse("oid:" + oid);
    }

    /** 列名=值 打印；列名经 registry 解析（miss 或越界退化为 "#i"），值渲染规则见 {@link #renderValue}。 */
    private static String tupleOf(int oid, TupleData tuple, RelationLookup registry) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < tuple.columns().size(); i++) {
            final int idx = i; // lambda 引用要求实际 final
            String column = registry.find(oid)
                    .filter(rel -> idx < rel.columns().size())
                    .map(rel -> rel.columns().get(idx).name())
                    .orElse("#" + idx);
            parts.add(column + "=" + renderValue(tuple.columns().get(i)));
        }
        return parts.toString();
    }
}
