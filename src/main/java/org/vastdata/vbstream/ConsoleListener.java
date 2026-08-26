package org.vastdata.vbstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.PgOutputListener;
import org.vastdata.vbstream.replication.RelationRegistry;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;

/** 控制台打印 listener：每条消息一行可读输出。 */
public final class ConsoleListener implements PgOutputListener {

    /** CDC 数据通道专用 logger 名：生产可单独调整级别或重定向到独立 appender，与诊断日志区分流。 */
    private static final Logger CDC = LoggerFactory.getLogger("org.vastdata.vbstream.cdc");

    @Override
    public void onMessage(PgOutputMessage message, RelationRegistry registry) {
        CDC.info("{}", render(message, registry));
    }

    // 注：record pattern switch 是 Java 21 正式特性，本项目约束 Java 17，故用 instanceof 链
    private String render(PgOutputMessage msg, RelationRegistry registry) {
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

    private static String suffix(OptionalLong streamXid) {
        return streamXid.isPresent() ? " [streamed xid=" + streamXid.getAsLong() + "]" : "";
    }

    private static String tableOf(int oid, RelationRegistry registry) {
        return registry.find(oid)
                .map(rel -> rel.schema() + "." + rel.table())
                .orElse("oid:" + oid);
    }

    /** 列名=值 打印；TOAST 未变与 NULL 显式标注（打印 text 值截断到 64 字符）。 */
    private static String tupleOf(int oid, TupleData tuple, RelationRegistry registry) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < tuple.columns().size(); i++) {
            final int idx = i; // lambda 引用要求实际 final
            String column = registry.find(oid)
                    .filter(rel -> idx < rel.columns().size())
                    .map(rel -> rel.columns().get(idx).name())
                    .orElse("#" + idx);
            TupleValue value = tuple.columns().get(i);
            String rendered;
            if (value instanceof TupleValue.Null) {
                rendered = "NULL";
            } else if (value instanceof TupleValue.UnchangedToast) {
                rendered = "<toast-unchanged>";
            } else if (value instanceof TupleValue.Text t) {
                String s = t.value();
                rendered = s.length() > 64 ? s.substring(0, 64) + "...(" + s.length() + "B)" : s;
            } else if (value instanceof TupleValue.Binary b) {
                rendered = "0x" + HexFormat.of().formatHex(b.value());
            } else {
                throw new IllegalStateException("未知列值类型: " + value.getClass());
            }
            parts.add(column + "=" + rendered);
        }
        return parts.toString();
    }
}
