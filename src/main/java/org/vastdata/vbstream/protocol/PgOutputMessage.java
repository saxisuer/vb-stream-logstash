package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** pgoutput 顶层消息。streamXid 非空表示该消息处于流式块内（协议会前置 Int32 xid）。 */
public sealed interface PgOutputMessage {

    record Begin(long finalLsn, Instant commitTimestamp, long xid) implements PgOutputMessage {}

    record Commit(long commitLsn, long endLsn, Instant commitTimestamp) implements PgOutputMessage {}

    record Origin(long originCommitLsn, String originName) implements PgOutputMessage {}

    record Relation(OptionalLong streamXid, int relationOid, String schema, String table,
                    char replicaIdentity, List<Column> columns) implements PgOutputMessage {}

    record Type(OptionalLong streamXid, int typeOid, String schema, String name) implements PgOutputMessage {}

    record Insert(OptionalLong streamXid, int relationOid, TupleData newTuple) implements PgOutputMessage {}

    record Update(OptionalLong streamXid, int relationOid,
                  Optional<TupleData> oldTuple, TupleData newTuple) implements PgOutputMessage {}

    record Delete(OptionalLong streamXid, int relationOid, TupleData oldTuple) implements PgOutputMessage {}

    record Truncate(OptionalLong streamXid, EnumSet<TruncateOption> options,
                    int[] relationOids) implements PgOutputMessage {}

    record LogicalMsg(OptionalLong streamXid, boolean transactional, long lsn,
                      String prefix, byte[] content) implements PgOutputMessage {}

    record StreamStart(long xid, boolean firstSegment) implements PgOutputMessage {}

    record StreamStop() implements PgOutputMessage {}

    record StreamCommit(long xid, long commitLsn, long endLsn, Instant commitTimestamp) implements PgOutputMessage {}

    /** abortLsn/abortTimestamp 仅 streaming=parallel 时随消息携带（微秒原值），否则为 empty。 */
    record StreamAbort(long xid, long subxid, OptionalLong abortLsn, OptionalLong abortTimestamp) implements PgOutputMessage {}

    record BeginPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}

    record Prepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}

    record CommitPrepared(long commitLsn, long endLsn, Instant commitTimestamp, long xid, String gid) implements PgOutputMessage {}

    record RollbackPrepared(long prepareEndLsn, long rollbackEndLsn, Instant prepareTimestamp,
                            Instant rollbackTimestamp, long xid, String gid) implements PgOutputMessage {}

    record StreamPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}
}
