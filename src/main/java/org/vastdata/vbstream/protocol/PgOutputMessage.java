package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.Arrays;
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

    /** relationOids 为 int[] 组件，需值相等语义（record 默认 equals 对数组退化为引用相等），故显式 override。 */
    record Truncate(OptionalLong streamXid, EnumSet<TruncateOption> options,
                    int[] relationOids) implements PgOutputMessage {

        @Override
        public boolean equals(Object o) {
            return o == this || o instanceof Truncate other
                    && streamXid.equals(other.streamXid)
                    && options.equals(other.options)
                    && Arrays.equals(relationOids, other.relationOids);
        }

        @Override
        public int hashCode() {
            int result = streamXid.hashCode();
            result = 31 * result + options.hashCode();
            result = 31 * result + Arrays.hashCode(relationOids);
            return result;
        }
    }

    /** content 为 byte[] 组件，需值相等语义（record 默认 equals 对数组退化为引用相等），故显式 override。 */
    record LogicalMsg(OptionalLong streamXid, boolean transactional, long lsn,
                      String prefix, byte[] content) implements PgOutputMessage {

        @Override
        public boolean equals(Object o) {
            return o == this || o instanceof LogicalMsg other
                    && streamXid.equals(other.streamXid)
                    && transactional == other.transactional
                    && lsn == other.lsn
                    && prefix.equals(other.prefix)
                    && Arrays.equals(content, other.content);
        }

        @Override
        public int hashCode() {
            int result = streamXid.hashCode();
            result = 31 * result + Boolean.hashCode(transactional);
            result = 31 * result + Long.hashCode(lsn);
            result = 31 * result + prefix.hashCode();
            result = 31 * result + Arrays.hashCode(content);
            return result;
        }
    }

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
