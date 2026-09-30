package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * pgoutput 顶层消息模型：sealed interface + 19 个嵌套 record，一条消息一个形态，
 * 由 Task 4-6 的 parser 族构造、解码器按类型字节分派后交付下游。
 * 引擎 {@code org.vastdata.vbstream.protocol.PgOutputMessage} 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），组件名与类型逐一一致。
 *
 * <p>公共约定：
 * <ul>
 *   <li>{@code streamXid} 为 {@link OptionalLong}，非空表示该消息处于流式大事务块内
 *       （协议在类型字节后前置了 Int32 xid，解码器流块状态机据此判定）；顶层消息为 empty</li>
 *   <li>时间戳组件一律 {@link Instant}（parser 侧经 WireReader.pgMicrosToInstant 从
 *       PG 纪元微秒换算），唯一例外 {@link StreamAbort#abortTimestamp()} 保留微秒原值</li>
 *   <li>数组组件（{@link Truncate#relationOids}、{@link LogicalMsg#content}）显式
 *       override equals/hashCode 为值相等——record 默认对数组退化为引用相等</li>
 * </ul>
 */
public sealed interface PgOutputMessage {

    /**
     * 事务开始（类型字节 'B'）——先于该事务的所有 DML 到达。
     *
     * @param finalLsn 该事务 commit 记录将出现的最终 LSN
     * @param commitTimestamp 事务提交时间戳
     * @param xid 事务号（无符号语义，parser 以 readUnsignedInt 读入）
     */
    record Begin(long finalLsn, Instant commitTimestamp, long xid) implements PgOutputMessage {}

    /**
     * 事务提交结束（类型字节 'C'）——消息首字节 flags 读掉不建模。
     *
     * @param commitLsn commit 记录本身的 LSN
     * @param endLsn 事务日志的结束 LSN（消费位点推进的锚点）
     * @param commitTimestamp 事务提交时间戳
     */
    record Commit(long commitLsn, long endLsn, Instant commitTimestamp) implements PgOutputMessage {}

    /**
     * 逻辑复制源节点位点（类型字节 'O'，级联复制场景的 origin 记录）。
     *
     * @param originCommitLsn 源节点上对应提交的 LSN
     * @param originName 源节点名（replication origin 名）
     */
    record Origin(long originCommitLsn, String originName) implements PgOutputMessage {}

    /**
     * 表元数据（类型字节 'R'）——先于同表的 DML 到达，下游须缓存 oid → 列布局以解码元组。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param relationOid 表 oid
     * @param schema 表所在 schema 名
     * @param table 表名
     * @param replicaIdentity 复制标识单字符：'d' 默认 / 'i' 索引 / 'f' FULL / 'n' 无
     * @param columns 列元组（列序即元组值的列序）；不可变 List 由 parser 构造侧
     *                {@code List.copyOf} 保证，record 自身不再复制
     */
    record Relation(OptionalLong streamXid, int relationOid, String schema, String table,
                    char replicaIdentity, List<RelationColumn> columns) implements PgOutputMessage {}

    /**
     * 复合类型定义（类型字节 'Y'，仅在 publication 含复合类型列时出现）。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param typeOid 类型 oid
     * @param schema 类型所在 schema 名
     * @param name 类型名
     */
    record Type(OptionalLong streamXid, int typeOid, String schema, String name) implements PgOutputMessage {}

    /**
     * INSERT（类型字节 'I'）。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param relationOid 目标表 oid（布局查 Relation 缓存）
     * @param newTuple 插入后的新行元组
     */
    record Insert(OptionalLong streamXid, int relationOid, TupleData newTuple) implements PgOutputMessage {}

    /**
     * UPDATE（类型字节 'U'）——旧元组是否存在取决于表 replica identity 与语句形态。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param relationOid 目标表 oid
     * @param oldTuple 旧元组：'K' 主键元组或 'O' 旧整行时非空，语句未携带旧值时 empty
     * @param newTuple 更新后的新行元组
     */
    record Update(OptionalLong streamXid, int relationOid,
                  Optional<TupleData> oldTuple, TupleData newTuple) implements PgOutputMessage {}

    /**
     * DELETE（类型字节 'D'）——协议规定必带旧元组（'K' 或 'O'）。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param relationOid 目标表 oid
     * @param oldTuple 被删行的旧元组
     */
    record Delete(OptionalLong streamXid, int relationOid, TupleData oldTuple) implements PgOutputMessage {}

    /**
     * TRUNCATE（类型字节 'T'）——一条语句可同时截断多张表，oids 打包在同一消息内。
     * relationOids 为 int[] 组件，需值相等语义（record 默认 equals 对数组退化为引用相等），故显式 override。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param options 语句修饰符集合（CASCADE / RESTART_IDENTITY）
     * @param relationOids 被截断的表 oid 数组
     */
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

    /**
     * 逻辑复制自定义消息（类型字节 'M'，{@code pg_logical_emit_message} 发出）。
     * content 为 byte[] 组件，需值相等语义（record 默认 equals 对数组退化为引用相等），故显式 override。
     *
     * @param streamXid 流式块内为块的事务号，顶层为 empty
     * @param transactional true = 事务性（随事务回滚而丢弃）；false = 立即送达非持久
     * @param lsn 消息发出位点的 LSN
     * @param prefix 消息前缀（接收方按前缀路由/过滤）
     * @param content 消息内容原始字节
     */
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

    /**
     * 流式块开始（类型字节 'S'）——其后直至 StreamStop 的 M/R/Y/I/U/D/T 消息前置 xid。
     *
     * @param xid 本流块承载的事务号
     * @param firstSegment true = 该事务的首个流段（此前未发送过该 xid 的流块）
     */
    record StreamStart(long xid, boolean firstSegment) implements PgOutputMessage {}

    /**
     * 流式块结束（类型字节 'E'）——无字段；解码器据此复位流块状态机。
     */
    record StreamStop() implements PgOutputMessage {}

    /**
     * 流式事务提交（类型字节 'c'）——消息含 flags 字节，parser 读掉不建模。
     *
     * @param xid 被提交的流式事务号
     * @param commitLsn commit 记录本身的 LSN
     * @param endLsn 事务日志的结束 LSN
     * @param commitTimestamp 事务提交时间戳
     */
    record StreamCommit(long xid, long commitLsn, long endLsn, Instant commitTimestamp) implements PgOutputMessage {}

    /**
     * 流式事务回滚（类型字节 'A'）——子事务粒度，用于从组装中剔除被回滚的子事务变更。
     * abortLsn/abortTimestamp 仅 streaming=parallel 时随消息携带（否则为 empty）。
     *
     * @param xid 被回滚的（父）事务号
     * @param subxid 被回滚的子事务号
     * @param abortLsn 回滚位点 LSN，仅 parallel 模式非 empty
     * @param abortTimestamp 回滚时间戳——距 2000-01-01 UTC 的微秒<b>原值</b>（long 装在
     *                      OptionalLong 内，刻意不转 Instant：parallel 语义下仅作比对用，
     *                      与其余消息的 Instant 约定不同，勿“顺手”换算）
     */
    record StreamAbort(long xid, long subxid, OptionalLong abortLsn, OptionalLong abortTimestamp) implements PgOutputMessage {}

    /**
     * 两阶段 PREPARE 开始（类型字节 'b'）。
     *
     * @param prepareLsn prepare 记录将出现的 LSN
     * @param endLsn 事务日志的结束 LSN
     * @param prepareTimestamp prepare 时间戳
     * @param xid 事务号
     * @param gid 全局事务标识（两阶段提交的全局协调键）
     */
    record BeginPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}

    /**
     * 两阶段 PREPARE 完成（类型字节 'P'）——消息含 flags 字节，parser 读掉不建模。
     *
     * @param prepareLsn prepare 记录本身的 LSN
     * @param endLsn 事务日志的结束 LSN
     * @param prepareTimestamp prepare 时间戳
     * @param xid 事务号
     * @param gid 全局事务标识
     */
    record Prepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}

    /**
     * 两阶段 COMMIT PREPARED（类型字节 'K'）——消息含 flags 字节，parser 读掉不建模。
     *
     * @param commitLsn commit 记录本身的 LSN
     * @param endLsn 事务日志的结束 LSN
     * @param commitTimestamp 提交时间戳
     * @param xid 事务号
     * @param gid 全局事务标识（与 Prepare 阶段同 gid 配对）
     */
    record CommitPrepared(long commitLsn, long endLsn, Instant commitTimestamp, long xid, String gid) implements PgOutputMessage {}

    /**
     * 两阶段 ROLLBACK PREPARED（类型字节 'r'）——唯一双时间戳消息。
     *
     * @param prepareEndLsn prepare 阶段事务日志的结束 LSN
     * @param rollbackEndLsn 回滚阶段事务日志的结束 LSN
     * @param prepareTimestamp 原 prepare 的时间戳
     * @param rollbackTimestamp 回滚时间戳
     * @param xid 事务号
     * @param gid 全局事务标识
     */
    record RollbackPrepared(long prepareEndLsn, long rollbackEndLsn, Instant prepareTimestamp,
                            Instant rollbackTimestamp, long xid, String gid) implements PgOutputMessage {}

    /**
     * 流式事务的两阶段 PREPARE（类型字节 'p'）——消息体与 {@link Prepare} 同构，
     * parser 经同一读取路径复用，仅构造的 record 类型不同。
     *
     * @param prepareLsn prepare 记录本身的 LSN
     * @param endLsn 事务日志的结束 LSN
     * @param prepareTimestamp prepare 时间戳
     * @param xid 事务号
     * @param gid 全局事务标识
     */
    record StreamPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}
}
