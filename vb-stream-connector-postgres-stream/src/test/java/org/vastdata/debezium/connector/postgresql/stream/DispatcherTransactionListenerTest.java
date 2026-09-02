package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleData;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DispatcherTransactionListener 单测(离线,真库归 Task 8):事务边界 offset 语义
 * (Begin 后 offsetContext.getOffset() 的 lsn=lsn_commit=endLsn)、事件 → dispatcher
 * 映射面(Started/DataChange/Committed)、asOf 版本安装调用面(新版本装一次、同版本
 * 不重复装、变更版本重装——经假 {@link StreamPostgresSchema} 子类观测)、Truncate/MsgChange
 * 的 MS2 跳过口径。dispatcher 用真实 {@code PostgresEventDispatcher} 的记录子类
 * (离线装配:noop 心跳 + null signalProcessor/headerProducer,被测方法全部覆写记录)。
 *
 * <p>夹具约定:offset 经 Loader 从空 map 装载;桶快照经真实
 * {@link VersionedRelationRegistry#snapshot} 构造(bind/resolve 同 consumer 线程的
 * R1 单线程纪律在单测里即调用线程)。
 */
class DispatcherTransactionListenerTest {

    /** 测试表 oid。 */
    private static final int OID = 16386;

    /** 事务边界 LSN(Begin/End 事件的 endLsn 组件)。 */
    private static final long END_LSN = 0x16000000L;

    /** 提交时间戳(Begin 的 commitTimestamp,End 事件不带、由 listener 记住复用)。 */
    private static final Instant COMMIT_TS = Instant.ofEpochSecond(1_800_000_000L);

    /** 顶层事务 xid。 */
    private static final long XID = 4242L;

    /** 责任:离线构造最小合法配置(topic.prefix 是 TopicNamingStrategy 的必填项)。 */
    private static PostgresStreamConnectorConfig config() {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("topic.prefix", "tsrv");
        return new PostgresStreamConnectorConfig(Configuration.from(props));
    }

    /** 责任:离线构造任务上下文(StreamPostgresSchema 的 protected 构造链只读它的 config)。 */
    private static CdcSourceTaskContext<PostgresConnectorConfig> taskContext(PostgresConnectorConfig config) {
        return new CdcSourceTaskContext<>(config.getConfig(), config, Map.of());
    }

    /** 责任:空 map 装载的 offset 上下文(全部 LSN null——Begin 置位前)。 */
    private static PostgresOffsetContext offset() {
        return new PostgresOffsetContext.Loader(config()).load(Map.of());
    }

    /**
     * 假 schema 子类:覆写版本安装调用面的两个方法做记录(避开真实 refresh 的
     * TableSchemaBuilder 路径——其 valueConverter 需连库);tableFor 返回已装版本。
     */
    private static final class RecordingSchema extends StreamPostgresSchema {
        final List<Table> installed = new ArrayList<>();
        Table current;

        RecordingSchema(CdcSourceTaskContext<PostgresConnectorConfig> taskContext) {
            super(taskContext, null, null, null, null);
        }

        @Override
        public Table tableFor(int relationId) {
            return relationId == OID ? current : null;
        }

        @Override
        public void applySchemaChangesForTable(int relationId, Table table) {
            installed.add(table);
            current = table;
        }
    }

    /** 记录型 dispatcher:真实 PostgresEventDispatcher 离线装配 + 三方法覆写记录(不真发)。 */
    private static final class RecordingDispatcher extends PostgresEventDispatcher<TableId> {
        final List<String> calls = new ArrayList<>();
        final List<Object[]> dataChangeArgs = new ArrayList<>();

        RecordingDispatcher(PostgresStreamConnectorConfig config, RecordingSchema schema) {
            super(config, config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY),
                    schema, null, config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new,
                    (partition, id, emitter) -> Optional.empty(),
                    new StreamEventMetadataProvider(), Heartbeat.ScheduledHeartbeat.NOOP_HEARTBEAT,
                    config.schemaNameAdjuster(), null, null);
        }

        @Override
        public void dispatchTransactionStartedEvent(PostgresPartition partition, String transactionId, OffsetContext offset, Instant timestamp) {
            calls.add("started:" + transactionId + "@" + timestamp);
        }

        @Override
        public void dispatchTransactionCommittedEvent(PostgresPartition partition, OffsetContext offset, Instant timestamp) {
            calls.add("committed@" + timestamp);
        }

        @Override
        public boolean dispatchDataChangeEvent(PostgresPartition partition, TableId dataCollectionId, ChangeRecordEmitter<PostgresPartition> changeRecordEmitter) {
            calls.add("data:" + dataCollectionId);
            dataChangeArgs.add(new Object[]{ partition, dataCollectionId, changeRecordEmitter });
            return true;
        }
    }

    /** 责任:按列名集合造 v1/v2 两版 wire Relation + Table(经共享夹具,列名集合即版本差异)。 */
    private static ResolvedRelation resolved(String... colNames) {
        PgOutputMessage.Relation wire = new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l",
                'd', Arrays.stream(colNames).map(n -> new RelationColumn(n, 25, -1, n.equals("id"))).toList());
        return new ResolvedRelation(wire, TestRelations.tableOf(wire));
    }

    /** 责任:构造 listener + 已绑定快照的 resolver(v1 版本已记入 registry)。 */
    private static Fixture fixture(String... colNames) {
        PostgresStreamConnectorConfig config = config();
        RecordingSchema schema = new RecordingSchema(taskContext(config));
        RecordingDispatcher dispatcher = new RecordingDispatcher(config, schema);
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, resolved(colNames));
        BucketTableResolver resolver = BucketTableResolver.snapshotBacked();
        resolver.bind(registry.snapshot(Set.of(OID), 20L));
        DispatcherTransactionListener listener = new DispatcherTransactionListener(
                new PostgresPartition("server", "db"), offset(), dispatcher, schema,
                Clock.system(), config, new PassthroughMapper(),
                resolver);
        return new Fixture(listener, schema, dispatcher);
    }

    /** 用例夹具聚合(listener + 两个记录器)。 */
    private record Fixture(DispatcherTransactionListener listener, RecordingSchema schema, RecordingDispatcher dispatcher) {
    }

    /** 直通值映射器(text 原样返回、TOAST 回哨兵常量)——emitter 值面非本类断言重点。 */
    private static final class PassthroughMapper implements ColumnValueMapper {
        @Override
        public Object text(String columnName, int typeId, String typeExpression, String rawValue) {
            return rawValue;
        }

        @Override
        public Object unchangedToast(String columnName, int typeId, String typeExpression, boolean optional) {
            return new Object();
        }
    }

    /** 事务头 Begin 事件(NORMAL 形态)。 */
    private static TransactionEvent.Begin begin() {
        return new TransactionEvent.Begin(XID, TransactionKind.NORMAL, null, END_LSN, END_LSN, COMMIT_TS, 1L);
    }

    /** 一条 INSERT RowChange(seq 指向 v1 版本)。 */
    private static RowChange rowChange(long seq) {
        return new RowChange(DmlKind.INSERT,
                new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l", 'd',
                        List.of(new RelationColumn("id", 25, -1, true), new RelationColumn("v", 25, -1, false))),
                Optional.empty(), Optional.of(new TupleData(List.of(TupleValue.NULL, new TupleValue.Text("x")))),
                OptionalLong.empty(), seq);
    }

    /**
     * 事务边界 offset:Begin 后 offsetContext.getOffset() 的 lsn 与 lsn_commit 皆等于
     * 事务 endLsn(此后本事务每条记录的 offset 同值——事务边界原子性的 offset 面),
     * 同时 dispatcher 收到纯数字 xid 的 transactionStarted(带 Begin 的提交时间戳)——
     * id 形态必须与 StreamEventMetadataProvider.getTransactionId 同源,否则
     * TransactionMonitor 按事务变更补发空 END+新 BEGIN(Task 8 IT 实测)。
     */
    @Test
    void beginAnchorsOffsetAtTransactionBoundaryAndStartsTransaction() {
        Fixture f = fixture("id", "v");
        f.listener().onEvent(begin());

        Map<String, ?> offset = f.listener().offsetForTest().getOffset();
        assertEquals(END_LSN, offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY), "lsn_commit = endLsn");
        assertEquals(END_LSN, offset.get("lsn"), "lsn = endLsn(记录随事务边界推进)");
        assertEquals(List.of("started:" + XID + "@" + COMMIT_TS), f.dispatcher().calls);
    }

    /**
     * 版本安装 + 数据映射:首条 RowChange 装一次 asOf 版本并 dispatch 数据事件
     * (tableId 取 asOf Table);同版本第二条不重装(值相等短路);变更版本(新列集)重装。
     */
    @Test
    void rowChangeInstallsVersionOnceAndDispatchesDataChange() {
        Fixture f = fixture("id", "v");
        f.listener().onEvent(begin());
        f.listener().onEvent(rowChange(11L));
        f.listener().onEvent(rowChange(12L));

        assertEquals(1, f.schema().installed.size(), "同版本不重复装(值相等短路)");
        assertEquals(2, f.dispatcher().dataChangeArgs.size());
        TableId tableId = (TableId) f.dispatcher().dataChangeArgs.get(0)[1];
        assertEquals(new TableId(null, "public", "t_l"), tableId);
        ChangeRecordEmitter<?> emitter = (ChangeRecordEmitter<?>) f.dispatcher().dataChangeArgs.get(0)[2];
        assertTrue(emitter instanceof RowChangeEmitter, "数据事件经 RowChangeEmitter 发射");

        // 新版本(v2 多一列):按 (oid, seq) 解析回新 Table → 重装
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, resolved("id", "v"));
        registry.accept(30L, resolved("id", "v", "extra"));
        BucketTableResolver resolver = BucketTableResolver.snapshotBacked();
        resolver.bind(registry.snapshot(Set.of(OID), 40L));
        DispatcherTransactionListener listener = new DispatcherTransactionListener(
                new PostgresPartition("server", "db"), f.listener().offsetForTest(), f.dispatcher(),
                f.schema(), Clock.system(), config(), new PassthroughMapper(), resolver);
        listener.onEvent(rowChange(35L));
        assertEquals(2, f.schema().installed.size(), "变更版本(列集变化)重装");
        assertEquals(3, f.dispatcher().dataChangeArgs.size());
    }

    /** Truncate/MsgChange:MS2 跳过发射(DEBUG 口径)——不装版本、不发数据事件。 */
    @Test
    void truncateAndMsgChangeAreSkippedInMs2() {
        Fixture f = fixture("id", "v");
        f.listener().onEvent(begin());
        f.listener().onEvent(new TruncateChange(
                List.of(new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l", 'd',
                        List.of(new RelationColumn("id", 25, -1, true)))),
                Set.of(), OptionalLong.empty(), 11L));
        f.listener().onEvent(new MsgChange(true, "pfx", new byte[0], OptionalLong.empty(), 12L));

        assertTrue(f.schema().installed.isEmpty(), "Truncate/Msg 不触发版本安装");
        assertTrue(f.dispatcher().dataChangeArgs.isEmpty(), "Truncate/Msg 不发数据事件(Truncate 族 MS3 补)");
    }

    /** 事务尾:End 事件映射 dispatchTransactionCommittedEvent(时间戳沿用 Begin 的提交时间戳)。 */
    @Test
    void endDispatchesTransactionCommittedWithBeginTimestamp() {
        Fixture f = fixture("id", "v");
        f.listener().onEvent(begin());
        f.listener().onEvent(new TransactionEvent.End(XID, 0L));

        assertEquals(List.of("started:" + XID + "@" + COMMIT_TS, "committed@" + COMMIT_TS),
                f.dispatcher().calls, "End 不带时间戳组件——listener 记住 Begin 的提交时间戳复用");
    }
}
