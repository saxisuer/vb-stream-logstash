package org.vastdata.debezium.connector.postgresql.stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope.Operation;
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

import java.nio.charset.StandardCharsets;
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
 * 不重复装、变更版本重装——经假 {@link StreamPostgresSchema} 子类观测)、Truncate 的
 * skipped.operations 门控(默认 "t" 跳过、none 才逐表发射)与 MsgChange 的回放期
 * INFO 留痕口径(MS3.5:记但不 dispatch,发射仍延期)。
 * dispatcher 用真实 {@code PostgresEventDispatcher} 的记录子类
 * (离线装配:noop 心跳 + null signalProcessor/headerProducer,被测方法全部覆写记录)。
 *
 * <p>夹具约定:offset 经 Loader 从空 map 装载;桶快照经真实
 * {@link VersionedRelationRegistry#snapshot} 构造(bind/resolve 同 consumer 线程的
 * R1 单线程纪律在单测里即调用线程)。
 */
class DispatcherTransactionListenerTest {

    /** 测试表 oid。 */
    private static final int OID = 16386;

    /** 第二张测试表 oid(多表 TRUNCATE 用例的受影响第二表)。 */
    private static final int OID_B = 16387;

    /** 事务边界 LSN(Begin/End 事件的 endLsn 组件)。 */
    private static final long END_LSN = 0x16000000L;

    /** 提交时间戳(Begin 的 commitTimestamp,End 事件不带、由 listener 记住复用)。 */
    private static final Instant COMMIT_TS = Instant.ofEpochSecond(1_800_000_000L);

    /** 顶层事务 xid。 */
    private static final long XID = 4242L;

    /** 流式单元携带的子事务 xid(SAVEPOINT 层级,与顶层 XID 不同)。 */
    private static final long SUBXID = 9001L;

    /** 责任:离线构造最小合法配置(topic.prefix 是 TopicNamingStrategy 的必填项)。 */
    private static PostgresStreamConnectorConfig config() {
        return config(Map.of());
    }

    /**
     * 责任:离线构造最小合法配置并叠加覆盖项(如 skipped.operations=none——Truncate
     * 发射门控的对照配置;覆盖项冲突时以覆盖项为准,Configuration.from 语义)。
     */
    private static PostgresStreamConnectorConfig config(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("topic.prefix", "tsrv");
        props.putAll(overrides);
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
        final Map<Integer, Table> current = new HashMap<>();

        RecordingSchema(CdcSourceTaskContext<PostgresConnectorConfig> taskContext) {
            super(taskContext, null, null, null, null);
        }

        @Override
        public Table tableFor(int relationId) {
            return current.get(relationId);
        }

        @Override
        public void applySchemaChangesForTable(int relationId, Table table) {
            installed.add(table);
            current.put(relationId, table);
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
        return resolved(OID, "t_l", colNames);
    }

    /**
     * 责任:按 oid/表名/列名集合造 wire Relation + Table(多表 TRUNCATE 用例需要
     * 第二张表:oid 与表名都得独立可配)。
     */
    private static ResolvedRelation resolved(int oid, String tableName, String... colNames) {
        PgOutputMessage.Relation wire = new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", tableName,
                'd', Arrays.stream(colNames).map(n -> new RelationColumn(n, 25, -1, n.equals("id"))).toList());
        return new ResolvedRelation(wire, TestRelations.tableOf(wire));
    }

    /** 责任:构造 listener + 已绑定快照的 resolver(v1 版本已记入 registry)。 */
    private static Fixture fixture(String... colNames) {
        return fixture(config(), List.of(resolved(colNames)));
    }

    /**
     * 责任:按显式配置与预登记版本构造 listener(快照覆盖全部登记 oid;多表 TRUNCATE
     * 用例经本重载注入第二张表与 skipped.operations=none 的对照配置)。
     */
    private static Fixture fixture(PostgresStreamConnectorConfig config, List<ResolvedRelation> registered) {
        RecordingSchema schema = new RecordingSchema(taskContext(config));
        RecordingDispatcher dispatcher = new RecordingDispatcher(config, schema);
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        Set<Integer> oids = new java.util.HashSet<>();
        for (ResolvedRelation relation : registered) {
            registry.accept(10L, relation);
            oids.add(relation.wire().relationOid());
        }
        BucketTableResolver resolver = BucketTableResolver.snapshotBacked();
        resolver.bind(registry.snapshot(oids, 20L));
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

    /** 一条 INSERT RowChange(seq 指向 v1 版本,streamXid 空 = 非流式单元)。 */
    private static RowChange rowChange(long seq) {
        return new RowChange(DmlKind.INSERT,
                new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l", 'd',
                        List.of(new RelationColumn("id", 25, -1, true), new RelationColumn("v", 25, -1, false))),
                Optional.empty(), Optional.of(new TupleData(List.of(TupleValue.NULL, new TupleValue.Text("x")))),
                OptionalLong.empty(), seq);
    }

    /**
     * 一条带子事务 xid 的流式 INSERT RowChange:streamXid 是 SAVEPOINT 层级的子事务
     * xid(终审修复的触发形态——子事务提交存活的变更到达 listener 时携带它)。
     */
    private static RowChange streamedSubxidRowChange(long seq) {
        return new RowChange(DmlKind.INSERT,
                new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l", 'd',
                        List.of(new RelationColumn("id", 25, -1, true), new RelationColumn("v", 25, -1, false))),
                Optional.empty(), Optional.of(new TupleData(List.of(TupleValue.NULL, new TupleValue.Text("x")))),
                OptionalLong.of(SUBXID), seq);
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

    /**
     * 流式子事务单元的 source txId 同源:RowChange 携带子事务 xid(SUBXID,SAVEPOINT
     * 层级——aborted 过滤正依赖该语义区分),但 updateWalPosition 写入 source 块的
     * txId 必须恒取<b>顶层</b> xid(Begin 记住的 currentXid,与事务块 id 同源)——
     * 若此处透传 subxid,SAVEPOINT 提交存活的变更会让首条记录 source.txId ≠ 事务块 id,
     * TransactionMonitor 按"事务变更"补发空 END+BEGIN,事务元数据 topic 分裂(终审修复,
     * Task 8 修的 id 同源缺口在子事务边界的复现形态)。断言面:offset 的 txId 键(记录
     * source 块 txId 的同源写点)等于顶层 XID 而非 SUBXID;数据事件照常发射不受影响。
     */
    @Test
    void streamedSubxidRowChangeReportsTopLevelTxId() {
        Fixture f = fixture("id", "v");
        f.listener().onEvent(begin());
        f.listener().onEvent(streamedSubxidRowChange(11L));

        assertEquals(XID, f.listener().offsetForTest().getOffset().get("txId"),
                "source 块 txId 恒取顶层 xid——与事务块 id 同源(子事务 xid 不得透传)");
        assertEquals(1, f.dispatcher().dataChangeArgs.size(), "数据事件照常发射");
    }

    /**
     * Truncate 门控(默认配置):skipped.operations 默认 "t"(CommonConnectorConfig 继承,
     * vanilla 同默认——TRUNCATE 默认跳过)→ TruncateChange 零 dispatch、零版本安装;
     * MsgChange 自 MS3.5 起 INFO 留痕但仍不 dispatch(断言见
     * {@link #msgChangeLogsInfoWithTransactionalTrueAndNeverDispatches()})。
     */
    @Test
    void truncateChangeSkippedByDefaultConfigAndMsgChangeStillDeferred() {
        Fixture f = fixture("id", "v");
        f.listener().onEvent(begin());
        f.listener().onEvent(truncateChange());
        f.listener().onEvent(new MsgChange(true, "pfx", new byte[0], OptionalLong.empty(), 12L));

        assertTrue(f.schema().installed.isEmpty(), "门控跳过的 Truncate/Msg 不触发版本安装");
        assertTrue(f.dispatcher().dataChangeArgs.isEmpty(), "默认配置下 Truncate 零 dispatch(MsgChange 仅留痕)");
    }

    /**
     * Truncate 发射(skipped.operations=none):逐表 dispatch——每张受影响表一条数据事件,
     * 每表一个 emitter(TruncateEmitter,Operation=TRUNCATE——key=null 的无 before/after
     * 形态,vanilla PostgresChangeRecordEmitter.emitTruncateRecord 同款);表版本按
     * (oid, seq) asOf 解析并安装(与 RowChange 同路径);事务块 BEGIN/COMMITTED 照常
     * (每表一条 dataEvent,TransactionMonitor 自动计数)。
     */
    @Test
    void truncateChangeWithNoneSkippedDispatchesPerTable() {
        Fixture f = fixture(config(Map.of("skipped.operations", "none")),
                List.of(resolved(OID, "t_l", "id", "v"), resolved(OID_B, "t_l_b", "id")));
        f.listener().onEvent(begin());
        f.listener().onEvent(new TruncateChange(
                List.of(new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l", 'd',
                                List.of(new RelationColumn("id", 25, -1, true))),
                        new PgOutputMessage.Relation(OptionalLong.empty(), OID_B, "public", "t_l_b", 'd',
                                List.of(new RelationColumn("id", 25, -1, true)))),
                Set.of(), OptionalLong.empty(), 11L));
        f.listener().onEvent(new TransactionEvent.End(XID, 0L));

        assertEquals(2, f.dispatcher().dataChangeArgs.size(), "两表 TRUNCATE 应逐表各发一条数据事件");
        assertEquals(2, f.schema().installed.size(), "两表版本各装一次(asOf 解析路径与 RowChange 同)");
        TableId first = (TableId) f.dispatcher().dataChangeArgs.get(0)[1];
        TableId second = (TableId) f.dispatcher().dataChangeArgs.get(1)[1];
        assertEquals(new TableId(null, "public", "t_l"), first, "首表 tableId 取 asOf Table");
        assertEquals(new TableId(null, "public", "t_l_b"), second, "第二表 tableId 取 asOf Table");
        for (Object[] args : f.dispatcher().dataChangeArgs) {
            ChangeRecordEmitter<?> emitter = (ChangeRecordEmitter<?>) args[2];
            assertTrue(emitter instanceof TruncateEmitter, "每表一个 TruncateEmitter(普通数据事件路径)");
            assertEquals(Operation.TRUNCATE, emitter.getOperation(), "TRUNCATE 形态(op=t、key=null 的信封由 emitter 构造)");
        }
        assertEquals(List.of("started:" + XID + "@" + COMMIT_TS,
                "data:public.t_l", "data:public.t_l_b", "committed@" + COMMIT_TS),
                f.dispatcher().calls, "BEGIN → 逐表数据事件 → COMMIT 的完整事件序列");
    }

    /** 一条单表 TruncateChange(oid=OID;多表形态由 fixture 的第二表 + 本事件组合覆盖)。 */
    private static TruncateChange truncateChange() {
        return new TruncateChange(
                List.of(new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t_l", 'd',
                        List.of(new RelationColumn("id", 25, -1, true)))),
                Set.of(), OptionalLong.empty(), 11L);
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

    /**
     * MsgChange 回放期留痕(MS3.5 spec §3.2 事务性时点):INFO 恰一行(prefix 与 content
     * 从 MsgChange 组件直取、事务性恒 true——到达此分支的必是随事务输出的消息;aborted
     * 子事务的消息在回放过滤阶段已被剔除,天然不记,与 CDC 数据语义对齐),content 经
     * {@link MessagePreview} 预览截断(100 字节可打印载荷 → 前 64 字符 + "...(100B)")。
     * 仍不 dispatch(dispatcher 零 data/事务中间事件)、不计入 event_count——发射仍延期
     * (begin/end 事务块头尾照常,事件序列不含消息)。
     */
    @Test
    void msgChangeLogsInfoWithTransactionalTrueAndNeverDispatches() {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Logger log = (Logger) org.slf4j.LoggerFactory.getLogger(DispatcherTransactionListener.class);
        log.addAppender(appender);
        try {
            Fixture f = fixture("id", "v");
            f.listener().onEvent(begin());
            f.listener().onEvent(new MsgChange(true, "pfx",
                    "y".repeat(100).getBytes(StandardCharsets.US_ASCII), OptionalLong.empty(), 12L));
            f.listener().onEvent(new TransactionEvent.End(XID, 0L));

            List<String> lines = appender.list.stream()
                    .filter(e -> e.getLevel() == Level.INFO)
                    .map(ILoggingEvent::getFormattedMessage)
                    .toList();
            assertEquals(1, lines.size(), "恰一行 INFO 留痕: " + lines);
            String line = lines.get(0);
            assertTrue(line.contains("prefix=pfx"), line);
            assertTrue(line.contains("事务性=true"), line);
            assertTrue(line.contains("content=" + "y".repeat(64) + "...(100B)"), line);
            assertEquals(List.of("started:" + XID + "@" + COMMIT_TS, "committed@" + COMMIT_TS),
                    f.dispatcher().calls, "零 dispatch:事件序列只有事务块头尾,消息不进任何 topic");
            assertTrue(f.dispatcher().dataChangeArgs.isEmpty(), "无数据事件");
        } finally {
            log.detachAppender(appender);
        }
    }
}
