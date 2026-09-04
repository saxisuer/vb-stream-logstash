package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleData;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue;

import io.debezium.config.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * RowChangeEmitter 值映射单测(离线,真库归 Task 8):四种 {@link TupleValue} 形态的
 * Java 值映射面(Text 经 {@link ColumnValueMapper} 接缝、Binary 原样 byte[]、Null 直 null、
 * UnchangedToast 的 before 同列沿用/无 before 走哨兵)+ Operation 映射 + INSERT/UPDATE/DELETE
 * 的 before/after 取舍。类型化转换本体(vanilla {@code PgOutputReplicationMessage.getValue})
 * 在 {@code TypeRegistryColumnValueMapper}(生产实现,需真 TypeRegistry),此处经可注入的
 * 假 mapper 观察调用面与结果数组。
 *
 * <p>夹具约定:Table 取共享 {@link TestRelations#tableOf}(列沿 wire 列序);offset 经
 * {@code PostgresOffsetContext.Loader} 从空 offset 装载(离线构造,全部 LSN 为 null);
 * 分区用 {@code PostgresPartition} 直构。无并发面。
 */
class RowChangeEmitterTest {

    /** 测试表 oid(wire Relation 与 Table 同源互证)。 */
    private static final int OID = 16384;

    /** TOAST 哨兵的测试替身:与任何生产标记对象均不同引用,断言"经 mapper 产出的值"用。 */
    private static final Object TOAST_SENTINEL = new Object();

    /**
     * 责任:离线构造最小合法配置(必填四项 + snapshot.mode=no_data 镜像 taskConfigs
     * 注入面——MS5 起构造器对非 no_data 快照模式 fail-fast)——PostgresOffsetContext.Loader
     * 与 emitter 的 connectorConfig 形参都吃这个形态。
     */
    private static PostgresStreamConnectorConfig config() {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "no_data");
        return new PostgresStreamConnectorConfig(Configuration.from(props));
    }

    /** 责任:经 Loader 从空 offset 离线装载 offset 上下文(LSN 全 null,事件值不影响本类断言)。 */
    private static PostgresOffsetContext offset() {
        return new PostgresOffsetContext.Loader(config()).load(Map.of());
    }

    /**
     * 责任:构造四列 (id, v, raw, note) 的 wire Relation——typeId 取 23/25/17/25
     * (int4/text/bytea/text),供 mapper 调用面的 typeId/typeExpression 断言对位。
     */
    private static PgOutputMessage.Relation wire() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t4", 'd', List.of(
                new RelationColumn("id", 23, -1, true),
                new RelationColumn("v", 25, -1, false),
                new RelationColumn("raw", 17, -1, false),
                new RelationColumn("note", 25, -1, false)));
    }

    /** 责任:按 wire 构造 Table(共享夹具,列序=元组位序)。 */
    private static Table table() {
        return TestRelations.tableOf(wire());
    }

    /** 假值映射器:text 调用逐次记录(名字/类型/类型表达式/原值),unchangedToast 只回哨兵并计数。 */
    private static final class RecordingMapper implements ColumnValueMapper {
        final List<String> textCalls = new ArrayList<>();
        int toastCalls;

        @Override
        public Object text(String columnName, int typeId, String typeExpression, String rawValue) {
            textCalls.add(columnName + "|" + typeId + "|" + rawValue);
            return "T(" + rawValue + ")";
        }

        @Override
        public Object unchangedToast(String columnName, int typeId, String typeExpression, boolean optional) {
            toastCalls++;
            return TOAST_SENTINEL;
        }
    }

    /** 责任:四列元组的速记(Text 两列 + Binary + Null)。 */
    private static TupleData tuple(Object... values) {
        var cols = new ArrayList<TupleValue>();
        for (Object v : values) {
            if (v instanceof TupleValue tv) {
                cols.add(tv);
            }
            else if (v == null) {
                cols.add(TupleValue.NULL);
            }
            else if (v instanceof byte[] b) {
                cols.add(new TupleValue.Binary(b));
            }
            else {
                cols.add(new TupleValue.Text((String) v));
            }
        }
        return new TupleData(cols);
    }

    /** 责任:构造 emitter(假 mapper 注入)。 */
    private static RowChangeEmitter emitter(RecordingMapper mapper, Table table, RowChange change) {
        return new RowChangeEmitter(new PostgresPartition("server", "db"), offset(), Clock.system(),
                config(), mapper, table, change);
    }

    /** Operation 映射:INSERT→CREATE、UPDATE→UPDATE、DELETE→DELETE(vanilla 同款三角)。 */
    @Test
    void operationMapsDmlKindToEnvelopeOperation() {
        RecordingMapper mapper = new RecordingMapper();
        Table table = table();
        assertEquals(Operation.CREATE, emitter(mapper, table,
                new RowChange(DmlKind.INSERT, wire(), Optional.empty(), Optional.of(tuple("1")), OptionalLong.empty(), 1L)).getOperation());
        assertEquals(Operation.UPDATE, emitter(mapper, table,
                new RowChange(DmlKind.UPDATE, wire(), Optional.empty(), Optional.of(tuple("1")), OptionalLong.empty(), 1L)).getOperation());
        assertEquals(Operation.DELETE, emitter(mapper, table,
                new RowChange(DmlKind.DELETE, wire(), Optional.of(tuple("1")), Optional.empty(), OptionalLong.empty(), 1L)).getOperation());
    }

    /**
     * 四种 TupleValue 的映射面(INSERT 行):Text 经 mapper(带列名/typeId/原值)、
     * Binary 原样 byte[] 引用、Null 直 null、UnchangedToast(INSERT 无 before 可沿用)
     * 经 mapper.unchangedToast 取哨兵;old 侧恒 null。
     */
    @Test
    void insertMapsFourTupleValueKinds() {
        RecordingMapper mapper = new RecordingMapper();
        byte[] raw = { 1, 2, 3 };
        RowChange change = new RowChange(DmlKind.INSERT, wire(), Optional.empty(),
                Optional.of(tuple("7", "x", raw, TupleValue.UNCHANGED_TOAST)), OptionalLong.empty(), 11L);
        RowChangeEmitter emitter = emitter(mapper, table(), change);

        assertNull(emitter.getOldColumnValues(), "INSERT 无 before 侧");
        Object[] values = emitter.getNewColumnValues();
        assertArrayEquals(new Object[] { "T(7)", "T(x)", raw, TOAST_SENTINEL }, values,
                "Text 经 mapper、Binary 原样引用、Null 为 null、UnchangedToast 走哨兵");
        assertSame(raw, values[2], "Binary 列保留原 byte[] 引用(零拷贝)");
        assertEquals(1, mapper.toastCalls, "恰 1 列 UnchangedToast → 恰 1 次哨兵调用");
        assertEquals(List.of("id|23|7", "v|25|x"), mapper.textCalls,
                "text 调用带 wire 列名与 typeId(顺序=元组位序)");
    }

    /** DELETE:old 侧按 before 映射、new 侧恒 null。 */
    @Test
    void deleteMapsBeforeOnly() {
        RecordingMapper mapper = new RecordingMapper();
        RowChange change = new RowChange(DmlKind.DELETE, wire(), Optional.of(tuple("9", "y", null, "n")),
                Optional.empty(), OptionalLong.empty(), 12L);
        RowChangeEmitter emitter = emitter(mapper, table(), change);

        assertNull(emitter.getNewColumnValues(), "DELETE 无 after 侧");
        assertArrayEquals(new Object[] { "T(9)", "T(y)", null, "T(n)" }, emitter.getOldColumnValues());
        assertEquals(0, mapper.toastCalls);
    }

    /**
     * UPDATE 的 UnchangedToast 沿用 before 同列值(DBZ361 PostgresChangeRecordEmitter 的
     * cachedOldToastedValues 口径,位置版):after 的 TOAST 列取 old 侧已映射值,
     * 不落哨兵、不调 mapper.unchangedToast。
     */
    @Test
    void updateRecoversUnchangedToastFromBefore() {
        RecordingMapper mapper = new RecordingMapper();
        RowChange change = new RowChange(DmlKind.UPDATE, wire(),
                Optional.of(tuple("9", "old-v", null, "old-note")),
                Optional.of(tuple("9", TupleValue.UNCHANGED_TOAST, null, "new-note")),
                OptionalLong.empty(), 13L);
        RowChangeEmitter emitter = emitter(mapper, table(), change);

        // 调用序按基类契约:emitUpdateRecord 先取 old 再取 new——old 侧映射即恢复缓存的填充点
        assertArrayEquals(new Object[] { "T(9)", "T(old-v)", null, "T(old-note)" }, emitter.getOldColumnValues());
        assertArrayEquals(new Object[] { "T(9)", "T(old-v)", null, "T(new-note)" },
                emitter.getNewColumnValues(),
                "TOAST 列恢复为 before 同列的映射值 T(old-v)");
        assertEquals(0, mapper.toastCalls, "before 同列值可得——不落哨兵");
    }

    /** UPDATE 无 before(REPLICA IDENTITY DEFAULT 常态):UnchangedToast 经 mapper 哨兵,不抛不丢。 */
    @Test
    void updateWithoutBeforeFallsBackToToastSentinel() {
        RecordingMapper mapper = new RecordingMapper();
        RowChange change = new RowChange(DmlKind.UPDATE, wire(), Optional.empty(),
                Optional.of(tuple("9", TupleValue.UNCHANGED_TOAST, null, "new-note")),
                OptionalLong.empty(), 14L);
        RowChangeEmitter emitter = emitter(mapper, table(), change);

        assertNull(emitter.getOldColumnValues(), "无 before 元组 → old 侧 null");
        assertArrayEquals(new Object[] { "T(9)", TOAST_SENTINEL, null, "T(new-note)" },
                emitter.getNewColumnValues());
        assertEquals(1, mapper.toastCalls);
    }
}
