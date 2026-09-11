package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.vastdata.vbstream.reader.format.ColumnDef;
import org.vastdata.vbstream.reader.format.Op;
import org.vastdata.vbstream.reader.format.TableDef;
import org.vastdata.vbstream.reader.format.TypeCode;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Debezium envelope（{@link SourceRecord}）→ VBFG 内部精简格式。
 *
 * <p>时间类型（{@code io.debezium.time.*} schema）在捕获端格式化为字符串；
 * decimal 由 connector 配置 {@code decimal.handling.mode=string} 直接以字符串到达
 * （跨网闸同步的"时间/decimal 统一 string 落地"设计决策，两端对齐）。
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 EnvelopeParser（2026-09-11 快照）。
 */
final class EnvelopeParser {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private EnvelopeParser() {
    }

    /**
     * 责任：识别事务元数据记录——<b>按结构特征</b>而非 schema 名（Debezium 各版本 schema 名
     * 不稳定）：value 是 Struct 且有 {@code status}/{@code id} 字段、没有 envelope 的
     * {@code op} 字段。边界：value 非 Struct 返回 false。
     */
    static boolean isTransactionMetadata(SourceRecord record) {
        Object value = record.value();
        if (!(value instanceof Struct s)) {
            return false;
        }
        return s.schema().field("status") != null
                && s.schema().field("id") != null
                && s.schema().field("op") == null;
    }

    /**
     * 责任：解析事务元数据记录为 {@link TransactionMarker}。
     * 关键步骤：status（"BEGIN"/其他=END）→ id（txid 字符串）→ event_count/ts_ms
     * （均 optional，缺失落 -1）。边界：字段值 null 落 -1。
     */
    static TransactionMarker parseTransaction(SourceRecord record) {
        Struct value = (Struct) record.value();
        String status = value.getString("status");
        Long eventCount = value.getInt64("event_count");
        Long tsMs = value.getInt64("ts_ms");
        return new TransactionMarker("BEGIN".equals(status), value.getString("id"),
                eventCount == null ? -1 : eventCount, tsMs == null ? -1 : tsMs);
    }

    /**
     * 责任：解析数据记录为 {@link ParsedEvent}。关键步骤：op/before/after/source/ts_ms →
     * TRUNCATE（op="t"，镜像为空）走独立形态；DELETE 取 before 镜像（REPLICA IDENTITY
     * DEFAULT 时只有主键列）、其余取 after → 列定义从镜像 schema 推导（key 列取自 record
     * 的 key Struct）→ 逐列按 schema 类型读值（时间为字符串化、整型归一 long、BYTES 的
     * ByteBuffer 转 byte[]）。边界：镜像缺失抛 IAE（协议破坏）；source 块的 lsn/txid
     * 按 schema 存在性先行（decoderbufs 无 txid）。
     */
    static ParsedEvent parse(SourceRecord record) {
        Struct envelope = (Struct) record.value();
        String op = envelope.getString("op");
        Struct after = envelope.getStruct("after");
        Struct before = envelope.getStruct("before");
        Struct source = envelope.getStruct("source");
        Long tsMs = envelope.getInt64("ts_ms");

        String db = source.getString("db");
        String schema = source.getString("schema");
        String table = source.getString("table");
        Long lsn = optLong(source, "lsn");
        Long txid = optLong(source, "txid");

        if ("t".equals(op)) {
            // TRUNCATE：before/after 为空
            List<String> keys = keyColumns(record.key());
            List<ColumnDef> cols = imageColumns(before != null ? before : after);
            return new ParsedEvent(TableDef.of(db, schema, table, keys, cols),
                    null, new Object[0], true, orZero(lsn), orZero(txid), orZero(tsMs));
        }

        // DELETE 用 before（REPLICA IDENTITY DEFAULT 时只有主键列）；其余用 after
        Struct image = "d".equals(op) ? (before != null ? before : after) : after;
        if (image == null) {
            throw new IllegalArgumentException("记录缺少 before/after 镜像: op=%s table=%s".formatted(op, table));
        }

        List<ColumnDef> cols = imageColumns(image);
        List<String> keys = keyColumns(record.key());
        Object[] values = new Object[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            values[i] = readValue(image, image.schema().fields().get(i));
        }
        return new ParsedEvent(TableDef.of(db, schema, table, keys, cols),
                mapOp(op), values, false, orZero(lsn), orZero(txid), orZero(tsMs));
    }

    /** envelope op 字符 → {@link Op}；未知值抛 IAE。 */
    private static Op mapOp(String op) {
        return switch (op) {
            case "c" -> Op.CREATE;
            case "u" -> Op.UPDATE;
            case "d" -> Op.DELETE;
            case "r" -> Op.READ;
            default -> throw new IllegalArgumentException("未知 envelope op: " + op);
        };
    }

    /** key Struct 的字段名序列（key 非 Struct 时为空——无主键表）。 */
    private static List<String> keyColumns(Object key) {
        if (!(key instanceof Struct keyStruct)) {
            return List.of();
        }
        List<String> keys = new ArrayList<>();
        for (Field f : keyStruct.schema().fields()) {
            keys.add(f.name());
        }
        return keys;
    }

    /** 镜像 Struct 的字段 → 列定义（含时间类型推导）。 */
    private static List<ColumnDef> imageColumns(Struct image) {
        if (image == null) {
            return List.of();
        }
        List<ColumnDef> cols = new ArrayList<>();
        for (Field f : image.schema().fields()) {
            cols.add(new ColumnDef(f.name(), typeCode(f.schema())));
        }
        return cols;
    }

    /**
     * 责任：Connect schema → {@link TypeCode}。关键步骤：先按 schema 名匹配
     * {@code io.debezium.time.*} 的五种时间类型；其余按原生 Connect 类型映射
     * （整数族归一 INT、浮点两种、STRING/BYTES）。边界：ARRAY/MAP/STRUCT 等复合类型
     * 抛 IAE（VBFG 精简格式不支持——connector 的 values.as.string 模式可消解）。
     */
    private static TypeCode typeCode(Schema schema) {
        TypeCode temporal = temporalTypeCode(schema.name());
        if (temporal != null) {
            return temporal;
        }
        Schema.Type t = schema.type();
        return switch (t) {
            case BOOLEAN -> TypeCode.BOOL;
            case INT8, INT16, INT32, INT64 -> TypeCode.INT;
            case FLOAT32 -> TypeCode.FLOAT32;
            case FLOAT64 -> TypeCode.FLOAT64;
            case STRING -> TypeCode.STRING;
            case BYTES -> TypeCode.BYTES;
            default -> throw new IllegalArgumentException(
                    "暂不支持的列类型: %s (schema=%s)".formatted(t, schema.name()));
        };
    }

    /** {@code io.debezium.time.*} → 时间类型码；非时间 schema 返回 null。 */
    private static TypeCode temporalTypeCode(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        return switch (schemaName) {
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp",
                 "io.debezium.time.NanoTimestamp" -> TypeCode.TIMESTAMP;
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.ZonedTime" -> TypeCode.TIMESTAMPTZ;
            case "io.debezium.time.Date" -> TypeCode.DATE;
            case "io.debezium.time.Time", "io.debezium.time.MicroTime",
                 "io.debezium.time.NanoTime" -> TypeCode.TIME;
            case "io.debezium.time.MicroDuration" -> TypeCode.INTERVAL;
            default -> null;
        };
    }

    /**
     * 责任：读单列值——时间 schema 字符串化（统一 string 落地）、整型族归一 long、
     * BYTES 的 ByteBuffer 归一 byte[]（pgoutput 等插件的 BYTES 值形态）、其余原样。
     * 边界：null 由上层位图表达，此处 raw==null 直接返回 null。
     */
    private static Object readValue(Struct image, Field field) {
        Schema schema = field.schema();
        Object raw = image.get(field);
        if (raw == null) {
            return null;
        }
        String name = schema.name();
        if (name != null && name.startsWith("io.debezium.time.")) {
            return temporalString(name, raw);
        }
        return switch (schema.type()) {
            case INT8, INT16, INT32 -> ((Number) raw).longValue();
            case BYTES -> toByteArray(raw);
            default -> raw;
        };
    }

    private static byte[] toByteArray(Object raw) {
        if (raw instanceof byte[] bytes) {
            return bytes;
        }
        ByteBuffer buf = (ByteBuffer) raw;
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    /**
     * {@code io.debezium.time.*} → 字符串（时间统一 string 落地策略）；未知 schema 抛 IAE。
     *
     * <p><b>移植偏离记档</b>（相对 vb-cdc-file-transform 的 cdc-capture，2026-09-11）：无时区
     * timestamp 按 <b>UTC</b> 读墙钟而非 systemDefault——Debezium 对 no-timezone timestamp 的
     * epoch 表示即"墙钟值当 UTC"，用 UTC 读回恰得源库原值（systemDefault 会产生时区偏移，
     * cdc-capture 的 doc §2.6 第 6 条已记档该偏差，本移植修正）；且 MicroTimestamp 保留微秒
     * 精度（移植源经毫秒中转会截断为 .SSS000）。ZonedTimestamp 的 ISO 原文与 TIME 各分支
     * 不受影响。
     */
    private static String temporalString(String schemaName, Object raw) {
        return switch (schemaName) {
            case "io.debezium.time.Date" -> LocalDate.ofEpochDay(((Number) raw).longValue()).toString();
            case "io.debezium.time.Time" -> LocalTime.ofNanoOfDay(((Number) raw).longValue() * 1_000_000L).format(TIME_FMT);
            case "io.debezium.time.MicroTime" -> LocalTime.ofNanoOfDay(((Number) raw).longValue() * 1_000L).format(TIME_FMT);
            case "io.debezium.time.NanoTime" -> LocalTime.ofNanoOfDay(((Number) raw).longValue()).format(TIME_FMT);
            case "io.debezium.time.Timestamp" -> tsFromMillis(((Number) raw).longValue());
            case "io.debezium.time.MicroTimestamp" -> tsFromMicros(((Number) raw).longValue());
            case "io.debezium.time.NanoTimestamp" -> tsFromMicros(((Number) raw).longValue() / 1_000L);
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.ZonedTime" -> (String) raw;
            case "io.debezium.time.MicroDuration" -> ((Number) raw).longValue() + " microseconds";
            default -> throw new IllegalArgumentException("未知时间 schema: " + schemaName);
        };
    }

    /** 毫秒精度 timestamp → UTC 墙钟字符串（Timestamp schema 的原生精度即毫秒）。 */
    private static String tsFromMillis(long millis) {
        return LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), ZoneOffset.UTC).format(TS_FMT);
    }

    /** 微秒精度 timestamp → UTC 墙钟字符串（墙钟原值往返,微秒保真）。 */
    private static String tsFromMicros(long micros) {
        return LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                (int) (Math.floorMod(micros, 1_000_000L) * 1_000L), ZoneOffset.UTC).format(TS_FMT);
    }

    private static long orZero(Long v) {
        return v == null ? 0L : v;
    }

    /** source 块的字段随插件/版本有差异（如 decoderbufs 无 txid），按 schema 判定后再读。 */
    private static Long optLong(Struct source, String fieldName) {
        return source.schema().field(fieldName) != null ? source.getInt64(fieldName) : null;
    }
}
