package org.vastdata.vbstream.reader.file;

import io.debezium.engine.ChangeEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/**
 * file 包测试共用的 {@link SourceRecord}/{@link ChangeEvent} 手造辅助——事务元数据
 * (status/id 结构特征、无 envelope 的 op 字段)与数据记录(op=c、after 镜像、source 块
 * lsn/txid、key 结构)的最小可解析形态。
 */
final class ConnectTestRecords {

    /** 事务元数据 schema(结构特征识别的判定面)。 */
    private static final Schema TX_SCHEMA = SchemaBuilder.struct()
            .field("status", Schema.STRING_SCHEMA)
            .field("id", Schema.STRING_SCHEMA)
            .field("event_count", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    /** 数据记录的 source 块 schema(lsn/txid optional——decoderbufs 无 txid 的通用形态)。 */
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .field("txid", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private static final Schema AFTER_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("payload", Schema.STRING_SCHEMA)
            .build();

    /** before 镜像的 optional 形态(Struct.put 对 required 字段拒 null——手造 op=c 时 before 恒 null)。 */
    private static final Schema AFTER_OPTIONAL = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("payload", Schema.STRING_SCHEMA)
            .optional()
            .build();

    /** envelope schema——before 字段必须存在(Struct.getStruct 对 schema 外字段抛 DataException)。 */
    private static final Schema ENVELOPE_SCHEMA = SchemaBuilder.struct()
            .field("op", Schema.STRING_SCHEMA)
            .field("before", AFTER_OPTIONAL)
            .field("after", AFTER_SCHEMA)
            .field("source", SOURCE_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .build();

    private ConnectTestRecords() {
    }

    /** 事务元数据记录(status="BEGIN"/"END",id 为 txid 字符串)。 */
    static SourceRecord txRecord(String status, String txid) {
        Struct value = new Struct(TX_SCHEMA).put("status", status).put("id", txid);
        return new SourceRecord(Map.of(), Map.of(), "p.transaction", null, null, TX_SCHEMA, value);
    }

    /** 数据记录(op=c,public.t 表两列 id/payload,lsn=1000、txid=769)。 */
    static SourceRecord dataRecord(int id, String payload) {
        Struct source = new Struct(SOURCE_SCHEMA)
                .put("db", "postgres").put("schema", "public").put("table", "t")
                .put("lsn", 1000L).put("txid", 769L);
        Struct after = new Struct(AFTER_SCHEMA).put("id", id).put("payload", payload);
        Struct envelope = new Struct(ENVELOPE_SCHEMA)
                .put("op", "c").put("before", null).put("after", after)
                .put("source", source).put("ts_ms", 1L);
        Struct key = new Struct(KEY_SCHEMA).put("id", id);
        return new SourceRecord(Map.of(), Map.of(), "p.public.t", KEY_SCHEMA, key, ENVELOPE_SCHEMA, envelope);
    }

    /** Connect 直通形态的 ChangeEvent 手造(key 恒 null——宿主不用;value=null 即 tombstone 形态)。 */
    static ChangeEvent<SourceRecord, SourceRecord> event(SourceRecord value) {
        return new ChangeEvent<>() {
            @Override
            public SourceRecord key() {
                return null;
            }

            @Override
            public SourceRecord value() {
                return value;
            }

            @Override
            public String destination() {
                return value != null ? value.topic() : null;
            }

            @Override
            public Integer partition() {
                return null;
            }
        };
    }

    /** 事务标记手造(滚动逻辑只感知 begin 与 txid)。 */
    static TransactionMarker marker(boolean begin, String txid) {
        return new TransactionMarker(begin, txid, -1, 42L);
    }
}
