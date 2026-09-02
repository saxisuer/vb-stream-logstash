package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleData;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue;

import java.util.Objects;
import java.util.Optional;

/**
 * 行变更 → Debezium 变更记录发射器:{@link RowChange}(wire 元组)按 (partition, offset,
 * operation, before/after) 契约交给 dispatcher 的 emitter 体系——基类
 * {@link RelationalChangeRecordEmitter} 负责信封组装(含 PK 变更拆 delete+re-insert),
 * 本类只提供 Operation 映射与 before/after 的<b>值数组</b>。
 *
 * <p><b>与 vanilla {@code PostgresChangeRecordEmitter} 的关系(DBZ 3.6.1.Final sources
 * 实证口径,javadoc 记档)</b>:
 * <ul>
 *   <li>取值口径一致:CREATE 只 after / UPDATE 先 after 后 before / DELETE 只 before
 *       (vanilla getOldColumnValues/getNewColumnValues 的 switch 同三角)</li>
 *   <li><b>TOAST 未变(DBZ-1258 口径,位置版)</b>:UPDATE 的 before 侧映射结果即 vanilla 的
 *       {@code cachedOldToastedValues}(vanilla 按列名缓存,本模型列名↔位序一一对应故按位缓存);
 *       after 侧遇 {@link TupleValue.UnchangedToast} 时优先沿用 before 同列已映射值
 *       (best case),不可得才经 {@link ColumnValueMapper#unchangedToast} 取类型专属哨兵
 *       (vanilla 的 {@code UnchangedToastedReplicationMessageColumn} 标记,值转换器渲染占位)。
 *       DELETE 的 before 侧不做恢复缓存(vanilla sourceOfToasted=false 同款)</li>
 *   <li><b>schema 同步差异</b>:vanilla 在 emitChangeRecords 里检测列数/类型漂移并回查库
 *       (synchronizeTableSchema);本连接器的表定义由 'R' 版本日志 + asOf 快照先行保证
 *       (RelationTableFactory 在 reader 期已 enrich,listener 已装版本),此处不做库回查</li>
 *   <li>skipEmptyMessages=true 保留(vanilla 同款:无主键表 + 某些 replica identity 下
 *       空元组直接跳过,不发半空记录)</li>
 *   <li>值映射:Text 经 {@link ColumnValueMapper}(生产实现即 vanilla 的类型化解析)、
 *       Binary 原样 byte[](binary publish 场景,类型化反序列化 MS2 未接,Task 8 验证)、
 *       Null 直 null</li>
 * </ul>
 *
 * <p>线程约束:实例仅 consumer 线程触碰(构造于 TxChange 回调内、即用即弃);
 * before 缓存字段因此无需并发原语(vanilla 同款单写者假设)。
 */
public class RowChangeEmitter extends RelationalChangeRecordEmitter<PostgresPartition> {

    /** 值映射接缝(Text 类型化 / TOAST 哨兵)。 */
    private final ColumnValueMapper mapper;

    /** 变更时刻的表定义(asOf 解析产物,列序 = wire 列序 = 元组位序)。 */
    private final Table table;

    /** 行变更本体(嵌入 wire Relation,typeId/typmod 的真源)。 */
    private final RowChange change;

    /** before 侧已映射值(位置版 TOAST 恢复缓存;getOldColumnValues 首调时填充,仅 UPDATE 恢复路径读取)。 */
    private Object[] cachedOldValues;

    /**
     * 构造发射器(不发射——发射发生在 dispatcher 调 {@link #emitChangeRecords} 时)。
     *
     * @param partition      事件分区(记录随行)
     * @param offset         事务边界 offset(Begin 已置 lsn=lsn_commit=endLsn)
     * @param clock          记录时间戳时钟
     * @param connectorConfig 连接器配置(基类信封行为读取)
     * @param mapper         值映射接缝(生产实现 TypeRegistryColumnValueMapper / 测试假实现)
     * @param table          变更时刻表定义(列名/typeExpression 取自它,位序对齐元组)
     * @param change         行变更(嵌入 wire Relation 提供 typeId)
     */
    public RowChangeEmitter(PostgresPartition partition, PostgresOffsetContext offset, Clock clock,
                            PostgresStreamConnectorConfig connectorConfig, ColumnValueMapper mapper,
                            Table table, RowChange change) {
        super(partition, offset, clock, connectorConfig);
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.table = Objects.requireNonNull(table, "table");
        this.change = Objects.requireNonNull(change, "change");
    }

    /**
     * 责任:DML 种类 → 信封 Operation(vanilla 同三角)。
     * 边界:TRUNCATE/MESSAGE 不经本类(MS2 跳过发射,见 DispatcherTransactionListener)。
     */
    @Override
    public Operation getOperation() {
        return switch (change.dml()) {
            case INSERT -> Operation.CREATE;
            case UPDATE -> Operation.UPDATE;
            case DELETE -> Operation.DELETE;
        };
    }

    /**
     * 责任:旧值数组——CREATE 恒 null(INSERT 无 before);UPDATE/DELETE 映射 before 元组。
     * 关键步骤:UPDATE 的映射结果写入 {@link #cachedOldValues}(TOAST 恢复缓存,位置版);
     * DELETE 不缓存(vanilla sourceOfToasted=false:被删行无 after 可恢复)。
     * 边界:before 元组缺失(UPDATE 无旧镜像)返回 null——基类按"无旧值更新"处理。
     */
    @Override
    protected Object[] getOldColumnValues() {
        if (change.dml() == DmlKind.INSERT) {
            return null;
        }
        Object[] values = mapTuple(change.before());
        if (change.dml() == DmlKind.UPDATE) {
            cachedOldValues = values;   // 位置版 TOAST 恢复缓存(仅 UPDATE 需要,DELETE 无 after 可恢复)
        }
        return values;
    }

    /**
     * 责任:新值数组——INSERT/UPDATE 映射 after 元组;DELETE 恒 null。
     * 关键步骤:after 侧遇 UnchangedToast 时查 {@link #cachedOldValues}(getOldColumnValues
     * 必然先于本方法被基类调用——emitUpdateRecord 先取 old 再取 new);命中非 null 即沿用,
     * 否则经 mapper.unchangedToast 落哨兵。
     * 边界:after 元组缺失(DELETE)返回 null;列数以 Table 列数为准(协议保证元组列数
     * 与 Relation 列数一致,短出部分保持 null——防御性,不发越界)。
     */
    @Override
    protected Object[] getNewColumnValues() {
        if (change.dml() == DmlKind.DELETE) {
            return null;
        }
        return mapTuple(change.after());
    }

    /**
     * 责任:按位序把一个元组映射成 Java 值数组(位置 = Table 列位,wire 列的 typeId/typeModifier
     * 与 Table 列的 name/typeExpression 同位对齐)。
     * 关键步骤:逐列 switch TupleValue 形态——Text 经 mapper.text(类型化)、Binary 原样
     * byte[] 引用(零拷贝)、Null 置 null、UnchangedToast 先查恢复缓存(仅 after 侧且
     * 缓存非 null)再落哨兵。
     * 边界:tuple 为 empty(元组整体缺失)返回 null;元组列数超出 Table 列数时忽略超出部分
     * (协议不允许,防御);mapper 抛出的异常原样上抛(dispatcher 的失败处理模式接管)。
     *
     * @param tuple 待映射元组(before 或 after)
     * @return 位序对齐 Table 列的值数组;元组缺失为 null
     */
    private Object[] mapTuple(Optional<TupleData> tuple) {
        if (tuple.isEmpty()) {
            return null;
        }
        var columns = tuple.orElseThrow().columns();
        var wireColumns = change.relation().columns();
        Object[] values = new Object[table.columns().size()];
        int limit = Math.min(columns.size(), values.length);
        for (int i = 0; i < limit; i++) {
            TupleValue value = columns.get(i);
            var wireColumn = wireColumns.get(i);
            var tableColumn = table.columns().get(i);
            if (value instanceof TupleValue.Text text) {
                values[i] = mapper.text(wireColumn.name(), wireColumn.typeId(),
                        tableColumn.typeExpression(), text.value());
            }
            else if (value instanceof TupleValue.Binary binary) {
                values[i] = binary.value();
            }
            else if (value instanceof TupleValue.Null) {
                values[i] = null;
            }
            else if (value instanceof TupleValue.UnchangedToast) {
                values[i] = recoverUnchangedToast(i, wireColumn.name(), wireColumn.typeId(),
                        tableColumn.typeExpression(), tableColumn.isOptional());
            }
            // 密封接口四形态穷尽,无 default
        }
        return values;
    }

    /**
     * 责任:UnchangedToast 单列的恢复/哨兵两段决策(DBZ-1258 口径)。
     * 关键步骤:UPDATE 且 before 侧同位有非 null 已映射值 → 沿用(本事务旧镜像可得,
     * 值即"未变"的真值);否则(INSERT 的 TOAST 形态罕见但协议允许、UPDATE 无 before、
     * before 同位也是 TOAST/NULL)经 mapper 落类型专属哨兵。
     * 边界:cacheOldSide 的映射自身不调本方法之后才生效——old 侧 TOAST 列也走哨兵
     * (vanilla 缓存哨兵对象后 new 侧"恢复"到哨兵,行为等价)。
     */
    private Object recoverUnchangedToast(int position, String columnName, int typeId,
                                         String typeExpression, boolean optional) {
        if (change.dml() == DmlKind.UPDATE && cachedOldValues != null
                && position < cachedOldValues.length && cachedOldValues[position] != null) {
            return cachedOldValues[position];
        }
        return mapper.unchangedToast(columnName, typeId, typeExpression, optional);
    }

    /**
     * 责任:空元组跳过(vanilla PG 同款 true)——无主键表在 REPLICA IDENTITY DEFAULT 下
     * DELETE/UPDATE 可能只携带键列甚至空载荷,发半空记录不如整条跳过。
     */
    @Override
    protected boolean skipEmptyMessages() {
        return true;
    }
}
