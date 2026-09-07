package org.vastdata.vbstream.reader;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * CDC 记录行渲染 consumer——{@link ChangeConsumer} 的批形态实现:一批记录逐条 INFO 到
 * 专用 logger 后经 committer 记账,批尾触发提交判定。render 与 offset 记账在同一循环里
 * 交错进行,<b>INFO 打印成功即视为该条已消费</b>(markProcessed 随后推进 offset 到该条)。
 *
 * <p>取数口径:{@code event.value()} 是原始 {@link SourceRecord}(Connect 格式直通,
 * {@code event.key()} 恒 null 不用)。数据记录的 op 取 value Struct 顶层 {@code op} 字段、
 * txId 取 {@code source} 块的 {@code txId}(与连接器 source 块同源);事务元数据记录
 * (topic {@code <prefix>.transaction},provide.transaction.metadata 默认开)取 {@code status}/
 * {@code id}。lsn/lsn_commit 取 {@link SourceRecord#sourceOffset()} map 的同名键(连接器
 * 事务边界 offset 双写的产物)。
 *
 * <p>线程约束:handleBatch 由 engine 任务轮询线程按提交序串行调用(ORDERED 处理器),
 * 本类无状态可安全复用;抛 InterruptedException 会停引擎(契约)。
 */
final class LogChangeConsumer implements ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

    /** CDC 数据专用 logger(与系统日志分离、可独立调级——项目规约;名字见类 javadoc 的包约定)。 */
    private static final Logger CDC = LoggerFactory.getLogger("org.vastdata.vbstream.reader.cdc");

    /** 值预览上限(TOAST 载荷可达 16KB+,截断防刷屏;引擎 ConsoleRenderer 截 64 字符同思路,冒烟面放宽)。 */
    private static final int PREVIEW_LIMIT = 512;

    /** 缺失字段的占位符。 */
    private static final String ABSENT = "-";

    /**
     * 责任:消费一批变更事件——逐条渲染 INFO 行并记账,批尾收口。
     * 关键步骤:批头判定 {@code CDC.isInfoEnabled()} 一次(logger 调 OFF/调高级别时整批
     * <b>短路渲染</b>——slf4j 的参数求值发生在级别判定之前,不守卫的话 value 的
     * {@code Struct.toString()} 每条都会白构造,压测实测这是消费端最贵的单条操作之一)→
     * 循环内 {@code event.value()} 取原始记录 → 渲染分支 CDC.info 一行(topic/key/op/txId/
     * lsn/lsn_commit/value 预览)→ {@code committer.markProcessed(event)} 推进 offset 到该条
     * (返回后引擎视其为已消费);批尾 {@code markBatchFinished()} 触发按提交策略的 offset
     * flush(offset.flush.interval.ms,默认 1000ms 周期)。
     * 边界:记录形态异常(字段缺失)以 {@code -} 占位不抛——渲染层不做协议断言;
     * 空批直接 markBatchFinished(保持引擎批语义完整);InterruptedException 原样上抛
     * (停机请求,吞掉会让引擎无法收敛)。
     *
     * @param records  本批事件(Connect 格式直通,value 即 SourceRecord)
     * @param committer offset 记账器
     * @throws InterruptedException 停机中断(引擎契约,原样上抛)
     */
    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records,
                            RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        boolean render = CDC.isInfoEnabled();
        for (ChangeEvent<SourceRecord, SourceRecord> event : records) {
            if (render) {
                SourceRecord record = event.value();
                CDC.info("topic={} key={} op={} txId={} lsn={} lsn_commit={} value={}",
                        record.topic(), preview(record.key()), opOf(record), txIdOf(record),
                        offsetValue(record, "lsn"), offsetValue(record, "lsn_commit"), preview(record.value()));
            }
            committer.markProcessed(event);
        }
        committer.markBatchFinished();
    }

    /**
     * 责任:取记录的操作形态——数据记录取 value Struct 顶层 {@code op}(c/u/d/t/r);
     * 事务元数据记录取 {@code status}(BEGIN/END)。两者皆无(意外形态)落 {@code -} 占位。
     * 边界:value 非 Struct 或字段缺失不抛,渲染层不做协议断言——Connect 的
     * {@code Struct.get(字段)} 对 schema 不存在的字段抛 DataException(非返回 null,
     * 冒烟实测),故一律经 {@link #fieldValue} 的存在性检查先行。
     */
    private static String opOf(SourceRecord record) {
        if (record.value() instanceof Struct struct) {
            Object op = fieldValue(struct, "op");
            if (op != null) {
                return String.valueOf(op);
            }
            Object status = fieldValue(struct, "status");
            if (status != null) {
                return String.valueOf(status);
            }
        }
        return ABSENT;
    }

    /**
     * 责任:取记录的事务 id——数据记录取 source 块的 {@code txId}(连接器写顶层 xid 的纯数字
     * 形态,与事务元数据记录同源);事务元数据记录取顶层 {@code id}。皆无落 {@code -}。
     * 边界:source 块缺失或字段为 null 不抛(存在性检查先行,见 {@link #opOf})。
     */
    private static String txIdOf(SourceRecord record) {
        if (record.value() instanceof Struct struct) {
            Struct source = struct.schema().field("source") != null ? struct.getStruct("source") : null;
            if (source != null && fieldValue(source, "txId") != null) {
                return String.valueOf(fieldValue(source, "txId"));
            }
            Object id = fieldValue(struct, "id");
            if (id != null) {
                return String.valueOf(id);
            }
        }
        return ABSENT;
    }

    /**
     * 责任:按字段名安全取值——字段不在 schema 返回 null(不抛)。存在的必要性:
     * {@code Struct.get(字段)} 对 schema 外的字段名抛 DataException,而两类记录
     * (数据 Envelope 与事务元数据)的 schema 字段集不同,按名探测必须先查存在性。
     *
     * @param struct 值载体
     * @param field  字段名
     * @return 字段值;字段不存在或值为 null 均返回 null
     */
    private static Object fieldValue(Struct struct, String field) {
        return struct.schema().field(field) != null ? struct.get(field) : null;
    }

    /**
     * 责任:取 sourceOffset map 的键值渲染——连接器在事务边界双写 {@code lsn}/
     * {@code lsn_commit}(均锚事务 endLsn)。边界:map 为 null 或键缺失落 {@code -}。
     */
    private static String offsetValue(SourceRecord record, String key) {
        Object value = record.sourceOffset() == null ? null : record.sourceOffset().get(key);
        return value == null ? ABSENT : String.valueOf(value);
    }

    /**
     * 责任:任意对象的日志预览——null 落 {@code -};toString 超 {@link #PREVIEW_LIMIT} 字符
     * 截断并以 "...(N chars)" 标注原始长度(TOAST 大载荷防刷屏,又保留长度可感知)。
     * 边界:value 为 null 是合法形态(如 TRUNCATE 记录 key=null),不当异常处理。
     */
    private static String preview(Object value) {
        if (value == null) {
            return ABSENT;
        }
        String text = String.valueOf(value);
        return text.length() <= PREVIEW_LIMIT ? text : text.substring(0, PREVIEW_LIMIT) + "...(" + text.length() + " chars)";
    }
}
