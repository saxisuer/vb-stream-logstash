package org.vastdata.vbstream.format.sql;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.format.ColumnDef;
import org.vastdata.vbstream.format.Op;
import org.vastdata.vbstream.format.ParsedEvent;
import org.vastdata.vbstream.format.TableDef;
import org.vastdata.vbstream.format.TypeCode;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * IR → SQL 文本渲染规则钉行为(移植自 vb-cdc-file-transform 的 SqlRendererTest,断言逐字符
 * 保留——两仓渲染行为可对照)。语句语义与对方仓 cdc-sink 的 RecordSqlGenerator 对齐:
 * INSERT 全列 / UPDATE SET after 全列 WHERE 主键 / DELETE WHERE 主键。
 */
class SqlRendererTest {

    /**
     * 责任:构造带主键与列集的表定义(全部用例共用的组装便捷形态)。
     *
     * @param keys 主键列名集
     * @param cols 列定义
     * @return 表定义(db 固定 "db",schema "public",表 "orders")
     */
    private static TableDef table(List<String> keys, ColumnDef... cols) {
        return TableDef.of("db", "public", "orders", keys, List.of(cols));
    }

    /**
     * 责任:构造非 truncate 的数据事件(lsn/txid/ts 用占位常量——渲染不消费)。
     */
    private static ParsedEvent event(TableDef def, Op op, Object[] values) {
        return new ParsedEvent(def, op, values, false, 1L, 1L, 0L);
    }

    /**
     * 场景:INSERT 全列 + 各 TypeCode 字面量——INT 十进制、BOOL、FLOAT64、BYTES decode()
     * 小写 hex、时间五种单引号原样、INTERVAL 微秒分解、STRING 单引号翻倍、null→NULL。
     */
    @Test
    void insertRendersAllColumnsAndTypeLiterals() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("name", TypeCode.STRING),
                new ColumnDef("amount", TypeCode.STRING),
                new ColumnDef("flag", TypeCode.BOOL),
                new ColumnDef("price", TypeCode.FLOAT64),
                new ColumnDef("raw", TypeCode.BYTES),
                new ColumnDef("ts", TypeCode.TIMESTAMP),
                new ColumnDef("tstz", TypeCode.TIMESTAMPTZ),
                new ColumnDef("d", TypeCode.DATE),
                new ColumnDef("t", TypeCode.TIME),
                new ColumnDef("iv", TypeCode.INTERVAL),
                new ColumnDef("memo", TypeCode.STRING));
        String sql = SqlRenderer.render(event(def, Op.CREATE, new Object[]{
                1L, "alice's", "123.45", Boolean.TRUE, 9.5, new byte[]{(byte) 0xA1, 0x03},
                "2026-09-15 10:00:00.000000", "2026-09-10T07:30:00.000000Z", "2026-09-10",
                "12:34:56.789456", "45296789000 microseconds", null}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"name\", \"amount\", \"flag\", "
                + "\"price\", \"raw\", \"ts\", \"tstz\", \"d\", \"t\", \"iv\", \"memo\") VALUES "
                + "(1, 'alice''s', '123.45', TRUE, 9.5, decode('a103','hex'), "
                + "'2026-09-15 10:00:00.000000', '2026-09-10T07:30:00.000000Z', '2026-09-10', "
                + "'12:34:56.789456', '0 days 12:34:56.789000', NULL);\n", sql);
    }

    /**
     * 场景:列名含双引号翻倍转义;Float NaN 渲染带引号形态(非合法裸字面量)。
     */
    @Test
    void insertEscapesQuotedIdentifiersAndRendersFloatSpecialValues() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("\"weird\"col", TypeCode.STRING),
                new ColumnDef("f", TypeCode.FLOAT32));
        String sql = SqlRenderer.render(event(def, Op.READ, new Object[]{1L, "含\"引号", Float.NaN}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"\"\"weird\"\"col\", \"f\") "
                + "VALUES (1, '含\"引号', 'NaN');\n", sql);
    }

    /**
     * 场景:UPDATE SET after 全列、WHERE 主键列。
     */
    @Test
    void updateSetsAllColumnsAndFiltersByPrimaryKey() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("name", TypeCode.STRING));
        String sql = SqlRenderer.render(event(def, Op.UPDATE, new Object[]{1L, "alice2"}));
        assertEquals("UPDATE \"public\".\"orders\" SET \"id\" = 1, \"name\" = 'alice2' "
                + "WHERE \"id\" = 1;\n", sql);
    }

    /**
     * 场景:DELETE 只渲染 WHERE 主键。
     */
    @Test
    void deleteFiltersByPrimaryKey() {
        TableDef def = table(List.of("id"), new ColumnDef("id", TypeCode.INT));
        assertEquals("DELETE FROM \"public\".\"orders\" WHERE \"id\" = 2;\n",
                SqlRenderer.render(event(def, Op.DELETE, new Object[]{2L})));
    }

    /**
     * 场景:TRUNCATE 渲染为逐表 TRUNCATE TABLE 语句(op 与 values 均不消费)。
     */
    @Test
    void truncateRendersPerTableStatement() {
        ParsedEvent ev = new ParsedEvent(table(List.of(), new ColumnDef[0]), null,
                new Object[0], true, 1L, 1L, 0L);
        assertEquals("TRUNCATE TABLE \"public\".\"orders\";\n", SqlRenderer.render(ev));
    }

    /**
     * 场景:无主键表的 UPDATE/DELETE 抛 IAE(捕获端失败优于静默丢;与 binary"不支持类型抛异常"
     * 同构——引擎停止、offset 不动、重启重放)。
     */
    @Test
    void updateAndDeleteWithoutPrimaryKeyThrow() {
        TableDef def = table(List.of(), new ColumnDef("id", TypeCode.INT));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.UPDATE, new Object[]{1L})));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.DELETE, new Object[]{1L})));
    }

    /**
     * 场景:字符串载荷含 NUL 字符抛 IAE(SQL 文本无法表达,不产出损坏文件)。
     */
    @Test
    void stringWithNulThrows() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT), new ColumnDef("v", TypeCode.STRING));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "a\0b"})));
    }

    /**
     * 场景:多行字符串值原样进单引号字面量(换行不转义)。
     */
    @Test
    void multiLineStringValueRendersVerbatim() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT), new ColumnDef("v", TypeCode.STRING));
        String sql = SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "第一行\n第二行"}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"v\") VALUES (1, '第一行\n第二行');\n", sql);
    }

    /**
     * 场景:负值 INTERVAL 天与时分秒两段都带符号;非微秒形态载荷抛 IAE(VastBase 拒收
     * microseconds 直写,渲染侧分解——对方仓实测裁定)。
     */
    @Test
    void intervalNegativeDecomposesAndNonMicrosecondFormThrows() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT), new ColumnDef("iv", TypeCode.INTERVAL));
        String sql = SqlRenderer.render(event(def, Op.CREATE,
                new Object[]{1L, "-45296789000 microseconds"}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"iv\") "
                + "VALUES (1, '-0 days -12:34:56.789000');\n", sql);
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "abc"})));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "12x microseconds"})));
    }

    /**
     * 场景:空 byte[] 渲染 decode('','hex');±Infinity 走带引号路径。
     */
    @Test
    void emptyByteaAndSignedInfinityLiterals() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("raw", TypeCode.BYTES),
                new ColumnDef("f1", TypeCode.FLOAT32),
                new ColumnDef("f2", TypeCode.FLOAT64));
        String sql = SqlRenderer.render(event(def, Op.CREATE,
                new Object[]{1L, new byte[0], Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"raw\", \"f1\", \"f2\") "
                + "VALUES (1, decode('','hex'), 'Infinity', '-Infinity');\n", sql);
    }
}
