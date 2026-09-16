package org.vastdata.vbstream.format.sql;

import org.vastdata.vbstream.format.ColumnDef;
import org.vastdata.vbstream.format.Op;
import org.vastdata.vbstream.format.ParsedEvent;
import org.vastdata.vbstream.format.TableDef;
import org.vastdata.vbstream.format.TypeCode;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.StringJoiner;

/**
 * IR → 可直接执行的 SQL 文本(PG/VastBase 方言)。纯函数、零状态、零第三方依赖。
 *
 * <p>语句语义与 vb-cdc-file-transform 仓 cdc-sink 的 RecordSqlGenerator 对齐:INSERT 全列、
 * UPDATE SET after 全列 WHERE 主键、DELETE WHERE 主键;无主键表的 u/d 抛异常(失败优于静默丢,
 * 引擎停止、offset 不推进、重启重放)。值以字面量渲染:时间五种与 decimal 沿"字符串落地"策略,
 * 由目标库按目标列类型隐式转换。裸 SQL 语义:如实反映源端操作序列,重复执行的主键冲突由执行方
 * 处理。移植自对方仓 cdc-sql-file-format(2026-09-16 快照,裁掉 renderDdl——本项目不做 DDL)。
 */
public final class SqlRenderer {

    private SqlRenderer() {
    }

    /**
     * 责任:渲染一条数据事件(INSERT/UPDATE/DELETE/TRUNCATE)为以 {@code ;\n} 结尾的语句。
     * 边界:无主键 u/d、INTERVAL 载荷非微秒形态、字符串含 NUL 抛 IAE(见类 javadoc)。
     */
    public static String render(ParsedEvent event) {
        if (event.truncate()) {
            return truncate(event.tableDef());
        }
        return switch (event.op()) {
            case CREATE, READ -> insert(event.tableDef(), event.values());
            case UPDATE -> update(event.tableDef(), event.values());
            case DELETE -> delete(event.tableDef(), event.values());
        };
    }

    /** 责任:TRUNCATE 语句(源端一条多表 TRUNCATE 按表到达多条事件,逐表一条语句)。 */
    private static String truncate(TableDef def) {
        return "TRUNCATE TABLE " + qualified(def) + ";\n";
    }

    /** 责任:INSERT 全列语句。边界:values 与 def.columns 长度对齐由上游 EnvelopeParser 保证。 */
    private static String insert(TableDef def, Object[] values) {
        StringJoiner cols = new StringJoiner(", ");
        StringJoiner vals = new StringJoiner(", ");
        for (int i = 0; i < def.columns().size(); i++) {
            cols.add(identifier(def.columns().get(i).name()));
            vals.add(literal(def.columns().get(i).type(), values[i]));
        }
        return "INSERT INTO " + qualified(def) + " (" + cols + ") VALUES (" + vals + ");\n";
    }

    /** 责任:UPDATE 语句(SET after 镜像全列,WHERE 主键)。边界:无主键抛 IAE。 */
    private static String update(TableDef def, Object[] values) {
        if (def.keyColumns().isEmpty()) {
            throw new IllegalArgumentException("UPDATE 渲染需要主键,表 %s 无主键".formatted(def.table()));
        }
        StringJoiner set = new StringJoiner(", ");
        for (int i = 0; i < def.columns().size(); i++) {
            ColumnDef col = def.columns().get(i);
            set.add(identifier(col.name()) + " = " + literal(col.type(), values[i]));
        }
        return "UPDATE " + qualified(def) + " SET " + set + whereKey(def, values) + ";\n";
    }

    /** 责任:DELETE 语句(WHERE 主键)。边界:无主键抛 IAE。 */
    private static String delete(TableDef def, Object[] values) {
        if (def.keyColumns().isEmpty()) {
            throw new IllegalArgumentException("DELETE 渲染需要主键,表 %s 无主键".formatted(def.table()));
        }
        return "DELETE FROM " + qualified(def) + whereKey(def, values) + ";\n";
    }

    /**
     * 责任:WHERE 主键列 = 镜像值(UPDATE 用 after 主键定位——主键值变更场景是已知限制,
     * 旧主键行残留,与对方仓 sink 同条)。边界:主键列不在镜像列中抛 IAE(TableDef 自相矛盾)。
     */
    private static String whereKey(TableDef def, Object[] values) {
        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < def.columns().size(); i++) {
            String name = def.columns().get(i).name();
            if (def.keyColumns().contains(name)) {
                conditions.add(identifier(name) + " = " + literal(def.columns().get(i).type(), values[i]));
            }
        }
        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("主键列不在镜像列中,无法构造 WHERE(表 %s)".formatted(def.table()));
        }
        return " WHERE " + String.join(" AND ", conditions);
    }

    /** 责任:带引号的限定表名 "schema"."table"(保留大小写原样,内部引号翻倍)。 */
    private static String qualified(TableDef def) {
        return identifier(def.schema()) + "." + identifier(def.table());
    }

    /** 责任:标识符加引号——内部 {@code "} 翻倍。 */
    private static String identifier(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    /**
     * 责任:TypeCode 驱动的字面量渲染。关键步骤:null → NULL;INTERVAL 先于 String 通用分派
     * 拦截(载荷虽是字符串但需分解渲染);String 加单引号内部翻倍;其余按类型出裸字面量。
     * 边界:字符串含 NUL、非字符串载荷的类型错位均抛 IAE。
     */
    private static String literal(TypeCode type, Object value) {
        if (value == null) {
            return "NULL";
        }
        // INTERVAL 载荷虽是字符串,但需分解渲染("<n> microseconds" 直写服务端拒收),须在 String 通用分派之前拦截
        if (type == TypeCode.INTERVAL) {
            if (!(value instanceof String s)) {
                throw new IllegalArgumentException("该类型应以字符串载荷到达: " + type);
            }
            return intervalLiteral(s);
        }
        if (value instanceof String s) {
            if (s.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("字符串含 NUL 字符,无法渲染为 SQL 文本");
            }
            return "'" + s.replace("'", "''") + "'";
        }
        return switch (type) {
            case BOOL -> (Boolean) value ? "TRUE" : "FALSE";
            case INT -> value.toString(); // EnvelopeParser 已把整型归一为 Long
            case FLOAT32, FLOAT64 -> {
                double d = ((Number) value).doubleValue();
                // NaN/Infinity 非合法裸字面量,PG 系接受带引号形态再按目标列转换
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    yield "'" + value + "'";
                }
                yield value.toString();
            }
            // VastBase 的 bytea 解析器不认 '\x' 十六进制前缀格式(按字面文本存储,数据损坏),
            // decode() 双端可用且返回 bytea 无需 cast
            case BYTES -> "decode('" + HexFormat.of().formatHex((byte[]) value) + "','hex')";
            // STRING 与时间五种必须以 String 载荷到达(上方 instanceof 已分派),否则属上游错位
            default -> throw new IllegalArgumentException("该类型应以字符串载荷到达: " + type);
        };
    }

    /**
     * 责任:INTERVAL 落地串 {@code "<n> microseconds"} → 总时长无损的分解形态(如
     * {@code '429 days 07:05:06.000000'};负值天与时分秒两段都带符号)。
     * 边界:非微秒形态(后缀不符/数字段不合法)抛 IAE。年月结构不可还原(IR 只保留总微秒)。
     */
    private static String intervalLiteral(String s) {
        String suffix = " microseconds";
        if (!s.endsWith(suffix)) {
            throw new IllegalArgumentException("INTERVAL 载荷非微秒形态,无法渲染: " + s);
        }
        long micros;
        try {
            micros = Long.parseLong(s.substring(0, s.length() - suffix.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("INTERVAL 载荷非微秒形态,无法渲染: " + s, e);
        }
        boolean neg = micros < 0;
        long abs = Math.absExact(micros);
        long days = abs / 86_400_000_000L;
        long rem = abs % 86_400_000_000L;
        String sign = neg ? "-" : "";
        return "'" + sign + days + " days " + sign
                + "%02d:%02d:%02d.%06d".formatted(rem / 3_600_000_000L, (rem / 60_000_000L) % 60,
                        (rem / 1_000_000L) % 60, rem % 1_000_000L) + "'";
    }
}
