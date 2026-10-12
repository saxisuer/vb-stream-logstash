package org.vastdata.vbstream.walsource.replay;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV17;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OG 档引导分区母表 WARN 痕迹的离线断言（终审 I1 修复）——"有痕跳过"承诺的运行时
 * 落地面：seedClassRows 检出 parttype='p' 行（放行入字典的同一判别列）时，整个引导
 * <b>恰一行 WARN</b>（节流）携带 relname 与"分区 DML 行将有痕跳过——完整捕获归 M3"
 * 指引；无分区母表、或 V17/V18 vanilla 引导（投影根本不含 parttype 列）零 WARN
 * ——vanilla 零回归锚。
 *
 * <p>驱动形态：动态代理脚本化 JDBC 会话（{@code api} 包 FakeJdbc 的同构本地小样，
 * replay 包不可见故就地复刻）按 bootstrap 真实调用序回放——SET TRANSACTION /
 * 位点函数 / 三 relfilenode 回填 / 三目录种子 SQL 全部按字面脚本化，未脚本化方法
 * 一经调用即 AssertionError（防"假驱动吞掉真实调用"的静默假绿）。WARN 断言走
 * logback {@link ListAppender} 挂 {@link CatalogBootstrap} logger（M1 捕获形态）。</p>
 */
class OgCatalogBootstrapSeedWarnTest {

    /** OG 档种子投影的 42 列行宽（ctid + oid + 40 数据列）。 */
    private static final int OG_COLS = 42;

    /** vanilla（V17）种子投影的 11 列行宽。 */
    private static final int VANILLA_COLS = 11;

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    /**
     * 挂 ListAppender 捕获 {@link CatalogBootstrap} 日志——WARN"恰一次且带上下文"
     * 的断言面（M1 的 ListAppender 捕获形态）。
     */
    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(CatalogBootstrap.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
    }

    /**
     * 摘除 appender，防泄漏到后续测试类。
     */
    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
    }

    /**
     * 用例 ①：OG 引导检出两张分区母表 → <b>整引导恰一行 WARN</b>（节流语义——两张
     * 'p' 行不产生两行日志），消息含首张母表 relname、分区母表计数、"分区 DML"跳过
     * 语义与 "M3" 指引四锚；分区母表行仍放行入字典（"不砖"语义零变化——stores 行数
     * 含 'p' 行）。
     */
    @Test
    void ogSeedWithPartitionParentsWarnsExactlyOnce() throws SQLException {
        ScriptedJdbc jdbc = baseScript(WalLayoutVOpenGauss.INSTANCE)
                .row(ogClassRow(16390L, "t_part_range", 16390L, "p"))
                .row(ogClassRow(16391L, "t_part_list", 16391L, "p"))
                .row(ogClassRow(16392L, "t_normal", 16392L, "n"))
                .build();
        CatalogStores stores = new CatalogStores();

        new CatalogBootstrap(jdbc.connection(), WalLayoutVOpenGauss.INSTANCE).bootstrap(stores);

        List<ILoggingEvent> warns = warns();
        assertEquals(1, warns.size(), "两张分区母表 → 节流后恰一行 WARN，实得: " + warns);
        String msg = warns.get(0).getFormattedMessage();
        assertTrue(msg.contains("t_part_range"), "WARN 须含分区母表 relname，实得: " + msg);
        assertTrue(msg.contains("2"), "WARN 须含分区母表计数，实得: " + msg);
        assertTrue(msg.contains("分区 DML"), "WARN 须含分区 DML 跳过语义，实得: " + msg);
        assertTrue(msg.contains("M3"), "WARN 须含 M3 指引，实得: " + msg);
        assertEquals(3, stores.classRows().size(), "'p' 行仍放行入字典（不砖语义零变化）");
    }

    /**
     * 用例 ②：OG 引导无分区母表（全 'n' 行）→ 零 WARN（痕迹只在真有分区面时打）。
     */
    @Test
    void ogSeedWithoutPartitionParentsStaysQuiet() throws SQLException {
        ScriptedJdbc jdbc = baseScript(WalLayoutVOpenGauss.INSTANCE)
                .row(ogClassRow(16392L, "t_normal", 16392L, "n"))
                .build();
        new CatalogBootstrap(jdbc.connection(), WalLayoutVOpenGauss.INSTANCE).bootstrap(new CatalogStores());
        assertEquals(0, warns().size(), "无分区母表不得 WARN");
    }

    /**
     * 用例 ③（零回归锚）：V17 vanilla 引导（投影不含 parttype 列）→ 零 WARN——新增
     * 痕迹面严格 OG 档位门控，vanilla 路径零扰动。
     */
    @Test
    void vanillaSeedNeverWarns() throws SQLException {
        Object[] row = new Object[VANILLA_COLS];
        row[0] = "(0,1)";
        row[1] = 16392L;
        row[2] = "t_normal";
        for (int i = 3; i <= 9; i++) {
            row[i] = 0L;
        }
        row[10] = "r";
        ScriptedJdbc jdbc = baseScript(WalLayoutV17.INSTANCE).row(row).build();
        new CatalogBootstrap(jdbc.connection(), WalLayoutV17.INSTANCE).bootstrap(new CatalogStores());
        assertEquals(0, warns().size(), "vanilla 引导不得 WARN");
    }

    /**
     * 摘 WARN 级事件清单。
     */
    private List<ILoggingEvent> warns() {
        List<ILoggingEvent> out = new ArrayList<>();
        for (ILoggingEvent e : appender.list) {
            if (e.getLevel() == Level.WARN) {
                out.add(e);
            }
        }
        return out;
    }

    /**
     * 基础脚本入口（链式）：登记 bootstrap 真实调用序的全部非 pg_class 种子查询
     * （位点函数[布局感知]/三 relfilenode 回填/pg_attribute 与 pg_namespace 空种子），
     * 返回 pg_class 种子行的追加面。
     *
     * @param layout 版本布局（决定位点函数与 pg_class 种子 SQL 文本）
     * @return pg_class 种子行追加器
     */
    private static SeedRows baseScript(WalLayout layout) {
        return new SeedRows(layout);
    }

    /**
     * pg_class 种子行的链式追加器（脚本化到布局对应的种子 SQL 文本上）。
     */
    private static final class SeedRows {

        private final WalLayout layout;
        private final List<Object[]> rows = new ArrayList<>();

        SeedRows(WalLayout layout) {
            this.layout = layout;
        }

        /**
         * 追加一行 pg_class 种子行。
         *
         * @param row 行（列数须匹配布局投影宽）
         * @return 本追加器（链式）
         */
        SeedRows row(Object[] row) {
            rows.add(row);
            return this;
        }

        /**
         * 收口为脚本化 JDBC 会话。
         *
         * @return 代理会话
         */
        ScriptedJdbc build() {
            return new ScriptedJdbc(layout, rows);
        }
    }

    /**
     * 造 OG 42 列 pg_class 种子行——只填 seedClassRows 消费的判别位（ctid/oid/
     * relname/relfilenode/护栏五列），其余列 0（未消费位不进断言面）。
     *
     * @param oid         关系 oid（测试里 oid == relfilenode）
     * @param relname     关系名
     * @param relfilenode relfilenode
     * @param parttype    'n'=普通表 / 'p'=分区母表
     * @return 42 元素行
     */
    private static Object[] ogClassRow(long oid, String relname, long relfilenode, String parttype) {
        Object[] row = new Object[OG_COLS];
        row[0] = "(0," + (oid - 16380L) + ")";   // ctid（列 1；oid 派生保证互异键）
        row[1] = oid;                             // oid（列 2）
        row[2] = relname;                         // relname（列 3）
        row[8] = relfilenode;                     // relfilenode（列 9）
        row[13] = 0L;                             // reltoastrelid（列 14）
        row[15] = 0L;                             // reldeltarelid（列 16，护栏判别）
        row[17] = 0L;                             // relcudescrelid（列 18，护栏判别）
        row[22] = "r";                            // relkind（列 23）
        row[33] = parttype;                       // parttype（列 34，护栏判别）
        row[36] = null;                           // reloptions（列 37，护栏判别）
        row[39] = null;                           // relbucket（列 40，护栏判别 NULL）
        return row;
    }

    /**
     * 脚本化 JDBC 会话（动态代理本地小样——replay 包对 api 包的 FakeJdbc 不可见）：
     * 按 SQL 文本回放行集，未脚本化方法一经调用即 AssertionError。覆盖面严格等于
     * {@link CatalogBootstrap#bootstrap} 的调用足迹（Connection 的 autoCommit/commit
     * /createStatement + Statement 的 execute/executeQuery + ResultSet 的游标取值与
     * wasNull）。线程约束：单线程顺序使用（bootstrap 本就装配线程单次调用）。
     */
    private static final class ScriptedJdbc {

        private final Map<String, List<Object[]>> scripted = new HashMap<>();

        /**
         * 构造即登记 bootstrap 的公共脚本（位点函数版本感知 + 三 relfilenode 回填 +
         * pg_attribute/pg_namespace 空种子）与 pg_class 种子行集。
         *
         * @param layout     版本布局（位点函数与 pg_class 种子 SQL 文本的分派键）
         * @param classRows  pg_class 种子行集
         */
        ScriptedJdbc(WalLayout layout, List<Object[]> classRows) {
            scripted.put(CatalogBootstrap.flushLsnSql(layout), List.<Object[]>of(new Object[] {"0/0"}));
            scripted.put("SELECT pg_relation_filenode('pg_attribute'::regclass)", List.<Object[]>of(new Object[] {1249L}));
            scripted.put("SELECT pg_relation_filenode('pg_class'::regclass)", List.<Object[]>of(new Object[] {1259L}));
            scripted.put("SELECT pg_relation_filenode('pg_namespace'::regclass)", List.<Object[]>of(new Object[] {2615L}));
            scripted.put("SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                    + " FROM pg_attribute WHERE attnum > 0", List.of());
            scripted.put(CatalogBootstrap.classSeedSql(layout), classRows);
            scripted.put("SELECT ctid::text, oid, nspname FROM pg_namespace", List.of());
        }

        /**
         * 造 Connection 代理。
         *
         * @return 代理连接
         */
        Connection connection() {
            return proxy(Connection.class, (p, method, args) -> switch (method.getName()) {
                case "getAutoCommit" -> Boolean.TRUE;
                case "setAutoCommit", "commit", "close" -> null;
                case "createStatement" -> statement();
                case "prepareStatement" -> emptyPreparedStatement();
                default -> throw new AssertionError("Connection." + method.getName() + " 未脚本化");
            });
        }

        /**
         * Statement 代理：execute（SET TRANSACTION 静默真）、executeQuery 按脚本回放。
         */
        private Statement statement() {
            return proxy(Statement.class, (p, method, args) -> switch (method.getName()) {
                case "execute" -> Boolean.TRUE;
                case "executeQuery" -> resultSet((String) args[0]);
                case "close" -> null;
                default -> throw new AssertionError("Statement." + method.getName() + " 未脚本化");
            });
        }

        /**
         * PreparedStatement 代理：locateTracked 的 interest oid 面在测试快照里恒空
         * 集合——语句创建须应答、查询永不发起（executeQuery 即 AssertionError）。
         */
        private java.sql.PreparedStatement emptyPreparedStatement() {
            return proxy(java.sql.PreparedStatement.class, (p, method, args) -> switch (method.getName()) {
                case "setLong" -> null;
                case "close" -> null;
                default -> throw new AssertionError("PreparedStatement." + method.getName() + " 未脚本化");
            });
        }

        /**
         * ResultSet 代理：游标推进 + 类型化取值；wasNull 按最近一次取值列的 null 性
         * 应答（relbucket NULL 判别的输入面）。
         */
        private ResultSet resultSet(String sql) {
            Iterator<Object[]> rows = scripted.getOrDefault(sql, List.of()).iterator();
            Object[][] current = new Object[][] {null};
            boolean[] lastNull = new boolean[] {true};
            return proxy(ResultSet.class, (p, method, args) -> switch (method.getName()) {
                case "next" -> {
                    current[0] = rows.hasNext() ? rows.next() : null;
                    yield current[0] != null;
                }
                case "getString" -> {
                    Object v = current[0][((Integer) args[0]) - 1];
                    lastNull[0] = v == null;
                    yield v == null ? null : String.valueOf(v);
                }
                case "getLong" -> {
                    Object v = current[0][((Integer) args[0]) - 1];
                    lastNull[0] = v == null;
                    yield v == null ? 0L : Long.parseLong(String.valueOf(v));
                }
                case "wasNull" -> lastNull[0];
                case "close" -> null;
                default -> throw new AssertionError("ResultSet." + method.getName() + " 未脚本化");
            });
        }

        /**
         * 单接口动态代理快捷构造。
         */
        private static <T> T proxy(Class<T> iface, InvocationHandler handler) {
            @SuppressWarnings("unchecked")
            T out = (T) Proxy.newProxyInstance(ScriptedJdbc.class.getClassLoader(), new Class<?>[] {iface}, handler);
            return out;
        }
    }
}
