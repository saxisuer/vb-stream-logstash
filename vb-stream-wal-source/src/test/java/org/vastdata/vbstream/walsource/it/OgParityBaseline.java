package org.vastdata.vbstream.walsource.it;

import org.opengauss.PGConnection;
import org.opengauss.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.changes.ColumnMeta;
import org.vastdata.vbstream.walsource.changes.TableMeta;
import org.vastdata.vbstream.walsource.changes.ToastAssembler;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M2 双路对拍的<strong>逻辑解码基准路</strong>（T6，按 T0 选型 PARITY=LOGICAL
 * 路②）——opengauss-jdbc 5.0.0-og in-process 逻辑流收 OG pgoutput 二进制帧 + 归一化器
 * 对成 v2 事务块对齐面，与 {@code WalSource} DML 面（wal 直解路）按 xid 交集逐行 diff。
 *
 * <p><b>为什么不用 engine 的 {@code PgOutputDecoder}</b>：OG 帧与 vanilla 有两处帧级差异
 * ——B 帧 xid 是 Int64（vanilla Int32）与 TupleData 带 Int16 ncols 前缀（vanilla 列读到
 * 消息尾），归一化器按 spike 文档 {@code docs/wal-source-opengauss-m2-spike.md} §2 的
 * 实测锚定布局<b>独立新写</b>（spike 坑 4：勿共享解码路径）。帧布局（proto_version=1）：</p>
 * <ul>
 *   <li>{@code B}：finalLsn Int64 + commitTs Int64（µs since 2000）+ xid Int64</li>
 *   <li>{@code R}：relId Int32 + ns/relname cstring + replica-identity 字节 + Int16 ncols
 *       + 每列 {flags Int8 + name cstring + typeId Int32 + typmod Int32}（与 vanilla 等同）</li>
 *   <li>{@code I}：relId + {@code 'N'} + TupleData；{@code U}：relId + {@code 'O'|'K'} +
 *       TupleData(old) + {@code 'N'} + TupleData(new)；{@code D}：relId +
 *       {@code 'O'|'K'} + TupleData(old)——<b>FULL 身份发 'O' 整行旧元组、DEFAULT 身份发
 *       'K'（OG 形态：ncols=全列宽、非身份列以 'n' 占位——og-plan 容器 2026-10-12 实测锚，
 *       两种形态归一化器都收）</b></li>
 *   <li>{@code C}：flags Int8 + commitLsn Int64 + endLsn Int64 + ts Int64</li>
 *   <li>TupleData：Int16 ncols + 每列 kind {@code 't'}+Int32 len+文本 / {@code 'n'}=NULL /
 *       {@code 'u'}=unchanged-TOAST 哨兵</li>
 * </ul>
 *
 * <p><b>时序契约（spike 坑 2/3）</b>：publication 必须先于被解码事务存在、
 * {@code publication_names} 槽选项必传——IT 固定时序：建 publication → 建逻辑槽 → 写
 * DML → {@link #capture} 起流收帧（槽建于 DML 前，起流晚于提交也完整收达）。</p>
 *
 * <p><b>驱动坑位（spike §3 坑 5）</b>：{@code stream.close()} 恒抛 EOF 竞态异常（吞掉）；
 * {@code readPending()} 不暴露服务端断流——drain 循环空闲期周期
 * {@code forceUpdateStatus()}（连接死亡即 SQLException 上抛，不以"流恒存活"为前提）。
 * DDL/TRUNCATE/两阶段帧不在 M2 对拍面——到达即显式拒绝（不静默吞）。</p>
 */
final class OgParityBaseline {

    private static final Logger LOG = LoggerFactory.getLogger(OgParityBaseline.class);

    /** 基准路复制连接的 application_name（逻辑槽清理面按名匹配 walsender pid）。 */
    private static final String APP_NAME = "og-m2-parity-baseline";

    /** 空闲期活性探测间隔（毫秒）——forceUpdateStatus 周期（spike 坑 5 防御位）。 */
    private static final long LIVENESS_PROBE_MS = 1_000;

    /** drain 轮询间隔（毫秒，readPending 返回 null 的空轮睡眠）。 */
    private static final long POLL_MS = 20;

    /** 反馈 statusInterval（秒，流构造参数——与服务端保活节奏同量级）。 */
    private static final int STATUS_INTERVAL_SEC = 5;

    /** 值渲染截断阈值（对齐 {@code OutputRenderer}/engine ConsoleRenderer 的 64 字符规则）。 */
    private static final int VALUE_TRUNCATE_LIMIT = 64;

    /** 私有构造器：静态工具面，不可实例。 */
    private OgParityBaseline() {
    }

    /**
     * 建逻辑槽（pgoutput 插件，建在 m2pg 库）——须在写被解码 DML 之前调用（槽只从建槽
     * 位点起解码）；publication 须更早存在（历史快照语义，spike 坑 2）。
     *
     * @param slotName 槽名（测试前缀，避开 M1 物理槽清理面）
     * @throws SQLException 建槽失败（如重名残留）
     */
    static void createLogicalSlot(String slotName) throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT * FROM pg_create_logical_replication_slot(?, 'pgoutput')")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                // 返回 record 的列名与 vanilla 不同（spike §0）——只消费游标不取值
                assertTrue(rs.next(), "pg_create_logical_replication_slot 应返回一行");
            }
        }
    }

    /**
     * 静默删逻辑槽（先杀本端基准路 walsender 再删；对齐
     * {@code OgTestEnv.dropPhysicalSlotQuietly} 的形态——OG 无 active_pid 列，pid 从
     * {@code pg_stat_replication} 按本路 application_name 前缀取、先物化再按值 terminate
     * 防视图谓词下推自杀）。槽不存在等情况仅 WARN——测试清理路径不得掩盖断言失败。
     *
     * @param slotName 槽名
     */
    static void dropLogicalSlotQuietly(String slotName) {
        try (Connection c = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG)) {
            List<Long> pids = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT pid FROM pg_stat_replication WHERE application_name LIKE '%"
                                 + APP_NAME + "%'")) {
                while (rs.next()) {
                    pids.add(rs.getLong(1));
                }
            }
            for (long pid : pids) {
                try (Statement st = c.createStatement()) {
                    st.execute("SELECT pg_terminate_backend(" + pid + ")");
                }
            }
            Thread.sleep(200);   // walsender 退出竞态：立即 drop 会报 slot is active
            try (PreparedStatement ps = c.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                LOG.debug("OG 逻辑槽 {} 不存在——前置清理空转", slotName);
            } else {
                LOG.warn("清理 OG 逻辑槽 {} 失败: {}", slotName, e.getMessage());
            }
        }
    }

    /**
     * 起一次基准路逻辑流并收帧至期望事务数（T7 复用的主入口）：opengauss-jdbc 复制连接
     * （{@code replication=database&assumeMinServerVersion=9.4}，spike §1 路② 实测锚；
     * 认证走 {@link OgTestEnv#PARITY_ROLE} 的 hba sha256 行——码 11 混合挑战是驱动的
     * 实锚死角，见 {@code OgTestEnv.ensureParityBaselineRole}）→ {@code logical()} 槽选项
     * proto_version=1 + publication_names → readPending drain 至收到 expectedTxns 个
     * C 帧（或超时 fail）→ 关流（EOF 竞态异常吞掉）→ 关连接。
     *
     * <p>关键步骤：帧逐条喂 {@link Normalizer}（B/R/I/U/D/C → 事务块）；空轮睡眠 +
     * 周期 {@code forceUpdateStatus()} 活性护栏（连接死亡即 SQLException 上抛而非挂死）。
     * 边界与异常语义：超时抛 AssertionError（附已收块数与诊断）；未知帧类型/孤儿行/未闭合
     * 块抛 IllegalStateException（格式漂移显式拒绝，不静默吞）。线程约束：调用线程单次
     * 执行（对拍 IT 的主线程）。</p>
     *
     * @param slotName      逻辑槽名（{@link #createLogicalSlot} 所建）
     * @param publication   publication 名（槽选项必传——spike 坑 3）
     * @param expectedTxns  期望收到的已提交事务数（C 帧计数）
     * @param timeoutMs     drain 上限（毫秒）
     * @return 归一化事务块（出现序；行文本与 wal 直解路渲染面对齐）
     * @throws SQLException 连接/起流/活性探测失败
     */
    static List<OgBlock> capture(String slotName, String publication, long expectedTxns, long timeoutMs)
            throws SQLException {
        String url = OgTestEnv.sqlJdbcUrl(OgTestEnv.DATABASE_M2PG)
                + "?replication=database&assumeMinServerVersion=9.4&ApplicationName=" + APP_NAME;
        Normalizer n = new Normalizer();
        try (Connection c = DriverManager.getConnection(url, OgTestEnv.PARITY_ROLE,
                OgTestEnv.PARITY_ROLE_PASSWORD)) {
            PGReplicationStream stream = c.unwrap(PGConnection.class).getReplicationAPI()
                    .replicationStream().logical()
                    .withSlotName(slotName)
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", publication)
                    .withStatusInterval(STATUS_INTERVAL_SEC, TimeUnit.SECONDS)
                    .start();
            long deadline = System.currentTimeMillis() + timeoutMs;
            long lastProbe = System.currentTimeMillis();
            try {
                while (n.committedTxns() < expectedTxns) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("基准路 drain 超时（" + timeoutMs + "ms）：期望 "
                                + expectedTxns + " 个提交事务，实收 " + n.committedTxns()
                                + "（已收块 xid=" + n.blocks().stream().map(b -> b.xid).toList() + "）");
                    }
                    ByteBuffer msg = stream.readPending();
                    if (msg != null) {
                        n.frame(msg);
                    } else {
                        long now = System.currentTimeMillis();
                        if (now - lastProbe >= LIVENESS_PROBE_MS) {
                            stream.forceUpdateStatus();   // 活性护栏：断流即 SQLException 而非恒 null 挂死
                            lastProbe = now;
                        }
                        Thread.sleep(POLL_MS);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("基准路 drain 被中断", e);
            } finally {
                closeStreamQuietly(stream);
            }
        }
        return n.blocks();
    }

    /**
     * 关基准路逻辑流（spike 坑 5：{@code close()} 恒抛 "Database connection failed when
     * ending copy" 的 EOF 竞态异常——服务端在 close 时刻正常收尾，异常属驱动侧收尾竞态，
     * 吞掉记 DEBUG）。
     *
     * @param stream 已起或已死的流
     */
    private static void closeStreamQuietly(PGReplicationStream stream) {
        try {
            stream.close();
        } catch (SQLException e) {
            LOG.debug("基准路流 close 的 EOF 竞态异常（吞掉，spike 坑 5 实测锚）: {}", e.getMessage());
        }
    }

    // ---- 归一化器：OG pgoutput 帧 → 事务块 ----

    /**
     * OG pgoutput 二进制帧的归一化状态机（spike §2 映射表的实现）：B 开块（xid 取 Int64
     * 整值）、R 进本地 relId → 表字典缓存（对应 wal 直解路的 as-of 字典角色）、I/U/D 渲染
     * 行（值取 't' 文本、'n'→NULL/'K' 段缺席、'u'→unchanged-TOAST 哨兵——与
     * {@code OutputRenderer} 的行格式逐字面同构）、C 闭块（commitLsn 取第一 LSN 字段）。
     * abort 事务不发帧（proto v1 语义），与 wal 路过滤 aborted 一致。
     */
    private static final class Normalizer {

        private final List<OgBlock> done = new ArrayList<>();
        private final Map<Integer, Relation> relations = new LinkedHashMap<>();
        private Pending cur;

        /**
         * 单帧分发：类型字节路由到 B/R/I/U/D/C 六个已锚定分支，其余（'T' truncate、
         * 两阶段族、未知）抛 ISE 显式拒绝——M2 对拍面外形态静默吞会产出缺行假绿。
         *
         * @param msg 一条完整帧的载荷（含类型字节，opengauss-jdbc CopyData 形态）
         */
        void frame(ByteBuffer msg) {
            ByteBuffer b = msg.duplicate();
            byte type = b.get();
            switch (type) {
                case 'B' -> {
                    assertTrue(cur == null, "B 帧嵌套（前一事务未闭合）");
                    long finalLsn = b.getLong();
                    Instant commitTs = instantOfPgMicros(b.getLong());
                    long xid = b.getLong();
                    cur = new Pending(xid, finalLsn, commitTs);
                }
                case 'R' -> parseRelation(b);
                case 'I' -> {
                    int relId = b.getInt();
                    expectSectionKind(b, 'N', "INSERT");
                    cur.rows.add(renderRow("INSERT", relations.get(relId), parseTuple(b, "N")));
                }
                case 'U' -> {
                    int relId = b.getInt();
                    char oldKind = readOldSectionKind(b, "UPDATE");
                    List<TupCol> old = parseTuple(b, String.valueOf(oldKind));
                    expectSectionKind(b, 'N', "UPDATE");
                    List<TupCol> neu = parseTuple(b, "N");
                    cur.rows.add(renderRow("UPDATE", relations.get(relId), oldKind, old, neu));
                }
                case 'D' -> {
                    int relId = b.getInt();
                    char oldKind = readOldSectionKind(b, "DELETE");
                    cur.rows.add(renderRow("DELETE", relations.get(relId), oldKind,
                            parseTuple(b, String.valueOf(oldKind)), null));
                }
                case 'C' -> {
                    assertTrue(cur != null, "C 帧先于任何 B 帧（孤儿提交）");
                    b.get();   // flags Int8（0——proto v1 无两阶段标志位消费面）
                    long commitLsn = b.getLong();
                    // endLsn（第二 LSN 字段）与 ts（尾 Int64）不在 v2 头行对齐面，跳过
                    done.add(cur.closeBaseline(commitLsn));
                    cur = null;
                }
                default -> throw new IllegalStateException("基准路收到 M2 对拍面外的帧类型 '"
                        + (char) type + "'（DDL/TRUNCATE/两阶段族归 T7+，未知类型显式拒绝）: "
                        + hexPrefix(msg));
            }
        }

        /** @return 已闭合事务块（出现序的只读拷贝）。 */
        List<OgBlock> blocks() {
            return List.copyOf(done);
        }

        /** @return 已收 C 帧数（提交事务计数）。 */
        long committedTxns() {
            return done.size();
        }

        /**
         * 'R' 帧解析：relId → 表身份（ns.relname + 列名序列）入本地缓存。同 relId 重发
         * （DDL 后新版本）后写覆盖——M2 场景无此形态，覆盖语义留给 T7 的 as-of 场景记档。
         */
        private void parseRelation(ByteBuffer b) {
            int relId = b.getInt();
            String ns = cstring(b);
            String rel = cstring(b);
            b.get();   // replica-identity 字节（'d'/'f'——渲染面不用，'K'/'O' 已携带实际身份）
            int ncols = b.getShort();
            List<String> cols = new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                b.get();   // 列 flags Int8（1=复制身份列——渲染按 'K' 段实际形态走，不消费）
                cols.add(cstring(b));
                b.getInt();   // typeId
                b.getInt();   // typmod
            }
            relations.put(relId, new Relation(ns + "." + rel, cols));
        }

        /**
         * TupleData 解析（OG 形态：Int16 ncols 前缀 + 逐列 kind）。
         *
         * @param b            游标（位于 ncols 前缀）
         * @param sectionKind  所在段类型（"N"/"O"/"K"——'K' 段的 'n' 语义是缺席占位）
         * @return 逐列解析值（保持列序）
         */
        private static List<TupCol> parseTuple(ByteBuffer b, String sectionKind) {
            int ncols = b.getShort();
            List<TupCol> out = new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                byte kind = b.get();
                switch (kind) {
                    case 't' -> {
                        int len = b.getInt();
                        byte[] raw = new byte[len];
                        b.get(raw);
                        out.add(new TupCol('t', new String(raw, StandardCharsets.UTF_8)));
                    }
                    case 'n' -> out.add(new TupCol('n', null));
                    case 'u' -> out.add(new TupCol('u', null));
                    default -> throw new IllegalStateException("TupleData 未知列 kind '" + (char) kind
                            + "'（段 " + sectionKind + "）——帧布局漂移，显式拒绝");
                }
            }
            return out;
        }

        /**
         * INSERT 行渲染（before 恒无像）。
         *
         * @param dml   行操作名
         * @param rel   表身份（relId 解析；null 即 ISE——R 帧缺位属帧序破坏）
         * @param after 新元组列值
         */
        private static String renderRow(String dml, Relation rel, List<TupCol> after) {
            return renderRow(dml, rel, (char) 0, null, after);
        }

        /**
         * U/D 行渲染（旧元组段参数化）：行文本 {@code DML ns.rel BEFORE=.. AFTER=..} 与
         * {@code OutputRenderer.renderChange} 逐字面同构——'K' 段缺席列（OG 的 'n' 占位
         * 形态）不入像、'O'/'N' 段 'n' 渲 NULL、'u' 渲 unchanged-TOAST 哨兵、't' 超 64
         * 字符截断附原长。
         *
         * @param dml       行操作名（INSERT/UPDATE/DELETE）
         * @param rel       表身份
         * @param oldKind   旧元组段类型（'K'/'O'；INSERT 传 0 = 无旧段）
         * @param old       旧元组列值（null = 无旧段）
         * @param neu       新元组列值（DELETE 传 null）
         */
        private static String renderRow(String dml, Relation rel, char oldKind, List<TupCol> old,
                List<TupCol> neu) {
            assertTrue(rel != null, "行帧先于关系 R 帧（relId 未解析）——帧序破坏");
            return "%s %s BEFORE=%s AFTER=%s".formatted(dml, rel.qualified,
                    old == null ? "-" : tupleOf(old, rel, oldKind == 'K'),
                    neu == null ? "-" : tupleOf(neu, rel, false));
        }

        /**
         * 元组渲染（对齐 {@code OutputRenderer.tupleOf}）：按关系列序，'K' 段跳过 'n'
         * 占位列（缺席语义）、其余段 'n' 渲 NULL 字面；值截断同 64 字符规则。
         *
         * @param cols       逐列解析值
         * @param rel        表身份（列序权威）
         * @param skipAbsent 'K' 段形态（'n' = 缺席占位而非 NULL）
         * @return tuple 文本
         */
        private static String tupleOf(List<TupCol> cols, Relation rel, boolean skipAbsent) {
            List<String> parts = new ArrayList<>();
            for (int i = 0; i < rel.columns.size() && i < cols.size(); i++) {
                TupCol col = cols.get(i);
                if (skipAbsent && col.kind == 'n') {
                    continue;
                }
                parts.add(rel.columns.get(i) + "=" + valueOf(col));
            }
            return parts.toString();
        }

        /**
         * 单列值文本：'n'→NULL 字面、'u'→{@link ToastAssembler#UNCHANGED_TOAST_TEXT}
         * （与 wal 直解路的哨兵渲染同形）、't'→文本截断。
         *
         * @param col 解析列值
         * @return 列值文本
         */
        private static String valueOf(TupCol col) {
            if (col.kind == 'n') {
                return "NULL";
            }
            if (col.kind == 'u') {
                return ToastAssembler.UNCHANGED_TOAST_TEXT;
            }
            String s = col.text;
            return s.length() > VALUE_TRUNCATE_LIMIT
                    ? s.substring(0, VALUE_TRUNCATE_LIMIT) + "...(" + s.length() + "B)"
                    : s;
        }

        /**
         * 读旧元组段类型字节并校验（U/D 的首段——期望 'O'（FULL 身份整行旧元组）或
         * 'K'（DEFAULT 身份键段，OG 以 ncols=全列宽 + 'n' 占位形态发，og-plan 容器
         * 2026-10-12 双形态实测锚）；其他字节即帧布局漂移，显式拒绝）。
         *
         * @param b    游标（位于段类型字节）
         * @param what 上下文（失败消息）
         * @return 段类型（'O'/'K'）
         */
        private static char readOldSectionKind(ByteBuffer b, String what) {
            char kind = (char) b.get();
            assertTrue(kind == 'O' || kind == 'K', what + " 旧元组段类型字节漂移: 期望 'O'|'K' 实得 '"
                    + kind + "'");
            return kind;
        }

        /**
         * 校验段类型字节（I/U 的 'N' 段与 U 的第二段——期望形态外即帧布局漂移）。
         *
         * @param b      游标（位于段类型字节）
         * @param expect 期望字节
         * @param what   上下文（失败消息）
         */
        private static void expectSectionKind(ByteBuffer b, char expect, String what) {
            char actual = (char) b.get();
            assertTrue(actual == expect, what + " 段类型字节漂移: 期望 '" + expect + "' 实得 '" + actual + "'");
        }

        /** cstring 读取（NUL 终止 UTF-8）。 */
        private static String cstring(ByteBuffer b) {
            int start = b.position();
            while (b.get() != 0) {
                // 走到 NUL
            }
            byte[] raw = new byte[b.position() - 1 - start];
            b.position(start);
            b.get(raw);
            b.get();   // 越过 NUL
            return new String(raw, StandardCharsets.UTF_8);
        }

        /** 帧首 16 字节 hex（诊断失败消息用）。 */
        private static String hexPrefix(ByteBuffer msg) {
            ByteBuffer b = msg.duplicate();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 16 && b.hasRemaining(); i++) {
                sb.append(String.format("%02x ", b.get()));
            }
            return sb.toString().trim();
        }
    }

    /**
     * PG 微秒时间戳（epoch 2000）→ Instant（floorDiv/floorMod 保负值正确——与
     * {@code XactGrouper.instantOfPgMicros} 同源换算，双路 commitTs 对齐的前提）。
     *
     * @param micros epoch-2000 微秒
     * @return Instant
     */
    private static Instant instantOfPgMicros(long micros) {
        long epochMicros = micros + 946_684_800_000_000L;
        return Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * 1_000L);
    }

    // ---- wal 直解路捕获面 ----

    /**
     * wal 直解路的离线捕获 listener（M2 T6：对拍捕获不经 {@code OutputRenderer} 的
     * logger 面——事件面直接攒块）：行文本渲染规则与 {@code OutputRenderer} 同构
     * （值截 64 附原长/NULL 字面/列序权威 TableMeta/BEFORE·AFTER 缺位 {@code -}），
     * 但攒进内存列表而非 CDC logger。
     *
     * <p>线程约束：单写者（wal-receiver 线程回调）；{@link #blocks()} 在 WalSource
     * close（含接收线程 join）之后调用——happens-before 经线程结束建立。</p>
     */
    static final class WalCapture implements ChangeOutputListener {

        private final List<OgBlock> done = new ArrayList<>();
        private Pending cur;

        /**
         * 批量回放开始：开待决桶缓存头字段（commitLsn/commitTs 双路对齐面；kind/gid
         * 不入块——基准路 proto v1 恒 NORMAL/null，两阶段对拍是 T7+ 面记档）。
         *
         * @param begin 事务头事件
         */
        @Override
        public void onBegin(BatchBegin begin) {
            cur = new Pending(begin.xid(), begin.commitLsn(), begin.commitTs());
        }

        /**
         * 单行变更：渲染行文本攒进当前桶（渲染面镜像 {@code OutputRenderer.renderChange}
         * ——契约源 engine ConsoleRenderer，见其 javadoc）。
         *
         * @param row 行变更事件
         */
        @Override
        public void onRow(RowChange row) {
            assertTrue(cur != null, "onRow 先于 onBegin（契约违背）");
            cur.rows.add("%s %s.%s BEFORE=%s AFTER=%s".formatted(row.dml(),
                    row.table().schema(), row.table().table(),
                    tupleOf(row.before(), row.table()), tupleOf(row.after(), row.table())));
        }

        /**
         * 批量回放结束：闭桶（changes 取 {@code BatchEnd.expectedChanges} 记账终值——
         * 头行语义与 DualPathParityIT 的 wal 路记账面一致，对拍归一化按 aborted 差吸收）。
         *
         * @param end 事务尾事件
         */
        @Override
        public void onEnd(BatchEnd end) {
            assertTrue(cur != null && cur.xid == end.xid(), "onEnd 与待决桶 xid 不符");
            done.add(cur.closeWal(end.expectedChanges()));
            cur = null;
        }

        /**
         * 回滚事务：丢弃整桶（零输出——与基准路 proto v1 的"abort 不发帧"天然对齐）。
         *
         * @param aborted 回滚事件
         */
        @Override
        public void onAborted(BatchAborted aborted) {
            if (cur != null && cur.xid == aborted.xid()) {
                cur = null;
            }
        }

        /**
         * 已闭合块快照（须在 WalSource close 之后调用——线程 join 后读无并发追加）。
         *
         * @return 事务块（出现序）
         */
        List<OgBlock> blocks() {
            return List.copyOf(done);
        }

        /**
         * 元组渲染（镜像 {@code OutputRenderer.tupleOf}）：按 TableMeta 列序渲 map 中
         * 存在的列，dropped ∅ / null NULL / 值截 64 附原长。
         *
         * @param tuple 列名→渲染值 map（null = 无像 → "-"）
         * @param table 表身份（列序权威）
         * @return tuple 文本或 "-"
         */
        private static String tupleOf(Map<String, Object> tuple, TableMeta table) {
            if (tuple == null) {
                return "-";
            }
            List<String> parts = new ArrayList<>();
            for (ColumnMeta col : table.columns()) {
                if (!tuple.containsKey(col.name())) {
                    continue;
                }
                Object v = tuple.get(col.name());
                if (col.dropped()) {
                    parts.add(col.name() + "=∅");
                } else if (v == null) {
                    parts.add(col.name() + "=NULL");
                } else {
                    String s = v.toString();
                    parts.add(col.name() + "=" + (s.length() > VALUE_TRUNCATE_LIMIT
                            ? s.substring(0, VALUE_TRUNCATE_LIMIT) + "...(" + s.length() + "B)" : s));
                }
            }
            return parts.toString();
        }
    }

    // ---- 对拍归一化 ----

    /**
     * 归一化对拍（对齐 {@code DualPathParityIT.assertParity} 形态）：两路块各自按 xid
     * 建模（重复块全等容忍——wal 路 at-least-once 重发面），求交集后断言：交集非空 +
     * 交集内行文本序列、commitLsn、commitTs（Instant）逐项相等 + changes 语义归一化
     * （基准路无记账面恒 = 行数；wal 路记账 = 行数 + 场景 aborted 数）。交集外事务仅
     * INFO 留痕（两路起停窗口差异属固有，不作断言失败）。
     *
     * @param baseline  基准路块（{@link #capture}）
     * @param wal       wal 直解路块（{@link WalCapture#blocks()}）
     * @param scenario  场景名（失败消息上下文）
     * @param walAborted 场景内被回滚的子事务行数（wal 记账与实付的期望差）
     */
    static void assertParity(List<OgBlock> baseline, List<OgBlock> wal, String scenario, long walAborted) {
        Map<Long, OgBlock> base = byXid(baseline, scenario + " 基准路");
        Map<Long, OgBlock> w = byXid(wal, scenario + " wal 路");
        Set<Long> intersection = new LinkedHashSet<>(base.keySet());
        intersection.retainAll(w.keySet());
        assertFalse(intersection.isEmpty(), () -> scenario + ": 对拍交集为空——基准路 xids=" + base.keySet()
                + ", wal 路 xids=" + w.keySet());

        Set<Long> baseOnly = new LinkedHashSet<>(base.keySet());
        baseOnly.removeAll(w.keySet());
        Set<Long> walOnly = new LinkedHashSet<>(w.keySet());
        walOnly.removeAll(base.keySet());
        if (!baseOnly.isEmpty() || !walOnly.isEmpty()) {
            LOG.info("对拍窗口差（{}）: 基准路独有 xid={}, wal 路独有 xid={}——起停窗口差异属两路固有，"
                    + "交集内已逐行对齐", scenario, baseOnly, walOnly);
        }

        for (Long xid : intersection) {
            OgBlock b = base.get(xid);
            OgBlock v = w.get(xid);
            assertEquals(b.rows, v.rows, () -> scenario + ": xid=" + xid + " 行序列 diff（基准路="
                    + b.rows + ", wal 路=" + v.rows + "）");
            assertEquals(b.commitLsn, v.commitLsn, () -> scenario + ": xid=" + xid + " commitLsn diff（基准路 "
                    + Lsn.format(b.commitLsn) + ", wal 路 " + Lsn.format(v.commitLsn) + "）");
            assertEquals(b.commitTs, v.commitTs, () -> scenario + ": xid=" + xid + " commitTs diff（基准路 "
                    + b.commitTs + ", wal 路 " + v.commitTs + "）");
            assertEquals(b.rows.size(), (int) b.changes, () -> scenario + ": xid=" + xid
                    + " 基准路 changes 应等于行数（B 帧无记账面，按实收计）");
            assertEquals(v.rows.size() + walAborted, (int) v.changes, () -> scenario + ": xid=" + xid
                    + " wal 路 changes 记账异常: 期望=实付 " + v.rows.size() + " + 场景 aborted "
                    + walAborted + "，实得 " + v.changes);
        }
    }

    /**
     * 块列表 → xid 键控 map（出现序）：同 xid 重复块（wal 路 at-least-once 重发）逐字段
     * 全等则丢弃、不等即 fail（重发非幂等即语义破坏信号——对齐 DualPathParityIT.parse
     * 的重复块容忍面）。
     *
     * @param blocks 块列表
     * @param side   路径名（失败消息上下文）
     * @return xid → 块（保持出现序）
     */
    private static Map<Long, OgBlock> byXid(List<OgBlock> blocks, String side) {
        Map<Long, OgBlock> out = new LinkedHashMap<>();
        for (OgBlock blk : blocks) {
            OgBlock existing = out.get(blk.xid);
            if (existing == null) {
                out.put(blk.xid, blk);
            } else {
                assertEquals(existing, blk, () -> side + " 重发块不等（重发须逐字段全等）: xid=" + blk.xid);
            }
        }
        assertFalse(out.isEmpty(), side + " 捕获无任何事务块: " + blocks);
        return out;
    }

    // ---- 模型与辅助 record ----

    /**
     * 对齐面事务块：基准路与 wal 直解路共用的比较单元——xid / commitLsn / commitTs /
     * 行文本序列 / changes 记账（基准路 = 行数；wal 路 = {@code BatchEnd.expectedChanges}）。
     * kind/gid 不入模型（基准路 proto v1 恒 NORMAL/null——两阶段对齐面是 T7+ 记档项）。
     *
     * @param xid       事务 id
     * @param commitLsn 提交记录 LSN
     * @param commitTs  提交时间戳
     * @param rows      行文本序列（两路渲染规则同构，逐字面可比）
     * @param changes   记账变更数（语义按路别，见上）
     */
    record OgBlock(long xid, long commitLsn, Instant commitTs, List<String> rows, long changes) {
    }

    /** 归一化器的关系缓存条目：限定名 + 列名序列（列序权威）。 */
    private record Relation(String qualified, List<String> columns) {
    }

    /** TupleData 单列解析值：kind（'t'/'n'/'u'）+ 文本（仅 't' 有值）。 */
    private record TupCol(char kind, String text) {
    }

    /** 待决桶：头字段缓存 + 攒行；C 帧/End 闭块（changes 按路别注入）。 */
    private static final class Pending {

        final long xid;
        final long commitLsn;
        final Instant commitTs;
        final List<String> rows = new ArrayList<>();

        Pending(long xid, long commitLsn, Instant commitTs) {
            this.xid = xid;
            this.commitLsn = commitLsn;
            this.commitTs = commitTs;
        }

        /**
         * 闭块（基准路档）：以 C 帧 commitLsn 为权威（与 B 帧 finalLsn 同值、与 wal 路
         * 提交记录 lsn 同源），changes 按实收行数计（B 帧无记账面——spike §2 映射表）。
         *
         * @param commitLsnOfCommit C 帧第一 LSN 字段
         * @return 已闭合块
         */
        OgBlock closeBaseline(long commitLsnOfCommit) {
            return new OgBlock(xid, commitLsnOfCommit, commitTs, List.copyOf(rows), rows.size());
        }

        /**
         * 闭块（wal 路档）：记账终值由 {@code BatchEnd.expectedChanges} 携带（对拍
         * 归一化按 aborted 差吸收——见 {@code OgParityBaseline.assertParity}）。
         *
         * @param expectedChanges BatchEnd.expectedChanges 记账终值
         * @return 已闭合块
         */
        OgBlock closeWal(long expectedChanges) {
            return new OgBlock(xid, commitLsn, commitTs, List.copyOf(rows), expectedChanges);
        }
    }
}
