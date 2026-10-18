package org.vastdata.vbstream.walsource.it.vastbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.it.OgParityBaseline;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.BaselineEndpoint;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.WalCapture;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * VastBase B 档 DML 专形对拍 IT（M4 Task 8，手动档 {@code -Dvb.it.vastbase=true}）：连
 * {@link VastBEnv}（172.16.107.133:55443/test_b，B 兼容档——MySQL 风格）验收四场景：
 * ①13 类型全矩阵（双路对拍收 server-text 同形 9 型 + blob 族 4 型 wal 路字面锚）；
 * ②'' 保留与 datetime 微秒截断语义面；③varlena 6B 头混合（多列/NULL/TOAST 宽值）；
 * ④UPDATE/DELETE 前像（uint8 无符号/year/blob 的新类型前像渲染，REPLICA IDENTITY
 * FULL）。基准路 = opengauss-jdbc 逻辑流 + {@link OgParityBaseline} 归一化（M3 先例：
 * A 档 pgoutput 帧面无分叉，B 档同预期——实跑证实）；wal 路 = {@link WalSource} DML 面
 * （direct 通道，M4 Task 6 护栏白名单 {'PG','A','B'} 已放行 B 档）。
 *
 * <p><b>blob 族四 oid（88/95/9779/17757）不进双路对拍——渲染有意分叉（Task 4 记档）：
 * wal 路 {@code DiskValueRenderer} 渲 {@code \x}+小写 hex，服务端 text 输出是裸字节/
 * MySQL 风格 hex 前缀混形（B 档实查锚：blob 裸 {@code DEADBEEF}、tinyblob
 * {@code \x00ff10}、binary {@code \xaabbccdd}、varbinary {@code 0x11223344}）——同一字节的
 * 多种方言形态，走 wal 路字面断言；raw（86，大写 hex 无前缀）与服务端同形参与对拍。
 * 对拍场景①因此拆两表：t_types（9 型，进 publication）+ t_blob（blob 族，仅 wal 白名单
 * ——基准路零帧到达，wal 路独立事务发射，xid 交集对拍天然吸收）。</p>
 *
 * <p><b>B 档语义锚（spike + Task 8 实跑前置探针）</b>：'' 保留空串（vs A 档 NULL）；
 * datetime typmod=0 微秒输入服务端截断（存整秒）；{@code decode('..','hex')} 与 brief 的
 * 13 类型 DDL 全谱在 B 档原样可用；wal_level=logical + pgoutput 逻辑槽/publication 建删可用。</p>
 *
 * <p><b>真库卫生（长活共享环境纪律）</b>：schema/两路槽/publication 一律 {@code vb_wal_b}
 * 前缀（{@code vb_wal_bm} / {@code vb_wal_bm_logic_<suffix>} / {@code vb_wal_bm_wal_<suffix>}
 * / {@code vb_wal_bm_pub}——与 A 档 {@code vb_wal_par_*} 及既有业务对象隔离），每用例
 * finally 兜底清理；publication 显式 {@code FOR TABLE} 场景表白名单（窗口钉场景表，背景
 * 业务活动零帧到达）；wal 路 {@code vb.wal.tables} 白名单恒钉场景表。表恒
 * {@code REPLICA IDENTITY FULL}（DEFAULT 身份 'K' 段占位分叉沿 M2 惯例规避）。</p>
 *
 * <p><b>连接纪律（test_b 握手抖动）</b>：单持久连接干完 DDL/DML/查询 + 建连重试环
 * （{@link VastBEnv} 全连接面）；wal 路门面启动经 {@link VastTestEnv#startOnlineWithRetry}
 * 重试环。需真库 172.16.107.133:55443 可达（连接参数经 {@code -Dvb.it.vastb.*} 可覆盖）。</p>
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastBModeIT {

    private static final Logger LOG = LoggerFactory.getLogger(VastBModeIT.class);

    /** 对拍 schema（vb_wal_b 前缀——reset 以 schema CASCADE 整体清场）。 */
    private static final String SCHEMA = "vb_wal_bm";

    /** publication 名（每场景重建——显式 FOR TABLE 场景表白名单，见类 javadoc 卫生段）。 */
    private static final String PUBLICATION = "vb_wal_bm_pub";

    /** 基准路复制连接的 application_name（walsender 清理面按名匹配——与 wal 路 vb-wal-source 区分）。 */
    private static final String BASELINE_APP = "vb-wal-bm-parity-base";

    /** 两路各自的追平等待上限（毫秒）——含 wal 路引导余量（真库跨网档）。 */
    private static final long AWAIT_MS = 60_000;

    /** 轮询间隔（毫秒）。 */
    private static final long POLL_MS = 200;

    /** 基准路 capture 的 drain 上限（毫秒）。 */
    private static final long CAPTURE_MS = 60_000;

    /** 当前用例的两路槽名（AfterEach 兜底清理——正常路径测试内已完成）。 */
    private String logicalSlot;

    private String physicalSlot;

    /** 当前用例的 WalSource（AfterEach 兜底关闭）。 */
    private WalSource wal;

    // ---- 场景 ①：13 类型全矩阵（双路对拍 9 型 + blob 族 wal 路字面锚） ----

    /**
     * 场景①：13 类型全矩阵。双路对拍腿（t_types 九型 server-text 同形：int1 ±边界 /
     * uint1/uint2/uint4/uint8 边界值含 u64 顶值 / year / clob 中文 / nvarchar2 中文 /
     * raw 大写 hex）——行 1 边界值 → UPDATE（int1 负边界 + year 变更）→ DELETE（行 2
     * 零值 + '' 空串 + NULL 混排）；blob 族腿（t_blob 四型）wal 路字面锚：
     * {@code c_bl=\xdeadbeef}/{@code c_tb=\x00ff10}/{@code c_bi=\xaabbccdd}/
     * {@code c_vb=\x11223344}（渲染有意分叉记档面，见类 javadoc）。
     */
    @Test
    void bModeTypeMatrixParityAndBlobLiterals() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_bm.t_types (id int primary key, c_i1 int1, c_u1 int1 unsigned,"
                        + " c_u2 smallint unsigned, c_u4 int unsigned, c_u8 bigint unsigned, c_yr year,"
                        + " c_cl clob, c_nv nvarchar2(32), c_rw raw(16))",
                "ALTER TABLE vb_wal_bm.t_types REPLICA IDENTITY FULL",
                "CREATE TABLE vb_wal_bm.t_blob (id int primary key, c_bl blob, c_tb tinyblob,"
                        + " c_bi binary(4), c_vb varbinary(8))",
                "ALTER TABLE vb_wal_bm.t_blob REPLICA IDENTITY FULL",
        };
        // publication 只收 t_types（blob 族不进基准路面——渲染分叉，见类 javadoc）；
        // wal 白名单两表都收（blob 腿独立事务发射，xid 交集对拍吸收窗口差）
        ParityRun run = runParity("B 档 13 类型矩阵", "bm1", ddl, new String[]{"vb_wal_bm.t_types"},
                new String[]{"vb_wal_bm.t_types", "vb_wal_bm.t_blob"}, 2, 1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_bm.t_types VALUES (1, 127, 255, 65535, 4294967295,"
                            + " 18446744073709551615, 2026, '中文CLOB值', '你好nvarchar',"
                            + " decode('0102030405060708090A0B0C0D0E0F10','hex'))");
                    exec(conn, "INSERT INTO vb_wal_bm.t_types VALUES (2, 0, 0, 0, 0, 0, 1901, '', '', NULL)");
                    exec(conn, "UPDATE vb_wal_bm.t_types SET c_i1 = -128, c_yr = 2027 WHERE id = 1");
                    exec(conn, "DELETE FROM vb_wal_bm.t_types WHERE id = 2");
                    conn.commit();
                    conn.setAutoCommit(true);
                    exec(conn, "INSERT INTO vb_wal_bm.t_blob VALUES (1, decode('deadbeef','hex'),"
                            + " decode('00ff10','hex'), decode('aabbccdd','hex'), decode('11223344','hex'))");
                });
        List<String> rows = run.capture().blocks().stream().flatMap(b -> b.rows().stream()).toList();
        assertEquals(5, rows.size(), "两事务块共 5 行（对拍腿 4 + blob 腿 1）:\n" + rows);
        // 对拍腿字面锚（uint8 u64 顶值无符号 / raw 大写 hex / '' 空串保留 / NULL 混排）
        assertEquals("INSERT vb_wal_bm.t_types BEFORE=- AFTER=[id=1, c_i1=127, c_u1=255, c_u2=65535,"
                + " c_u4=4294967295, c_u8=18446744073709551615, c_yr=2026, c_cl=中文CLOB值,"
                + " c_nv=你好nvarchar, c_rw=0102030405060708090A0B0C0D0E0F10]", rows.get(0),
                "边界值行：uint 族全上限 + year 2026 + 中文 varlena + raw 大写 hex");
        assertEquals("INSERT vb_wal_bm.t_types BEFORE=- AFTER=[id=2, c_i1=0, c_u1=0, c_u2=0, c_u4=0,"
                + " c_u8=0, c_yr=1901, c_cl=, c_nv=, c_rw=NULL]", rows.get(1),
                "零值行：'' 空串保留（B 档语义——非 NULL）+ year 下界 1901 + NULL 混排");
        assertEquals("UPDATE vb_wal_bm.t_types BEFORE=[id=1, c_i1=127, c_u1=255, c_u2=65535,"
                + " c_u4=4294967295, c_u8=18446744073709551615, c_yr=2026, c_cl=中文CLOB值,"
                + " c_nv=你好nvarchar, c_rw=0102030405060708090A0B0C0D0E0F10]"
                + " AFTER=[id=1, c_i1=-128, c_u1=255, c_u2=65535, c_u4=4294967295,"
                + " c_u8=18446744073709551615, c_yr=2027, c_cl=中文CLOB值,"
                + " c_nv=你好nvarchar, c_rw=0102030405060708090A0B0C0D0E0F10]", rows.get(2),
                "UPDATE 前后像：int1 负边界 -128 + year 2026→2027，uint8 前像无符号");
        assertEquals("DELETE vb_wal_bm.t_types BEFORE=[id=2, c_i1=0, c_u1=0, c_u2=0, c_u4=0,"
                + " c_u8=0, c_yr=1901, c_cl=, c_nv=, c_rw=NULL] AFTER=-", rows.get(3),
                "DELETE 前像：'' 空串保留（前像同语义）");
        // blob 族字面锚（wal 路渲染有意分叉——\x + 小写 hex，Task 4 记档面）
        assertEquals("INSERT vb_wal_bm.t_blob BEFORE=- AFTER=[id=1, c_bl=\\xdeadbeef, c_tb=\\x00ff10,"
                + " c_bi=\\xaabbccdd, c_vb=\\x11223344]", rows.get(4),
                "blob 族四型 wal 路字面锚（\\x + 小写 hex 有意分叉，不进对拍）");
    }

    // ---- 场景 ②：'' 保留 + datetime 微秒截断 ----

    /**
     * 场景②：B 档语义面——'' 保留（vs A 档入库即 NULL）与 datetime 微秒截断（typmod=0
     * 存整秒，服务端 MySQL DATETIME 精度 0 语义——wal 渲染忠实非 bug）。wal 路字面断言：
     * {@code c_vc=}（空串非 NULL）+ {@code c_dt=2026-10-18 12:30:45}（微秒被截）；
     * 行 2 全 NULL 对照（NULL 字面与空串字面的分界锚）。
     */
    @Test
    void bModeEmptyStringPreservedAndDatetimeSecondPrecision() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_bm.t_sem (id int primary key, c_vc varchar(32), c_dt datetime)",
                "ALTER TABLE vb_wal_bm.t_sem REPLICA IDENTITY FULL",
        };
        List<String> rows = runWalOnly("B 档 '' 保留与 datetime 截断", "bm2", ddl,
                new String[]{"vb_wal_bm.t_sem"}, 1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_bm.t_sem VALUES (1, '', '2026-10-18 12:30:45.123456')");
                    exec(conn, "INSERT INTO vb_wal_bm.t_sem VALUES (2, NULL, NULL)");
                    conn.commit();
                }).blocks().stream().flatMap(b -> b.rows().stream()).toList();
        assertEquals(2, rows.size(), "单事务两行:\n" + rows);
        assertEquals("INSERT vb_wal_bm.t_sem BEFORE=- AFTER=[id=1, c_vc=, c_dt=2026-10-18 12:30:45]",
                rows.get(0), "'' 保留空串（c_vc= 非空格非 NULL）+ datetime 微秒截断存整秒");
        assertEquals("INSERT vb_wal_bm.t_sem BEFORE=- AFTER=[id=2, c_vc=NULL, c_dt=NULL]",
                rows.get(1), "NULL 对照行（空串与 NULL 的字面分界锚）");
    }

    // ---- 场景 ③：varlena 6B 头混合（多列/NULL/TOAST 宽值） ----

    /**
     * 场景③：varlena 混合——三 varlena 列（clob/nvarchar2/blob）多列混排 + NULL 混排 +
     * 32KB clob TOAST external 重组（{@code repeat('x', 32768)}；标题沿 6B 头历史名——
     * Task 1 勘误后 B 档 varlena 与 A 档同 5B 头，本场景实质验收 varlena 族多列混合与
     * TOAST 面）。双路对拍（值面全 text 同形）；blob 列恒 NULL（渲染分叉不进对拍，字面
     * 锚归场景①）。足迹断言：宽值 32768B 截断尾注恰 3 处（INSERT 后像 + UPDATE 前后双像
     * ——OG 系语义 UPDATE 未变 external 列恒带完整重写值）+ 无降级字面。
     */
    @Test
    void bModeVarlenaMixedNullsAndToastWideValues() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_bm.t_var (id int, a clob, b nvarchar2(64), c blob)",
                "ALTER TABLE vb_wal_bm.t_var REPLICA IDENTITY FULL",
        };
        List<OgBlock> blocks = runParity("B 档 varlena 混合", "bm3", ddl, new String[]{"vb_wal_bm.t_var"},
                new String[]{"vb_wal_bm.t_var"}, 1, 1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_bm.t_var VALUES (1, 'hello clob', '你好nvarchar', NULL)");
                    exec(conn, "INSERT INTO vb_wal_bm.t_var VALUES (2, NULL, NULL, NULL)");
                    exec(conn, "INSERT INTO vb_wal_bm.t_var VALUES (3, repeat('x', 32768), 'wide-row-b', NULL)");
                    exec(conn, "UPDATE vb_wal_bm.t_var SET b = 'upd-wide' WHERE id = 3");
                    exec(conn, "DELETE FROM vb_wal_bm.t_var WHERE id = 2");
                    conn.commit();
                }).capture().blocks();
        String joined = String.join("\n", blocks.get(0).rows());
        assertFalse(joined.contains("toast-unavailable"), "TOAST 宽值不应出现降级字面:\n" + joined);
        assertEquals(3, countLiteral(joined, "...(32768B)"),
                "32KB clob 应在 INSERT 后像 + UPDATE 前后双像共 3 处渲染截断尾注"
                        + "（UPDATE 未变 external 列恒带完整重写值——OG 系语义）:\n" + joined);
        assertTrue(blocks.get(0).rows().get(0).contains("AFTER=[id=1, a=hello clob, b=你好nvarchar, c=NULL]"),
                "行内多 varlena 列混排（含 blob 列 NULL）:\n" + blocks.get(0).rows().get(0));
        assertTrue(blocks.get(0).rows().get(1).contains("AFTER=[id=2, a=NULL, b=NULL, c=NULL]"),
                "全 NULL 行:\n" + blocks.get(0).rows().get(1));
    }

    // ---- 场景 ④：UPDATE/DELETE 前像（新类型前像渲染） ----

    /**
     * 场景④：UPDATE/DELETE 前像（REPLICA IDENTITY FULL）——uint8 前像无符号（u64 顶值
     * 18446744073709551615）、year 前像（2026）、blob 前像 {@code \xdeadbeef}（渲染分叉
     * 面，blob 前像同走 wal 路字面锚——本场景不进对拍）。UPDATE 后像保留未变列全值、
     * DELETE 前像取 UPDATE 后态（c_yr=2027）。
     */
    @Test
    void bModeUpdateDeleteBeforeImagesWithNewTypes() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_bm.t_bi (id int primary key, c_u8 bigint unsigned, c_yr year, c_bl blob)",
                "ALTER TABLE vb_wal_bm.t_bi REPLICA IDENTITY FULL",
        };
        List<String> rows = runWalOnly("B 档前像", "bm4", ddl, new String[]{"vb_wal_bm.t_bi"},
                1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_bm.t_bi VALUES (1, 18446744073709551615, 2026,"
                            + " decode('deadbeef','hex'))");
                    exec(conn, "UPDATE vb_wal_bm.t_bi SET c_yr = 2027 WHERE id = 1");
                    exec(conn, "DELETE FROM vb_wal_bm.t_bi WHERE id = 1");
                    conn.commit();
                }).blocks().stream().flatMap(b -> b.rows().stream()).toList();
        assertEquals(3, rows.size(), "单事务三行:\n" + rows);
        assertEquals("INSERT vb_wal_bm.t_bi BEFORE=- AFTER=[id=1, c_u8=18446744073709551615,"
                + " c_yr=2026, c_bl=\\xdeadbeef]", rows.get(0), "INSERT 后像：uint8 顶值 + blob \\x 形态");
        assertEquals("UPDATE vb_wal_bm.t_bi BEFORE=[id=1, c_u8=18446744073709551615, c_yr=2026,"
                + " c_bl=\\xdeadbeef] AFTER=[id=1, c_u8=18446744073709551615, c_yr=2027,"
                + " c_bl=\\xdeadbeef]", rows.get(1),
                "UPDATE 前像：uint8 无符号 + year + blob \\x（未变列后像保留全值）");
        assertEquals("DELETE vb_wal_bm.t_bi BEFORE=[id=1, c_u8=18446744073709551615, c_yr=2027,"
                + " c_bl=\\xdeadbeef] AFTER=-", rows.get(2), "DELETE 前像：取 UPDATE 后态（c_yr=2027）");
    }

    // ---- 对拍骨架（VastDmlParityIT 双路形态浓缩，连接面全换 VastBEnv） ----

    /**
     * 对拍结局快照：已关闭的 WalSource（观测计数面停流后读）+ 捕获器（块/头事件面）。
     *
     * @param source  wal 直解路门面（已 close——计数读取安全）
     * @param capture 捕获器（blocks/begins 快照）
     */
    record ParityRun(WalSource source, WalCapture capture) {
    }

    /**
     * 单场景双路对拍骨架（无 state.dir 档）：重置场景 → 建逻辑槽 → 起 wal 直解路
     * （{@link VastTestEnv#startOnlineWithRetry} 重试环——test_b 握手抖动纪律；判别 →
     * {@code WalLayoutVVastbase} → direct 通道 + opengauss-jdbc 前缀 + 白名单钉场景表）→
     * DML → await 桶数 → 基准路 capture → close → assertParity，返回对拍结局快照。
     *
     * @param scenario       场景名（断言消息上下文）
     * @param slotSuffix     槽名后缀（前缀 vb_wal_bm_logic_/vb_wal_bm_wal_ 固定）
     * @param tableDdl       场景建表语句（vb_wal_bm schema 内）
     * @param publicationTbl publication 的 FOR TABLE 集（基准路窗口钉对拍表）
     * @param whitelist      DML 白名单表全名集（可与 publicationTbl 不同——blob 族腿只进白名单）
     * @param walTxns        wal 路追平目标（发射桶数）
     * @param baselineTxns   基准路 capture 的 C 帧计数目标
     * @param dml            DML 执行器
     * @return 对拍结局快照（已关闭 WalSource + 捕获器）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    ParityRun runParity(String scenario, String slotSuffix, String[] tableDdl, String[] publicationTbl,
            String[] whitelist, long walTxns, long baselineTxns, SqlConsumer<Connection> dml) throws Exception {
        resetScenario(tableDdl, publicationTbl);
        slots(slotSuffix);
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        WalCapture capture = new WalCapture();
        wal = VastTestEnv.startOnlineWithRetry(walSourceConfig(physicalSlot, whitelist, null), capture);
        assertTrue(wal.dmlEnabled(), scenario + ": wal 直解路 DML 面应装配（B 档护栏 M4 已放行）");
        try (Connection conn = VastBEnv.newSqlConnection()) {
            dml.accept(conn);
        }
        await(() -> wal.dmlEmittedBuckets() >= walTxns,
                scenario + ": wal 路未发射 " + walTxns + " 个桶（buckets=" + wal.dmlEmittedBuckets()
                        + ", terminal=" + wal.receiverTerminalFailure() + "）");
        List<OgBlock> baseline = OgParityBaseline.capture(endpoint(), BASELINE_APP, logicalSlot,
                PUBLICATION, baselineTxns, CAPTURE_MS);
        wal.close();
        OgParityBaseline.assertParity(baseline, capture.blocks(), scenario, 0);
        return new ParityRun(wal, capture);
    }

    /**
     * 单场景 wal 路独跑骨架（场景②④——blob 族渲染分叉不进对拍，断言面全在 wal 路字面）：
     * 重置场景 → 起 wal 直解路（重试环同 {@link #runParity}）→ DML → await → close →
     * 返回捕获器。无逻辑槽/无基准路。
     *
     * @param scenario   场景名（断言消息上下文）
     * @param slotSuffix 槽名后缀（前缀 vb_wal_bm_wal_ 固定）
     * @param tableDdl   场景建表语句
     * @param whitelist  DML 白名单表全名集
     * @param walTxns    wal 路追平目标（发射桶数）
     * @param dml        DML 执行器
     * @return wal 路捕获器（已 close 后读）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    WalCapture runWalOnly(String scenario, String slotSuffix, String[] tableDdl, String[] whitelist,
            long walTxns, SqlConsumer<Connection> dml) throws Exception {
        resetScenario(tableDdl, whitelist);
        physicalSlot = "vb_wal_bm_wal_" + slotSuffix;
        VastBEnv.dropPhysicalSlotQuietly(physicalSlot);
        WalCapture capture = new WalCapture();
        wal = VastTestEnv.startOnlineWithRetry(walSourceConfig(physicalSlot, whitelist, null), capture);
        assertTrue(wal.dmlEnabled(), scenario + ": wal 直解路 DML 面应装配（B 档护栏 M4 已放行）");
        try (Connection conn = VastBEnv.newSqlConnection()) {
            dml.accept(conn);
        }
        await(() -> wal.dmlEmittedBuckets() >= walTxns,
                scenario + ": wal 路未发射 " + walTxns + " 个桶（buckets=" + wal.dmlEmittedBuckets()
                        + ", terminal=" + wal.receiverTerminalFailure() + "）");
        wal.close();
        return capture;
    }

    /**
     * 场景重置：schema CASCADE 整体清场 + 场景建表 + publication 重建（<b>显式 FOR TABLE
     * 场景表白名单</b>——窗口钉场景表，背景业务活动零帧到达）。DDL 先于建槽/引导（spike
     * 坑 2 的时序契约）。单持久连接干完全部 DDL（test_b 连接纪律——每语句一连接会触发
     * 连片失败）。
     *
     * @param tableDdl       场景建表语句
     * @param publicationTbl publication 的 FOR TABLE 集（schema 限定全名）
     * @throws SQLException DDL 失败
     */
    static void resetScenario(String[] tableDdl, String[] publicationTbl) throws SQLException {
        try (Connection c = VastBEnv.newSqlConnection(); Statement st = c.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            st.execute("CREATE SCHEMA " + SCHEMA);
            for (String ddl : tableDdl) {
                st.execute(ddl);
            }
            st.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION);
            st.execute("CREATE PUBLICATION " + PUBLICATION + " FOR TABLE " + String.join(", ", publicationTbl));
        }
    }

    /**
     * 设定当前用例的两路槽名（{@code vb_wal_bm_} 前缀——B 档真库卫生纪律，与 A 档
     * {@code vb_wal_par_*} 隔离）并前置清理残留。
     *
     * @param suffix 后缀（槽名 = vb_wal_bm_logic_/vb_wal_bm_wal_ + suffix）
     */
    void slots(String suffix) {
        logicalSlot = "vb_wal_bm_logic_" + suffix;
        physicalSlot = "vb_wal_bm_wal_" + suffix;
        OgParityBaseline.dropLogicalSlotQuietly(endpoint(), BASELINE_APP, logicalSlot);
        VastBEnv.dropPhysicalSlotQuietly(physicalSlot);
    }

    /**
     * 基准路连接档：{@link VastBEnv} 的 SQL 会话 URL 与凭证（opengauss-jdbc——md5/sha256
     * 认证兼容，A 档同驱动面）。
     *
     * @return 基准路连接档（建槽/删槽/收流共用凭证）
     */
    static BaselineEndpoint endpoint() {
        return new BaselineEndpoint(VastBEnv.jdbcUrl(), VastBEnv.user(), VastBEnv.pass());
    }

    /**
     * wal 直解路门面配置：{@link VastBEnv#params()} 六基础键 + OG 档 direct 通道 +
     * opengauss-jdbc 前缀（门面判别 test_b → {@code WalLayoutVVastbase} → OG 装配分支
     * → B 档护栏 {'PG','A','B'} 放行）+ DML 白名单钉场景表；stateDir 非 null 时启用
     * 检查点（本四场景无续传面，恒 null）。
     *
     * @param physicalSlot 物理槽名
     * @param whitelist    DML 白名单表全名集
     * @param stateDir     检查点目录（null = 禁用）
     * @return WalSource 构造入参配置
     */
    static Properties walSourceConfig(String physicalSlot, String[] whitelist, String stateDir) {
        Properties cfg = VastBEnv.params();
        cfg.setProperty(WalSource.KEY_SLOT, physicalSlot);
        cfg.setProperty(WalSource.KEY_TABLES, String.join(",", whitelist));
        if (stateDir != null) {
            cfg.setProperty(WalSource.KEY_STATE_DIR, stateDir);
        }
        return cfg;
    }

    /**
     * 兜底清理：关 WalSource（幂等）→ 删两路槽（异常路径保险；逻辑槽按本基准路
     * application_name 杀 walsender 后删）→ 删 publication + schema（真库卫生——
     * {@code vb_wal_bm} 前缀对象全数退场，不触碰既有业务对象）。
     */
    @AfterEach
    void cleanupScenario() {
        if (wal != null) {
            wal.close();
        }
        if (logicalSlot != null) {
            OgParityBaseline.dropLogicalSlotQuietly(endpoint(), BASELINE_APP, logicalSlot);
        }
        if (physicalSlot != null) {
            VastBEnv.dropPhysicalSlotQuietly(physicalSlot);
        }
        try {
            VastBEnv.sql("DROP PUBLICATION IF EXISTS " + PUBLICATION,
                    "DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        } catch (SQLException e) {
            LOG.warn("清理 B 档对拍 publication/schema 失败（忽略）: {}", e.getMessage());
        }
    }

    /**
     * 轮询等待条件成立（{@link #POLL_MS} 间隔、{@link #AWAIT_MS} 上限），超时 fail。
     *
     * @param cond 条件（幂等读）
     * @param what 等待对象描述（超时消息上下文）
     * @throws InterruptedException 睡眠被中断
     */
    static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MS;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + what);
            }
            Thread.sleep(POLL_MS);
        }
    }

    /**
     * 在连接上顺序执行语句（各自独立执行——事务边界由调用方的 autoCommit/commit 控制）。
     *
     * @param conn 连接
     * @param sqls 语句
     * @throws SQLException 任一语句失败
     */
    static void exec(Connection conn, String... sqls) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : sqls) {
                st.execute(sql);
            }
        }
    }

    /**
     * 字面计数（indexOf 循环做纯字面语义——正则元字符不参与）。
     *
     * @param s       目标串
     * @param literal 字面子串
     * @return 出现次数（不重叠）
     */
    private static int countLiteral(String s, String literal) {
        int count = 0;
        int idx = 0;
        while ((idx = s.indexOf(literal, idx)) >= 0) {
            count++;
            idx += literal.length();
        }
        return count;
    }

    /** 单参 DML 执行器（可抛受检异常的消费者）。 */
    @FunctionalInterface
    interface SqlConsumer<T> {

        /** 执行场景 DML。 @param t 连接 @throws Exception 底层异常 */
        void accept(T t) throws Exception;
    }
}
