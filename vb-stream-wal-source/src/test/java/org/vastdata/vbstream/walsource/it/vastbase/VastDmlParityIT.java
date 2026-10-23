package org.vastdata.vbstream.walsource.it.vastbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.it.OgParityBaseline;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.BaselineEndpoint;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
 * VastBase G100 DML 双路对拍（手动档 {@code -Dvb.it.vastbase=true}，vastbase T6）：逻辑流
 * 基准路（opengauss-jdbc {@code logical()}——T0 spike 裁定<b>同形</b>：VastBase pgoutput 帧
 * 与 OG 5.0.0 逐帧同构，B 帧 xid Int64 / TupleData Int16 ncols 前缀两处帧级差异原样成立，
 * 见 {@code docs/wal-source-vastbase-adaptation-spike.md} §3——基座零复制复用
 * {@link OgParityBaseline}，仅连接参数换 {@link VastTestEnv}、槽/publication 用
 * {@code vb_wal_} 前缀）vs WalSource 直解路（{@code WalLayoutVVastbase} 判别 + direct
 * 通道 + opengauss-jdbc 前缀——门面装配面经 T5 {@code VastCatalogSyncIT} 验收），六场景
 * 按 xid 交集逐行 diff 空（+ COPY 批量装载补充——vastbase T7 场景 1g，MULTI_INSERT
 * 真几何的端到端收口；+ <b>场景 7 TRUNCATE 双路不可见</b>——spike 裁决 C 记录面实锚
 * [rmid=10 全窗零 0x30] + 帧面按 OG 同内核推定，2026-10-22 truncate 线 Task 7）
 * ——{@code OgDmlParityIT} 场景面的真库翻译（无 CompatMode 维度
 * ——真库 test_a 恒 A 兼容档，M3 起护栏白名单内）。
 *
 * <p><b>A 档专面（真库恒 A 档）</b>：①<b>{@code date} 列落私有类型 {@code oradate}
 * (oid 9040)</b>——非 OG A 档的 1114 timestamp（T6 首跑实测反转 M3 预期），磁盘表示
 * 同构（8B epoch-2000 微秒、整秒粒度），DML 面经 {@code WalLayout#remapDmlTypeOid}
 * 归一到 timestamp 渲染（T6 主代码接缝）、基准路 oradate_out 同形——时段零真值损失，
 * 专项场景 {@link #aCompatDateColumnKeepsTimeOfDayViaTimestampStorageParity} 四面字面
 * 钉；②{@code ''} 空串入库即 NULL（服务端语义——两路同见 NULL，对拍天然吸收，本 IT
 * 不构造"空串非 NULL"断言）。</p>
 *
 * <p><b>VastBase 服务端行为裁定（T6 首跑实测记档，2026-10-13）</b>：①heap UPDATE/
 * DELETE 记录 main 前缀较 OG 各 +8（xid u64 殿后——{@code WalLayoutVVastbase}
 * sizeOfHeapUpdate=21/Delete=19，主代码修）；②DDL 事务发空 B..C 块（与 OG 同——
 * 场景 6 基准路 C 帧计数含它）；③<b>事务内 DDL 后续行正常解码</b>（与 vanilla 同形、
 * 与 OG 的静默丢弃分叉——场景 4 双块全量 parity）；④两处输出函数方言：uuid_out RAW
 * 风格（大写无连字符）+ numeric_out 剥小数尾零——场景 2 避开 + javadoc 记档（沿 OG
 * float 先例）；⑤TOAST 行为与 OG 同源（UPDATE 未变 external 列恒带完整重写值——
 * 场景 3 足迹计数同 OG 锚）。</p>
 *
 * <p><b>真库卫生（长活共享环境纪律）</b>：schema/两路槽/publication 一律 {@code vb_wal_}
 * 前缀（{@code vb_wal_par} / {@code vb_wal_par_logic_<suffix>} /
 * {@code vb_wal_par_wal_<suffix>} / {@code vb_wal_par_pub}），每用例 finally 兜底清理；
 * publication <b>显式 {@code FOR TABLE} 场景表白名单</b>（A 档形态沿 OG 分区先例——且真库
 * 上把基准路窗口钉到场景表：背景业务活动零帧到达，C 帧计数不受污染）；wal 路
 * {@code vb.wal.tables} 白名单恒钉场景表（真库背景行不入发射面）。不触碰既有业务表
 * （orders/snapshot/gs_系）与既有逻辑槽（sync_orders_slot/vb_e2e_*）。</p>
 *
 * <p><b>骨架时序（沿 spike 坑 2 契约）</b>：resetScenario（DDL + publication 重建）→
 * 建逻辑槽 → 起 wal 直解路（引导后）→ DML → await 桶数 → 基准路 capture → close →
 * assertParity。表恒 {@code REPLICA IDENTITY FULL}（DEFAULT 身份 'K' 段占位 vs wal 路
 * 尾列补 null 的归一化是记档差异面，沿 M2 惯例规避——T0 未探测 'K' 形态）。需真库
 * 172.16.107.133:55433 可达（连接参数经 {@code -Dvb.it.vastbase.*} 可覆盖）。</p>
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastDmlParityIT {

    private static final Logger LOG = LoggerFactory.getLogger(VastDmlParityIT.class);

    /** 对拍 schema（vb_wal_ 前缀——reset 以 schema CASCADE 整体清场）。 */
    private static final String SCHEMA = "vb_wal_par";

    /** publication 名（每场景重建——显式 FOR TABLE 场景表白名单，见类 javadoc 卫生段）。 */
    private static final String PUBLICATION = "vb_wal_par_pub";

    /** 基准路复制连接的 application_name（walsender 清理面按名匹配——与 wal 路 vb-wal-source 区分）。 */
    private static final String BASELINE_APP = "vb-wal-parity-base";

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

    // ---- 场景 1：基础 DML 六形态（单行/多语句/交错/回滚/SAVEPOINT/截断中段 UPDATE） ----

    /**
     * 场景 1a 单行形态：三个自动提交单语句事务（I→U→D 各一笔）——最小行颗粒的双路对齐面
     * （每笔恰一块一行，行序与值双路逐字节对齐；单行独立提交使块边界无歧义）。
     */
    @Test
    void singleRowTxnsParityAcrossPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        runParity("单行 DML 三事务", "dml0", ddl, new String[]{"vb_wal_par.t_parity"}, 3, 3, conn -> {
            conn.setAutoCommit(true);
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'solo')");
            exec(conn, "UPDATE vb_wal_par.t_parity SET name = 'solo-u' WHERE id = 1");
            exec(conn, "DELETE FROM vb_wal_par.t_parity WHERE id = 1");
        });
    }

    /** 场景 1b：单事务多语句 I/U/D 混合 + 全 NULL 行——行序与值双路逐字节对齐。 */
    @Test
    void multiStatementTxnParityAcrossPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                        + " d date, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        runParity("基础 DML 多语句", "dml1", ddl, new String[]{"vb_wal_par.t_parity"}, 1, 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'alice', true, 12.345, '2026-10-07')");
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (2, NULL, false, NULL, NULL)");
            exec(conn, "UPDATE vb_wal_par.t_parity SET id = 11, name = 'alice2', flag = false, val = 99.5,"
                    + " d = '2026-11-01' WHERE id = 1");
            exec(conn, "UPDATE vb_wal_par.t_parity SET id = 22, name = 'bob2', flag = true, val = 88.25,"
                    + " d = '2026-11-02' WHERE id = 2");
            exec(conn, "DELETE FROM vb_wal_par.t_parity WHERE id = 11");
            conn.commit();
        });
    }

    /** 场景 1c：双连接交错事务——两路各自两个事务桶、桶内行归属正确、提交序一致。 */
    @Test
    void interleavedTxnsParityAcrossPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                        + " d date, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        runParity("交错事务", "dml2", ddl, new String[]{"vb_wal_par.t_parity"}, 2, 2, conn -> {
            try (Connection c1 = VastTestEnv.newSqlConnection();
                 Connection c2 = VastTestEnv.newSqlConnection()) {
                c1.setAutoCommit(false);
                c2.setAutoCommit(false);
                exec(c1, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'a1', true, 1.1, '2026-10-07')");
                exec(c2, "INSERT INTO vb_wal_par.t_parity VALUES (101, 'b1', false, 2.2, '2026-10-08')");
                exec(c1, "INSERT INTO vb_wal_par.t_parity VALUES (2, 'a2', true, 3.3, '2026-10-09')");
                exec(c2, "INSERT INTO vb_wal_par.t_parity VALUES (102, 'b2', false, 4.4, '2026-10-10')");
                c1.commit();
                c2.commit();
            }
        });
    }

    /** 场景 1d：回滚零输出——两路都无回滚事务的块，随后的提交事务对拍完整。 */
    @Test
    void rolledBackTxnEmitsNothingOnBothPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        runParity("回滚零输出", "dml3", ddl, new String[]{"vb_wal_par.t_parity"}, 1, 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'doomed')");
            conn.rollback();
            conn.setAutoCommit(true);
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (2, 'committed')");
        });
    }

    /**
     * 场景 1e：SAVEPOINT 子事务行剔除——两路都只见 s1/s3（OG 档同裁定：ROLLBACK TO
     * SAVEPOINT 的 ABORT 记录即整弃子桶——被回滚行不进顶层记账，数据面等价，故
     * walAborted=0）。
     */
    @Test
    void savepointSubTxnFilteredRowsParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        runParity("子事务 SAVEPOINT", "dml4", ddl, new String[]{"vb_wal_par.t_parity"}, 1, 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 's1')");
            exec(conn, "SAVEPOINT sp");
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (2, 's2-doomed')");
            exec(conn, "ROLLBACK TO SAVEPOINT sp");
            exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (3, 's3')");
            conn.commit();
        });
    }

    /**
     * 场景 1f：中段列 UPDATE（FULL 身份整行前像）——截断跳过/重建双计数 0 哨兵
     * （wal_level=logical 下与 OG/vanilla 同为全量记录面；VastBase 记录层与 OG 同构
     * ——T1-T3 布局档）。
     */
    @Test
    void midColumnUpdateFullIdentityParityAndZeroTruncationCounters() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                        + " d date, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        ParityRun run = runParity("中段列 UPDATE", "dml5", ddl, new String[]{"vb_wal_par.t_parity"},
                1, 1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'alice', true, 12.345, '2026-10-07')");
                    exec(conn, "UPDATE vb_wal_par.t_parity SET name = 'alice-mid', flag = false, val = 99.5,"
                            + " d = '2026-11-01' WHERE id = 1");   // 首列 id 不变
                    conn.commit();
                });
        assertEquals(0, run.source().dmlSkippedTruncatedRows(), "截断 UPDATE 跳过计数应为 0");
        assertEquals(0, run.source().dmlReconstructedTruncatedRows(), "截断 UPDATE 重建计数应为 0（全量记录面）");
    }

    /**
     * 场景 1g（vastbase T7 补钉，T6 延后项——COPY/MULTI_INSERT 真几何端到端收口）：
     * 客户端侧 CopyManager COPY FROM STDIN 100 行单事务，走 heap2 MULTI_INSERT 记录
     * （块 data 逐 entry {@code [datalen u16 只计 tail][xl_heap_header 5B][tail]}、步进
     * SHORTALIGN(7+datalen)——OG M2 T7 修复 A 的真几何；VastBase 记录层与 OG 同构，
     * main 前缀的 xid 殿后差异不在 entry walk 面上）。断言 100 行值全对（walk 错位在
     * 第二 entry 起即红）。CSV 载荷含 NULL/奇长 text 值（奇长 entry 钉对齐面）；A 档
     * 空串语义下 CSV 非引号空串两路同见 NULL。
     */
    @Test
    void copyBatchMultiInsertParityRealEntryGeometry() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_copy (id int4 NOT NULL, name text, v numeric, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_copy REPLICA IDENTITY FULL",
        };
        StringBuilder csv = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            // CSV 格式的 NULL 是非引号空串（A 档空串语义两路同见 NULL，无分叉面）
            String name = (i % 3 == 0) ? "" : "row-" + i + (i % 2 == 0 ? "-odd" : "");
            String val = (i % 4 == 0) ? "" : i + ".25";
            csv.append(i).append(',').append(name).append(',').append(val).append('\n');
        }
        ParityRun run = runParity("COPY 批量", "dml6", ddl, new String[]{"vb_wal_par.t_copy"},
                1, 1, conn -> {
                    copyIn("COPY vb_wal_par.t_copy FROM STDIN WITH (FORMAT csv)", csv.toString());
                });
        List<OgBlock> blocks = run.capture().blocks();
        assertEquals(1, blocks.size(), "COPY 单事务应恰一个块");
        assertEquals(100, blocks.get(0).rows().size(), "100 行 COPY 应逐 entry 全解出:\n"
                + blocks.get(0).rows().subList(0, Math.min(5, blocks.get(0).rows().size())));
        assertTrue(blocks.get(0).rows().get(0).contains("AFTER=[id=1, name=row-1, v=1.25]"),
                "首行值全对:\n" + blocks.get(0).rows().get(0));
        assertTrue(blocks.get(0).rows().get(2).contains("name=NULL"),
                "CSV 非引号空串应解为 NULL:\n" + blocks.get(0).rows().get(2));
    }

    // ---- 场景 2：17 类型边界值 + A 档 date 专面 ----

    /**
     * 场景 2：类型矩阵边界值对拍（值面取两路渲染矩阵交面内的可达值——沿 OG 档裁定避开
     * float (0,1) 前导零省略区间与缺省精度分叉）：负数全族 / float 科学计数法交面值 /
     * numeric 深小数（&gt;1，无尾零 scale）与 NaN / UTF-8 π 与多字节 / bytea hex /
     * 微秒精度 timestamp·timestamptz / time {@code 24:00:00} 闭上端 / timetz 偏移 /
     * 全 NULL 行 → UPDATE 全列赋值（长文本 &gt;64 截断）→ DELETE 两行——双路 diff 空。
     *
     * <p><b>A 档专面差异（javadoc 记档）</b>：①{@code ''} 空串入库即 NULL（行 3 的
     * c_text/c_varchar/c_bpchar 与 {@code decode('','hex')}——两路同见 NULL 天然吸收，
     * 不作非 NULL 断言）；②{@code date} 列底层 timestamp 化——{@code '1999-12-31'}
     * 存取渲 {@code 1999-12-31 00:00:00}（时段形态，两路同渲染——专面钉于
     * {@link #aCompatDateColumnKeepsTimeOfDayViaTimestampStorageParity}）。</p>
     *
     * <p><b>值面裁定（2026-10-13 T6 首跑实测锚，沿 OG float 先例避开 + 记档）</b>：
     * VastBase 服务端两处输出函数方言与 vanilla 分叉——①{@code uuid_out} 渲 <b>RAW
     * 风格</b>（32 位大写 hex 无连字符，如 {@code A0B1C2D3...}），wal 路 {@code
     * DiskValueRenderer} 渲 PG 风格（小写带连字符）——同一 16B 值的两种合法文本形态，
     * 本矩阵<u>不收 uuid 列</u>（任何值形态都分叉，解码/渲染正确性由离线
     * DiskValueRendererTest 覆盖）；②{@code numeric_out} 剥小数尾零（Oracle NUMBER
     * 语义——{@code .1234567890} 渲 {@code .123456789}），本矩阵取无尾零 scale 值
     * 避开。方言渲染分支（wal 路按档切 uuid/numeric 文本风格）记档归后续任务裁定，
     * 沿 OG M2"主代码渲染面方言分支超出任务范围"先例。</p>
     */
    @Test
    void typeMatrixBoundaryRowsParityAcrossPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_types (id int4 NOT NULL,"
                        + " c_bool bool, c_int2 int2, c_int8 int8,"
                        + " c_float4 float4, c_float8 float8, c_numeric numeric,"
                        + " c_text text, c_varchar varchar(100), c_bpchar bpchar(10), c_json json,"
                        + " c_bytea bytea, c_date date, c_time time, c_timetz timetz,"
                        + " c_timestamp timestamp, c_timestamptz timestamptz,"
                        + " PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_types REPLICA IDENTITY FULL",
        };
        runParity("类型矩阵边界值", "typ1", ddl, new String[]{"vb_wal_par.t_types"}, 1, 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn,
                    "INSERT INTO vb_wal_par.t_types VALUES (1, true, -32768, -9223372036854775808,"
                            + " '3.25'::float4, '1e20'::float8, '123456789012345678901234567890.123456789',"
                            + " 'π α β γ δ', '你好 wörld', 'abc', '{\"k\":\"v\",\"n\":[1,2,3]}'::json,"
                            + " decode('deadbeef00ff','hex'), '1999-12-31', '24:00:00',"
                            + " '12:34:56.123456+05:30', '2026-10-07 12:34:56.123456',"
                            + " '2026-10-07 04:34:56.789012+00')");
            exec(conn, "INSERT INTO vb_wal_par.t_types VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL,"
                    + " NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            exec(conn,
                    "INSERT INTO vb_wal_par.t_types VALUES (3, false, 0, 0, 'Infinity'::float4, 'NaN'::float8,"
                            + " 'NaN'::numeric, '', '', '', 'null'::json, decode('','hex'),"
                            + " '2000-01-01', '00:00:00', '00:00:00+00', '2000-01-01 00:00:00',"
                            + " '2000-01-01 00:00:00+00')");
            exec(conn,
                    "UPDATE vb_wal_par.t_types SET id = 11, c_bool = false, c_int2 = 32767,"
                            + " c_int8 = 9223372036854775807, c_float4 = '2.5'::float4,"
                            + " c_float8 = '3.5'::float8,"
                            + " c_numeric = '987654321098765432109876543210.987654321',"
                            + " c_text = repeat('π', 100), c_varchar = '-42.5', c_bpchar = 'xyz',"
                            + " c_json = '[1, \"two\", null]'::json, c_bytea = decode('ab','hex'),"
                            + " c_date = '2026-10-07', c_time = '12:34:56.1',"
                            + " c_timetz = '23:59:59.999999-08:00', c_timestamp = '2026-10-07 12:34',"
                            + " c_timestamptz = '2026-10-07 23:59:59.999999+00' WHERE id = 1");
            exec(conn, "DELETE FROM vb_wal_par.t_types WHERE id = 2");
            exec(conn, "DELETE FROM vb_wal_par.t_types WHERE id = 3");
            conn.commit();
        });
    }

    /**
     * 场景 2b（A 档专形——真库恒 A 档，OG M3 探针语义的 VastBase 复验）：<b>date 列底层
     * timestamp 化</b>——DDL 关键字 {@code date} 映射 atttypid=1114（8B 存时段；预期与
     * OG A 档同形）。四面钉：
     * <ul>
     *   <li><b>时段完整存储与渲染零损失</b>：{@code '2026-10-13 14:30:25'} 原样存取——wal
     *       路按列 typeOid 1114 走 timestamp 渲染、基准路 timestamp_out 同形，双路 diff
     *       空 + wal 路字面含时段（对拍不吞值）；</li>
     *   <li><b>整秒粒度锚</b>：微秒输入被服务端截断（Oracle DATE 语义——{@code .123456}
     *       存 {@code 14:30:25}）；</li>
     *   <li><b>零时段形态</b>：纯日期值渲 {@code 00:00:00} 时段；</li>
     *   <li><b>UPDATE 前后像双含时段</b>（FULL 身份整行前像）。</li>
     * </ul>
     * 任一面红即 VastBase A 档 date 语义与 OG 分叉——按实际值改锚并记档（不断言 OG 值
     * 硬凑）。
     */
    @Test
    void aCompatDateColumnKeepsTimeOfDayViaTimestampStorageParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_adate (id int4 NOT NULL, d date, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_adate REPLICA IDENTITY FULL",
        };
        List<OgBlock> blocks = runParity("A 档 date 时段", "adate", ddl,
                new String[]{"vb_wal_par.t_adate"}, 1, 1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_par.t_adate VALUES (1, '2026-10-13 14:30:25')");
                    exec(conn, "INSERT INTO vb_wal_par.t_adate VALUES (2, DATE '2026-10-07')");
                    exec(conn, "INSERT INTO vb_wal_par.t_adate VALUES (3, '2026-10-13 14:30:25.123456')");
                    exec(conn, "UPDATE vb_wal_par.t_adate SET d = '2026-12-31 23:59:59' WHERE id = 1");
                    conn.commit();
                }).capture().blocks();
        assertEquals(1, blocks.size(), "A 档 date 时段: 应恰一个事务块:\n" + blocks);
        List<String> rows = blocks.get(0).rows();
        assertEquals(4, rows.size(), "四行（3 I + 1 U）:\n" + rows);
        assertEquals("INSERT vb_wal_par.t_adate BEFORE=- AFTER=[id=1, d=2026-10-13 14:30:25]", rows.get(0),
                "时段值应完整存取（wal 路 typeOid 1114 走 timestamp 渲染——零真值损失）");
        assertEquals("INSERT vb_wal_par.t_adate BEFORE=- AFTER=[id=2, d=2026-10-07 00:00:00]", rows.get(1),
                "纯日期值渲 00:00:00 时段（timestamp 渲染恒 HH:MM:SS，与基准路 timestamp_out 同形）");
        assertEquals("INSERT vb_wal_par.t_adate BEFORE=- AFTER=[id=3, d=2026-10-13 14:30:25]", rows.get(2),
                "微秒输入被服务端截断（Oracle DATE 整秒粒度锚）");
        assertEquals("UPDATE vb_wal_par.t_adate BEFORE=[id=1, d=2026-10-13 14:30:25]"
                + " AFTER=[id=1, d=2026-12-31 23:59:59]", rows.get(3),
                "UPDATE 前后像双含时段（FULL 身份整行前像）");
    }

    // ---- 场景 3：TOAST 三形态 + 重启 unchanged ----

    /**
     * 场景 3a：TOAST 三存储形态窗口内重组——external 未压缩（md5 链 7040B）/ external
     * pglz（半可压缩 24600B）/ 行内压缩（{@code repeat('x',10000)}）+ 全行 UPDATE（未变宽
     * 列双路同渲染）+ DELETE，双路 diff 空；足迹断言（截断尾注计数 + 无降级字面）钉三形态
     * 的输出面。TOAST 引擎阈值/压缩行为预期与 OG 同源（9.2 系 pglz）；UPDATE 未变 external
     * 列的后像按 OG 语义恒带完整重写值（heap_update 重写 toast 值——足迹计数
     * 7040B×3 / 24600B×4 / 10000B×3 / unchanged×0），实测红即按 VastBase 真值改锚记档。
     */
    @Test
    void toastThreeStorageFormsWideValuesParityAcrossPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity_toast (id int4, w text)",
                "ALTER TABLE vb_wal_par.t_parity_toast REPLICA IDENTITY FULL",
        };
        List<OgBlock> blocks = runParity("TOAST 三形态", "tst1", ddl, new String[]{"vb_wal_par.t_parity_toast"},
                1, 1, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO vb_wal_par.t_parity_toast VALUES (1,"
                            + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
                    exec(conn, "INSERT INTO vb_wal_par.t_parity_toast VALUES (2,"
                            + " (SELECT string_agg(md5(i::text)||repeat('y',50),'') FROM generate_series(1,300) i))");
                    exec(conn, "INSERT INTO vb_wal_par.t_parity_toast VALUES (3, repeat('x',10000))");
                    exec(conn, "UPDATE vb_wal_par.t_parity_toast SET id = id + 100");
                    exec(conn, "DELETE FROM vb_wal_par.t_parity_toast WHERE id = 102");
                    conn.commit();
                }).capture().blocks();
        String joined = String.join("\n", blocks.get(0).rows());
        assertFalse(joined.contains("toast-unavailable"),
                "TOAST 三形态不应出现降级字面:\n" + joined);
        assertEquals(3, countLiteral(joined, "...(7040B)"),
                "形态① external 未压缩宽值应在 INSERT 后像 + UPDATE 前后双像共 3 处渲染 7040B 截断尾注:\n" + joined);
        assertEquals(4, countLiteral(joined, "...(24600B)"),
                "形态② external pglz 宽值应在 INSERT 后像 + UPDATE 前后双像 + DELETE 前像共 4 处渲染:\n" + joined);
        assertEquals(3, countLiteral(joined, "...(10000B)"),
                "形态③ 行内压缩宽值应在 INSERT 后像 + UPDATE 前后双像共 3 处渲染:\n" + joined);
        assertEquals(0, countLiteral(joined, "<toast-unchanged>"),
                "UPDATE 未变 external 列应恒带完整值（重写值、非 'u' 占位——OG 语义预期同形）"
                        + "——unchanged 应零处:\n" + joined);
    }

    /** 场景 3 重启回查用例的检查点目录（@TempDir 每用例独立——一目录一活实例契约）。 */
    @TempDir
    Path toastStateDir;

    /**
     * 场景 3b：重启续传 + 未变宽列 unchanged——wal 路两段会话（state.dir 检查点续传）：
     * 段一 INSERT 宽值、close 落检查点；段二续传（{@code resumedFromState} 钉前提）后
     * UPDATE 只改 id。基准路单逻辑槽收两块，按 xid 交集对拍 diff 空；值面断言：UPDATE
     * 后像双路同渲染（OG 语义恒带完整重写值——Java 独立复算截断形态）、无降级字面；段一
     * 事务的重发（页对齐重叠窗口）由重复块全等容忍面吸收。
     */
    @Test
    void restartUpdateUnchangedWideColumnRendersUnchangedToastParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity_toast (id int4, w text)",
                "ALTER TABLE vb_wal_par.t_parity_toast REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"vb_wal_par.t_parity_toast"});
        slots("tst2");
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_parity_toast"},
                toastStateDir.toString()), capture);
        WalSource wal2 = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_parity_toast"},
                toastStateDir.toString()), capture);
        boolean resumed = false;
        try {
            wal.start();
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                exec(conn, "INSERT INTO vb_wal_par.t_parity_toast VALUES (1,"
                        + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
            }
            await(() -> wal.dmlEmittedBuckets() >= 1,
                    "TOAST 重启续传: 段一 wal 路未发射 INSERT 桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            wal.close();   // close = 最终 best-effort 检查点 + 槽推进（段二续传起点）
            wal = wal2;
            wal2.start();
            resumed = wal2.resumedFromState();
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                exec(conn, "UPDATE vb_wal_par.t_parity_toast SET id = 101 WHERE id = 1");
            }
            await(() -> wal2.dmlEmittedBuckets() >= 1,
                    "TOAST 重启续传: 段二 wal 路未发射 UPDATE 桶（buckets=" + wal2.dmlEmittedBuckets()
                            + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        } finally {
            wal2.close();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）");
        List<OgBlock> baseline = OgParityBaseline.capture(endpoint(), BASELINE_APP, logicalSlot,
                PUBLICATION, 2, CAPTURE_MS);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "TOAST 重启续传", 0);

        String wide = md5Chain(220);
        String truncated = wide.substring(0, 64) + "...(" + wide.length() + "B)";
        List<String> updateRows = capture.blocks().stream()
                .flatMap(b -> b.rows().stream()).filter(l -> l.contains("UPDATE vb_wal_par.t_parity_toast")).toList();
        assertEquals(1, updateRows.size(), "应恰一行 UPDATE（宽列未变）:\n" + capture.blocks());
        String update = updateRows.get(0);
        // OG 值面语义预期同形（2026-10-12 OG 实测锚）：UPDATE 未变 external 列的后像恒带
        // 完整值（重写 toast 值、基准路发真值），非 vanilla 的 'u' 占位——窗口前指针在段二
        // 由重写的新 chunk 归集面覆盖，双路同渲染完整宽值截断形态。
        assertEquals(2, countLiteral(update, truncated),
                "UPDATE 前后双像应各恰一处渲染完整宽值截断形态:\n" + update);
        assertTrue(update.contains("AFTER=[id=101, w=" + wide.substring(0, 16)),
                "UPDATE 后像应带完整宽值（非 unchanged 占位）:\n" + update);
        assertFalse(capture.blocks().stream().flatMap(b -> b.rows().stream())
                .anyMatch(l -> l.contains("toast-unavailable")), "不应出现降级字面");
    }

    // ---- 场景 4：DDL-in-txn as-of ----

    /**
     * 场景 4：事务内 ADD COLUMN 前后段 as-of——单事务先 INSERT 两列行 → ADD COLUMN →
     * INSERT 三列行 → UPDATE 旧行（前像两列元组 + 缺席新列 NULL、后像三列），COMMIT 后
     * 追加一笔 DDL 后控制事务（三列行直插 + 更新）。
     *
     * <p><b>双路形态裁定（T6 首跑实测锚，2026-10-13）</b>：VastBase 逻辑解码器对事务内
     * DDL 的后续行<b>正常解码</b>——基准路 DDL 事务块三行与 wal 路逐字面全等（<b>与
     * vanilla 同形、与 OG 的静默丢弃分叉</b>——OG 档的"基准路恰一行"裁定在 VastBase
     * 反转）。断言面：wal 直解路（as-of 字典的真正受测面）DDL 事务块三行逐字面断言
     * （前段行两列渲染/后段行三列渲染/UPDATE 前像三列词典缺席新列渲 NULL）+ 两块
     * <b>全量 parity</b>（DDL 事务 + 控制事务——字典跨 DDL 演进后双路全程全等）。</p>
     */
    @Test
    void inTxnAddColumnAsOfRenderingParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_ddlin (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_ddlin REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"vb_wal_par.t_ddlin"});
        slots("ddl1");
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_ddlin"}, null), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "事务内 ADD COLUMN as-of: wal 直解路 DML 面应装配");
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                conn.setAutoCommit(true);
                exec(conn, "BEGIN");
                exec(conn, "INSERT INTO vb_wal_par.t_ddlin VALUES (1, 'before-ddl')");
                exec(conn, "ALTER TABLE vb_wal_par.t_ddlin ADD COLUMN extra text");
                exec(conn, "INSERT INTO vb_wal_par.t_ddlin VALUES (2, 'after-ddl', 'new-col')");
                exec(conn, "UPDATE vb_wal_par.t_ddlin SET name = 'before-ddl-upd', extra = 'filled' WHERE id = 1");
                exec(conn, "COMMIT");
                exec(conn, "BEGIN");
                exec(conn, "INSERT INTO vb_wal_par.t_ddlin VALUES (3, 'post-ddl-ctrl', 'ctrl')");
                exec(conn, "UPDATE vb_wal_par.t_ddlin SET extra = 'ctrl-upd' WHERE id = 3");
                exec(conn, "COMMIT");
            }
            await(() -> wal.dmlEmittedBuckets() >= 2,
                    "事务内 ADD COLUMN as-of: wal 路未发射 2 个桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } finally {
            wal.close();
        }
        List<OgBlock> baseline = OgParityBaseline.capture(endpoint(), BASELINE_APP, logicalSlot,
                PUBLICATION, 2, CAPTURE_MS);
        List<OgBlock> walBlocks = capture.blocks();
        assertEquals(2, walBlocks.size(), "wal 路应恰两块（DDL 事务 + 控制事务）:\n" + walBlocks);
        List<String> ddlTxnRows = walBlocks.get(0).rows();
        assertEquals(3, ddlTxnRows.size(), "DDL 事务块应恰三行（as-of 前后段 + UPDATE）:\n" + ddlTxnRows);
        assertEquals("INSERT vb_wal_par.t_ddlin BEFORE=- AFTER=[id=1, name=before-ddl]", ddlTxnRows.get(0),
                "DDL 前段行应按两列词典渲染");
        assertEquals("INSERT vb_wal_par.t_ddlin BEFORE=- AFTER=[id=2, name=after-ddl, extra=new-col]",
                ddlTxnRows.get(1), "DDL 后段行应按三列词典渲染");
        assertEquals("UPDATE vb_wal_par.t_ddlin BEFORE=[id=1, name=before-ddl, extra=NULL]"
                        + " AFTER=[id=1, name=before-ddl-upd, extra=filled]", ddlTxnRows.get(2),
                "UPDATE 前像三列词典缺席新列渲 NULL、后像三列（as-of 语义）");
        // VastBase 解码器不丢弃 DDL 后续行（vanilla 同形、OG 分叉——首跑实测锚）：
        // DDL 事务 + 控制事务两块全量 parity，字典跨 DDL 演进后双路全程全等
        OgParityBaseline.assertParity(baseline, walBlocks, "事务内 ADD COLUMN", 0);
    }

    // ---- 场景 5：2PC 四形态 ----

    /**
     * 场景 5 前三形态（沿 OG 档裁定走 assertParity + kind 归一化）：①挂起期零输出——
     * {@code PREPARE TRANSACTION 'gt1'} 后 1.5s 挂起窗口两路皆零输出；②弃桶——
     * {@code ROLLBACK PREPARED 'gt2'} 零帧；③{@code COMMIT PREPARED 'gt3'} 双路各发完整
     * 事务块（基准路 NORMAL、wal 路 TWO_PHASE+gid——同一事务两侧表述）。
     *
     * <p>事务控制面沿 OG 档裁定：全程 autoCommit=true + 显式 BEGIN——PREPARE TRANSACTION
     * 会在服务端结束会话事务，驱动的自动提交记账不知情。收尾 {@code ROLLBACK PREPARED
     * 'gt1'} 释放挂起（仍零输出）。</p>
     */
    @Test
    void twoPhaseSuspendDiscardAndCommitPreparedParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        ParityRun run = runParity("2PC 挂起/弃桶/COMMIT PREPARED", "tp1", ddl,
                new String[]{"vb_wal_par.t_parity"}, 1, 1, conn -> {
                    conn.setAutoCommit(true);
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'gt1-pending')");
                    exec(conn, "PREPARE TRANSACTION 'gt1'");
                    Thread.sleep(1500);   // 挂起窗口实宽——任一路早发即块数超 1 暴露
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (2, 'gt2-doomed')");
                    exec(conn, "PREPARE TRANSACTION 'gt2'");
                    exec(conn, "ROLLBACK PREPARED 'gt2'");
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (3, 'gt3-commit')");
                    exec(conn, "PREPARE TRANSACTION 'gt3'");
                    exec(conn, "COMMIT PREPARED 'gt3'");
                    exec(conn, "ROLLBACK PREPARED 'gt1'");   // 收尾释放 gt1 挂起（弃桶二次覆盖）
                });
        List<OgBlock> blocks = run.capture().blocks();
        assertEquals(1, blocks.size(), "2PC 场景 wal 路应恰一个事务块（gt3）:\n" + blocks);
        List<ChangeOutputListener.BatchBegin> begins = run.capture().begins();
        assertEquals(1, begins.size(), "块数与头事件数应一一对应");
        ChangeOutputListener.BatchBegin only = begins.get(0);
        assertTrue(only.twoPhase(), "COMMIT PREPARED 块应为 TWO_PHASE（基准路 proto v1 同事务以 NORMAL 块出现——归一化裁定）");
        assertEquals("gt3", only.gid(), "COMMIT PREPARED 块 gid 应为 gt3");
        assertEquals(1, blocks.get(0).rows().size(), "gt3 块应恰一行");
        assertTrue(blocks.get(0).rows().get(0).contains("gt3-commit"), "gt3 行值全对:\n"
                + blocks.get(0).rows().get(0));
    }

    /** 场景 5 挂起期重启续传用例的检查点目录。 */
    @TempDir
    Path tpStateDir;

    /**
     * 场景 5 第四形态：挂起期重启续传——wal 路两段会话：段一 DML+{@code PREPARE
     * TRANSACTION 'gtx'} 后追加 &gt;8KB filler 事务（250 行，把 close 检查点前沿确定性
     * 推过 PREPARE 所在页——纯页重叠救不回桶，钉 pendingFloorLsn 重放重建路径），close
     * 落检查点；段二同 state.dir 续传，{@code COMMIT PREPARED 'gtx'} 到达后发射。基准路
     * 单逻辑槽收 filler+gtx 两块，按 xid 交集对拍 diff 空；wal 侧专项断言 TWO_PHASE+
     * gid=gtx（重放重建桶的终态钉）。
     */
    @Test
    void twoPhasePreparedPendingSurvivesRestartResumeParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"vb_wal_par.t_parity"});
        slots("tp2");
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_parity"},
                tpStateDir.toString()), capture);
        WalSource wal2 = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_parity"},
                tpStateDir.toString()), capture);
        boolean resumed = false;
        try {
            wal.start();
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                conn.setAutoCommit(true);
                exec(conn, "BEGIN");
                exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (10, 'gtx-pending')");
                exec(conn, "PREPARE TRANSACTION 'gtx'");
                exec(conn, "INSERT INTO vb_wal_par.t_parity SELECT g, 'filler-' || g FROM generate_series(11, 260) g");
            }
            await(() -> wal.dmlEmittedBuckets() >= 1,
                    "2PC 重启续传: 段一 wal 路未发射 filler 桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            wal.close();   // close = 最终检查点（前沿已过 PREPARE 所在页；槽推进按挂起桶 floor 封顶）
            wal = wal2;
            wal2.start();
            resumed = wal2.resumedFromState();
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                exec(conn, "COMMIT PREPARED 'gtx'");
            }
            await(() -> wal2.dmlEmittedBuckets() >= 2,
                    "2PC 重启续传: 段二 wal 路未发射 COMMIT PREPARED 桶（重放重建桶失效——buckets="
                            + wal2.dmlEmittedBuckets() + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        } finally {
            wal2.close();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）");
        List<OgBlock> baseline = OgParityBaseline.capture(endpoint(), BASELINE_APP, logicalSlot,
                PUBLICATION, 2, CAPTURE_MS);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "2PC 重启续传", 0);

        ChangeOutputListener.BatchBegin gtxBegin = capture.begins().stream()
                .filter(b -> "gtx".equals(b.gid())).findFirst()
                .orElseThrow(() -> new AssertionError("wal 路应有 gid=gtx 的 TWO_PHASE 块（重放重建桶）: "
                        + capture.begins()));
        assertTrue(gtxBegin.twoPhase(), "gtx 块应为 TWO_PHASE（重放重建桶的终态钉）");
    }

    // ---- 场景 6：中途停续 ----

    /** 场景 6 的检查点目录。 */
    @TempDir
    Path lifecycleStateDir;

    /**
     * 场景 6：普通 DML 中途停续（at-least-once）——wal 路两段会话：段一（state.dir）捕
     * 两笔 INSERT 事务，close 落检查点；段二续传（{@code resumedFromState} 钉前提）先
     * {@code ADD COLUMN}（字典经检查点持久化后流内演进——OG 档对 DDL 事务发空 B..C 块、
     * 基准路 C 帧计数含它，VastBase 同源预期；实测无空块即 capture 超时红——按真值改
     * 计数记档）再一笔 I/U/D 混合事务。基准路单逻辑槽收 4 帧（2 行事务 + 1 DDL 空块 +
     * 1 混合事务），按 xid 交集对拍 diff 空——段一同页事务的整桶幂等重发由重复块全等容忍
     * 面吸收；专项：wal 并集覆盖两行块 + 混合块。
     */
    @Test
    void midStreamStopResumePlainDmlParityAcrossPaths() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_parity REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"vb_wal_par.t_parity"});
        slots("lf1");
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_parity"},
                lifecycleStateDir.toString()), capture);
        WalSource wal2 = new WalSource(walSourceConfig(physicalSlot, new String[]{"vb_wal_par.t_parity"},
                lifecycleStateDir.toString()), capture);
        boolean resumed = false;
        try {
            wal.start();
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (1, 'seg1-a')");
                exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (2, 'seg1-b')");
            }
            await(() -> wal.dmlEmittedBuckets() >= 2,
                    "生命周期停续: 段一 wal 路未发射 2 个桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            wal.close();
            wal = wal2;
            wal2.start();
            resumed = wal2.resumedFromState();
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                exec(conn, "ALTER TABLE vb_wal_par.t_parity ADD COLUMN extra text");
                conn.setAutoCommit(false);
                exec(conn, "INSERT INTO vb_wal_par.t_parity VALUES (3, 'seg2-c', 'seg2-new')");
                exec(conn, "UPDATE vb_wal_par.t_parity SET name = 'seg1-a-upd', extra = 'backfilled' WHERE id = 1");
                exec(conn, "DELETE FROM vb_wal_par.t_parity WHERE id = 2");
                conn.commit();
            }
            await(() -> wal2.dmlEmittedBuckets() >= 1,
                    "生命周期停续: 段二 wal 路未发射段二桶（buckets=" + wal2.dmlEmittedBuckets()
                            + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        } finally {
            wal2.close();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）");
        List<OgBlock> baseline = OgParityBaseline.capture(endpoint(), BASELINE_APP, logicalSlot,
                PUBLICATION, 4, CAPTURE_MS);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "生命周期停续", 0);

        // 专项：wal 两段会话并集覆盖基准路全部行事务 xid（空块 xid 属 wal 侧零发射面，天然缺席）
        assertTrue(capture.blocks().size() >= 3,
                "wal 路并集应至少含段一 2 块 + 段二 1 块（重发只增不改）:\n" + capture.blocks());
        LOG.info("生命周期停续重发观测: wal 块总数={}（含 at-least-once 重发面）", capture.blocks().size());
    }

    // ---- 场景 7：VastBase 档 TRUNCATE 双路不可见（spike 裁决 C） ----

    /**
     * 场景 7：VastBase 档 TRUNCATE「双路不可见」语义钉（spike 裁决 C——记录面<b>实锚</b>：
     * 真库 test_a rmid=10 全窗 50318 条零 0x30，TRUNCATE 纯 catalog 手术；帧面按 OG 同内核
     * 推定不发 'T'——本场景即该未复验面的端到端补验，首跑红即按真值改锚记档，
     * {@code docs/wal-source-truncate-spike.md} §4）。三面断言（Task 6 OG 档同构，A 档
     * 语法形态）：
     * <ul>
     *   <li><b>① 零事件</b>：两笔 TRUNCATE 事务（单表 + 多表）wal 路零桶零发射——接收
     *       前沿越过末笔 TRUNCATE 的 write 位点后才 close（杜绝未消费窄窗的假绿），精确
     *       断言桶数 = 三笔 INSERT、{@code dmlEmittedTruncates} 恒 0、块内零 TRUNCATE 行；
     *       基准路恰 2 个空 {@code B..C} 块（VastBase pgoutput proto v1 无 'T' 帧——spike
     *       §4.2 推定面，DDL 事务空块先例场景 6）且按 xid 交集外不进对拍面，三笔 INSERT
     *       对拍 diff 空。</li>
     *   <li><b>② 截后字典不倒</b>：种子行（截前段号）与截后 INSERT（TRUNCATE 的 catalog
     *       手术换段号后的新段号行——VastBase 分叉：pg_class UPDATE main 前缀 21B，既有
     *       ctid 重放字典面跟踪）双路同捕获，逐字面钉 TableMeta（表名/列序/值渲染）。</li>
     *   <li><b>③ 不砖流</b>：全程接收器无终态失败——TRUNCATE 语句族的 WAL 记录（Standby
     *       LOCK info=0x01 / SMGR CREATE / NEW_CID / pg_class UPDATE / Btree / NEWPAGE FPI /
     *       HOT_UPDATE / XACT 大记录）流经 grouper 全静默，无 ISE。</li>
     * </ul>
     * 不用 {@code RESTART IDENTITY}——A 档 SQL 层拒绝（{@code ERROR: TRUNCATE .. RESTART
     * IDENTITY is only supported in PG compatibility}，spike §4 实锚）；单表/多表形态沿
     * spike 工作负载实测语法（{@code TRUNCATE t1} / {@code TRUNCATE t1, t2}）。
     */
    @Test
    void truncateInvisibleOnBothPathsPostTruncateDmlParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE vb_wal_par.t_trunc (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_trunc REPLICA IDENTITY FULL",
                "CREATE TABLE vb_wal_par.t_trunc2 (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE vb_wal_par.t_trunc2 REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"vb_wal_par.t_trunc", "vb_wal_par.t_trunc2"});
        slots("trunc1");
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot,
                new String[]{"vb_wal_par.t_trunc", "vb_wal_par.t_trunc2"}, null), capture);
        long boundary;
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "TRUNCATE 双路不可见: wal 直解路 DML 面应装配");
            try (Connection conn = VastTestEnv.newSqlConnection()) {
                conn.setAutoCommit(true);
                // 种子行：截前段号上的正常捕获（对照面——截后行与它同走字典解析）
                exec(conn, "INSERT INTO vb_wal_par.t_trunc VALUES (1, 'seed-1')");
                exec(conn, "INSERT INTO vb_wal_par.t_trunc2 VALUES (1, 'seed-2')");
                // TRUNCATE 事务一（单表）：spike 锚——纯 catalog 手术、零 heap 记录
                exec(conn, "TRUNCATE vb_wal_par.t_trunc");
                // 截后 INSERT：catalog 手术换段号后的新段号行——字典正确性受测面
                exec(conn, "INSERT INTO vb_wal_par.t_trunc VALUES (2, 'after-truncate')");
                // TRUNCATE 事务二（多表同形）
                exec(conn, "TRUNCATE vb_wal_par.t_trunc, vb_wal_par.t_trunc2");
                boundary = VastTestEnv.writeLsn(conn);
            }
            await(() -> wal.dmlEmittedBuckets() >= 3,
                    "TRUNCATE 双路不可见: wal 路未发射 3 个 INSERT 桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            // 接收前沿越过末笔 TRUNCATE 后才收尾——“零桶零发射”的精确断言不容未消费窄窗
            await(() -> wal.consumedLsn() >= boundary,
                    "TRUNCATE 双路不可见: wal 路接收前沿未越过末笔 TRUNCATE（consumed="
                            + Lsn.format(wal.consumedLsn()) + ", boundary=" + Lsn.format(boundary)
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } finally {
            wal.close();
        }
        List<OgBlock> baseline = OgParityBaseline.capture(endpoint(), BASELINE_APP, logicalSlot,
                PUBLICATION, 5, CAPTURE_MS);
        List<OgBlock> walBlocks = capture.blocks();
        OgParityBaseline.assertParity(baseline, walBlocks, "TRUNCATE 双路不可见", 0);

        // ③ 不砖流：全程无终态失败（TRUNCATE 语句族 WAL 记录流经 grouper 全静默——无 ISE）
        assertTrue(wal.receiverTerminalFailure().isEmpty(),
                "全程应无接收器终态失败（TRUNCATE 语句族 WAL 记录流经 grouper 应全静默）: "
                        + wal.receiverTerminalFailure());
        // ① 零事件——wal 路无桶零发射（接收前沿已越过末笔 TRUNCATE，精确断言）
        assertEquals(3, wal.dmlEmittedBuckets(), "桶数应恰 = 三笔 INSERT（两笔 TRUNCATE 事务零桶）");
        assertEquals(3, wal.dmlEmittedRows(), "行数应恰 = 三笔 INSERT 各一行");
        assertEquals(0, wal.dmlEmittedTruncates(),
                "VastBase 档 TRUNCATE 记录面缺席（rmid=10 全窗零 0x30）——truncate 发射计数应恒 0（spike 裁决 C）");
        assertEquals(3, walBlocks.size(), "wal 捕获应恰 3 块:\n" + walBlocks);
        assertTrue(walBlocks.stream().flatMap(b -> b.rows().stream()).noneMatch(r -> r.startsWith("TRUNCATE")),
                "wal 块内不应有任何 TRUNCATE 行（两笔 TRUNCATE 事务零发射）:\n" + walBlocks);
        // ① 零事件——基准路恰 2 个空 B..C 块（无 'T' 帧，spike §4.2 推定面）且不进对拍交集
        assertEquals(5, baseline.size(), "基准路应恰 5 块（3 INSERT 块 + 2 TRUNCATE 空块）:\n" + baseline);
        assertEquals(2, baseline.stream().filter(b -> b.rows().isEmpty()).count(),
                "两笔 TRUNCATE 事务应解码为恰 2 个空 B..C 块（VastBase pgoutput 无 'T' 帧——推定面）:\n" + baseline);
        // ② 截后字典不倒：种子行（截前段号）与截后行（换段号后）逐字面钉 TableMeta
        assertEquals(1, walBlocks.get(0).rows().size(), "块一应恰一行:\n" + walBlocks.get(0));
        assertEquals(1, walBlocks.get(1).rows().size(), "块二应恰一行:\n" + walBlocks.get(1));
        assertEquals(1, walBlocks.get(2).rows().size(), "块三应恰一行:\n" + walBlocks.get(2));
        assertEquals("INSERT vb_wal_par.t_trunc BEFORE=- AFTER=[id=1, name=seed-1]",
                walBlocks.get(0).rows().get(0), "种子行（截前段号）应按正确 TableMeta 渲染");
        assertEquals("INSERT vb_wal_par.t_trunc2 BEFORE=- AFTER=[id=1, name=seed-2]",
                walBlocks.get(1).rows().get(0), "第二表种子行应按正确 TableMeta 渲染");
        assertEquals("INSERT vb_wal_par.t_trunc BEFORE=- AFTER=[id=2, name=after-truncate]",
                walBlocks.get(2).rows().get(0),
                "截后 INSERT 应按正确 TableMeta 渲染——TRUNCATE 的 pg_class 换段号 UPDATE（main 前缀 21B 分叉）由既有 ctid 重放字典面跟踪");
    }

    // ---- 对拍骨架 ----

    /**
     * 对拍结局快照：已关闭的 WalSource（观测计数面停流后读）+ 捕获器（块/头事件面，2PC
     * 专项断言的 gid/twoPhase 取用）。
     *
     * @param source  wal 直解路门面（已 close——计数读取安全）
     * @param capture 捕获器（blocks/begins 快照）
     */
    record ParityRun(WalSource source, OgParityBaseline.WalCapture capture) {
    }

    /**
     * 单场景对拍骨架（无 state.dir 档）：重置场景 → 建逻辑槽 → 起 wal 直解路（判别 →
     * {@code WalLayoutVVastbase} → direct 通道 + opengauss-jdbc 前缀 + 白名单钉场景表）→
     * DML → await 桶数 → 基准路 capture → close → assertParity，返回对拍结局快照。
     *
     * @param scenario      场景名（断言消息上下文）
     * @param slotSuffix    槽名后缀（前缀 vb_wal_par_logic_/vb_wal_par_wal_ 固定）
     * @param tableDdl      场景建表语句（vb_wal_par schema 内）
     * @param whitelist     DML 白名单表全名集（兼 publication 的 FOR TABLE 集）
     * @param walTxns       wal 路追平目标（发射桶数）
     * @param baselineTxns  基准路 capture 的 C 帧计数目标
     * @param dml           DML 执行器
     * @return 对拍结局快照（已关闭 WalSource + 捕获器）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    ParityRun runParity(String scenario, String slotSuffix, String[] tableDdl, String[] whitelist,
            long walTxns, long baselineTxns, SqlConsumer<Connection> dml) throws Exception {
        resetScenario(tableDdl, whitelist);
        slots(slotSuffix);
        OgParityBaseline.createLogicalSlot(endpoint(), logicalSlot);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, whitelist, null), capture);
        wal.start();
        assertTrue(wal.dmlEnabled(), scenario + ": wal 直解路 DML 面应装配");
        try (Connection conn = VastTestEnv.newSqlConnection()) {
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
     * 场景重置：schema CASCADE 整体清场 + 场景建表 + publication 重建（<b>显式 FOR TABLE
     * 场景表白名单</b>——A 档形态 + 真库窗口钉场景表，见类 javadoc 卫生段）。DDL 先于
     * 建槽/引导（spike 坑 2 的时序契约）。
     *
     * @param tableDdl  场景建表语句
     * @param whitelist publication 的 FOR TABLE 集（schema 限定全名）
     * @throws SQLException DDL 失败
     */
    static void resetScenario(String[] tableDdl, String[] whitelist) throws SQLException {
        try (Connection c = VastTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            st.execute("CREATE SCHEMA " + SCHEMA);
            for (String ddl : tableDdl) {
                st.execute(ddl);
            }
            st.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION);
            st.execute("CREATE PUBLICATION " + PUBLICATION + " FOR TABLE " + String.join(", ", whitelist));
        }
    }

    /**
     * 设定当前用例的两路槽名（{@code vb_wal_} 前缀——真库卫生纪律）并前置清理残留。
     *
     * @param suffix 后缀（槽名 = vb_wal_par_logic_/vb_wal_par_wal_ + suffix）
     */
    void slots(String suffix) {
        logicalSlot = "vb_wal_par_logic_" + suffix;
        physicalSlot = "vb_wal_par_wal_" + suffix;
        OgParityBaseline.dropLogicalSlotQuietly(endpoint(), BASELINE_APP, logicalSlot);
        VastTestEnv.dropPhysicalSlotQuietly(physicalSlot);
    }

    /**
     * 基准路连接档：{@link VastTestEnv} 的 SQL 会话 URL 与凭证——md5 码 5 直连（T0 spike
     * §1 实测锚，无 OG 档码 11 死角，无需专用角色）。
     *
     * @return 基准路连接档（复制/建槽/删槽共用凭证）
     */
    static BaselineEndpoint endpoint() {
        return new BaselineEndpoint(VastTestEnv.jdbcUrl(), VastTestEnv.user(), VastTestEnv.pass());
    }

    /**
     * wal 直解路门面配置：{@link VastTestEnv#params()} 六基础键 + OG 档 direct 通道 +
     * opengauss-jdbc 前缀（门面判别真库 → {@code WalLayoutVVastbase} → OG 装配分支）+
     * DML 白名单钉场景表；stateDir 非 null 时启用检查点（续传场景）。
     *
     * @param physicalSlot 物理槽名
     * @param whitelist    DML 白名单表全名集
     * @param stateDir     检查点目录（null = 禁用）
     * @return WalSource 构造入参配置
     */
    static Properties walSourceConfig(String physicalSlot, String[] whitelist, String stateDir) {
        Properties cfg = VastTestEnv.params();
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
     * {@code vb_wal_} 前缀对象全数退场，不触碰既有业务对象）。
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
            VastTestEnv.dropPhysicalSlotQuietly(physicalSlot);
        }
        try {
            VastTestEnv.sql("DROP PUBLICATION IF EXISTS " + PUBLICATION,
                    "DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        } catch (SQLException e) {
            LOG.warn("清理 VastBase 对拍 publication/schema 失败（忽略）: {}", e.getMessage());
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
     * 客户端侧 COPY FROM STDIN（{@code OgDmlParityIT#copyIn} 的真库形态——固定
     * {@link VastTestEnv} 连接档，vastbase T7 场景 1g 供用）：服务端侧 COPY 文件被
     * OG 系安全策略禁止（OG M1 实测锚，VastBase 同源）。
     *
     * @param sql COPY 命令
     * @param csv 装载文本
     * @return 装载行数
     * @throws SQLException COPY 失败
     */
    private static long copyIn(String sql, String csv) throws SQLException {
        try (Connection c = VastTestEnv.newSqlConnection()) {
            CopyManager cp = new CopyManager(c.unwrap(BaseConnection.class));
            return cp.copyIn(sql, new StringReader(csv));
        } catch (IOException e) {
            throw new SQLException("COPY FROM STDIN 流读写失败", e);
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

    /**
     * 复算场景值的确定性 md5 链（与 SQL {@code string_agg(md5(i::text),'')} 逐字面一致）。
     *
     * @param count 链节数（220 → 7040 字符）
     * @return 拼接后的宽值原文
     */
    private static String md5Chain(int count) {
        StringBuilder out = new StringBuilder(32 * count);
        for (int i = 1; i <= count; i++) {
            out.append(md5Hex(Integer.toString(i)));
        }
        return out.toString();
    }

    /**
     * 单值 md5 小写 hex（PG {@code md5()} 同形——32 位零补齐小写）。
     *
     * @param s 输入串
     * @return 32 位 hex
     */
    private static String md5Hex(String s) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM 缺 MD5 算法（规范保证提供）", e);
        }
        StringBuilder out = new StringBuilder(32);
        for (byte b : md.digest(s.getBytes(StandardCharsets.UTF_8))) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    /** 单参 DML 执行器（可抛受检异常的消费者）。 */
    @FunctionalInterface
    interface SqlConsumer<T> {

        /** 执行场景 DML。 @param t 连接 @throws Exception 底层异常 */
        void accept(T t) throws Exception;
    }
}
