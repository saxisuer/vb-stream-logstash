package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;

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
 * openGauss M2 双路对拍六场景全量 IT（T7 Step 1）——{@code DualPathParityIT}（PG 18 版）
 * 的场景配方逐个翻译到 m2pg（PG 兼容档）库：wal 直解路（{@link WalSource} DML 面 +
 * {@link OgParityBaseline.WalCapture}）vs 逻辑解码基准路（{@link OgParityBaseline}），
 * 按 xid 交集归一化 diff 空（对齐 PG 版对拍语义；engine 路不可用——vanilla pgjdbc 对
 * OG 逻辑流首读 FATAL，T0 路③实证）。六场景对照：
 *
 * <ul>
 *   <li><b>场景 1 基础 DML 六形态</b>：单事务多语句（I/U/D 混合 + NULL 行）/ 双连接
 *       交错 / 回滚零输出 / SAVEPOINT 子事务剔除（walAborted=1）/ 中段列 UPDATE
 *       （FULL 身份整行前像）+ <b>COPY 批量装载</b>（MULTI_INSERT 真几何的端到端
 *       验收——T7 修复 A：客户端侧 CopyManager STDIN，服务端文件 COPY 被 OG 禁）。</li>
 *   <li><b>场景 2 17 类型边界值</b>：类型全集与边界值取两路渲染矩阵交面内的
 *       OG-可达值——<b>值面裁定（og-plan 2026-10-12 实测）</b>：OG 服务端缺省
 *       {@code extra_float_digits=0} 且 (0,1) 区间值的文本输出<b>省略前导零</b>
 *       （{@code 0.0001} 渲 {@code .0001}，float4 max 渲 6 位有效数字）——与
 *       wal 路 {@code DiskValueRenderer}/{@code PgFloatFormat} 的 vanilla 同形渲染
 *       真实分叉，本场景避开该区间/该精度形态（取 {@code 1e+06}/{@code 1e+20}/
 *       {@code 3.5}/{@code Infinity}/{@code NaN} 与 &gt;1 numeric），差异面与归一化
 *       建议记档 T7 报告（主代码渲染面 OG 方言分支超出本任务 A/B 范围）。</li>
 *   <li><b>场景 3 TOAST 三形态 + 重启 unchanged</b>：external 未压缩 / external pglz /
 *       行内压缩 + 两段会话（检查点续传）后未变宽列的双路同渲染。</li>
 *   <li><b>场景 4 DDL-in-txn as-of</b>：事务内 ADD COLUMN 前后段各按变更时刻列集渲染
 *       ——<b>OG 基准路丢弃面裁定（og-plan 实测锚，改编裁定⑤）</b>：OG 逻辑解码器对
 *       事务内 DDL 的后续行<b>静默丢弃</b>（帧序恰 B..R(旧列)..I(首行)..C），DDL 后新
 *       事务正常解码——故本场景 wal 路三行逐字面断言（as-of 的真正受测面）+ 基准路
 *       DDL 事务块恰一行（丢弃面钉）+ DDL 后控制事务双路 full parity。</li>
 *   <li><b>场景 5 2PC 四形态</b>：<b>探测裁定（og-plan 2026-10-12 实测锚）</b>——
 *       proto v1 逻辑路（无 two_phase 槽选项）对 PREPARE→COMMIT PREPARED 以普通
 *       B..C 块出现（解码推迟到 COMMIT PREPARED 时点：prepared xid 的块出现在更晚
 *       普通事务块之后；ROLLBACK PREPARED 零帧），故走 assertParity + kind 归一化
 *       （wal 路 TWO_PHASE+gid vs 基准路 NORMAL——同一服务端事务的两侧表述，gid
 *       断言走 {@link OgParityBaseline.WalCapture#begins()} 事件面）。</li>
 *   <li><b>场景 6 中途停续</b>：state.dir 两轮启停 + 段二 DDL+DML，at-least-once 重发
 *       由重复块全等容忍面吸收。</li>
 * </ul>
 *
 * <p><b>A 档全量复验（M3 Task 5 Step 1——T4 参数化基建的消费面）</b>：全部 13 场景经
 * {@link OgTestEnv.CompatMode} 维度参数化（场景体零复制——每个场景是单一
 * {@code xxxScenario(CompatMode)} 方法，PG/A 两档 {@code @Test} 各自委派），在 m2a
 * （A 档 = Oracle 风格）整套重跑；<b>对拍断言零放宽</b>（两路仍按 xid 交集逐行 diff
 * 空，A 档 PG 档共用同一 assertParity 与全部字面断言）。A 档实测差异面（og-plan
 * 2026-10-13 双档矩阵对拍）：①{@code ''} 空串在 A 档入库即 NULL（服务端语义——两路
 * 同见 NULL，对拍天然吸收）；②<b>date 列底层 timestamp 化</b>——A 档 DDL 把
 * {@code date} 映射 atttypid=1114（8B 存时段，整秒粒度；PG 档 1082/4B 日粒度且带时段
 * 输入静默截断），wal 路按列 typeOid 走 timestamp 渲染、基准路 timestamp_out 同形
 * ——<b>时段零真值损失</b>，双路天然一致（专项钉 {@link #aCompatDateColumnKeepsTimeOfDayViaTimestampStorageParity}
 * ——fix round 1 探针锚）；其余 16 类型双档文本全等。槽名 A 档带 {@code _a} 后缀
 * （物理槽是集群级对象，跨库重名会互踩清理面）。</p>
 *
 * <p><b>空块语义（OG 分叉记档）</b>：OG pgoutput 对无已发布变更的事务（如 DDL）仍发
 * B..C 空块（vanilla PG 15+ 跳过空事务）——基准路 capture 的 expectedTxns 是 C 帧
 * 计数，含场景内建槽后的 DDL 空块（场景 6 的段二 ADD COLUMN）；wal 路对空块零发射，
 * 该 xid 落交集外（窗口差 INFO 留痕，不作断言失败）。</p>
 *
 * <p>骨架时序（spike 坑 2）：resetScenario（DDL）→ 建逻辑槽 → 起 wal 直解路 → DML →
 * await 桶数 → 基准路 capture → close → assertParity。表恒 {@code REPLICA IDENTITY
 * FULL}（DEFAULT 身份 'K' 段占位 vs wal 路尾列补 null 的归一化是记档差异面，T6 锚）。
 * 需本机 Docker。</p>
 */
class OgDmlParityIT {

    private static final Logger LOG = LoggerFactory.getLogger(OgDmlParityIT.class);

    /** 对拍 schema（m2pg/m2a 内——reset 以 schema CASCADE 整体清场）。 */
    private static final String SCHEMA = "parity";

    /** publication 名（FOR ALL TABLES——m2pg/m2a 专用库）。 */
    private static final String PUBLICATION = "m2par_pub";

    /** 两路各自的追平等待上限（毫秒）——含 wal 路引导余量。 */
    private static final long AWAIT_MS = 60_000;

    /** 轮询间隔（毫秒）。 */
    private static final long POLL_MS = 200;

    /** 基准路 capture 的 drain 上限（毫秒）。 */
    private static final long CAPTURE_MS = 30_000;

    /** 当前用例的两路槽名（AfterEach 兜底清理——正常路径测试内已完成）。 */
    private String logicalSlot;

    private String physicalSlot;

    /** 当前用例的兼容档（缺省 PG——{@link #slots} 的 A 档槽名后缀依据）。 */
    private OgTestEnv.CompatMode mode = OgTestEnv.CompatMode.PG;

    /** 当前用例的 WalSource（AfterEach 兜底关闭）。 */
    private WalSource wal;

    // ---- 场景 1：基础 DML 六形态 ----

    /** 场景 1a：单事务多语句 I/U/D 混合 + 全 NULL 行——行序与值双路逐字节对齐。 */
    @Test
    void multiStatementTxnParityAcrossPaths() throws Exception {
        multiStatementTxnScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 1a 的 A 档全量复验形态（M3 T5——场景体零复制，见类 javadoc）。 */
    @Test
    void multiStatementTxnParityAcrossPathsACompat() throws Exception {
        multiStatementTxnScenario(OgTestEnv.CompatMode.A);
    }

    private void multiStatementTxnScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                        + " d date, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        runParity(compatMode, "基础 DML 多语句", "dml1", ddl, new String[]{"parity.t_parity"}, 1, 1, 0, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345, '2026-10-07')");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, NULL, false, NULL, NULL)");
            exec(conn, "UPDATE parity.t_parity SET id = 11, name = 'alice2', flag = false, val = 99.5,"
                    + " d = '2026-11-01' WHERE id = 1");
            exec(conn, "UPDATE parity.t_parity SET id = 22, name = 'bob2', flag = true, val = 88.25,"
                    + " d = '2026-11-02' WHERE id = 2");
            exec(conn, "DELETE FROM parity.t_parity WHERE id = 11");
            conn.commit();
        });
    }

    /** 场景 1b：双连接交错事务——两路各自两个事务桶、桶内行归属正确、提交序一致。 */
    @Test
    void interleavedTxnsParityAcrossPaths() throws Exception {
        interleavedTxnsScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 1b 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void interleavedTxnsParityAcrossPathsACompat() throws Exception {
        interleavedTxnsScenario(OgTestEnv.CompatMode.A);
    }

    private void interleavedTxnsScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                        + " d date, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        String db = compatMode.database();
        runParity(compatMode, "交错事务", "dml2", ddl, new String[]{"parity.t_parity"}, 2, 2, 0, conn -> {
            try (Connection c1 = OgTestEnv.newSqlConnection(db);
                 Connection c2 = OgTestEnv.newSqlConnection(db)) {
                c1.setAutoCommit(false);
                c2.setAutoCommit(false);
                exec(c1, "INSERT INTO parity.t_parity VALUES (1, 'a1', true, 1.1, '2026-10-07')");
                exec(c2, "INSERT INTO parity.t_parity VALUES (101, 'b1', false, 2.2, '2026-10-08')");
                exec(c1, "INSERT INTO parity.t_parity VALUES (2, 'a2', true, 3.3, '2026-10-09')");
                exec(c2, "INSERT INTO parity.t_parity VALUES (102, 'b2', false, 4.4, '2026-10-10')");
                c1.commit();
                c2.commit();
            }
        });
    }

    /** 场景 1c：回滚零输出——两路都无回滚事务的块，随后的提交事务对拍完整。 */
    @Test
    void rolledBackTxnEmitsNothingOnBothPaths() throws Exception {
        rolledBackTxnScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 1c 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void rolledBackTxnEmitsNothingOnBothPathsACompat() throws Exception {
        rolledBackTxnScenario(OgTestEnv.CompatMode.A);
    }

    private void rolledBackTxnScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        runParity(compatMode, "回滚零输出", "dml3", ddl, new String[]{"parity.t_parity"}, 1, 1, 0, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'doomed')");
            conn.rollback();
            conn.setAutoCommit(true);
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'committed')");
        });
    }

    /**
     * 场景 1d：SAVEPOINT 子事务行剔除——两路都只见 s1/s3。<b>记账口径分叉记档（OG vs
     * vanilla）</b>：vanilla 路子回滚把被回滚行并入顶层桶记账（changes=实付+1）后发射期
     * 过滤；OG 路（T2 接缝）在 ROLLBACK TO SAVEPOINT 的 ABORT 记录即整弃子桶——被回滚
     * 行不进顶层记账（changes=实付），数据面等价（被剔除行两路都不发射），故
     * walAborted=0。T2 记档的"映射缺位子回滚可能多发一条 BatchAborted"由 WalCapture
     * 的零输出容忍面吸收（onAborted 丢桶不发块）。
     */
    @Test
    void savepointSubTxnFilteredRowsParity() throws Exception {
        savepointSubTxnScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 1d 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void savepointSubTxnFilteredRowsParityACompat() throws Exception {
        savepointSubTxnScenario(OgTestEnv.CompatMode.A);
    }

    private void savepointSubTxnScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        runParity(compatMode, "子事务 SAVEPOINT", "dml4", ddl, new String[]{"parity.t_parity"}, 1, 1, 0, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 's1')");
            exec(conn, "SAVEPOINT sp");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 's2-doomed')");
            exec(conn, "ROLLBACK TO SAVEPOINT sp");
            exec(conn, "INSERT INTO parity.t_parity VALUES (3, 's3')");
            conn.commit();
        });
    }

    /**
     * 场景 1e：中段列 UPDATE（FULL 身份整行前像）——截断跳过/重建双计数 0 哨兵
     * （wal_level=logical 下 OG 与 vanilla 同为全量记录面）。
     */
    @Test
    void midColumnUpdateFullIdentityParityAndZeroTruncationCounters() throws Exception {
        midColumnUpdateScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 1e 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void midColumnUpdateFullIdentityParityAndZeroTruncationCountersACompat() throws Exception {
        midColumnUpdateScenario(OgTestEnv.CompatMode.A);
    }

    private void midColumnUpdateScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                        + " d date, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        ParityRun run = runParity(compatMode, "中段列 UPDATE", "dml5", ddl, new String[]{"parity.t_parity"},
                1, 1, 0, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345, '2026-10-07')");
                    exec(conn, "UPDATE parity.t_parity SET name = 'alice-mid', flag = false, val = 99.5,"
                            + " d = '2026-11-01' WHERE id = 1");   // 首列 id 不变
                    conn.commit();
                });
        assertEquals(0, run.source().dmlSkippedTruncatedRows(), "截断 UPDATE 跳过计数应为 0");
        assertEquals(0, run.source().dmlReconstructedTruncatedRows(), "截断 UPDATE 重建计数应为 0（全量记录面）");
    }

    /**
     * 场景 1f：COPY 批量装载（MULTI_INSERT 真几何端到端验收——T7 修复 A）——客户端侧
     * CopyManager COPY FROM STDIN 100 行单事务，走 heap2 MULTI_INSERT 记录（块 data
     * 逐 entry [datalen u16 只计 tail][xl_heap_header][tail]、步进 SHORTALIGN(7+datalen)）
     * ；断言 100 行值全对（walk 错位在第二 entry 起即红）。CSV 载荷含 NULL/多字节/奇长
     * text 值（奇长 entry 钉对齐面）。
     */
    @Test
    void copyBatchMultiInsertParityRealEntryGeometry() throws Exception {
        copyBatchScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 1f 的 A 档全量复验形态（M3 T5——CSV 空串 NULL 语义双档一致）。 */
    @Test
    void copyBatchMultiInsertParityRealEntryGeometryACompat() throws Exception {
        copyBatchScenario(OgTestEnv.CompatMode.A);
    }

    private void copyBatchScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_copy (id int4 NOT NULL, name text, v numeric, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_copy REPLICA IDENTITY FULL",
        };
        StringBuilder csv = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            // CSV 格式的 NULL 是非引号空串（\N 是 TEXT 格式缺省——OG/vanilla 同语义）
            String name = (i % 3 == 0) ? "" : "row-" + i + (i % 2 == 0 ? "-odd" : "");
            String val = (i % 4 == 0) ? "" : i + ".25";
            csv.append(i).append(',').append(name).append(',').append(val).append('\n');
        }
        List<OgBlock> blocks = runParity(compatMode, "COPY 批量", "dml6", ddl, new String[]{"parity.t_copy"},
                1, 1, 0, conn -> {
                    copyIn(compatMode, "COPY parity.t_copy FROM STDIN WITH (FORMAT csv)", csv.toString());
                }).capture().blocks();
        assertEquals(1, blocks.size(), "COPY 单事务应恰一个块");
        assertEquals(100, blocks.get(0).rows().size(), "100 行 COPY 应逐 entry 全解出:\n"
                + blocks.get(0).rows().subList(0, Math.min(5, blocks.get(0).rows().size())));
        assertTrue(blocks.get(0).rows().get(0).contains("AFTER=[id=1, name=row-1, v=1.25]"),
                "首行值全对:\n" + blocks.get(0).rows().get(0));
        assertTrue(blocks.get(0).rows().get(2).contains("name=NULL"),
                "CSV \\N 应解为 NULL:\n" + blocks.get(0).rows().get(2));
    }

    // ---- 场景 2：17 类型边界值 ----

    /**
     * 场景 2：17 类型边界值对拍（值面裁定见类 javadoc——OG 文本输出的 (0,1) 前导零
     * 省略与 float 缺省精度分叉避开）：负数全族 / float 科学计数法交面值 / numeric
     * 深小数（&gt;1）与 NaN / UTF-8 π 与多字节 / bytea hex / 微秒精度 timestamp·
     * timestamptz / time {@code 24:00:00} 闭上端 / timetz 偏移 / uuid / 全 NULL 行 →
     * UPDATE 全列赋值（长文本 &gt;64 截断）→ DELETE 两行——双路 diff 空。
     */
    @Test
    void typeMatrixBoundaryRowsParityAcrossPaths() throws Exception {
        typeMatrixScenario(OgTestEnv.CompatMode.PG);
    }

    /**
     * 场景 2 的 A 档全量复验形态（M3 T5）：date 列的基准路方言后缀归一化见
     * {@link OgParityBaseline} 类 javadoc；{@code ''} 空串 A 档入库即 NULL（两路同见）。
     */
    @Test
    void typeMatrixBoundaryRowsParityAcrossPathsACompat() throws Exception {
        typeMatrixScenario(OgTestEnv.CompatMode.A);
    }

    private void typeMatrixScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_types (id int4 NOT NULL,"
                        + " c_bool bool, c_int2 int2, c_int8 int8,"
                        + " c_float4 float4, c_float8 float8, c_numeric numeric,"
                        + " c_text text, c_varchar varchar(100), c_bpchar bpchar(10), c_json json,"
                        + " c_bytea bytea, c_date date, c_time time, c_timetz timetz,"
                        + " c_timestamp timestamp, c_timestamptz timestamptz, c_uuid uuid,"
                        + " PRIMARY KEY (id))",
                "ALTER TABLE parity.t_types REPLICA IDENTITY FULL",
        };
        runParity(compatMode, "类型矩阵边界值", "typ1", ddl, new String[]{"parity.t_types"}, 1, 1, 0, conn -> {
            conn.setAutoCommit(false);
            exec(conn,
                    "INSERT INTO parity.t_types VALUES (1, true, -32768, -9223372036854775808,"
                            + " '3.25'::float4, '1e20'::float8, '123456789012345678901234567890.1234567890',"
                            + " 'π α β γ δ', '你好 wörld', 'abc', '{\"k\":\"v\",\"n\":[1,2,3]}'::json,"
                            + " decode('deadbeef00ff','hex'), '1999-12-31', '24:00:00',"
                            + " '12:34:56.123456+05:30', '2026-10-07 12:34:56.123456',"
                            + " '2026-10-07 04:34:56.789012+00', 'a0b1c2d3-e4f5-6789-abcd-ef0123456789')");
            exec(conn, "INSERT INTO parity.t_types VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL,"
                    + " NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            exec(conn,
                    "INSERT INTO parity.t_types VALUES (3, false, 0, 0, 'Infinity'::float4, 'NaN'::float8,"
                            + " 'NaN'::numeric, '', '', '', 'null'::json, decode('','hex'),"
                            + " '2000-01-01', '00:00:00', '00:00:00+00', '2000-01-01 00:00:00',"
                            + " '2000-01-01 00:00:00+00', '00000000-0000-0000-0000-000000000000')");
            exec(conn,
                    "UPDATE parity.t_types SET id = 11, c_bool = false, c_int2 = 32767,"
                            + " c_int8 = 9223372036854775807, c_float4 = '2.5'::float4,"
                            + " c_float8 = '3.5'::float8,"
                            + " c_numeric = '987654321098765432109876543210.9876543210',"
                            + " c_text = repeat('π', 100), c_varchar = '-42.5', c_bpchar = 'xyz',"
                            + " c_json = '[1, \"two\", null]'::json, c_bytea = decode('ab','hex'),"
                            + " c_date = '2026-10-07', c_time = '12:34:56.1',"
                            + " c_timetz = '23:59:59.999999-08:00', c_timestamp = '2026-10-07 12:34',"
                            + " c_timestamptz = '2026-10-07 23:59:59.999999+00',"
                            + " c_uuid = 'ffffffff-ffff-ffff-ffff-ffffffffffff' WHERE id = 1");
            exec(conn, "DELETE FROM parity.t_types WHERE id = 2");
            exec(conn, "DELETE FROM parity.t_types WHERE id = 3");
            conn.commit();
        });
    }

    /**
     * 场景 2b（A 档专形，fix round 1 实测锚）：A 档 <b>date 列底层 timestamp 化</b>——
     * DDL 关键字 {@code date} 映射 atttypid=1114（{@code pg_type} 实查 typlen=8；PG 档
     * 恒 1082/4B 日粒度——og-plan 2026-10-13 探针）。三面钉：
     * <ul>
     *   <li><b>时段完整存储与渲染零损失</b>：{@code '2026-10-13 14:30:25'} 原样存取
     *       ——wal 路按列 typeOid 1114 走 timestamp 渲染、基准路 timestamp_out 同形，
     *       双路 diff 空 + wal 路字面含时段（对拍不吞值）；</li>
     *   <li><b>整秒粒度锚</b>：微秒输入被服务端截断（Oracle DATE 语义——
     *       {@code .123456} 存 {@code 14:30:25}）；</li>
     *   <li><b>零时段形态</b>：纯日期值渲 {@code 00:00:00} 时段（timestamp 渲染恒
     *       HH:MM:SS，与基准路 timestamp_out 同形）。</li>
     * </ul>
     * PG 档对照：date 列输入带时段<b>静默截断为日</b>（{@code '2026-10-13 14:30:25'}
     * 存 {@code 2026-10-13}）——两档真语义分叉，故本用例仅 A 档（无 PG 孪生）。
     */
    @Test
    void aCompatDateColumnKeepsTimeOfDayViaTimestampStorageParity() throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_adate (id int4 NOT NULL, d date, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_adate REPLICA IDENTITY FULL",
        };
        List<OgBlock> blocks = runParity(OgTestEnv.CompatMode.A, "A 档 date 时段", "adate", ddl,
                new String[]{"parity.t_adate"}, 1, 1, 0, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO parity.t_adate VALUES (1, '2026-10-13 14:30:25')");
                    exec(conn, "INSERT INTO parity.t_adate VALUES (2, DATE '2026-10-07')");
                    exec(conn, "INSERT INTO parity.t_adate VALUES (3, '2026-10-13 14:30:25.123456')");
                    exec(conn, "UPDATE parity.t_adate SET d = '2026-12-31 23:59:59' WHERE id = 1");
                    conn.commit();
                }).capture().blocks();
        assertEquals(1, blocks.size(), "A 档 date 时段: 应恰一个事务块:\n" + blocks);
        List<String> rows = blocks.get(0).rows();
        assertEquals(4, rows.size(), "四行（3 I + 1 U）:\n" + rows);
        assertEquals("INSERT parity.t_adate BEFORE=- AFTER=[id=1, d=2026-10-13 14:30:25]", rows.get(0),
                "时段值应完整存取（wal 路 typeOid 1114 走 timestamp 渲染——零真值损失）");
        assertEquals("INSERT parity.t_adate BEFORE=- AFTER=[id=2, d=2026-10-07 00:00:00]", rows.get(1),
                "纯日期值渲 00:00:00 时段（timestamp 渲染恒 HH:MM:SS，与基准路 timestamp_out 同形）");
        assertEquals("INSERT parity.t_adate BEFORE=- AFTER=[id=3, d=2026-10-13 14:30:25]", rows.get(2),
                "微秒输入被服务端截断（Oracle DATE 整秒粒度锚）");
        assertEquals("UPDATE parity.t_adate BEFORE=[id=1, d=2026-10-13 14:30:25]"
                + " AFTER=[id=1, d=2026-12-31 23:59:59]", rows.get(3),
                "UPDATE 前后像双含时段（FULL 身份整行前像）");
    }

    // ---- 场景 3：TOAST 三形态 + 重启 unchanged ----

    /**
     * 场景 3a：TOAST 三存储形态窗口内重组——external 未压缩（md5 链 7040B）/
     * external pglz（半可压缩 24600B）/ 行内压缩（{@code repeat('x',10000)}）+ 全行
     * UPDATE（未变宽列双路同渲染）+ DELETE，双路 diff 空；足迹断言（截断尾注计数 +
     * 无降级字面）钉三形态的输出面。OG 的 tuptoaster 阈值/压缩行为与 vanilla 同源
     * （9.2 系 pglz），chunk 采集走 MULTI_INSERT 真几何（T7 修复 A 的第二验收面）。
     */
    @Test
    void toastThreeStorageFormsWideValuesParityAcrossPaths() throws Exception {
        toastThreeStorageScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 3a 的 A 档全量复验形态（M3 T5——TOAST 引擎与兼容档正交，双档同阈值）。 */
    @Test
    void toastThreeStorageFormsWideValuesParityAcrossPathsACompat() throws Exception {
        toastThreeStorageScenario(OgTestEnv.CompatMode.A);
    }

    private void toastThreeStorageScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity_toast (id int4, w text)",
                "ALTER TABLE parity.t_parity_toast REPLICA IDENTITY FULL",
        };
        List<OgBlock> blocks = runParity(compatMode, "TOAST 三形态", "tst1", ddl, new String[]{"parity.t_parity_toast"},
                1, 1, 0, conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (1,"
                            + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (2,"
                            + " (SELECT string_agg(md5(i::text)||repeat('y',50),'') FROM generate_series(1,300) i))");
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (3, repeat('x',10000))");
                    exec(conn, "UPDATE parity.t_parity_toast SET id = id + 100");
                    exec(conn, "DELETE FROM parity.t_parity_toast WHERE id = 102");
                    conn.commit();
                }).capture().blocks();
        String joined = String.join("\n", blocks.get(0).rows());
        assertFalse(joined.contains("toast-unavailable"),
                "TOAST 三形态不应出现降级字面:\n" + joined);
        // OG 值面分叉记档（2026-10-12 实测）：UPDATE 未变 external 列的后像 OG 恒带完整值
        // （OG heap_update 重写 toast 值、pgoutput 发真值——vanilla 发 'u' 占位），
        // wal 路 ToastAssembler 窗口内归集同值——双路一致（parity diff 空承载），
        // 足迹计数按 OG 语义钉：7040B×3 / 24600B×4 / 10000B×3 / unchanged×0。
        assertEquals(3, countLiteral(joined, "...(7040B)"),
                "形态① external 未压缩宽值应在 INSERT 后像 + UPDATE 前后双像共 3 处渲染 7040B 截断尾注:\n" + joined);
        assertEquals(4, countLiteral(joined, "...(24600B)"),
                "形态② external pglz 宽值应在 INSERT 后像 + UPDATE 前后双像 + DELETE 前像共 4 处渲染:\n" + joined);
        assertEquals(3, countLiteral(joined, "...(10000B)"),
                "形态③ 行内压缩宽值应在 INSERT 后像 + UPDATE 前后双像共 3 处渲染:\n" + joined);
        assertEquals(0, countLiteral(joined, "<toast-unchanged>"),
                "OG 语义下 UPDATE 未变 external 列恒带完整值（重写值、非 'u' 占位）——unchanged 应零处:\n" + joined);
    }

    /** 场景 3 重启回查用例的检查点目录（@TempDir 每用例独立——一目录一活实例契约）。 */
    @TempDir
    Path toastStateDir;

    /**
     * 场景 3b：重启续传 + 未变宽列 unchanged——wal 路两段会话（state.dir 检查点续传）：
     * 段一 INSERT 宽值、close 落检查点；段二续传（{@code resumedFromState} 钉前提）后
     * UPDATE 只改 id——新元组 external 指针指向窗口前 chunk（段二归集面空）。基准路
     * 单逻辑槽收两块，按 xid 交集对拍 diff 空；值面断言：UPDATE 后像双路同渲染
     * {@code <toast-unchanged>}（基准路 'u' 同形）、前像完整值（Java 独立复算）、
     * 无降级字面；段一事务的重发（页对齐重叠窗口）由重复块全等容忍面吸收。
     */
    @Test
    void restartUpdateUnchangedWideColumnRendersUnchangedToastParity() throws Exception {
        restartUpdateUnchangedScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 3b 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void restartUpdateUnchangedWideColumnRendersUnchangedToastParityACompat() throws Exception {
        restartUpdateUnchangedScenario(OgTestEnv.CompatMode.A);
    }

    private void restartUpdateUnchangedScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity_toast (id int4, w text)",
                "ALTER TABLE parity.t_parity_toast REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, compatMode);
        slots("tst2", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_parity_toast"},
                toastStateDir.toString(), compatMode), capture);
        WalSource wal2 = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_parity_toast"},
                toastStateDir.toString(), compatMode), capture);
        boolean resumed = false;
        try {
            wal.start();
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                exec(conn, "INSERT INTO parity.t_parity_toast VALUES (1,"
                        + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
            }
            await(() -> wal.dmlEmittedBuckets() >= 1,
                    "TOAST 重启续传: 段一 wal 路未发射 INSERT 桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            wal.close();   // close = 最终 best-effort 检查点 + 槽推进（段二续传起点）
            wal = wal2;
            wal2.start();
            resumed = wal2.resumedFromState();
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                exec(conn, "UPDATE parity.t_parity_toast SET id = 101 WHERE id = 1");
            }
            await(() -> wal2.dmlEmittedBuckets() >= 1,
                    "TOAST 重启续传: 段二 wal 路未发射 UPDATE 桶（buckets=" + wal2.dmlEmittedBuckets()
                            + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        } finally {
            wal2.close();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）");
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 2, CAPTURE_MS, compatMode);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "TOAST 重启续传", 0);

        String wide = md5Chain(220);
        String truncated = wide.substring(0, 64) + "...(" + wide.length() + "B)";
        List<String> updateRows = capture.blocks().stream()
                .flatMap(b -> b.rows().stream()).filter(l -> l.contains("UPDATE parity.t_parity_toast")).toList();
        assertEquals(1, updateRows.size(), "应恰一行 UPDATE（宽列未变）:\n" + capture.blocks());
        String update = updateRows.get(0);
        // OG 值面分叉记档（2026-10-12 实测，与场景 3a 同锚）：UPDATE 未变 external 列的
        // 后像恒带完整值（OG 重写 toast 值、基准路发真值），非 vanilla 的 'u' 占位——
        // 窗口前指针在段二由重写的新 chunk 归集面覆盖，双路同渲染完整宽值截断形态。
        assertEquals(2, countLiteral(update, truncated),
                "UPDATE 前后双像应各恰一处渲染完整宽值截断形态:\n" + update);
        assertTrue(update.contains("AFTER=[id=101, w=" + wide.substring(0, 16)),
                "UPDATE 后像应带完整宽值（OG 语义非 unchanged 占位）:\n" + update);
        assertFalse(capture.blocks().stream().flatMap(b -> b.rows().stream())
                .anyMatch(l -> l.contains("toast-unavailable")), "不应出现降级字面");
    }

    // ---- 场景 4：DDL-in-txn as-of ----

    /**
     * 场景 4：事务内 ADD COLUMN 前后段 as-of——单事务先 INSERT 两列行 → ADD COLUMN →
     * INSERT 三列行 → UPDATE 旧行（前像两列元组 + 缺席新列 NULL、后像三列），COMMIT
     * 后追加一笔 DDL 后控制事务（三列行直插 + 更新）。
     *
     * <p><b>双路形态裁定（og-plan 2026-10-12 投探实测锚——场景改编裁定⑤）</b>：OG
     * 逻辑解码器对<b>事务内 DDL 的后续行静默丢弃</b>——探针收到的帧序恰为
     * {@code B..R(两列)..I(首行)..C}（同事务的 DDL 后 INSERT/UPDATE 零帧；DDL 后的
     * 新事务以三列 R 帧正常解码）。这是服务端解码面限制而非接缝错位，本场景按两侧
     * <b>各自真值</b>精确断言（不放宽对拍、不弱化预期）：</p>
     * <ul>
     *   <li>wal 直解路（as-of 字典的真正受测面）：DDL 事务块恰三行、逐字面断言——
     *       前段行两列渲染、后段行三列渲染、UPDATE 前像三列词典缺席新列渲 NULL；</li>
     *   <li>基准路 DDL 事务块恰一行（OG 丢弃面的钉）；</li>
     *   <li>DDL 后控制事务双路逐行 diff 空（字典跨 DDL 演进后两路回到全等）。</li>
     * </ul>
     */
    @Test
    void inTxnAddColumnAsOfRenderingParity() throws Exception {
        inTxnAddColumnScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 4 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void inTxnAddColumnAsOfRenderingParityACompat() throws Exception {
        inTxnAddColumnScenario(OgTestEnv.CompatMode.A);
    }

    private void inTxnAddColumnScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_ddlin (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_ddlin REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, compatMode);
        slots("ddl1", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_ddlin"}, null, compatMode), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "事务内 ADD COLUMN as-of: wal 直解路 DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(true);
                exec(conn, "BEGIN");
                exec(conn, "INSERT INTO parity.t_ddlin VALUES (1, 'before-ddl')");
                exec(conn, "ALTER TABLE parity.t_ddlin ADD COLUMN extra text");
                exec(conn, "INSERT INTO parity.t_ddlin VALUES (2, 'after-ddl', 'new-col')");
                exec(conn, "UPDATE parity.t_ddlin SET name = 'before-ddl-upd', extra = 'filled' WHERE id = 1");
                exec(conn, "COMMIT");
                exec(conn, "BEGIN");
                exec(conn, "INSERT INTO parity.t_ddlin VALUES (3, 'post-ddl-ctrl', 'ctrl')");
                exec(conn, "UPDATE parity.t_ddlin SET extra = 'ctrl-upd' WHERE id = 3");
                exec(conn, "COMMIT");
            }
            await(() -> wal.dmlEmittedBuckets() >= 2,
                    "事务内 ADD COLUMN as-of: wal 路未发射 2 个桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } finally {
            wal.close();
        }
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 2, CAPTURE_MS, compatMode);
        List<OgBlock> walBlocks = capture.blocks();
        assertEquals(2, walBlocks.size(), "wal 路应恰两块（DDL 事务 + 控制事务）:\n" + walBlocks);
        List<String> ddlTxnRows = walBlocks.get(0).rows();
        assertEquals(3, ddlTxnRows.size(), "DDL 事务块应恰三行（as-of 前后段 + UPDATE）:\n" + ddlTxnRows);
        assertEquals("INSERT parity.t_ddlin BEFORE=- AFTER=[id=1, name=before-ddl]", ddlTxnRows.get(0),
                "DDL 前段行应按两列词典渲染");
        assertEquals("INSERT parity.t_ddlin BEFORE=- AFTER=[id=2, name=after-ddl, extra=new-col]",
                ddlTxnRows.get(1), "DDL 后段行应按三列词典渲染");
        assertEquals("UPDATE parity.t_ddlin BEFORE=[id=1, name=before-ddl, extra=NULL]"
                        + " AFTER=[id=1, name=before-ddl-upd, extra=filled]", ddlTxnRows.get(2),
                "UPDATE 前像三列词典缺席新列渲 NULL、后像三列（v2 as-of 语义同 vanilla）");
        assertEquals(1, baseline.get(0).rows().size(),
                "基准路 DDL 事务块恰一行——OG 逻辑解码对事务内 DDL 后续行静默丢弃（og-plan 实测锚，"
                        + "场景改编裁定⑤）: " + baseline.get(0).rows());
        assertEquals(walBlocks.get(0).rows().get(0), baseline.get(0).rows().get(0),
                "DDL 前段行双路逐字面相等");
        // DDL 后控制事务：字典演进后双路回到全等（full parity 断言面）
        OgParityBaseline.assertParity(
                baseline.subList(1, 2), walBlocks.subList(1, 2), "事务内 ADD COLUMN 控制事务", 0);
    }

    // ---- 场景 5：2PC 四形态 ----

    /**
     * 场景 5 前三形态（探测裁定走 assertParity + kind 归一化，见类 javadoc）：①挂起期
     * 零输出——{@code PREPARE TRANSACTION 'gt1'} 后 1.5s 挂起窗口两路皆零输出；②弃桶
     * ——{@code ROLLBACK PREPARED 'gt2'} 零帧；③{@code COMMIT PREPARED 'gt3'} 双路各发
     * 完整事务块（基准路 NORMAL、wal 路 TWO_PHASE+gid——同一事务两侧表述）。
     *
     * <p>事务控制面与 PG 版同裁定：全程 autoCommit=true + 显式 BEGIN——PREPARE
     * TRANSACTION 会在服务端结束会话事务，驱动的自动提交记账不知情。收尾
     * {@code ROLLBACK PREPARED 'gt1'} 释放挂起（仍零输出）。</p>
     */
    @Test
    void twoPhaseSuspendDiscardAndCommitPreparedParity() throws Exception {
        twoPhaseSuspendScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 5a 的 A 档全量复验形态（M3 T5——PREPARE TRANSACTION 是集群级能力，与档位正交）。 */
    @Test
    void twoPhaseSuspendDiscardAndCommitPreparedParityACompat() throws Exception {
        twoPhaseSuspendScenario(OgTestEnv.CompatMode.A);
    }

    private void twoPhaseSuspendScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        ParityRun run = runParity(compatMode, "2PC 挂起/弃桶/COMMIT PREPARED", "tp1", ddl,
                new String[]{"parity.t_parity"}, 1, 1, 0, conn -> {
                    conn.setAutoCommit(true);
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'gt1-pending')");
                    exec(conn, "PREPARE TRANSACTION 'gt1'");
                    Thread.sleep(1500);   // 挂起窗口实宽——任一路早发即块数超 1 暴露
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'gt2-doomed')");
                    exec(conn, "PREPARE TRANSACTION 'gt2'");
                    exec(conn, "ROLLBACK PREPARED 'gt2'");
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO parity.t_parity VALUES (3, 'gt3-commit')");
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
     * 推过 PREPARE 所在页——纯页重叠救不回桶，钉 pendingFloorLsn 重放重建路径），
     * close 落检查点；段二同 state.dir 续传，{@code COMMIT PREPARED 'gtx'} 到达后发射。
     * 基准路单逻辑槽收 filler+gtx 两块，按 xid 交集对拍 diff 空；wal 侧专项断言
     * TWO_PHASE+gid=gtx（重放重建桶的终态钉）。
     */
    @Test
    void twoPhasePreparedPendingSurvivesRestartResumeParity() throws Exception {
        twoPhasePreparedPendingScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 5b 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void twoPhasePreparedPendingSurvivesRestartResumeParityACompat() throws Exception {
        twoPhasePreparedPendingScenario(OgTestEnv.CompatMode.A);
    }

    private void twoPhasePreparedPendingScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, compatMode);
        slots("tp2", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_parity"},
                tpStateDir.toString(), compatMode), capture);
        WalSource wal2 = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_parity"},
                tpStateDir.toString(), compatMode), capture);
        boolean resumed = false;
        try {
            wal.start();
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(true);
                exec(conn, "BEGIN");
                exec(conn, "INSERT INTO parity.t_parity VALUES (10, 'gtx-pending')");
                exec(conn, "PREPARE TRANSACTION 'gtx'");
                exec(conn, "INSERT INTO parity.t_parity SELECT g, 'filler-' || g FROM generate_series(11, 260) g");
            }
            await(() -> wal.dmlEmittedBuckets() >= 1,
                    "2PC 重启续传: 段一 wal 路未发射 filler 桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            wal.close();   // close = 最终检查点（前沿已过 PREPARE 所在页；槽推进按挂起桶 floor 封顶）
            wal = wal2;
            wal2.start();
            resumed = wal2.resumedFromState();
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                exec(conn, "COMMIT PREPARED 'gtx'");
            }
            await(() -> wal2.dmlEmittedBuckets() >= 2,
                    "2PC 重启续传: 段二 wal 路未发射 COMMIT PREPARED 桶（重放重建桶失效——buckets="
                            + wal2.dmlEmittedBuckets() + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        } finally {
            wal2.close();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）");
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 2, CAPTURE_MS, compatMode);
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
     * 场景 6：普通 DML 中途停续（at-least-once）——wal 路两段会话：段一（state.dir）
     * 捕两笔 INSERT 事务，close 落检查点；段二续传（{@code resumedFromState} 钉前提）
     * 先 {@code ADD COLUMN}（字典经检查点持久化后流内演进——OG 对 DDL 事务发空 B..C
     * 块，基准路 C 帧计数含它）再一笔 I/U/D 混合事务。基准路单逻辑槽收 4 帧
     * （2 行事务 + 1 DDL 空块 + 1 混合事务），按 xid 交集对拍 diff 空——段一同页事务
     * 的整桶幂等重发由重复块全等容忍面吸收；专项：wal 并集覆盖两行块 + 混合块。
     */
    @Test
    void midStreamStopResumePlainDmlParityAcrossPaths() throws Exception {
        midStreamStopResumeScenario(OgTestEnv.CompatMode.PG);
    }

    /** 场景 6 的 A 档全量复验形态（M3 T5）。 */
    @Test
    void midStreamStopResumePlainDmlParityAcrossPathsACompat() throws Exception {
        midStreamStopResumeScenario(OgTestEnv.CompatMode.A);
    }

    private void midStreamStopResumeScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, compatMode);
        slots("lf1", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_parity"},
                lifecycleStateDir.toString(), compatMode), capture);
        WalSource wal2 = new WalSource(walSourceConfig(physicalSlot, new String[]{"parity.t_parity"},
                lifecycleStateDir.toString(), compatMode), capture);
        boolean resumed = false;
        try {
            wal.start();
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'seg1-a')");
                exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'seg1-b')");
            }
            await(() -> wal.dmlEmittedBuckets() >= 2,
                    "生命周期停续: 段一 wal 路未发射 2 个桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            wal.close();
            wal = wal2;
            wal2.start();
            resumed = wal2.resumedFromState();
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                exec(conn, "ALTER TABLE parity.t_parity ADD COLUMN extra text");
                conn.setAutoCommit(false);
                exec(conn, "INSERT INTO parity.t_parity VALUES (3, 'seg2-c', 'seg2-new')");
                exec(conn, "UPDATE parity.t_parity SET name = 'seg1-a-upd', extra = 'backfilled' WHERE id = 1");
                exec(conn, "DELETE FROM parity.t_parity WHERE id = 2");
                conn.commit();
            }
            await(() -> wal2.dmlEmittedBuckets() >= 1,
                    "生命周期停续: 段二 wal 路未发射段二桶（buckets=" + wal2.dmlEmittedBuckets()
                            + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        } finally {
            wal2.close();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）");
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 4, CAPTURE_MS, compatMode);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "生命周期停续", 0);

        // 专项：wal 两段会话并集覆盖基准路全部行事务 xid（空块 xid 属 wal 侧零发射面，天然缺席）
        assertTrue(capture.blocks().size() >= 3,
                "wal 路并集应至少含段一 2 块 + 段二 1 块（重发只增不改）:\n" + capture.blocks());
        LOG.info("生命周期停续重发观测: wal 块总数={}（含 at-least-once 重发面）", capture.blocks().size());
    }

    // ---- 对拍骨架（OgDmlOgSpecificIT 复用） ----

    /**
     * 对拍结局快照：已关闭的 WalSource（观测计数面停流后读）+ 捕获器（块/头事件面，
     * 2PC 专项断言的 gid/twoPhase 取用）。
     *
     * @param source  wal 直解路门面（已 close——计数读取安全）
     * @param capture 捕获器（blocks/begins 快照）
     */
    record ParityRun(WalSource source, OgParityBaseline.WalCapture capture) {
    }

    /**
     * 单场景对拍骨架（无 state.dir 档，PG 档缺省形态——M2 既有签名，OgDmlOgSpecificIT
     * 消费面）：委派兼容档参数化形态（本类内 8 参重载，首参
     * {@code OgTestEnv.CompatMode}）的 {@code CompatMode.PG} 档。
     *
     * @param scenario      场景名（断言消息上下文）
     * @param slotSuffix    槽名后缀（前缀 m2par_logic_/m2par_wal_ 固定）
     * @param tableDdl      场景建表语句（parity schema 内）
     * @param whitelist     DML 白名单表全名集
     * @param walTxns       wal 路追平目标（发射桶数）
     * @param baselineTxns  基准路 capture 的 C 帧计数目标（含 DDL 空块——OG 分叉记档）
     * @param walAborted    场景内被回滚的子事务行数（wal 记账与实付的期望差）
     * @param dml           DML 执行器
     * @return 对拍结局快照（已关闭 WalSource + 捕获器）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    ParityRun runParity(String scenario, String slotSuffix, String[] tableDdl, String[] whitelist,
            long walTxns, long baselineTxns, long walAborted, SqlConsumer<Connection> dml) throws Exception {
        return runParity(OgTestEnv.CompatMode.PG, scenario, slotSuffix, tableDdl, whitelist,
                walTxns, baselineTxns, walAborted, dml);
    }

    /**
     * 单场景对拍骨架的兼容档参数化形态（M3 T5——场景体零复制的承载面）：重置场景 →
     * 建逻辑槽（按档取库）→ 起 wal 直解路（direct 通道 + opengauss-jdbc 前缀 + 白名单
     * 钉场景表 + 库名按档）→ DML → await 桶数 → 基准路 capture（按档取库）→ close →
     * assertParity，返回对拍结局快照。槽名 A 档带 {@code _a} 后缀（物理槽是集群级对象，
     * 跨库重名互踩清理面）。
     *
     * @param mode          兼容档（取场景库与槽名后缀）
     * @param scenario      场景名（断言消息上下文）
     * @param slotSuffix    槽名后缀（前缀 m2par_logic_/m2par_wal_ 固定，A 档再附 _a）
     * @param tableDdl      场景建表语句（parity schema 内）
     * @param whitelist     DML 白名单表全名集
     * @param walTxns       wal 路追平目标（发射桶数）
     * @param baselineTxns  基准路 capture 的 C 帧计数目标（含 DDL 空块——OG 分叉记档）
     * @param walAborted    场景内被回滚的子事务行数（wal 记账与实付的期望差）
     * @param dml           DML 执行器
     * @return 对拍结局快照（已关闭 WalSource + 捕获器）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    ParityRun runParity(OgTestEnv.CompatMode mode, String scenario, String slotSuffix, String[] tableDdl,
            String[] whitelist, long walTxns, long baselineTxns, long walAborted,
            SqlConsumer<Connection> dml) throws Exception {
        resetScenario(tableDdl, mode);
        slots(slotSuffix, mode);
        OgParityBaseline.createLogicalSlot(logicalSlot, mode);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(physicalSlot, whitelist, null, mode), capture);
        wal.start();
        assertTrue(wal.dmlEnabled(), scenario + ": wal 直解路 DML 面应装配");
        try (Connection conn = OgTestEnv.newSqlConnection(mode.database())) {
            dml.accept(conn);
        }
        await(() -> wal.dmlEmittedBuckets() >= walTxns,
                scenario + ": wal 路未发射 " + walTxns + " 个桶（buckets=" + wal.dmlEmittedBuckets()
                        + ", terminal=" + wal.receiverTerminalFailure() + "）");
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, baselineTxns, CAPTURE_MS, mode);
        wal.close();
        OgParityBaseline.assertParity(baseline, capture.blocks(), scenario, walAborted);
        return new ParityRun(wal, capture);
    }

    /**
     * 场景重置（PG 档缺省形态——OgDmlOgSpecificIT 消费面）：委派
     * {@link #resetScenario(String[], OgTestEnv.CompatMode)} 的 PG 形态。
     *
     * @param tableDdl 场景建表语句
     * @throws SQLException DDL 失败
     */
    static void resetScenario(String[] tableDdl) throws SQLException {
        resetScenario(tableDdl, OgTestEnv.CompatMode.PG);
    }

    /**
     * 场景重置的兼容档参数化形态（M3 T5）：两路槽位前置清理由调用方（{@link #slots}）
     * 承担；目标库内 schema CASCADE 整体清场 + 场景建表 + publication 重建（FOR ALL
     * TABLES——m2pg/m2a 专用库）。DDL 先于建槽/引导（spike 坑 2 的时序契约）。
     *
     * @param tableDdl 场景建表语句
     * @param mode     兼容档（取目标库）
     * @throws SQLException DDL 失败
     */
    static void resetScenario(String[] tableDdl, OgTestEnv.CompatMode mode) throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection(mode.database()); Statement st = c.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            st.execute("CREATE SCHEMA " + SCHEMA);
            for (String ddl : tableDdl) {
                st.execute(ddl);
            }
            st.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION);
            st.execute("CREATE PUBLICATION " + PUBLICATION + " FOR ALL TABLES");
        }
    }

    /**
     * 设定当前用例的兼容档与两路槽名并前置清理残留（A 档槽名附 {@code _a} 后缀——
     * 物理槽是集群级对象，与 PG 档同名互踩）。
     *
     * @param suffix 后缀（槽名 = m2par_logic_/m2par_wal_ + suffix [+ _a]）
     * @param mode   兼容档
     */
    void slots(String suffix, OgTestEnv.CompatMode mode) {
        this.mode = mode;
        String tagged = suffix + (mode == OgTestEnv.CompatMode.A ? "_a" : "");
        logicalSlot = "m2par_logic_" + tagged;
        physicalSlot = "m2par_wal_" + tagged;
        OgParityBaseline.dropLogicalSlotQuietly(logicalSlot, mode);
        OgTestEnv.dropPhysicalSlotQuietly(physicalSlot);
    }

    /**
     * 设定当前用例的两路槽名并前置清理残留（PG 档缺省——M2 既有签名保留）。
     *
     * @param suffix 后缀（槽名 = m2par_logic_/m2par_wal_ + suffix）
     */
    void slots(String suffix) {
        slots(suffix, OgTestEnv.CompatMode.PG);
    }

    /**
     * 当前用例的物理槽名（包内共享——OgDmlOgSpecificIT 自管 wal 路装配取用）。
     *
     * @return 物理槽名（须先经 {@link #slots} 设定）
     */
    String physicalSlotName() {
        return physicalSlot;
    }

    /**
     * wal 直解路门面配置（PG 档缺省形态——M2 既有签名）：六键指向 m2pg（PG 兼容档——
     * DML 护栏放行前提）+ direct 通道 + opengauss-jdbc SQL 会话前缀（OG 档 sha256）+
     * DML 白名单；stateDir 非 null 时启用检查点（续传场景）。
     *
     * @param physicalSlot 物理槽名
     * @param whitelist    DML 白名单表全名集
     * @param stateDir     检查点目录（null = 禁用）
     * @return WalSource 构造入参配置
     */
    static Properties walSourceConfig(String physicalSlot, String[] whitelist, String stateDir) {
        return walSourceConfig(physicalSlot, whitelist, stateDir, OgTestEnv.CompatMode.PG);
    }

    /**
     * wal 直解路门面配置的兼容档参数化形态（M3 T5）：库名按档取（m2pg/m2a），其余六键
     * 与 PG 档同构——direct 通道 + opengauss-jdbc SQL 会话前缀（OG 档 sha256）+ DML
     * 白名单；stateDir 非 null 时启用检查点（续传场景）。
     *
     * @param physicalSlot 物理槽名
     * @param whitelist    DML 白名单表全名集
     * @param stateDir     检查点目录（null = 禁用）
     * @param mode         兼容档（取库名）
     * @return WalSource 构造入参配置
     */
    static Properties walSourceConfig(String physicalSlot, String[] whitelist, String stateDir,
            OgTestEnv.CompatMode mode) {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, OgTestEnv.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(OgTestEnv.port()));
        cfg.setProperty(WalSource.KEY_DB, mode.database());
        cfg.setProperty(WalSource.KEY_USER, OgTestEnv.USER);
        cfg.setProperty(WalSource.KEY_PASS, OgTestEnv.PASSWORD);
        cfg.setProperty(WalSource.KEY_SLOT, physicalSlot);
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        cfg.setProperty(WalSource.KEY_TABLES, String.join(",", whitelist));
        if (stateDir != null) {
            cfg.setProperty(WalSource.KEY_STATE_DIR, stateDir);
        }
        return cfg;
    }

    /**
     * 客户端侧 COPY FROM STDIN（兼容档参数化——按档取库；{@code OgTestEnv.copyIn} 的
     * 库名参数化形态，供本 IT 的 MULTI_INSERT 场景）：服务端侧 COPY 文件被 OG 安全
     * 策略禁止（M1 实测锚）。
     *
     * @param mode 兼容档（取目标库）
     * @param sql  COPY 命令
     * @param csv  装载文本
     * @return 装载行数
     * @throws SQLException COPY 失败
     */
    private static long copyIn(OgTestEnv.CompatMode mode, String sql, String csv) throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection(mode.database())) {
            CopyManager cp = new CopyManager(c.unwrap(BaseConnection.class));
            return cp.copyIn(sql, new StringReader(csv));
        } catch (IOException e) {
            throw new SQLException("COPY FROM STDIN 流读写失败", e);
        }
    }

    /**
     * 兜底清理：关 WalSource（幂等）→ 删两路槽（异常路径保险；槽名带 m2par 前缀，
     * 不触碰 M1 物理槽清理面；删逻辑槽按当前用例的兼容档取库）。
     */
    @AfterEach
    void cleanupSlots() {
        if (wal != null) {
            wal.close();
        }
        if (logicalSlot != null) {
            OgParityBaseline.dropLogicalSlotQuietly(logicalSlot, mode);
        }
        if (physicalSlot != null) {
            OgTestEnv.dropPhysicalSlotQuietly(physicalSlot);
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
