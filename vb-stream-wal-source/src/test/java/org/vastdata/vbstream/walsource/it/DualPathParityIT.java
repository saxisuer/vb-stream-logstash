package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.OutputRenderer;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 双路对拍场景 1（v2 Task 8 验收核心）——同一 PG 18 容器、同一批 DML，engine 逻辑解码路
 * （in-process {@code PgReplicationSession + TransactionAssembler + ConsoleRenderer}，经
 * {@link EnginePathRunner}）与 wal 直解路（{@link WalSource} DML 面 + {@link OutputRenderer}）
 * 各自独立槽位捕获 CDC 输出行，归一化（按 xid 求交集）后逐行 diff 断言空。
 *
 * <p><b>对齐裁定（任务书 §Interfaces）</b>：xid 与 commitLsn/commitTs 双路必然一致
 * （同一 server 同一提交记录），唯一不稳定面是两路起停窗口造成的事务集合差——对拍前先按
 * xid 求交集，断言交集非空 + 交集内逐行 diff 空 + 窗口外事务在失败信息中说明。头行
 * {@code changes=N} 的已知语义差：engine 取 aborted 过滤前到达面（非流式形态下被回滚的
 * 子事务行从未到达组装器），wal 取桶记账（过滤前）——SAVEPOINT 场景 engine N=实付、
 * wal N=记账，断言按 "engine.changes == 行数 && wal.changes >= 行数" 归一化。</p>
 *
 * <p><b>截断 UPDATE 面（Task 8.5 更新）</b>：截断新元组的重建在 {@code XactGrouper} 已
 * 落（FULL 身份经旧元组字节拼装，离线锚在 {@code XactGrouperTest} 手造字节用例）；场景
 * 2 的 UPDATE 全列赋值保留（首尾列都变的对照形态），场景 6 追加中段列 UPDATE（首尾列
 * 不变——截断诱发值形态 + {@code skippedTruncatedRows==0} 哨兵）。<b>真实 PG 分布注记
 * （REL_18 heapam.c 实源钉）</b>：前缀/后缀省略门是 {@code !RelationIsLogicallyLogged}
 * （wal_level &lt; logical），与 CONTAINS_OLD 旧元组记录（需 ≥ logical）互斥——本环境
 * 逻辑流下用户表 UPDATE 恒全量记录，截断重建/跳过双计数恒 0 作回归哨兵。</p>
 *
 * <p>场景组（任务书场景矩阵第 1 组 + Task 8.5 场景 6 + Task 9 场景 2/4 + Task 10
 * 场景 3 + Task 11 场景 5 + Task 12 场景 6 与干扰矩阵）：单行事务 / 单事务多语句
 * （I+U+D）/ 双连接交错事务 /
 * 回滚零输出 / 子事务 SAVEPOINT 回滚剔除 / 截断 UPDATE 中段列 / 17 类型边界值矩阵 /
 * 事务内 ADD COLUMN as-of / TOAST 三存储形态 + 重启续传 unchanged 对齐（external
 * 重组 / external pglz / 行内压缩 / 续传后未变宽列指针双路同渲染
 * {@code <toast-unchanged>}——pgoutput 'u' 同形）/ 2PC 四形态（挂起期零输出 /
 * ROLLBACK PREPARED 弃桶 / COMMIT PREPARED 发射 {@code kind=TWO_PHASE}+gid /
 * 挂起期 wal-source 停机重启续传重放重建桶）/ 普通双段 DML 中途停续（Task 12 场景 6
 * ——at-least-once 重发双路一致）/ 流内 CREATE SCHEMA nsp 字典面（Task 4 延期项清账）
 * / 干扰矩阵（Task 12——catalog 风暴线程下场景 1/TOAST 复跑 ×2）。表形态
 * {@code REPLICA IDENTITY FULL}——UPDATE
 * 与 DELETE 双路恒携带整行前像（replica identity 面对称，BEFORE 渲染可对拍）。需要
 * 本机 Docker。</p>
 *
 * <p><b>场景编号对照（Task 13 注记）</b>——spec §6 场景矩阵号 → 测试方法（交付任务
 * 与 spec 号错位是并行拆任务所致）：场景 1 基础 DML = 前 6 个 @Test（单行/多语句/
 * 交错/回滚/SAVEPOINT/截断中段列——末者 Task 8.5 记作"场景 6 追加"，方法归组 1）；
 * 场景 2 类型矩阵 = {@code typeMatrixBoundaryRowsParityAcrossPaths}；场景 3 TOAST =
 * {@code toastThreeStorageFormsWideValuesParityAcrossPaths} +
 * {@code restartUpdateUnchangedWideColumnRendersUnchangedToastParity}；场景 4 DDL-in-txn
 * = {@code inTxnAddColumnAsOfRenderingParity}；场景 5 2PC =
 * {@code twoPhaseSuspendDiscardAndCommitPreparedParity} +
 * {@code twoPhasePreparedPendingSurvivesRestartResumeParity}；场景 6 生命周期 =
 * {@code midStreamStopResumePlainDmlParityAcrossPaths}；矩阵外补充 =
 * {@code createSchemaNamespaceDictionaryParityInStream}（Task 4 延期清账）+ 干扰矩阵
 * 2 个 @RepeatedTest(2)（Task 12）。</p>
 */
class DualPathParityIT {

    /** 两路各自的就绪/追平等待上限（毫秒）——wal 路首场景含 catalog 引导余量。 */
    private static final long AWAIT_MS = 60_000;

    /** 轮询间隔（毫秒）。 */
    private static final long POLL_MS = 200;

    /** 干扰线程持续时长（毫秒）——catalog 风暴在窗口内与场景 DML 并发推进（Task 12 干扰矩阵）。 */
    private static final long STORM_MS = 5_000;

    /** 干扰线程 join 上限（毫秒）。 */
    private static final long STORM_JOIN_MS = 12_000;

    /** wal 接收器终态失败的堆栈留痕通道（surefire 报告只留 toString，堆栈在此补面）。 */
    private static final Logger LOG_WAL_TERMINAL = LoggerFactory.getLogger(DualPathParityIT.class);

    /** 对拍表 DDL：六类型列（int4/text/bool/numeric/date/timestamptz 全在两路渲染矩阵交面）+ FULL 前像。 */
    private static final String[] TABLE_DDL = {
            "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                    + " d date, ts timestamptz, PRIMARY KEY (id))",
            "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
    };

    /**
     * 场景 2 表 DDL（Task 9）：17 类型列全集（两路渲染矩阵交面——wal 侧
     * {@code DecodeKinds}/{@code DiskValueRenderer} 与 engine 侧 pgoutput text 模式
     * 的公共覆盖面）+ FULL 前像。列类型含 task 书边界值所需的 float4/float8（科学
     * 计数法区段）、numeric（深 dscale/极大值/NaN）、timetz（Task 9 补的渲染面）、
     * time（{@code 24:00:00} 闭上端）、bytea/json/bpchar/uuid 与文本族多字节。
     */
    private static final String[] TYPES_TABLE_DDL = {
            "CREATE TABLE parity.t_types (id int4 NOT NULL,"
                    + " c_bool bool, c_int2 int2, c_int8 int8,"
                    + " c_float4 float4, c_float8 float8, c_numeric numeric,"
                    + " c_text text, c_varchar varchar(100), c_bpchar bpchar(10), c_json json,"
                    + " c_bytea bytea, c_date date, c_time time, c_timetz timetz,"
                    + " c_timestamp timestamp, c_timestamptz timestamptz, c_uuid uuid,"
                    + " PRIMARY KEY (id))",
            "ALTER TABLE parity.t_types REPLICA IDENTITY FULL",
    };

    /** 场景 4 表 DDL（Task 9）：两列起步——事务内 ADD COLUMN 扩到三列，as-of 前后段分界。 */
    private static final String[] DDLIN_TABLE_DDL = {
            "CREATE TABLE parity.t_ddlin (id int4 NOT NULL, name text, PRIMARY KEY (id))",
            "ALTER TABLE parity.t_ddlin REPLICA IDENTITY FULL",
    };

    /**
     * 场景 3 表 DDL（Task 10）：无 PK 两列表——宽值列 {@code w} 的 TOAST 存储形态
     * 实测锚（2026-10-07 docker PG 18.6 探针，{@code pg_column_size} + toast 表
     * chunk 计量双源）：md5 链 7040B → external 未压缩（4 chunk、extsize==rawsize-4）；
     * md5 串联定长 'y' 段的半可压缩值 24600B → external pglz 压缩（extsize 11040
     * &lt; 24596）；{@code repeat('x',10000)} → <b>行内压缩</b>（存储 125B、toast 表零
     * chunk——任务书要点②原述 repeat 型走 external 与实测不符：tuptoaster 先压缩、
     * 完全可压缩值压缩后 tuple 已低于阈值即行内落盘，external pglz 需"压缩后仍
     * 超 2KB"的半可压缩值，见 ToastAssembler javadoc 的 64000 字符 repeat 实测同证）。
     * 无 PK 避免索引记录干扰；FULL 前像对齐场景 1 前提（UPDATE/DELETE 旧元组以
     * 存储形态落 WAL——external 指针/压缩 datum 原样进前像，重组面双像都受验）。
     */
    private static final String[] TOAST_TABLE_DDL = {
            "CREATE TABLE parity.t_parity_toast (id int4, w text)",
            "ALTER TABLE parity.t_parity_toast REPLICA IDENTITY FULL",
    };

    /** 场景 3 重启回查用例的检查点目录（@TempDir 每用例独立——一目录一活实例契约）。 */
    @TempDir
    Path toastStateDir;

    /** 场景 5（Task 11）挂起期重启续传用例的检查点目录（同上——一目录一活实例契约）。 */
    @TempDir
    Path tpStateDir;

    /** 场景 6（Task 12）生命周期中途停/续传用例的检查点目录（同上——一目录一活实例契约）。 */
    @TempDir
    Path lifecycleStateDir;

    /** 头行解析模式：TXN-BEGIN xid=.. kind=.. gid=.. commitLsn=0x.. commitTs=.. changes=..（两路同格式）。 */
    private static final Pattern HEADER = Pattern.compile(
            "TXN-BEGIN xid=(\\d+) kind=(\\S+) gid=(\\S+) commitLsn=0x([0-9a-f]+) commitTs=(\\S+) changes=(\\d+)");

    /**
     * engine 生命周期控制行模式（two_phase=true 场景专属）：ConsoleRenderer 把 9 种事务
     * 生命周期控制消息升 INFO 打到 CDC logger（BEGIN-PREPARE/PREPARE/COMMIT-PREPARED/
     * ROLLBACK-PREPARED + 流式 5 种——streaming=OFF 下只有前 4 种出现），它们不是事务
     * 块行，进入 {@link #parse} 会触发行格式 fail——对拍前剥离（wal 路无对应输出面，
     * 剥离后两路捕获面同构）。
     */
    private static final Pattern ENGINE_LIFECYCLE = Pattern.compile(
            "^(BEGIN-PREPARE|PREPARE|COMMIT-PREPARED|ROLLBACK-PREPARED|STREAM-[A-Z]+)\\s+gid=.*");

    // ---- 场景 1：单行事务 ----

    /** 单行 INSERT 单事务：交集内头行/行/尾逐字节对齐。 */
    @Test
    void singleRowInsertParityAcrossPaths() throws Exception {
        runParity("单行事务", 1, conn -> {
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
        });
    }

    // ---- 场景 2：单事务多语句（I + U + D 混合，含 NULL 值）----

    /** 一笔事务内 INSERT×2（含 NULL 列）→ UPDATE×2（全列赋值防截断）→ DELETE×1：行序与值逐字节对齐。 */
    @Test
    void multiStatementTxnParityAcrossPaths() throws Exception {
        runParity("多语句事务", 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, NULL, false, NULL, NULL, NULL)");
            exec(conn, "UPDATE parity.t_parity SET id = 11, name = 'alice2', flag = false, val = 99.5,"
                    + " d = '2026-11-01', ts = timestamptz '2026-11-01 01:02:03.000001+00' WHERE id = 1");
            exec(conn, "UPDATE parity.t_parity SET id = 22, name = 'bob2', flag = true, val = 0.0001,"
                    + " d = '2026-11-02', ts = timestamptz '2026-11-02 05:06:07.123456+00' WHERE id = 2");
            exec(conn, "DELETE FROM parity.t_parity WHERE id = 11");
            conn.commit();
        });
    }

    // ---- 场景 3：双连接交错事务 ----

    /** 两连接交错写、按序提交：两路各自两个事务桶、桶内行归属正确、提交序一致。 */
    @Test
    void interleavedTxnsParityAcrossPaths() throws Exception {
        runParity("交错事务", 2, conn -> {
            try (Connection c1 = ParityEnv.newSqlConnection(); Connection c2 = ParityEnv.newSqlConnection()) {
                c1.setAutoCommit(false);
                c2.setAutoCommit(false);
                exec(c1, "INSERT INTO parity.t_parity VALUES (1, 'a1', true, 1.1, '2026-10-07',"
                        + " timestamptz '2026-10-07 01:00:00+00')");
                exec(c2, "INSERT INTO parity.t_parity VALUES (101, 'b1', false, 2.2, '2026-10-08',"
                        + " timestamptz '2026-10-08 02:00:00+00')");
                exec(c1, "INSERT INTO parity.t_parity VALUES (2, 'a2', true, 3.3, '2026-10-09',"
                        + " timestamptz '2026-10-09 03:00:00+00')");
                exec(c2, "INSERT INTO parity.t_parity VALUES (102, 'b2', false, 4.4, '2026-10-10',"
                        + " timestamptz '2026-10-10 04:00:00+00')");
                c1.commit();
                c2.commit();
            }
        });
    }

    // ---- 场景 4：回滚零输出 ----

    /** 回滚事务两路零输出（头/行/尾皆无），随后的提交事务对拍完整——证明流未被回滚污染。 */
    @Test
    void rolledBackTxnEmitsNothingOnBothPaths() throws Exception {
        runParity("回滚零输出", 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'doomed', true, 66.6,"
                    + " '2026-10-07', timestamptz '2026-10-07 07:07:07+00')");
            conn.rollback();
            conn.setAutoCommit(true);
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'committed', false, 77.7,"
                    + " '2026-10-08', timestamptz '2026-10-08 08:08:08+00')");
        });
    }

    // ---- 场景 5：子事务 SAVEPOINT 回滚剔除 ----

    /** SAVEPOINT 子事务行被剔除（两路都只见 s1/s3）；头行 changes=N 按语义归一化（engine=实付、wal=记账=实付+1）。 */
    @Test
    void savepointSubTxnFilteredRowsParity() throws Exception {
        runParity("子事务 SAVEPOINT", 1, 1L, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 's1', true, 1.0, '2026-10-07',"
                    + " timestamptz '2026-10-07 01:01:01+00')");
            exec(conn, "SAVEPOINT sp");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 's2-doomed', false, 2.0, '2026-10-07',"
                    + " timestamptz '2026-10-07 02:02:02+00')");
            exec(conn, "ROLLBACK TO SAVEPOINT sp");
            exec(conn, "INSERT INTO parity.t_parity VALUES (3, 's3', true, 3.0, '2026-10-07',"
                    + " timestamptz '2026-10-07 03:03:03+00')");
            conn.commit();
        });
    }

    // ---- 场景 6：截断 UPDATE 形态（中段列变更，Task 8.5）----

    /**
     * 截断 UPDATE 对拍（Task 8.5 场景 C）：UPDATE 只 SET 中段列（首列 id 与尾列 ts 不变
     * ——正是 PG 对新元组做前缀/后缀省略的值形态），REPLICA IDENTITY FULL 表双路 diff
     * 空 + wal 路 {@code skippedTruncatedRows==0}（截断行未被跳过——重建生效面或全量
     * 记录面，皆不出缺行）。
     *
     * <p><b>真实 PG 分布注记（REL_18 heapam.c 实源钉，2026-10 curl）</b>：前缀/后缀省略
     * 的门是 {@code oldbuf==newbuf && !RelationIsLogicallyLogged && !XLogCheckBufferNeedsBackup}
     * ——本环境 wal_level=logical 下用户表 UPDATE <b>恒全量记录新元组</b>（且 CONTAINS_OLD
     * 与截断在实源门上互斥）。故本场景实际钉住的是：中段列 UPDATE 的 FULL 前像/后像双路
     * 渲染逐字节等价 + 截断跳过计数零的回归哨兵；截断重建本体（拼装坐标/前缀取旧元组自身
     * t_hoff）由 {@code XactGrouperTest} 手造字节用例离线钉。</p>
     */
    @Test
    void midColumnUpdateFullIdentityParityAndZeroTruncationSkips() throws Exception {
        long skipped = runParity("截断 UPDATE 中段列", 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
            exec(conn, "UPDATE parity.t_parity SET name = 'alice-mid', flag = false, val = 99.5,"
                    + " d = '2026-11-01' WHERE id = 1");   // 首列 id / 尾列 ts 不变
            conn.commit();
        });
        assertEquals(0, skipped, "截断 UPDATE 跳过计数应为 0（重建生效/全量记录，不出缺行）");
    }

    // ---- 场景 7：类型矩阵边界值（Task 9 Step 1）----

    /**
     * 17 类型边界值对拍（Task 9 场景 2）：单事务内 INSERT 边界值行（负数全族/
     * float4·float8 科学计数法区段/numeric 深小数与 NaN/UTF-8 π 与多字节/bytea hex/
     * 微秒精度 timestamp·timestamptz/time {@code 24:00:00} 闭上端/timetz 存储区偏移/
     * uuid/负纪元日）+ 全 NULL 行 + NaN·Infinity 特值行 → UPDATE 全列赋值（长文本
     * &gt;64 触发渲染层截断形态，尾零时间、空 bytea、float4 低门限 {@code 1e+06}）→
     * DELETE 两行——双路 diff 空。
     *
     * <p><b>值面裁定（任务书要点）</b>：① 浮点科学计数法分叉（PG {@code 1e+20} vs
     * Java {@code 1.0E20}，Task 2 疑虑 2 遗留）由 {@code PgFloatFormat} 在本任务清账
     * ——engine text 档的浮点值是服务端 float4/8out 渲染，wal 侧经格式化器逐字复刻
     * （规则面 REL_18 d2s.c/f2s.c 源码钉 + docker 实测锚，离线归
     * {@code PgFloatFormatTest}）；② time {@code 24:00:00} 是 PG 合法值（Task 2
     * minor 清账——特判直出，java.time 无 24 点）；③ 长文本截断是<b>渲染层</b>规则
     * （两路同为 64 字符 + {@code ...(<原长>B)}，diff 天然一致）；④ numeric 走 text
     * 档不经 engine {@code BinaryValueDecoder}（其 1E-70 同型缺陷 ledger 记档，
     * ParityEnv.engineConfig 的 binary=false 钉住）。</p>
     */
    @Test
    void typeMatrixBoundaryRowsParityAcrossPaths() throws Exception {
        runParity("类型矩阵边界值", 1, TYPES_TABLE_DDL, conn -> {
            conn.setAutoCommit(false);
            exec(conn,
                    "INSERT INTO parity.t_types VALUES (1, true, -32768, -9223372036854775808,"
                            + " '3.4028235e38'::float4, '1e20'::float8, '1E-70'::numeric,"
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
                            + " c_int8 = 9223372036854775807, c_float4 = '1e6'::float4,"
                            + " c_float8 = '4.5e-8'::float8,"
                            + " c_numeric = '123456789012345678901234567890.1234567890',"
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

    // ---- 场景 8：事务内 ADD COLUMN 前后段 as-of（Task 9 Step 2）----

    /**
     * DDL-in-txn as-of 对拍（Task 9 场景 4）：单事务内先 INSERT 旧列形态行（两列）→
     * {@code ALTER TABLE ADD COLUMN} → INSERT 新列形态行（三列）→ 再 UPDATE 旧行
     * （前像是 DDL 前存储的<b>两列元组</b>、后像三列）——COMMIT 后双路 diff 空。
     *
     * <p><b>验证面</b>：① wal 路"字典 as-of（catalog 先应用）"的端到端正确性——
     * {@code pg_attribute} 的 INSERT WAL 记录先于第二条 INSERT 到达，重放序内字典
     * 先扩列、行后解码（首行两列/次行三列各按变更时刻的词典渲染）；② 旧行
     * UPDATE 的前像短元组（natts=2 &lt; 词典 3）尾列补 null 恰是正确值（ADD COLUMN
     * 无默认值语义即 NULL）；③ engine 路经 relation 版本日志 as-of 渲染同形（DDL
     * 后新 'R' 版本不追溯首行）。两路行序与列集逐字节对齐。</p>
     */
    @Test
    void inTxnAddColumnAsOfRenderingParity() throws Exception {
        runParity("事务内 ADD COLUMN as-of", 1, DDLIN_TABLE_DDL, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_ddlin VALUES (1, 'before-ddl')");
            exec(conn, "ALTER TABLE parity.t_ddlin ADD COLUMN extra text");
            exec(conn, "INSERT INTO parity.t_ddlin VALUES (2, 'after-ddl', 'new-col')");
            exec(conn, "UPDATE parity.t_ddlin SET name = 'before-ddl-upd', extra = 'filled'"
                    + " WHERE id = 1");
            conn.commit();
        });
    }

    // ---- 场景 9：TOAST 三存储形态窗口内重组（Task 10 / 任务书场景 3）----

    /**
     * TOAST 三存储形态对拍（Task 10 场景 3 前半）：单事务覆盖宽值的三种落盘形态——
     * ①external 未压缩（md5 链 7040B，chunk 拼装即原载荷）；②external pglz 压缩
     * （md5 串联 'y' 段的半可压缩值 24600B→extsize 11040，拼接剥 4B tcinfo 后解压）；
     * ③行内压缩（{@code repeat('x',10000)} 压缩至 ~121B 落元组内——TupleDecoder 的
     * VarlenaResolver 行内接线）——I×3 + UPDATE（只改 id，宽列不动：新旧元组都以
     * 存储形态落 WAL，external 指针/压缩 datum 进前像+后像双像）+ D×1，双路 diff 空。
     *
     * <p><b>值面裁定（首跑红→源码钉，Task 10 实测发现）</b>：UPDATE 未变 external 列的
     * <b>后像</b>两路都渲染 {@code <toast-unchanged>}——engine 侧是 pgoutput wire 的
     * 'u'（proto.c LOGICALREP_COLUMN_UNCHANGED：VARATT_IS_EXTERNAL_ONDISK 的列不发值；
     * 服务端 reorder buffer 对每条已施加用户变更后重置 toast hash，decode.c
     * clear_toast_afterwards），wal 侧镜像同构（ToastAssembler 归集面空 → unchanged
     * 哨兵 + XactGrouper 逐行清窗）。<b>前像恒带完整值</b>：FULL 身份旧元组在 WAL 写入
     * 时已被服务端 toast_flatten_tuple 拍扁内联（heapam.c ExtractReplicaIdentity）。
     * 行内压缩（形态③）非 external——proto.c 正常发文本，双像都是完整值。补充断言钉
     * 三形态的输出足迹（各尾注按行像数出现 + unchanged 恰 2 处——形态①②的 UPDATE
     * 后像）且全程无 {@code toast-unavailable} 降级字面。</p>
     *
     * <p><b>任务书勘误记档</b>：要点②原述 {@code repeat('x', N)} 走 external pglz 与
     * 实测不符（完全可压缩值压缩后 tuple 已低于 2KB 阈值即行内落盘，恰是形态③样本；
     * external pglz 需"压缩后仍超阈值"的半可压缩值），见 {@link #TOAST_TABLE_DDL}
     * javadoc 的实测锚。</p>
     */
    @Test
    void toastThreeStorageFormsWideValuesParityAcrossPaths() throws Exception {
        List<String> walLines = runParityWal("TOAST 三形态窗口内重组", 1, TOAST_TABLE_DDL, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity_toast VALUES (1,"
                    + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
            exec(conn, "INSERT INTO parity.t_parity_toast VALUES (2,"
                    + " (SELECT string_agg(md5(i::text)||repeat('y',50),'') FROM generate_series(1,300) i))");
            exec(conn, "INSERT INTO parity.t_parity_toast VALUES (3, repeat('x',10000))");
            exec(conn, "UPDATE parity.t_parity_toast SET id = id + 100");
            exec(conn, "DELETE FROM parity.t_parity_toast WHERE id = 102");
            conn.commit();
        });
        assertToastFootprints(walLines);
    }

    /**
     * TOAST 三形态场景的输出足迹专项断言（Task 12 从场景 9 抽取——干扰矩阵的
     * {@link #interferenceCatalogStormToastFormsParity} 复用同一值面锚）：三形态截断
     * 尾注各按行像数出现 + unchanged 恰 2 处（形态①②的 UPDATE 后像）+ 全程无
     * {@code toast-unavailable} 降级字面。
     *
     * @param walLines wal 路 CDC 捕获行
     */
    private static void assertToastFootprints(List<String> walLines) {
        String joined = String.join("\n", walLines);
        assertFalse(joined.contains("toast-unavailable"),
                "TOAST 三形态场景不应出现 toast-unavailable 降级字面:\n" + joined);
        assertEquals(2, countLiteral(joined, "...(7040B)"),
                "形态① external 未压缩宽值应在 INSERT 后像 + UPDATE 前像共 2 处渲染 7040B 截断尾注"
                        + "（UPDATE 后像是 unchanged，值不在 wire 上）:\n" + joined);
        assertEquals(3, countLiteral(joined, "...(24600B)"),
                "形态② external pglz 宽值应在 INSERT 后像 + UPDATE 前像 + DELETE 前像共 3 处渲染"
                        + " 24600B 截断尾注:\n" + joined);
        assertEquals(3, countLiteral(joined, "...(10000B)"),
                "形态③ 行内压缩宽值应在 INSERT 后像 + UPDATE 前后双像共 3 处渲染 10000B 截断尾注"
                        + "（行内压缩非 external，正常发文本）:\n" + joined);
        assertEquals(2, countLiteral(joined, "<toast-unchanged>"),
                "形态①②的 UPDATE 后像（未变 external 列）应各渲染一处 unchanged 占位:\n" + joined);
    }

    // ---- 场景 10：重启后续传 + 未变宽列指针的 unchanged 对齐（Task 10 / 任务书场景 3 收官）----

    /**
     * 重启续传对拍（Task 10 场景 3 后半，<b>值面裁定后重塑</b>）：wal 路<b>两段会话</b>
     * ——段一（带检查点 stateDir）捕 INSERT 宽值事务，close 落最终检查点；段二同
     * stateDir 续传（{@code resumedFromState} 钉前提），UPDATE 只改 id 不动宽列——
     * 新元组的 external 指针指向<b>窗口之前</b>写入的 chunk（段二 ToastAssembler 全新、
     * 归集面恒空）。engine 路全程单会话捕两个事务，按 xid 交集对拍 diff 空。
     *
     * <p><b>与任务书要点的偏差记档（源码钉）</b>：任务书预期"回查路径命中（值经
     * JdbcToastProbe 重建）"，实测首跑红揭示 engine 对该形态的 UPDATE <b>后像</b>恒发
     * pgoutput 'u'（proto.c——external-on-disk 列不发值；reorder buffer 的 toast hash
     * 只覆盖本事务新写值且逐变更重置）——engine 的 wire 上<b>根本没有值</b>，wal 侧
     * 若回查重组反而制造 engine 没有的输出、破坏逐字节对拍。故本场景断言重塑为：
     * ①UPDATE 后像双路同渲染 {@code w=<toast-unchanged>}；②<b>前像</b>（FULL 身份
     * 旧元组，服务端 toast_flatten_tuple 写入时已拍扁内联）双路同渲染完整值——Java
     * 侧独立复算 md5 链的 64 截断形态逐字面断言；③{@code JdbcToastProbe} 的对拍角色
     * 收窄为"补全窗口内新写值的 FPI 缺口"（离线 {@code ToastAssemblerTest} 锚定），
     * 窗口前指针不回查。另wal 段二的页对齐重叠窗口会重发已检查点的 INSERT 事务
     * （at-least-once，与 engine 重启重发同语义）——{@code parse} 的重复块全等容忍
     * 面吸收，重发块参与对拍断言（幂等重发逐字节全等）。</p>
     */
    @Test
    void restartUpdateUnchangedWideColumnRendersUnchangedToastParity() throws Exception {
        ParityEnv.resetScenario(TOAST_TABLE_DDL);
        EnginePathRunner engine = new EnginePathRunner(ParityEnv.engineConfig());
        ParityEnv.CdcCapture walCapture = ParityEnv.capture("org.vastdata.vbstream.walsource.cdc");
        Properties walCfg = ParityEnv.walSourceConfig();
        walCfg.setProperty(WalSource.KEY_STATE_DIR, toastStateDir.toString());
        WalSource wal1 = new WalSource(walCfg, new OutputRenderer());
        WalSource wal2 = new WalSource(walCfg, new OutputRenderer());
        Throwable primary = null;
        boolean resumed = false;
        List<String> engineLines = List.of();
        List<String> walLines = List.of();
        try {
            engine.start();
            try (wal1) {
                wal1.start();
                try (Connection conn = ParityEnv.newSqlConnection()) {
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (1,"
                            + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
                }
                await(() -> engine.emittedTxns() >= 1,
                        "TOAST 重启续传: 段一 engine 路未输出 INSERT 事务（emitted="
                                + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
                await(() -> wal1.dmlEmittedBuckets() >= 1,
                        "TOAST 重启续传: 段一 wal 路未发射 INSERT 桶（buckets="
                                + wal1.dmlEmittedBuckets() + ", terminal=" + wal1.receiverTerminalFailure() + "）");
            }   // close = 最终 best-effort 检查点 + 槽推进（段二续传的起点）
            try (wal2) {
                wal2.start();
                resumed = wal2.resumedFromState();
                try (Connection conn = ParityEnv.newSqlConnection()) {
                    exec(conn, "UPDATE parity.t_parity_toast SET id = 101 WHERE id = 1");
                }
                await(() -> engine.emittedTxns() >= 2,
                        "TOAST 重启续传: 段二 engine 路未输出 UPDATE 事务（emitted="
                                + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
                await(() -> wal2.dmlEmittedBuckets() >= 1,
                        "TOAST 重启续传: 段二 wal 路未发射 UPDATE 桶（buckets="
                                + wal2.dmlEmittedBuckets() + ", terminal=" + wal2.receiverTerminalFailure() + "）");
            }
        } catch (Throwable t) {
            primary = t;   // 不在 finally 内断言——finally 的断言失败会吞掉真正的根因
        } finally {
            walLines = walCapture.closeAndDrain();
            engineLines = engine.stopAndDrain();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）——否则重启前提不成立");
        if (primary != null) {
            wal1.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 段一接收器终态失败堆栈（TOAST 重启续传）", t));
            wal2.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 段二接收器终态失败堆栈（TOAST 重启续传）", t));
            fail("TOAST 重启续传: 对拍前置失败——" + primary + "\nengine 捕获=" + engineLines
                    + "\nwal 捕获=" + walLines, primary);
        }
        assertParity(engineLines, walLines, "TOAST 重启续传", 0);

        // 值面断言：UPDATE 后像 unchanged（engine 'u' 同形）、前像完整值（Java 独立复算）
        String wide = md5Chain(220);
        String truncated = wide.substring(0, 64) + "...(" + wide.length() + "B)";
        List<String> updateRows = walLines.stream()
                .filter(l -> l.contains("UPDATE parity.t_parity_toast")).toList();
        assertEquals(1, updateRows.size(), "应恰一行 UPDATE（宽列未变）:\n" + String.join("\n", walLines));
        String update = updateRows.get(0);
        assertTrue(update.contains("AFTER=[id=101, w=<toast-unchanged>]"),
                "UPDATE 后像的未变 external 列应渲染 unchanged 占位（engine 'u' 同形）:\n" + update);
        assertEquals(1, countLiteral(update, truncated),
                "UPDATE 前像应恰一处渲染完整宽值截断形态 " + truncated + "（FULL 身份旧元组写入时已拍扁内联）:\n"
                        + update);
        assertFalse(String.join("\n", walLines).contains("toast-unavailable"),
                "重启续传场景不应出现 toast-unavailable 降级字面:\n" + String.join("\n", walLines));
    }

    // ---- 场景 11：2PC 挂起零输出 / 弃桶 / COMMIT PREPARED 发射（Task 11 / 任务书场景 5 前三形态）----

    /**
     * 2PC 前三形态对拍（Task 11 场景 5）：①<b>挂起期零输出</b>——事务 DML 后
     * {@code PREPARE TRANSACTION 'gt1'}，1.5s 挂起窗口后仍未发射（engine
     * two_phase=true 下 BeginPrepare/Prepare 只是生命周期 INFO 行、不发事务块；wal 路
     * PREPARE 记录仅把桶标记挂起——两路挂起期皆零输出）；②<b>弃桶</b>——
     * {@code ROLLBACK PREPARED 'gt2'} 整桶弃（零 Begin/End）；③<b>COMMIT PREPARED
     * 发射</b>——{@code COMMIT PREPARED 'gt3'} 双路各发完整事务块且
     * {@code kind=TWO_PHASE} + {@code gid=gt3} 逐字符一致（engine 的 gid 取自
     * CommitPrepared 消息、wal 取自 COMMIT_PREPARED 记录 main 的 twophase chunk——
     * 同一服务端记录同源）。
     *
     * <p><b>事务控制面</b>：全程 autoCommit=true + 显式 {@code BEGIN}——PREPARE
     * TRANSACTION 会在服务端结束会话事务，pgjdbc 的 setAutoCommit(false) 内部记账不
     * 知情，后续语句不会补发 BEGIN（会退化成逐句自动提交，gt2/gt3 的行在 PREPARE 前
     * 就被提交）；显式 BEGIN 不经驱动事务状态机，跨 PREPARE 语句次序确定。</p>
     *
     * <p>断言面：归一化对拍（交集恰 gt3 块）+ 终态专项——两路捕获各恰一个块、
     * gid=gt3、kind=TWO_PHASE、gt1/gt2 的 xid 从不出现（挂起/弃桶零输出的终态钉）。
     * 收尾 {@code ROLLBACK PREPARED 'gt1'} 释放挂起事务（仍零输出）。</p>
     */
    @Test
    void twoPhaseSuspendDiscardAndCommitPreparedParity() throws Exception {
        ParityOutcome out = runParityTwoPhase("2PC 挂起/弃桶/COMMIT PREPARED", 1, TABLE_DDL, conn -> {
            exec(conn, "BEGIN");
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'gt1-pending', true, 1.0,"
                    + " '2026-10-07', timestamptz '2026-10-07 01:01:01+00')");
            exec(conn, "PREPARE TRANSACTION 'gt1'");
            Thread.sleep(1500);   // 挂起窗口实宽——若任一路早发（终态未到即出块），计数面即超 1 暴露
            exec(conn, "BEGIN");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'gt2-doomed', false, 2.0,"
                    + " '2026-10-07', timestamptz '2026-10-07 02:02:02+00')");
            exec(conn, "PREPARE TRANSACTION 'gt2'");
            exec(conn, "ROLLBACK PREPARED 'gt2'");
            exec(conn, "BEGIN");
            exec(conn, "INSERT INTO parity.t_parity VALUES (3, 'gt3-commit', true, 3.0,"
                    + " '2026-10-07', timestamptz '2026-10-07 03:03:03+00')");
            exec(conn, "PREPARE TRANSACTION 'gt3'");
            exec(conn, "COMMIT PREPARED 'gt3'");
            exec(conn, "ROLLBACK PREPARED 'gt1'");   // 收尾释放 gt1 挂起（弃桶路径二次覆盖）
        });
        Map<Long, TxBlock> blocks = parse(out.walLines());
        assertEquals(1, blocks.size(), "2PC 场景 wal 路应恰一个事务块（gt3）:\n"
                + String.join("\n", out.walLines()));
        TxBlock only = blocks.values().iterator().next();
        assertEquals("TWO_PHASE", only.kind, "COMMIT PREPARED 块 kind 应为 TWO_PHASE");
        assertEquals("gt3", only.gid, "COMMIT PREPARED 块 gid 应为 gt3");
        assertEquals(1, only.rows.size(), "gt3 块应恰一行（PREPARE 前的单行 INSERT）");
    }

    // ---- 场景 12：挂起期重启续传（Task 11 / 任务书场景 5 第四形态）----

    /**
     * 挂起期重启续传对拍（Task 11 场景 5 收官）：wal 路<b>两段会话</b>——段一捕
     * DML+{@code PREPARE TRANSACTION 'gtx'}（挂起桶跨检查点存活）后追加 &gt;8KB 的
     * filler 事务 WAL（250 行 INSERT，把 close 检查点前沿确定性推过 PREPARE 所在
     * 页——纯页重叠窗口救不回桶，钉"重放重建桶"路径），close 落最终检查点；段二同
     * stateDir 续传，{@code COMMIT PREPARED 'gtx'} 到达后发射（段二流起点回退到挂起
     * 桶首记录所在页，重放重建桶 + gid 后补发终态）。engine 路（two_phase=true）全程
     * 单会话，按 xid 交集对拍 diff 空。
     *
     * <p><b>缺陷发现记档（Task 11 实测红→修）</b>：Task 5 XactGrouper javadoc 原述
     * "检查点 LSN 必然落后 PREPARE 位点"不成立——检查点 lsn = catalog 已施加前沿，
     * 挂起桶的行记录可落在检查点页之前（filler 场景确定性如此），续传流起点（stored
     * lsn 页对齐）重放不到桶内行，COMMIT PREPARED 到达即无桶静默零发射 = 静默丢事务。
     * 修复：XactGrouper 暴露待决桶下界 {@code pendingFloorLsn}（桶首记录 lsn 的最小
     * 值），检查点随 lsn 一并落盘（StateStore formatVersion 3 增 dmlFloorLsn）、槽
     * 推进按 floor 封顶，续传时流起点取 min(stored, floor) 页对齐（catalog 过滤线仍
     * = stored，不二次施加字典）——重放段把桶内行 + PREPARE 记录重新喂给组装器，
     * 桶与 gid 重建、终态补发。段二重放段会重发已检查点的 filler 事务（幂等重发逐字
     * 节全等，{@code parse} 重复块容忍面吸收）。</p>
     */
    @Test
    void twoPhasePreparedPendingSurvivesRestartResumeParity() throws Exception {
        ParityEnv.resetScenario(TABLE_DDL);
        EnginePathRunner engine = new EnginePathRunner(ParityEnv.engineConfig(true));
        ParityEnv.CdcCapture walCapture = ParityEnv.capture("org.vastdata.vbstream.walsource.cdc");
        Properties walCfg = ParityEnv.walSourceConfig();
        walCfg.setProperty(WalSource.KEY_STATE_DIR, tpStateDir.toString());
        WalSource wal1 = new WalSource(walCfg, new OutputRenderer());
        WalSource wal2 = new WalSource(walCfg, new OutputRenderer());
        Throwable primary = null;
        boolean resumed = false;
        List<String> engineLines = List.of();
        List<String> walLines = List.of();
        try {
            engine.start();
            try (wal1) {
                wal1.start();
                try (Connection conn = ParityEnv.newSqlConnection()) {
                    exec(conn, "BEGIN");
                    exec(conn, "INSERT INTO parity.t_parity VALUES (10, 'gtx-pending', true, 10.0,"
                            + " '2026-10-07', timestamptz '2026-10-07 05:05:05+00')");
                    exec(conn, "PREPARE TRANSACTION 'gtx'");
                    // filler 事务：单语句 250 行（≥8KB WAL）——检查点前沿确定性越过 PREPARE 所在页
                    exec(conn, "INSERT INTO parity.t_parity SELECT g, 'filler-' || g, true, 0.1,"
                            + " '2026-10-07', timestamptz '2026-10-07 06:06:06+00'"
                            + " FROM generate_series(11, 260) g");
                }
                await(() -> engine.emittedTxns() >= 1,
                        "2PC 重启续传: 段一 engine 路未输出 filler 事务（emitted="
                                + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
                await(() -> wal1.dmlEmittedBuckets() >= 1,
                        "2PC 重启续传: 段一 wal 路未发射 filler 桶（buckets="
                                + wal1.dmlEmittedBuckets() + ", terminal=" + wal1.receiverTerminalFailure() + "）");
            }   // close = 最终 best-effort 检查点（前沿已过 PREPARE 所在页）+ 槽推进（按挂起桶 floor 封顶）
            try (wal2) {
                wal2.start();
                resumed = wal2.resumedFromState();
                try (Connection conn = ParityEnv.newSqlConnection()) {
                    exec(conn, "COMMIT PREPARED 'gtx'");
                }
                await(() -> engine.emittedTxns() >= 2,
                        "2PC 重启续传: 段二 engine 路未输出 COMMIT PREPARED 事务（emitted="
                                + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
                await(() -> wal2.dmlEmittedBuckets() >= 2,
                        "2PC 重启续传: 段二 wal 路未发射 COMMIT PREPARED 桶（重放重建桶失效——buckets="
                                + wal2.dmlEmittedBuckets() + ", terminal=" + wal2.receiverTerminalFailure() + "）");
            }
        } catch (Throwable t) {
            primary = t;   // 不在 finally 内断言——finally 的断言失败会吞掉真正的根因
        } finally {
            walLines = walCapture.closeAndDrain();
            engineLines = engine.stopAndDrain();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）——否则重启前提不成立");
        if (primary != null) {
            wal1.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 段一接收器终态失败堆栈（2PC 重启续传）", t));
            wal2.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 段二接收器终态失败堆栈（2PC 重启续传）", t));
            fail("2PC 重启续传: 对拍前置失败——" + primary + "\nengine 捕获=" + engineLines
                    + "\nwal 捕获=" + walLines, primary);
        }
        engineLines = stripEngineLifecycle(engineLines);
        assertParity(engineLines, walLines, "2PC 重启续传", 0);

        // 专项断言：两路各恰一个 gid=gtx 的 TWO_PHASE 块且 xid 同源（重放重建桶的终态钉）
        long gidXidEngine = soleTwoPhaseBlockXid(parse(engineLines), "gtx", "engine");
        long gidXidWal = soleTwoPhaseBlockXid(parse(walLines), "gtx", "wal");
        assertEquals(gidXidEngine, gidXidWal, "gtx 块 xid 双路应同源（同一 PREPARE 事务）");
    }

    // ---- 场景 13：生命周期中途停/续传——普通 DML（Task 12 / 任务书场景 6）----

    /**
     * 生命周期中途停/续传对拍（Task 12 场景 6）：wal 路<b>两段会话</b>——段一（带检查点
     * stateDir）捕两笔普通 DML 事务，close 落最终检查点；段二同 stateDir 续传
     * （{@code resumedFromState} 钉前提），<b>段二内</b>先 {@code ADD COLUMN}（DDL 与
     * DML 双段——字典经检查点持久化后流内继续演进）再一笔 I/U/D 混合事务。engine 路
     * 全程单会话，按 xid 交集对拍 diff 空。
     *
     * <p><b>at-least-once 重复语义两侧一致</b>：续传流起点按 stored lsn 页对齐下取整，
     * 段一落在该页的事务会整桶<b>幂等重发</b>——重发块由 {@link #parse} 的重复块容忍面
     * 吸收（逐字段+行序全等即丢，不等即 fail——重发非幂等即语义破坏信号）。DDL 安排在
     * 段二的<b>续传之后</b>：段一重发窗口内字典形态与原始发射时一致（检查点持久化的就是
     * 段一末态的 2 列形态），重发行逐字节全等；若 DDL 落在段一（跨停机点），段一的
     * DDL 前行重发时会按 3 列字典补尾 null 渲染、与原始 2 列块不等——重发面与 as-of 面
     * 交叉的已知形态，本场景以 DDL 后置钉确定性。</p>
     *
     * <p>专项断言：engine 全程恰 3 块（段 1 两笔 + 段 2 一笔）且 wal 路覆盖 engine 全部
     * xid——停机窗口无 DML 提交（段间只做 close/start），wal 的超集只能来自重发，
     * "双路都完整出现"由交集内逐行 diff 空承载。</p>
     */
    @Test
    void midStreamStopResumePlainDmlParityAcrossPaths() throws Exception {
        ParityEnv.resetScenario(TABLE_DDL);
        EnginePathRunner engine = new EnginePathRunner(ParityEnv.engineConfig());
        ParityEnv.CdcCapture walCapture = ParityEnv.capture("org.vastdata.vbstream.walsource.cdc");
        Properties walCfg = ParityEnv.walSourceConfig();
        walCfg.setProperty(WalSource.KEY_STATE_DIR, lifecycleStateDir.toString());
        WalSource wal1 = new WalSource(walCfg, new OutputRenderer());
        WalSource wal2 = new WalSource(walCfg, new OutputRenderer());
        Throwable primary = null;
        boolean resumed = false;
        List<String> engineLines = List.of();
        List<String> walLines = List.of();
        try {
            engine.start();
            try (wal1) {
                wal1.start();
                try (Connection conn = ParityEnv.newSqlConnection()) {
                    exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'seg1-a', true, 1.0,"
                            + " '2026-10-07', timestamptz '2026-10-07 01:01:01+00')");
                    exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'seg1-b', false, 2.0,"
                            + " '2026-10-07', timestamptz '2026-10-07 02:02:02+00')");
                }
                await(() -> engine.emittedTxns() >= 2,
                        "生命周期停续: 段一 engine 路未输出 2 个事务（emitted="
                                + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
                await(() -> wal1.dmlEmittedBuckets() >= 2,
                        "生命周期停续: 段一 wal 路未发射 2 个桶（buckets="
                                + wal1.dmlEmittedBuckets() + ", terminal=" + wal1.receiverTerminalFailure() + "）");
            }   // close = 最终 best-effort 检查点（段二续传起点）+ 槽推进
            try (wal2) {
                wal2.start();
                resumed = wal2.resumedFromState();
                try (Connection conn = ParityEnv.newSqlConnection()) {
                    // 段二 DDL：字典经检查点持久化后流内演进（续传态施加 ADD COLUMN 的 catalog 记录）
                    exec(conn, "ALTER TABLE parity.t_parity ADD COLUMN extra text");
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO parity.t_parity VALUES (3, 'seg2-c', true, 3.0,"
                            + " '2026-10-07', timestamptz '2026-10-07 03:03:03+00', 'seg2-new')");
                    exec(conn, "UPDATE parity.t_parity SET name = 'seg1-a-upd', extra = 'backfilled'"
                            + " WHERE id = 1");   // 前像 = 段一存储的两列元组、尾列补 null（as-of 语义）
                    exec(conn, "DELETE FROM parity.t_parity WHERE id = 2");
                    conn.commit();
                }
                await(() -> engine.emittedTxns() >= 3,
                        "生命周期停续: 段二 engine 路未输出段二事务（emitted="
                                + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
                await(() -> wal2.dmlEmittedBuckets() >= 1,
                        "生命周期停续: 段二 wal 路未发射段二桶（buckets="
                                + wal2.dmlEmittedBuckets() + ", terminal=" + wal2.receiverTerminalFailure() + "）");
            }
        } catch (Throwable t) {
            primary = t;   // 不在 finally 内断言——finally 的断言失败会吞掉真正的根因
        } finally {
            walLines = walCapture.closeAndDrain();
            engineLines = engine.stopAndDrain();
        }
        assertTrue(resumed, "段二应自检查点续传（段一 close 落盘有效检查点）——否则续传前提不成立");
        if (primary != null) {
            wal1.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 段一接收器终态失败堆栈（生命周期停续）", t));
            wal2.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 段二接收器终态失败堆栈（生命周期停续）", t));
            fail("生命周期停续: 对拍前置失败——" + primary + "\nengine 捕获=" + engineLines
                    + "\nwal 捕获=" + walLines, primary);
        }
        assertParity(engineLines, walLines, "生命周期停续", 0);

        // 专项：engine 全程恰 3 块 + wal 覆盖 engine 全部 xid（at-least-once：重发只增不改）
        Map<Long, TxBlock> engineBlocks = parse(engineLines);
        Map<Long, TxBlock> walBlocks = parse(walLines);
        assertEquals(3, engineBlocks.size(), "engine 全程应恰 3 个事务块（段一 2 + 段二 1）:\n"
                + String.join("\n", engineLines));
        assertTrue(walBlocks.keySet().containsAll(engineBlocks.keySet()),
                "wal 两段会话的并集应覆盖 engine 全部 xid（停机窗口无 DML，缺口即丢事务）:\n"
                        + "engine xids=" + engineBlocks.keySet() + ", wal xids=" + walBlocks.keySet());
        // 重发观测面（非断言——续传流起点的页对齐下取整使段一同页事务整桶重发，机会性命中；
        // parse 的重复块全等容忍面已吸收，此处留痕重发是否实际发生供场景质量归因）
        long beginCount = walLines.stream().filter(l -> l.startsWith("TXN-BEGIN ")).count();
        LOG_WAL_TERMINAL.info("生命周期停续重发观测: wal TXN-BEGIN 总数={}, 去重 xid 数={}（差>0 即重发发生）",
                beginCount, walBlocks.size());
    }

    // ---- 场景 14：CREATE SCHEMA nsp 字典面（Task 4 minor / Task 12 顺手清账）----

    /**
     * CREATE SCHEMA nsp 字典面对拍（Task 4 延期项，Task 12 顺手清账）：<b>流内</b>
     * {@code CREATE SCHEMA parity2} + 建表 + FULL 身份 → 单事务 I/I/U/D → 第二笔单行
     * INSERT，双路 diff 空。验证面：①wal 路 {@code pg_namespace} 的 WAL 重放面——
     * 引导种子（各路径 start 前）不含 parity2，nsp 字典行只能经流内 INSERT 记录重建；
     * ②DML 路径的 schema 名解析——行文本 {@code schema.table} 的 schema 段经 nsp 字典
     * 渲染（专项断言 wal 行含 {@code parity2.t_nsp}）；③engine 路经 pgoutput 'R' 的
     * namespace id 同源解析同形。reset 段仅清 parity2 残留（CASCADE 连表带 DDL 清），
     * 不预建——schema 诞生必须在两路起流之后才钉得住重放面。
     */
    @Test
    void createSchemaNamespaceDictionaryParityInStream() throws Exception {
        List<String> walLines = runParityWal("CREATE SCHEMA nsp 字典面", 2,
                new String[]{"DROP SCHEMA IF EXISTS parity2 CASCADE"}, conn -> {
                    exec(conn, "CREATE SCHEMA parity2");
                    exec(conn, "CREATE TABLE parity2.t_nsp (id int4 NOT NULL, name text,"
                            + " PRIMARY KEY (id))");
                    exec(conn, "ALTER TABLE parity2.t_nsp REPLICA IDENTITY FULL");
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO parity2.t_nsp VALUES (1, 'nsp-a')");
                    exec(conn, "INSERT INTO parity2.t_nsp VALUES (2, 'nsp-b')");
                    exec(conn, "UPDATE parity2.t_nsp SET name = 'nsp-a-upd' WHERE id = 1");
                    exec(conn, "DELETE FROM parity2.t_nsp WHERE id = 2");
                    conn.commit();
                    conn.setAutoCommit(true);
                    exec(conn, "INSERT INTO parity2.t_nsp VALUES (3, 'nsp-c')");   // 第二笔事务
                });
        assertTrue(walLines.stream().anyMatch(l -> l.contains("parity2.t_nsp")),
                "wal 行文本应含经 nsp 字典解析的 schema 限定名 parity2.t_nsp:\n"
                        + String.join("\n", walLines));
    }

    // ---- 场景 15/16：干扰矩阵——catalog 风暴下基础 DML / TOAST 场景复跑（Task 12）----

    /**
     * 干扰矩阵·基础 DML（Task 12）：场景 1 的单行事务对拍在 <b>catalog 风暴线程</b>
     * 并发下复跑（对齐 {@code WalAdversarialIT} 的干扰模式——独立连接循环
     * {@code ALTER TABLE SET (autovacuum_enabled)} 真/假交替 + {@code ANALYZE}，制造
     * pg_class 截断更新/INPLACE 的 catalog 噪声与目录死元组），×2 重复覆盖竞态。
     *
     * <p><b>容器裁定（dispatch 裁决记档）</b>：不换 Interference 容器、不给 ParityEnv
     * 加 autovacuum 拉满参数——加参数会影响所有场景的基线稳定性；干扰面由测试方法内
     * 起干扰线程承担（ParityEnv 默认容器）。干扰语句不产生用户表行变更（零事务块），
     * 捕获面仍只含场景事务——归一化对拍无需静默收尾。</p>
     */
    @RepeatedTest(2)
    void interferenceCatalogStormBasicDmlParity() throws Exception {
        runParityCore("干扰×基础 DML", 1, 0L, TABLE_DDL, ParityEnv.engineConfig(), conn -> {
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
        }, new String[]{"parity.t_parity"});
    }

    /**
     * 干扰矩阵·TOAST 三形态（Task 12）：场景 9 的三存储形态对拍在 catalog 风暴线程
     * 并发下复跑（干扰目标含主表与 toast 关系——{@code ANALYZE} 会触 toast 表/
     * 索引的统计面写放大），×2 重复。值面锚（截断尾注计数/unchanged 占位/无降级字面）
     * 复用 {@link #assertToastFootprints}——干扰不改发射行，足迹计数与场景 9 同构。
     */
    @RepeatedTest(2)
    void interferenceCatalogStormToastFormsParity() throws Exception {
        ParityOutcome out = runParityCore("干扰×TOAST 三形态", 1, 0L, TOAST_TABLE_DDL,
                ParityEnv.engineConfig(), conn -> {
                    conn.setAutoCommit(false);
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (1,"
                            + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (2,"
                            + " (SELECT string_agg(md5(i::text)||repeat('y',50),'') FROM generate_series(1,300) i))");
                    exec(conn, "INSERT INTO parity.t_parity_toast VALUES (3, repeat('x',10000))");
                    exec(conn, "UPDATE parity.t_parity_toast SET id = id + 100");
                    exec(conn, "DELETE FROM parity.t_parity_toast WHERE id = 102");
                    conn.commit();
                }, new String[]{"parity.t_parity_toast"});
        assertToastFootprints(out.walLines());
    }

    // ---- 对拍骨架 ----

    /**
     * 单场景对拍骨架（无 aborted 形态——walAborted=0 委派全参档；场景 1 系列表 DDL）。
     *
     * @param scenario     场景名（断言消息上下文）
     * @param expectedTxns 场景内已提交（产生用户表行）的事务数——两路各自的追平目标
     * @param dml          DML 执行器（收一条普通连接，语句异常即测试失败）
     * @return wal 路会话累计的截断 UPDATE 跳过计数（停流后读——close 含接收线程 join）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private long runParity(String scenario, long expectedTxns, SqlConsumer<Connection> dml) throws Exception {
        return runParity(scenario, expectedTxns, TABLE_DDL, dml);
    }

    /**
     * 单场景对拍骨架（无 aborted 形态——walAborted=0 委派全参档，自定义表 DDL——
     * 场景 2 类型矩阵表 / 场景 4 DDL-in-txn 表）。
     *
     * @param scenario     场景名（断言消息上下文）
     * @param expectedTxns 场景内已提交事务数
     * @param tableDdl     本场景建表语句（parity schema 内）
     * @param dml          DML 执行器
     * @return wal 路会话累计的截断 UPDATE 跳过计数
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private long runParity(String scenario, long expectedTxns, String[] tableDdl, SqlConsumer<Connection> dml)
            throws Exception {
        return runParity(scenario, expectedTxns, 0L, tableDdl, dml);
    }

    /**
     * 单场景对拍骨架：重置环境（槽/发布/表重建）→ 两路起流 → 执行 DML → 各自等待追平
     * （expectedTxns 个已提交事务输出完毕）→ 停流取捕获行 → 归一化对拍。
     *
     * <p>关键步骤：①{@link ParityEnv#resetScenario(String...)} 重建表与 publication、清两路
     * 槽位（每场景独立起点，窗口差最小化）；②engine 路（{@link EnginePathRunner}）与 wal 路
     * （{@link WalSource} + {@link OutputRenderer}）各挂各的 CDC logger 捕获 appender 后
     * 依次启动；③DML 执行器跑场景语句；④engine 路等待 emittedTxns 达标、wal 路等待
     * dmlEmittedBuckets 达标（volatile 计数轮询）；⑤先停 wal 再停 engine（engine 停机含
     * 毒丸排干——已提交未输出事务不丢）；⑥{@link #assertParity(List, List, String, long)}
     * 归一化对拍。边界与异常语义：任一路等待超时/启动失败即 fail（消息带场景名）。</p>
     *
     * @param scenario      场景名（断言消息上下文）
     * @param expectedTxns  场景内已提交（产生用户表行）的事务数——两路各自的追平目标
     * @param walAborted    场景内被回滚的子事务行数（wal 记账上界断言的期望差）
     * @param dml           DML 执行器（收一条普通连接，语句异常即测试失败）
     * @return wal 路会话累计的截断 UPDATE 跳过计数（停流后读——close 含接收线程 join，
     *                     happens-before 成立；场景断言回归哨兵用）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private long runParity(String scenario, long expectedTxns, long walAborted, SqlConsumer<Connection> dml)
            throws Exception {
        return runParity(scenario, expectedTxns, walAborted, TABLE_DDL, dml);
    }

    /**
     * 单场景对拍骨架全参档：重置环境（槽/发布/表重建）→ 两路起流 → 执行 DML → 各自
     * 等待追平（expectedTxns 个已提交事务输出完毕）→ 停流取捕获行 → 归一化对拍。
     *（关键步骤与边界语义见 {@link #runParity(String, long, long, SqlConsumer)} 的
     * 委派源——本档仅把表 DDL 参数化，供场景 2/4 的自定义表形态。）
     *
     * @param scenario      场景名（断言消息上下文）
     * @param expectedTxns  场景内已提交（产生用户表行）的事务数——两路各自的追平目标
     * @param walAborted    场景内被回滚的子事务行数（wal 记账上界断言的期望差）
     * @param tableDdl      本场景建表语句（parity schema 内，{@link ParityEnv#resetScenario}）
     * @param dml           DML 执行器（收一条普通连接，语句异常即测试失败）
     * @return wal 路会话累计的截断 UPDATE 跳过计数（停流后读——close 含接收线程 join，
     *                     happens-before 成立；场景断言回归哨兵用）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private long runParity(String scenario, long expectedTxns, long walAborted, String[] tableDdl,
            SqlConsumer<Connection> dml) throws Exception {
        return runParityCore(scenario, expectedTxns, walAborted, tableDdl, dml).skippedTruncatedRows();
    }

    /**
     * 单场景对拍骨架（无 aborted 形态）+ wal 捕获行透出档（Task 10 场景 3）：归一化
     * 对拍之外，场景还需对 wal 路输出行做值面补充断言（TOAST 三形态的截断尾注/降级
     * 字面出现性），本档把捕获行交回调用方。
     *
     * @param scenario     场景名（断言消息上下文）
     * @param expectedTxns 场景内已提交事务数——两路各自的追平目标
     * @param tableDdl     本场景建表语句（parity schema 内）
     * @param dml          DML 执行器
     * @return wal 路 CDC 捕获行（停流后快照——值面补充断言的原料）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private List<String> runParityWal(String scenario, long expectedTxns, String[] tableDdl,
            SqlConsumer<Connection> dml) throws Exception {
        return runParityCore(scenario, expectedTxns, 0L, tableDdl, dml).walLines();
    }

    /**
     * 单场景对拍核心委派档（Task 10 抽取、Task 11 增 engineCfg 全参档后转委派）：
     * engine 配置取缺省档（two_phase=false），实现体见
     * {@link #runParityCore(String, long, long, String[], ReplicationConfig, SqlConsumer)}。
     *
     * @param scenario      场景名（断言消息上下文）
     * @param expectedTxns  场景内已提交事务数——两路各自的追平目标
     * @param walAborted    场景内被回滚的子事务行数（wal 记账与实付的期望差）
     * @param tableDdl      本场景建表语句
     * @param dml           DML 执行器
     * @return 对拍结局快照（截断跳过哨兵 + wal 捕获行）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private ParityOutcome runParityCore(String scenario, long expectedTxns, long walAborted, String[] tableDdl,
            SqlConsumer<Connection> dml) throws Exception {
        return runParityCore(scenario, expectedTxns, walAborted, tableDdl, ParityEnv.engineConfig(), dml);
    }

    /**
     * 单场景对拍核心（Task 11 增 engineCfg 档）：重置环境 → 两路起流（engine 配置
     * 参数化——2PC 场景传 two_phase=true 档）→ 执行 DML → 各自等待追平 → 停流取
     * 捕获行 → 剥离 engine 生命周期控制行（two_phase=true 时 CDC logger 混入
     * BEGIN-PREPARE 等 INFO 行，非事务块面——见 {@link #ENGINE_LIFECYCLE}）→ 归一化
     * 对拍，返回截断哨兵与 wal 捕获行的快照。
     *
     * <p>关键步骤与边界语义同 {@link #runParity(String, long, long, SqlConsumer)} 的
     * 骨架描述（Task 12 起本档转委派——实现体在无干扰档
     * {@link #runParityCore(String, long, long, String[], ReplicationConfig, SqlConsumer, String[])}
     * （stormTables=null），two_phase=false 档剥离为 no-op，既有场景捕获面不变）。</p>
     *
     * @param scenario      场景名（断言消息上下文）
     * @param expectedTxns  场景内已提交事务数——两路各自的追平目标
     * @param walAborted    场景内被回滚的子事务行数（wal 记账与实付的期望差）
     * @param tableDdl      本场景建表语句
     * @param engineCfg     engine 路复制配置（two_phase 档参数化）
     * @param dml           DML 执行器
     * @return 对拍结局快照（截断跳过哨兵 + wal 捕获行）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private ParityOutcome runParityCore(String scenario, long expectedTxns, long walAborted, String[] tableDdl,
            ReplicationConfig engineCfg, SqlConsumer<Connection> dml) throws Exception {
        return runParityCore(scenario, expectedTxns, walAborted, tableDdl, engineCfg, dml, null);
    }

    /**
     * 单场景对拍核心全参档（Task 12 增干扰面）：在
     * {@link #runParityCore(String, long, long, String[], ReplicationConfig, SqlConsumer)}
     * 的骨架上参数化 <b>catalog 风暴线程</b>——{@code stormTables} 非 null 时，两路起流
     * 之后、DML 之前起 {@link #catalogStormThread(String, String...)}（干扰目标表已由
     * resetScenario 重建、两路引导已完成——风暴打在稳定的场景形态上），DML 执行完即
     * join（干扰窗口覆盖整个 DML 期，追平/停机期静默——干扰语句零用户表行变更，捕获面
     * 仍只含场景事务）；join 超时视为失败。null = 无干扰（既有场景路径不动）。
     *
     * @param scenario      场景名（断言消息上下文）
     * @param expectedTxns  场景内已提交（产生用户表行）的事务数——两路各自的追平目标
     * @param walAborted    场景内被回滚的子事务行数（wal 记账与实付的期望差）
     * @param tableDdl      本场景建表语句
     * @param engineCfg     engine 路复制配置（two_phase 档参数化）
     * @param dml           DML 执行器
     * @param stormTables   干扰目标表全名集（null = 无干扰线程）
     * @return 对拍结局快照（截断跳过哨兵 + wal 捕获行）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private ParityOutcome runParityCore(String scenario, long expectedTxns, long walAborted, String[] tableDdl,
            ReplicationConfig engineCfg, SqlConsumer<Connection> dml, String[] stormTables) throws Exception {
        ParityEnv.resetScenario(tableDdl);
        EnginePathRunner engine = new EnginePathRunner(engineCfg);
        ParityEnv.CdcCapture walCapture = ParityEnv.capture("org.vastdata.vbstream.walsource.cdc");
        WalSource wal = new WalSource(ParityEnv.walSourceConfig(), new OutputRenderer());
        Throwable primary = null;
        List<String> engineLines = List.of();
        List<String> walLines = List.of();
        Thread storm = null;
        try {
            engine.start();
            wal.start();
            if (stormTables != null) {
                storm = catalogStormThread(scenario, stormTables);
                storm.start();
            }
            try (Connection conn = ParityEnv.newSqlConnection()) {
                dml.accept(conn);
            }
            if (storm != null) {
                storm.join(STORM_JOIN_MS);
                assertTrue(!storm.isAlive(), scenario + ": 干扰线程应在 join 上限内退出");
            }
            await(() -> engine.emittedTxns() >= expectedTxns,
                    scenario + ": engine 路未输出 " + expectedTxns + " 个事务（emitted="
                            + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
            await(() -> wal.dmlEmittedBuckets() >= expectedTxns,
                    scenario + ": wal 路未发射 " + expectedTxns + " 个桶（buckets="
                            + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } catch (Throwable t) {
            primary = t;   // 不在 finally 内断言——finally 的断言失败会吞掉真正的根因
        } finally {
            wal.close();
            walLines = walCapture.closeAndDrain();
            engineLines = engine.stopAndDrain();
        }
        if (primary != null) {
            wal.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 接收器终态失败堆栈（{}）", scenario, t));
            fail(scenario + ": 对拍前置失败——" + primary + "\nengine 捕获=" + engineLines
                    + "\nwal 捕获=" + walLines, primary);
        }
        assertParity(stripEngineLifecycle(engineLines), walLines, scenario, walAborted);
        return new ParityOutcome(wal.dmlSkippedTruncatedRows(), walLines);
    }

    /**
     * catalog 风暴线程工厂（Task 12 干扰矩阵，对齐 {@code WalAdversarialIT} 的干扰模式）
     * ：独立连接循环 {@link #STORM_MS} 窗口——目标表逐个
     * {@code ALTER TABLE SET (autovacuum_enabled)} 真/假交替（合法地反复改 pg_class 行，
     * reloptions 深位列使截断更新走值编码/splice/自愈全路径，目录死元组随之累积）+
     * {@code ANALYZE} 风暴（pg_class 行 INPLACE + 统计面写放大）。
     *
     * <p>容错语义：单条语句失败（表在场景推进中的时间窗交错）仅 DEBUG 记录并继续下一轮；
     * 连接级失败（会话断）才终止线程并 WARN——干扰不得掩盖主断言。干扰语句不产生
     * 用户表行变更，两路捕获面不受污染。</p>
     *
     * @param scenario 场景名（日志上下文）
     * @param tables   干扰目标表全名集（已由 resetScenario 重建）
     * @return 未启动线程（调用方 start，DML 期并发、DML 后 join）
     */
    private Thread catalogStormThread(String scenario, String... tables) {
        return new Thread(() -> {
            try (Connection c = ParityEnv.newSqlConnection()) {
                boolean on = true;
                long deadline = System.currentTimeMillis() + STORM_MS;
                int rounds = 0;
                while (System.currentTimeMillis() < deadline) {
                    String toggle = on ? "true" : "false";
                    for (String tbl : tables) {
                        try (Statement st = c.createStatement()) {
                            st.execute("ALTER TABLE " + tbl + " SET (autovacuum_enabled = " + toggle + ")");
                            st.execute("ANALYZE " + tbl);
                        } catch (SQLException e) {
                            LOG_WAL_TERMINAL.debug("干扰语句跳过（{} 场景时序窗口内正常）: {}",
                                    tbl, e.getMessage());
                        }
                    }
                    on = !on;
                    rounds++;
                }
                LOG_WAL_TERMINAL.info("干扰线程退出（{}）: {} 轮 toggle+ANALYZE 风暴", scenario, rounds);
            } catch (SQLException e) {
                LOG_WAL_TERMINAL.warn("干扰线程连接级失败（{} 风暴提前终止）: {}", scenario, e.getMessage());
            }
        }, "parity-catalog-storm");
    }

    /**
     * 2PC 场景对拍骨架（Task 11）：engine 路换 two_phase=true 档配置（槽建带
     * two_phase、PREPARE 期 BeginPrepare/Prepare 生命周期行由对拍核心剥离）。
     *
     * @param scenario     场景名（断言消息上下文）
     * @param expectedTxns 场景内发射的事务块数（两阶段形态下 = COMMIT PREPARED 数）
     * @param tableDdl     本场景建表语句
     * @param dml          DML 执行器
     * @return 对拍结局快照（截断跳过哨兵 + wal 捕获行）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private ParityOutcome runParityTwoPhase(String scenario, long expectedTxns, String[] tableDdl,
            SqlConsumer<Connection> dml) throws Exception {
        return runParityCore(scenario, expectedTxns, 0L, tableDdl, ParityEnv.engineConfig(true), dml);
    }

    /**
     * 剥离 engine 生命周期控制行（{@link #ENGINE_LIFECYCLE} 匹配面）——two_phase=true
     * 场景 CDC logger 混入的 BEGIN-PREPARE/PREPARE/COMMIT-PREPARED/ROLLBACK-PREPARED
     * INFO 行不是事务块行，保留会触发行格式 fail；two_phase=false 档无此类行，本方法
     * 为恒等映射（输出为新列表，原列表不动）。
     *
     * @param lines engine 路捕获行
     * @return 剥离后的行列表（保持原序）
     */
    private static List<String> stripEngineLifecycle(List<String> lines) {
        return lines.stream().filter(l -> !ENGINE_LIFECYCLE.matcher(l).matches()).toList();
    }

    /**
     * 断言捕获块中恰一个指定 gid 的 TWO_PHASE 块并返回其 xid（2PC 专项断言的公共面
     * ——gid 语义上唯一，多块/零块都是终态面破坏）。
     *
     * @param blocks 捕获块（xid 键控）
     * @param gid    目标两阶段 gid
     * @param side   路径名（失败消息上下文）
     * @return 该块的 xid
     */
    private static long soleTwoPhaseBlockXid(Map<Long, TxBlock> blocks, String gid, String side) {
        List<TxBlock> hits = blocks.values().stream()
                .filter(b -> gid.equals(b.gid) && "TWO_PHASE".equals(b.kind)).toList();
        assertEquals(1, hits.size(), side + " 路应恰一个 gid=" + gid + " 的 TWO_PHASE 块:\n" + blocks);
        return hits.get(0).xid;
    }

    /**
     * 归一化对拍：两路捕获行各自解析为 xid → 事务块（头行字段 + 行文本序列），按 xid 求交集，
     * 断言交集非空 + 交集内逐行 diff 空 + 头行字段（kind/gid/commitLsn/commitTs）逐字符一致。
     *
     * <p>关键步骤：①行流按 TXN-BEGIN/TXN-END 切块（行文本 {@code "  [n] "} 前缀原样保留，
     * 行号两路天然同序）；②窗口差处理——交集外的 xid 只进失败信息描述，不作断言失败（起停
     * 窗口差异属两路固有）；③交集内断言：行文本序列与尾行逐字节相等、kind/gid/commitLsn
     * （hex）/commitTs（ISO-8601 字符串）相等；changes 归一化——engine.changes == 行数
     * （非流式形态下到达面即实付面），wal.changes &ge; 行数（桶记账 = aborted 过滤前，
     * SAVEPOINT 场景大于实付属正确语义）。边界与异常语义：任何 diff 以"场景 + xid + 两侧
     * 内容"组装失败消息（两侧全量块列表附后，防只看到首个差异）。</p>
     *
     * @param engineLines engine 路捕获行（logger org.vastdata.vbstream.cdc）
     * @param walLines    wal 路捕获行（logger org.vastdata.vbstream.walsource.cdc）
     * @param scenario    场景名（失败消息上下文）
     * @param walAborted  场景内被回滚的子事务行数（wal 桶记账与实付的期望差；无 aborted 为 0
     *                    ——Task 8.5 上界补丁：记账 = 实付 + aborted 恰等式，越过即记账异常）
     */
    private static void assertParity(List<String> engineLines, List<String> walLines, String scenario,
            long walAborted) {
        Map<Long, TxBlock> engine = parse(engineLines);
        Map<Long, TxBlock> wal = parse(walLines);
        Set<Long> intersection = new LinkedHashSet<>(engine.keySet());
        intersection.retainAll(wal.keySet());
        assertFalse(intersection.isEmpty(), () -> scenario + ": 对拍交集为空——engine xids=" + engine.keySet()
                + ", wal xids=" + wal.keySet());

        Set<Long> engineOnly = new LinkedHashSet<>(engine.keySet());
        engineOnly.removeAll(wal.keySet());
        Set<Long> walOnly = new LinkedHashSet<>(wal.keySet());
        walOnly.removeAll(engine.keySet());

        for (Long xid : intersection) {
            TxBlock e = engine.get(xid);
            TxBlock w = wal.get(xid);
            assertEquals(e.rows, w.rows, () -> scenario + ": xid=" + xid + " 行序列 diff");
            assertEquals(e.endLine, w.endLine, () -> scenario + ": xid=" + xid + " TXN-END 尾行整行 diff");
            assertEquals(e.kind, w.kind, () -> scenario + ": xid=" + xid + " kind diff");
            assertEquals(e.gid, w.gid, () -> scenario + ": xid=" + xid + " gid diff");
            assertEquals(e.commitLsnHex, w.commitLsnHex, () -> scenario + ": xid=" + xid + " commitLsn diff");
            assertEquals(e.commitTs, w.commitTs, () -> scenario + ": xid=" + xid + " commitTs diff"
                    + "（engine=" + e.commitTs + ", wal=" + w.commitTs + "）");
            assertEquals(e.rows.size(), (int) e.changes, () -> scenario + ": xid=" + xid
                    + " engine changes 应等于实付行数（非流式形态到达面即实付面）");
            assertEquals(w.rows.size() + walAborted, (int) w.changes, () -> scenario + ": xid=" + xid
                    + " wal changes 记账异常: 期望=实付 " + w.rows.size() + " + 场景 aborted "
                    + walAborted + "，实得 " + w.changes);
        }
        // 窗口外事务集合说明：不作断言失败，但两路捕获了交集外事务时在控制台留痕（起停窗口差异归因面）
        if (!engineOnly.isEmpty() || !walOnly.isEmpty()) {
            ParityEnv.logParityNotice(scenario, engineOnly, walOnly);
        }
    }

    /**
     * 捕获行流 → xid 有序事务块：TXN-BEGIN 开块（正则解头字段），{@code "  [n] "} 行文本
     * 逐条入块，TXN-END 闭块（尾行格式两路恒同——xid 相同即相等，不需单独保存）。
     *
     * <p><b>重复块容忍（Task 10）</b>：同 xid 的重复 TXN-BEGIN 允许——wal 路重启续传的
     * 页对齐重叠窗口会把已检查点的事务重发给 DML 面（at-least-once，engine 的
     * "重启重发不去重"文档承诺同语义）；闭块时若 xid 已有块，逐字段+行序全等则丢弃
     * 重复块、不等即 fail（重发不是幂等即语义破坏信号）。</p>
     *
     * <p>边界与异常语义：块未闭合/头行字段缺失抛 AssertionError（格式漂移即对拍基建失效，
     * 优于静默漏比对）。</p>
     *
     * @param lines 捕获行
     * @return xid → 块（保持出现序）
     */
    private static Map<Long, TxBlock> parse(List<String> lines) {
        Map<Long, TxBlock> out = new LinkedHashMap<>();
        TxBlock cur = null;
        TxBlock dupOf = null;   // 非 null = 当前块是既有 xid 的重发块，闭块时按全等校验后丢弃
        for (String line : lines) {
            if (line.startsWith("TXN-BEGIN ")) {
                Matcher m = HEADER.matcher(line);
                assertTrue(m.matches(), "TXN-BEGIN 头行格式漂移: " + line);
                cur = new TxBlock(Long.parseLong(m.group(1)), m.group(2), m.group(3),
                        m.group(4), m.group(5), Long.parseLong(m.group(6)));
                dupOf = out.get(cur.xid);
                if (dupOf == null) {
                    out.put(cur.xid, cur);
                }
            } else if (line.startsWith("  [")) {
                assertNotNull(cur, "行文本先于 TXN-BEGIN: " + line);
                cur.rows.add(line);
            } else if (line.startsWith("TXN-END")) {
                assertNotNull(cur, "TXN-END 先于 TXN-BEGIN: " + line);
                cur.endLine = line;
                if (dupOf != null) {
                    final TxBlock dup = cur;   // lambda 消息捕获用的 final 快照
                    assertEquals(dupOf.kind, dup.kind, () -> "重发块 kind 不等: xid=" + dup.xid);
                    assertEquals(dupOf.gid, dup.gid, () -> "重发块 gid 不等: xid=" + dup.xid);
                    assertEquals(dupOf.commitLsnHex, dup.commitLsnHex, () -> "重发块 commitLsn 不等: xid=" + dup.xid);
                    assertEquals(dupOf.commitTs, dup.commitTs, () -> "重发块 commitTs 不等: xid=" + dup.xid);
                    assertEquals(dupOf.changes, dup.changes, () -> "重发块 changes 不等: xid=" + dup.xid);
                    assertEquals(dupOf.rows, dup.rows, () -> "重发块行序列不等: xid=" + dup.xid);
                    assertEquals(dupOf.endLine, dup.endLine, () -> "重发块尾行不等: xid=" + dup.xid);
                    dupOf = null;
                }
                cur = null;
            } else {
                fail("未知 CDC 行格式（TXN-BEGIN / \"  [n] \" / TXN-END 三前缀均不匹配）: " + line);
            }
        }
        assertNull(cur, "事务块未闭合（TXN-END 缺失）");
        assertFalse(out.isEmpty(), "捕获行无任何事务块: " + lines);
        return out;
    }

    /**
     * 轮询等待条件成立（{@link #POLL_MS} 间隔、{@link #AWAIT_MS} 上限），超时 fail。
     *
     * @param cond 条件（幂等读）
     * @param what 等待对象描述（超时消息上下文）
     * @throws InterruptedException 睡眠被中断
     */
    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
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
    private static void exec(Connection conn, String... sqls) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : sqls) {
                st.execute(sql);
            }
        }
    }

    /** 单参 DML 执行器（可抛 SQLException 的受检消费者—— {@link java.util.function.Consumer} 不携检查异常）。 */
    @FunctionalInterface
    private interface SqlConsumer<T> {

        /** 执行场景 DML。 @param t 连接 @throws Exception 底层异常 */
        void accept(T t) throws Exception;
    }

    /**
     * 字面计数（{@code String.split} 以正则解释分隔串，{@code .()} 是元字符会假匹配
     * ——indexOf 循环做纯字面语义）。
     *
     * @param s       目标串（null 安全按空串）
     * @param literal 字面子串
     * @return 出现次数（不重叠——每次命中后游标跳过整个字面）
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
     * 复算场景值的确定性 md5 链（与 SQL {@code string_agg(md5(i::text),'')} 逐字面一致
     * ——int4 的 text 形态即十进制串，md5 输出恒 32 位小写 hex）。
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
        for (byte b : md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    /**
     * 对拍结局快照（Task 10 抽取）：截断跳过哨兵（既有场景断言用）+ wal 捕获行
     * （TOAST 场景的值面补充断言原料——engine 行经 diff 空已传递等价，不单独透出）。
     *
     * @param skippedTruncatedRows wal 路会话累计的截断 UPDATE 跳过计数
     * @param walLines             wal 路 CDC 捕获行（停流后快照）
     */
    private record ParityOutcome(long skippedTruncatedRows, List<String> walLines) {
    }

    /**
     * 对拍事务块：头行解析字段（xid/kind/gid/commitLsn hex/commitTs/changes）+ 行文本序列
     * （保持两路输出原样——含行号前缀，行号由各自 flush 期独立分配但序列同构）+ 尾行整行
     * （TXN-END——Task 8.5 补丁：整行保存参与逐字节比，两路格式恒同
     * {@code TXN-END   xid=N}，engine 尾行无 emitted 字段故无字段级归一化）。
     */
    private static final class TxBlock {
        final long xid;
        final String kind;
        final String gid;
        final String commitLsnHex;
        final String commitTs;
        final long changes;
        final List<String> rows = new ArrayList<>();
        String endLine;

        TxBlock(long xid, String kind, String gid, String commitLsnHex, String commitTs, long changes) {
            this.xid = xid;
            this.kind = kind;
            this.gid = gid;
            this.commitLsnHex = commitLsnHex;
            this.commitTs = commitTs;
            this.changes = changes;
        }
    }
}
