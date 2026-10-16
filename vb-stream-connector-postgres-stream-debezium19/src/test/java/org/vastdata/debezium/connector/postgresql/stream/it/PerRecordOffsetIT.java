package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 每条数据记录的 offset LSN 语义端到端实证(1.9.7 线;与 3.6.1 线 {@code PerRecordOffsetIT}
 * 断言面逐字同锚——两版分叉 = source 块 {@code origin} 字段与事务块 {@code id} 形态的
 * 版本际钉子,行内标注)。<b>行级 LSN 链路(2026-10-16)</b>:reader 在消息 append 进管道时
 * 逐帧记录 {@code stream.lastReceiveLSN()}(帧头 walStart = 产生该条输出的 WAL 记录位点,
 * pgjdbc 逻辑流 'w' 帧处理时覆写为本帧 walStart,取后即读),经 [8B walStart][payload]
 * 记录布局落盘,consumer 回放期把行级 lsn 接进 offset map 的 {@code lsn} 键与 source 块
 * (与 vanilla wal2json/decoderbufs 的帧位点路径语义同源)——{@code lsn_proc}/
 * {@code lsn_commit} 仍锚事务边界 endLsn(重启续传的事务原子单元不变)。
 *
 * <p>单用例双事务全证据面:
 * <ul>
 *   <li><b>事务一(3 行 INSERT)</b>:每条数据记录 {@code sourceOffset()} 的
 *       {@code lsn} 键为<b>行级真值</b>——行间严格递增、落在
 *       [提交前位点, endLsn) 半开区间内(首行可恰等于提交前读数——该读数即写前沿);
 *       {@code lsn_proc} == {@code lsn_commit} == 事务 endLsn(事务边界锚定);
 *       source 块 {@code lsn} 同源、{@code sequence} = [endLsn, 行级 lsn];
 *       事务块 BEGIN/END 记录的 lsn_proc/lsn_commit 锚 endLsn,lsn 亦恒 endLsn(onEnd 重锚),

 *       数据记录 {@code source.txId} 与事务块 {@code id} 同源;endLsn 被提交前后
 *       独立连接读的 {@code pg_current_wal_lsn()} 夹逼(确属本事务 commit LSN);</li>
 *   <li><b>事务二(1 行 INSERT)</b>:{@code lsn_commit} 与行级 {@code lsn} 均跨事务
 *       严格单调递增——行级严格递增断言同时是 pgjdbc 帧位点语义的驱动升级防回归锚。</li>
 * </ul>
 *
 * <p>版本际钉子(与 3.6.1 版的两处断言分叉):①vanilla 3.6.1 的
 * {@code PostgresSourceInfoStructMaker} 比 1.9.7 多 {@code origin}/{@code origin_lsn}
 * 两 source 块字段(本连接器不触达,3.6.1 侧恒 null),本侧断言 schema 无此字段;
 * ②事务块 {@code id} 形态——1.9.7 的 {@code TransactionMonitor} 原样透传传入 id
 * (纯数字 xid),而 vanilla 3.6.1 的 {@code PostgresTransactionMonitor.adjustTxId}
 * 把 id 强制重写为 {@code <xid>:<lsn>} 复合形态(3.6.1 版 IT 断言其复合形态,与本侧
 * 纯数字断言互文)。LSN 面(offset map 三键 / source 块 {@code lsn}/{@code sequence})
 * 两版本键名与语义零差异——双模块互证。
 *
 * <p>夹具约定:单表(id int PK + v text)与单表 publication 预建(DDL 先于建槽,
 * 不产生解码输出);独立槽 {@code offset_lsn_it} 前后清删;管道目录 @TempDir 绝对
 * 路径。小事务走 Begin..Commit 的 NORMAL 路径,provide.transaction.metadata=true
 * 下每事务另有 BEGIN/END 两条事务块记录。需要本机 Docker。
 */
class PerRecordOffsetIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "offset_lsn_it";

    /** 夹具表名。 */
    private static final String TABLE = "t_offset_lsn";

    /** 夹具 publication 名。 */
    private static final String PUBLICATION = "pub_offset_lsn";

    /** 表的数据记录 topic(逻辑名 ms2it,见 baseConfig)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir(cleanup = CleanupMode.NEVER)
    Path pipeDir;

    /**
     * 每用例前清残留槽:上次异常退出留下的同名槽会从旧 confirmed_flush_lsn 续传,
     * 静默吞掉建流前的写入使记录断言失真。幂等。
     */
    @BeforeEach
    void cleanResidualSlot() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /** 每用例后清理:先停引擎再删槽(次序见基类 {@link #stopEngineAndDropSlot})。 */
    @AfterEach
    void dropSlot() {
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 责任:实证行级 LSN 链路的端到端语义——offset 的 lsn 键/source 块为帧头 walStart 行级真值,lsn_proc/lsn_commit 锚事务 endLsn 不变。
     * 关键步骤:建夹具并起引擎 → 读事务前 WAL 位点 → 单事务 3 行 INSERT → 读事务后
     * WAL 位点(两读数夹逼本事务 commit LSN)→ 消费 5 条(BEGIN + 3 数据 + END)逐面
     * 断言(三键同值/行间同值/source 块 lsn 与 sequence/事务块 offset 同值/txId
     * 同源/夹逼)→ 事务二单行 INSERT 消费 3 条断言 lsn_commit 跨事务单调递增 →
     * 版本际钉子(source 块无 origin 字段)。
     * 边界:任一批凑不齐条数即 fail(消费超时内事务未达属失败而非跳过)。
     */
    @Test
    void perRecordOffsetLsnsAreAllTransactionCommitLsn() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, PUBLICATION, pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        // ---- 事务一:显式 3 行 INSERT,提交前后独立连接读 pg_current_wal_lsn() 夹逼 ----
        long before = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_lsn()");
        insertThreeRowsInOneTransaction();
        long after = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_lsn()");

        List<SourceRecord> tx1 = consumeRecordsUnchecked(5);
        assertEquals(5, tx1.size(), "事务一 BEGIN + 3 数据 + END 共 5 条应到达: " + describe(tx1));

        List<SourceRecord> dataRecords = recordsForTopic(tx1, TOPIC);
        assertEquals(3, dataRecords.size(), "事务一恰 3 条数据记录");

        Long tx1EndLsn = null;
        Long tx1SourceTxId = null;
        long prevRowLsn = -1L;
        long lastRowLsn = -1L;
        for (SourceRecord r : dataRecords) {
            Map<String, ?> offset = r.sourceOffset();
            long[] lsns = assertOffsetLsnRoles(offset, "事务一数据记录");   // [lsn, lsn_proc, lsn_commit]
            long rowLsn = lsns[0];
            if (tx1EndLsn == null) {
                tx1EndLsn = lsns[2];
            }
            assertEquals(tx1EndLsn, lsns[1], "lsn_proc == lsn_commit == 事务 endLsn(事务边界锚定不变)");
            assertEquals(tx1EndLsn, lsns[2], "lsn_commit == 事务 endLsn(Begin updateCommitPosition 双写)");
            assertTrue(prevRowLsn < rowLsn, "行级 lsn 严格递增(各行帧头 walStart = 各自 change 的 WAL 位点): "
                    + prevRowLsn + " -> " + rowLsn);
            prevRowLsn = rowLsn;
            assertTrue(before <= rowLsn && rowLsn < tx1EndLsn,
                    "行级 lsn 落在本事务 WAL 区间内(提交前位点 <= 各行 change 位点 < commit 末位点;"
                    + "首行可恰等于提交前读数——该读数即写前沿,事务首条 change 记录从此写入): "
                            + before + " <= " + rowLsn + " < " + tx1EndLsn);
            Struct source = ((Struct) r.value()).getStruct("source");
            assertNotNull(source, "数据记录 source 块必在");
            assertEquals(rowLsn, source.getInt64("lsn"), "source 块 lsn 与 offset map lsn 同源(行级)");
            String[] sequence = sequenceElements(source.getString("sequence"));
            assertEquals(2, sequence.length, "sequence 恒两元素 [lastCommitLsn, lsn]");
            assertEquals(Long.toString(tx1EndLsn), sequence[0], "sequence 首元素 = 事务 endLsn(lastCommitLsn)");
            assertEquals(Long.toString(rowLsn), sequence[1], "sequence 次元素 = 行级 lsn: " + source.getString("sequence"));
            if (tx1SourceTxId == null) {
                tx1SourceTxId = source.getInt64("txId");
                assertNotNull(tx1SourceTxId, "source.txId 必在(顶层 xid)");
            }
            else {
                assertEquals(tx1SourceTxId, source.getInt64("txId"), "同事务行间 txId 同值");
            }
            lastRowLsn = rowLsn;
        }
        assertTrue(tx1EndLsn <= after,
                "endLsn 被本事务提交前后的 pg_current_wal_lsn() 夹逼(证明确属本事务 commit LSN): "
                        + before + " < 各行级 lsn < " + tx1EndLsn + " <= " + after);

        // ---- 事务块 BEGIN/END 记录:三键均锚 endLsn(END 发出前 onEnd 重锚——BEGIN/END 对的 id 配对语义) ----
        List<SourceRecord> txBlocks = recordsForTopic(tx1, TX_TOPIC);
        assertEquals(2, txBlocks.size(), "事务一恰一对 BEGIN/END 事务块记录");
        long[] beginLsns = assertOffsetLsnRoles(txBlocks.get(0).sourceOffset(), "BEGIN 事务块记录");
        assertEquals(tx1EndLsn, beginLsns[0], "BEGIN 块 lsn = endLsn(onBegin 双写后发出)");
        assertEquals(tx1EndLsn, beginLsns[1], "BEGIN 块 lsn_proc = endLsn");
        assertEquals(tx1EndLsn, beginLsns[2], "BEGIN 块 lsn_commit = endLsn");
        long[] endLsns = assertOffsetLsnRoles(txBlocks.get(1).sourceOffset(), "END 事务块记录");
        assertEquals(tx1EndLsn, endLsns[0], "END 块 lsn = endLsn(onEnd 发出前重锚——BEGIN/END 事务块对的 id 配对语义,vanilla commit 路径同款)");
        assertEquals(tx1EndLsn, endLsns[1], "END 块 lsn_proc = endLsn(事务边界锚定)");
        assertEquals(tx1EndLsn, endLsns[2], "END 块 lsn_commit = endLsn(重启续传锚,事务原子单元不变)");
        Struct beginBlock = (Struct) txBlocks.get(0).value();
        assertEquals("BEGIN", beginBlock.getString("status"), "首条事务块为 BEGIN");
        assertEquals(Long.toString(tx1SourceTxId), beginBlock.getString("id"),
                "数据记录 source.txId 与事务块 id 同源(1.9.7 版为纯数字 xid;3.6.1 版被 vanilla PostgresTransactionMonitor 重写为 <xid>:<lsn> 复合形态——版本际差异)");
        Struct endBlock = (Struct) txBlocks.get(1).value();
        assertEquals("END", endBlock.getString("status"), "第二条事务块为 END");
        assertEquals(3L, endBlock.getInt64("event_count"), "事务一 END 计数 = 3 行");

        // ---- 事务二:单行 INSERT,lsn_commit 与行级 lsn 均跨事务严格单调递增 ----
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (100, 'tx2')");
        List<SourceRecord> tx2 = consumeRecordsUnchecked(3);
        assertEquals(3, tx2.size(), "事务二 BEGIN + 1 数据 + END 共 3 条应到达: " + describe(tx2));
        List<SourceRecord> tx2Data = recordsForTopic(tx2, TOPIC);
        assertEquals(1, tx2Data.size(), "事务二恰 1 条数据记录");
        long[] tx2Lsns = assertOffsetLsnRoles(tx2Data.get(0).sourceOffset(), "事务二数据记录");
        assertTrue(tx2Lsns[2] > tx1EndLsn, "跨事务 lsn_commit 单调递增: " + tx1EndLsn + " -> " + tx2Lsns[2]);
        assertTrue(tx2Lsns[0] > lastRowLsn, "跨事务行级 lsn 单调递增: " + lastRowLsn + " -> " + tx2Lsns[0]);

        // ---- 版本际钉子(与 3.6.1 版唯一断言分叉):1.9.7 的 source 块无 origin 字段 ----
        Struct tx2Source = ((Struct) tx2Data.get(0).value()).getStruct("source");
        assertNull(tx2Source.schema().field("origin"),
                "1.9.7 版 source 块无 origin 字段(3.6.1 的 vanilla StructMaker 才引入)");
        assertNull(tx2Source.schema().field("origin_lsn"),
                "1.9.7 版 source 块无 origin_lsn 字段(3.6.1 的 vanilla StructMaker 才引入)");
    }

    /**
     * 责任:建夹具表与 publication 并清空历史数据。关键步骤:DDL 先于建槽执行——DDL
     * 不产生解码输出,不污染记录计数;publication 覆盖夹具表(pgoutput 只为 publication
     * 内的表发数据消息);TRUNCATE 清残行(上次用例的行会让显式事务外的夹具状态失真)。
     * 边界:execSql 自动提交,各语句独立执行,SQLException 原样上抛。
     */
    private void createFixture() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS " + PUBLICATION,
                "CREATE PUBLICATION " + PUBLICATION + " FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /**
     * 责任:显式单事务插 3 行(事务一的夹具动作)。关键步骤:独立连接关自动提交,
     * 三条 INSERT 包拢在同一事务内提交——execSql 是逐语句自动提交,直接用它三条
     * INSERT 会拆成三个事务(BEGIN/数据/END 各三对),与本用例"单事务 3 行"的前提
     * 相悖;finally 块 commit 保证异常路径也收敛连接(未提交事务随连接关闭回滚)。
     * 边界:仅提交一次,失败时 SQLException 原样上抛。
     */
    private void insertThreeRowsInOneTransaction() throws Exception {
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.execute("INSERT INTO " + TABLE + " VALUES (1, 'row-1')");
                st.execute("INSERT INTO " + TABLE + " VALUES (2, 'row-2')");
                st.execute("INSERT INTO " + TABLE + " VALUES (3, 'row-3')");
            }
            finally {
                c.commit();
            }
        }
    }

    /**
     * 责任:断言一条记录的 offset map 三 LSN 键存在,返回三键值数组 [lsn, lsn_proc, lsn_commit]。
     * 关键步骤:逐键取 Long 并断言非 null(Begin 双写后 lsn_proc/lsn_commit 必在,数据记录的
     * lsn 键经行级 updateWalPosition 后亦必在;缺键即 offset 语义回归)。注意<b>不再断言三键
     * 同值</b>——lsn 是行级真值(帧头 walStart,事务内逐行递增),lsn_proc/lsn_commit 锚事务
     * 边界 endLsn,角色语义由调用方断言(行级 LSN 链路,2026-10-16)。
     * 边界:任一键缺失即 AssertionError,消息带调用方上下文(记录在事务中的角色)。
     *
     * @param offset  记录的 sourceOffset() map
     * @param context 断言失败消息的上下文前缀(记录角色)
     * @return [lsn, lsn_proc, lsn_commit] 三键值
     */
    private static long[] assertOffsetLsnRoles(Map<String, ?> offset, String context) {
        Long lsn = asLongOrNull(offset.get("lsn"));
        Long lsnProc = asLongOrNull(offset.get("lsn_proc"));
        Long lsnCommit = asLongOrNull(offset.get("lsn_commit"));
        assertNotNull(lsn, context + ": offset 缺 lsn 键");
        assertNotNull(lsnProc, context + ": offset 缺 lsn_proc 键(Begin 双写后必在)");
        assertNotNull(lsnCommit, context + ": offset 缺 lsn_commit 键(Begin 双写后必在)");
        return new long[]{ lsn, lsnProc, lsnCommit };
    }

    /**
     * 责任:offset map 值的宽容取数——Long 直取,其它 Number 子类(理论上不出现,
     * getOffset 放入的就是 Long)经 longValue 归一,非 Number 返回 null 交由调用方
     * 的非 null 断言报告。边界:null 输入返回 null。
     *
     * @param value offset map 里的值
     * @return Long 形态(非 Number 或 null 时为 null)
     */
    private static Long asLongOrNull(Object value) {
        if (value instanceof Number n) {
            return n instanceof Long l ? l : n.longValue();
        }
        return null;
    }

    /**
     * 责任:解析 source 块 sequence 字段(JSON 数组文本 {@code "[X, Y]"})为元素数组。
     * 关键步骤:剥方括号与双引号、按逗号切分、逐元素去空白——不引 JSON 库,两元素
     * 数值文本的轻量形态固定,解析失败面交由调用方断言(length/取值)报告。
     * 边界:JSON null 元素保留 "null" 文本原样(调用方断言非 null 面处理)。
     *
     * @param sequence source 块 sequence 字段原文
     * @return 去壳后的元素数组(保持原序)
     */
    private static String[] sequenceElements(String sequence) {
        String body = sequence.replace("[", "").replace("]", "").replace("\"", "");
        String[] parts = body.split(",");
        String[] trimmed = new String[parts.length];
        for (int i = 0; i < parts.length; i++) {
            trimmed[i] = parts[i].trim();
        }
        return trimmed;
    }
}
