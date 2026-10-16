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
 * 每条数据记录的 offset LSN 语义端到端实证(3.6.1 线;与 1.9.7 线 {@code PerRecordOffsetIT}
 * 断言面逐字同锚——两版唯一分叉 = source 块 {@code origin} 字段的版本际钉子,行内标注):
 * <b>事务内所有记录的 offset 恒为事务 commit LSN(endLsn),无行级 LSN</b>——
 * Begin 事件 {@code updateCommitPosition(endLsn, endLsn)} 双写锚定
 * ({@code DispatcherTransactionListener.onBegin}),其后本事务每条数据记录的
 * {@code updateWalPosition(currentEndLsn, currentEndLsn, ...)} 两 LSN 参依然是
 * 事务 endLsn(非该行的 WAL 位点)。根因是 pgoutput 的 I/U/D 行消息本身不携带 LSN
 * (仅 B 的 final_lsn / C 的 end_lsn 有),行级 LSN 协议层不可得;Debezium 的 offset
 * 原子单元就是事务,重启续传/去重的粒度亦如此。
 *
 * <p>单用例双事务全证据面:
 * <ul>
 *   <li><b>事务一(3 行 INSERT)</b>:每条数据记录 {@code sourceOffset()} 的
 *       {@code lsn}/{@code lsn_proc}/{@code lsn_commit} 三键同值;三条记录行间
 *       互相同值(<b>无行级 LSN 的实证</b>);source 块 {@code lsn} 同值;
 *       source 块 {@code sequence}(JSON {@code "[X, X]"} 两元素)同源;
 *       事务块 BEGIN/END 记录的 offset 三键与数据记录同值(<b>事务 = offset
 *       原子单元</b>);数据记录 {@code source.txId} 与事务块 {@code id} 同源;
 *       该 LSN 被提交前后独立连接读的 {@code pg_current_wal_lsn()} 夹逼
 *       (证明三条记录同的确实是本事务的 commit LSN,而非其它公共值);</li>
 *   <li><b>事务二(1 行 INSERT)</b>:{@code lsn_commit} 跨事务严格单调递增。</li>
 * </ul>
 *
 * <p>版本际钉子(与 1.9.7 版的两处断言分叉):①vanilla 3.6.1 的
 * {@code PostgresSourceInfoStructMaker} 比 1.9.7 多 {@code origin}/{@code origin_lsn}
 * 两 source 块字段(本连接器不触达,本侧恒 null);②事务块 {@code id} 形态——vanilla
 * 3.6.1 的 {@code PostgresTransactionMonitor.adjustTxId} 把 id 强制重写为
 * {@code <xid>:<lsn>} 复合形态(反汇编实证:{@code String.format("%s:%s",
 * transactionId, lastSeenLsn)},lsn 取 offset 的 lastSeenLsn——本连接器 onBegin
 * 先 {@code updateCommitPosition(endLsn)} 后 dispatch,后缀即事务 endLsn),1.9.7
 * 无该覆写、id 原样透传纯数字 xid。LSN 面(offset map 三键 / source 块
 * {@code lsn}/{@code sequence})两版本键名与语义零差异——双模块互证。
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
     * 责任:实证每条数据记录的 offset LSN 全部是事务 commit LSN(无行级 LSN)。
     * 关键步骤:建夹具并起引擎 → 读事务前 WAL 位点 → 单事务 3 行 INSERT → 读事务后
     * WAL 位点(两读数夹逼本事务 commit LSN)→ 消费 5 条(BEGIN + 3 数据 + END)逐面
     * 断言(三键同值/行间同值/source 块 lsn 与 sequence/事务块 offset 同值/txId
     * 同源/夹逼)→ 事务二单行 INSERT 消费 3 条断言 lsn_commit 跨事务单调递增 →
     * 版本际钉子(source 块含 origin 字段且恒 null)。
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

        Long tx1Lsn = null;
        Long tx1SourceTxId = null;
        for (SourceRecord r : dataRecords) {
            Map<String, ?> offset = r.sourceOffset();
            Long lsn = assertOffsetTripleUniform(offset, "事务一数据记录");
            if (tx1Lsn == null) {
                tx1Lsn = lsn;
            }
            else {
                assertEquals(tx1Lsn, lsn, "行间 offset 同值(无行级 LSN——每行的 LSN 位不是自己的 WAL 位点)");
            }
            Struct source = ((Struct) r.value()).getStruct("source");
            assertNotNull(source, "数据记录 source 块必在");
            assertEquals(tx1Lsn, source.getInt64("lsn"), "source 块 lsn 与 offset map lsn 同源");
            String[] sequence = sequenceElements(source.getString("sequence"));
            assertEquals(2, sequence.length, "sequence 恒两元素 [lastCommitLsn, lsn]");
            for (String element : sequence) {
                assertEquals(Long.toString(tx1Lsn), element,
                        "sequence 两元素与事务 endLsn 同值(Begin 双写后 lastCommitLsn 与 lsn 同源): " + source.getString("sequence"));
            }
            if (tx1SourceTxId == null) {
                tx1SourceTxId = source.getInt64("txId");
                assertNotNull(tx1SourceTxId, "source.txId 必在(顶层 xid)");
            }
            else {
                assertEquals(tx1SourceTxId, source.getInt64("txId"), "同事务行间 txId 同值");
            }
            assertTrue(before < lsn && lsn <= after,
                    "offset LSN 被本事务提交前后的 pg_current_wal_lsn() 夹逼(证明是本事务 commit LSN): "
                            + before + " < " + lsn + " <= " + after);
        }

        // ---- 事务块 BEGIN/END 记录:offset 三键与数据记录同值(事务 = offset 原子单元) ----
        List<SourceRecord> txBlocks = recordsForTopic(tx1, TX_TOPIC);
        assertEquals(2, txBlocks.size(), "事务一恰一对 BEGIN/END 事务块记录");
        for (SourceRecord r : txBlocks) {
            assertEquals(tx1Lsn, assertOffsetTripleUniform(r.sourceOffset(), "事务块记录"),
                    "事务块记录 offset 与数据记录同值(事务 = offset 原子单元)");
        }
        Struct beginBlock = (Struct) txBlocks.get(0).value();
        assertEquals("BEGIN", beginBlock.getString("status"), "首条事务块为 BEGIN");
        assertEquals(Long.toString(tx1SourceTxId) + ":" + tx1Lsn, beginBlock.getString("id"),
                "3.6.1 版事务块 id = <xid>:<endLsn> 复合形态(vanilla PostgresTransactionMonitor.adjustTxId 强制后缀 offset 的 lastSeenLsn;1.9.7 版为纯数字 xid)——数据记录 source.txId 与 id 前缀同源");
        Struct endBlock = (Struct) txBlocks.get(1).value();
        assertEquals("END", endBlock.getString("status"), "第二条事务块为 END");
        assertEquals(3L, endBlock.getInt64("event_count"), "事务一 END 计数 = 3 行");

        // ---- 事务二:单行 INSERT,lsn_commit 跨事务严格单调递增 ----
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (100, 'tx2')");
        List<SourceRecord> tx2 = consumeRecordsUnchecked(3);
        assertEquals(3, tx2.size(), "事务二 BEGIN + 1 数据 + END 共 3 条应到达: " + describe(tx2));
        List<SourceRecord> tx2Data = recordsForTopic(tx2, TOPIC);
        assertEquals(1, tx2Data.size(), "事务二恰 1 条数据记录");
        Long tx2Lsn = assertOffsetTripleUniform(tx2Data.get(0).sourceOffset(), "事务二数据记录");
        assertTrue(tx2Lsn > tx1Lsn, "跨事务 lsn_commit 单调递增: " + tx1Lsn + " -> " + tx2Lsn);

        // ---- 版本际钉子(与 1.9.7 版唯一断言分叉):3.6.1 的 source 块含 origin 字段且本连接器恒 null ----
        Struct tx2Source = ((Struct) tx2Data.get(0).value()).getStruct("source");
        assertNotNull(tx2Source.schema().field("origin"),
                "3.6.1 版 source 块含 origin 字段(vanilla StructMaker 版本际差异)");
        assertNotNull(tx2Source.schema().field("origin_lsn"),
                "3.6.1 版 source 块含 origin_lsn 字段(vanilla StructMaker 版本际差异)");
        assertNull(tx2Source.getString("origin"), "origin 恒 null(本连接器不触达 origin 消息面)");
        assertNull(tx2Source.getInt64("origin_lsn"), "origin_lsn 恒 null(本连接器不触达 origin 消息面)");
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
     * 责任:断言一条记录的 offset map 三 LSN 键存在且同值,返回该公共值。
     * 关键步骤:逐键取 Long 并断言非 null(Begin 双写后三键必在;缺键即 offset
     * 语义回归)→ 两两断言相等。边界:任一键缺失或不同值即 AssertionError,
     * 消息带调用方上下文(记录在事务中的角色)。
     *
     * @param offset  记录的 sourceOffset() map
     * @param context 断言失败消息的上下文前缀(记录角色)
     * @return 三键的公共 LSN 值
     */
    private static Long assertOffsetTripleUniform(Map<String, ?> offset, String context) {
        Long lsn = asLongOrNull(offset.get("lsn"));
        Long lsnProc = asLongOrNull(offset.get("lsn_proc"));
        Long lsnCommit = asLongOrNull(offset.get("lsn_commit"));
        assertNotNull(lsn, context + ": offset 缺 lsn 键(Begin 双写后必在)");
        assertNotNull(lsnProc, context + ": offset 缺 lsn_proc 键(Begin 双写后必在)");
        assertNotNull(lsnCommit, context + ": offset 缺 lsn_commit 键(Begin 双写后必在)");
        assertEquals(lsn, lsnProc, context + ": lsn == lsn_proc(每行 updateWalPosition 双参同为 endLsn)");
        assertEquals(lsn, lsnCommit, context + ": lsn == lsn_commit(Begin updateCommitPosition 双写)");
        return lsn;
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
