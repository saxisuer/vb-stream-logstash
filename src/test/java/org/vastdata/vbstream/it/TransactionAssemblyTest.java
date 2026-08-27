package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.DmlKind;
import org.vastdata.vbstream.replication.RelationRegistry;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.TransactionKind;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务组装集成测试：真库构造场景（spec §6.2），SessionHarness 录制 pgoutput 消息后
 * 离线回放给新 RelationRegistry + TransactionAssembler（组装器为确定性纯状态机，
 * 回放结果与在线组装一致），断言 Transaction 完整性。容器/槽清理复用 PgTestEnv。
 */
class TransactionAssemblyTest {

    /** 每用例后清理本测试专用槽：先杀 walsender 再删，避免槽残留跨用例干扰（槽不存在时静默）。 */
    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_assembly");
    }

    /**
     * 离线回放录制流：Relation 先经 registry（与 Main 装配顺序一致——PgReplicationSession.run
     * 在线时内置同样顺序），全部消息喂新组装器，收集输出的 Transaction。
     * 回放中组装器的 fail-fast 同样会抛（等效在线校验）。
     */
    private static List<Transaction> assembleRecording(List<PgOutputMessage> messages) {
        RelationRegistry registry = new RelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add);
        for (PgOutputMessage m : messages) {
            registry.accept(m);
            assembler.accept(m, registry);
        }
        return out;
    }

    /**
     * 提取事务内全部行变更首列（id）的文本值序列（逐值断言用）。
     * 仅对含 after 元组的 RowChange 安全：DELETE 变更 after 为 empty，orElseThrow 会抛
     * NoSuchElementException——场景 1 含 DELETE 故不走本辅助（dml 序列 + 逐值断言已覆盖），
     * 保留给 Task 8 的纯 INSERT 场景。
     */
    private static List<String> idsOf(Transaction t) {
        return t.changes().stream()
                .map(ch -> ((TupleValue.Text) ((RowChange) ch).after().orElseThrow()
                        .columns().get(0)).value())
                .toList();
    }

    /**
     * 场景 1（spec §6.2）：普通多语句事务组装完整性——单连接显式 BEGIN 内 I/I/U/D，COMMIT 后恰一个 NORMAL。
     * 关键步骤：建表/建 publication/TRUNCATE 在 harness.start 之前（槽从创建位点起，收不到之前的 DDL）→
     * 单 JDBC 连接显式事务执行两条 INSERT + 一条 UPDATE + 一条 DELETE 后 commit → 等首个 Commit 消息 →
     * 离线回放断言：事务数恰一、kind=NORMAL、gid 空、xid>0、changes 4 条且 dml 序列与数据值
     * （含 Relation 快照的表名/列名）正确。任何步骤失败由 awaitTermination 的超时断言或 assertEquals 抛出。
     */
    @Test
    void assemblesNormalMultiStatementTransaction() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_assembly",
                "CREATE PUBLICATION pub_assembly FOR TABLE t_assembly",
                "TRUNCATE t_assembly");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_assembly", "pub_assembly"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("INSERT INTO t_assembly VALUES (1,'a'),(2,'b')");
                st.execute("UPDATE t_assembly SET v='c' WHERE id=1");
                st.execute("DELETE FROM t_assembly WHERE id=2");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), "应恰一个事务: " + txns);
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.NORMAL, t.kind());
            assertNull(t.gid());
            assertTrue(t.xid() > 0, "xid 应来自 Begin: " + t.xid());
            assertEquals(4, t.changes().size());
            List<DmlKind> dmls = t.changes().stream()
                    .map(ch -> ((RowChange) ch).dml()).toList();
            assertEquals(List.of(DmlKind.INSERT, DmlKind.INSERT, DmlKind.UPDATE, DmlKind.DELETE), dmls);
            RowChange first = (RowChange) t.changes().get(0);
            assertEquals("t_assembly", first.relation().table());
            // 列名来自变更内嵌的 Relation 快照（spec §6.2 场景 1 断言点），而非仅当前 registry 状态
            assertEquals(List.of("id", "v"), first.relation().columns().stream()
                    .map(Column::name).toList());
            assertEquals("1", ((TupleValue.Text) first.after().orElseThrow().columns().get(0)).value());
            assertEquals("a", ((TupleValue.Text) first.after().orElseThrow().columns().get(1)).value());
        }
    }
}
