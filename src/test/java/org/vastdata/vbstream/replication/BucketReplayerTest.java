package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BucketReplayer 专项单测：**绕过组装器**直接手造 {@link PayloadUnit} 列表驱动回放，逐机制验证
 * （assembly-spill 设计 §4.4 的机制级切片）——I/U/D 三单元的 RowChange 语义与 Relation 快照嵌入、
 * aborted 子事务过滤（被剔单元不解码不回调）、同 oid 多版本下按单元 seq 的 asOf 渲染（DDL 中途
 * 换版不串位）、非数据消息类型的 fail-fast、空桶回放产出空列表（空桶提交路径）。
 *
 * <p>夹具约定：单元 payload 字节全部经 {@link PgWire} 构造（流式单元用 {@link PgWire#streamed}
 * 加 Int32 xid 前缀，满足"前缀值 == streamXid"的回放消费契约）；registry 预置版本用 Relation
 * record 直接构造（沿 {@link VersionedRelationRegistryTest} 的样本模式，表名区分版本）；
 * 回放器以 {@link StreamingMode#ON} 构造（decodeSingle 对白名单类型不分支于模式档位，
 * 仅保持与既有测试夹具一致）；OID 固定 16384，Relation 为两列 (id int, v text) 与元组对齐。
 */
class BucketReplayerTest {

    private static final int OID = 16384;
    /** 顶层流式事务 xid——aborted 过滤用例中应保留的流式单元归属。 */
    private static final long TOP = 7001L;
    /** 被回滚的子事务 xid——aborted 过滤用例的剔除目标。 */
    private static final long SUB = 7003L;

    /** 构造两列 (id int 键列, v text) 的 Relation 样本，仅表名随参数变化（断言读 {@code table()} 区分版本）。 */
    private static PgOutputMessage.Relation rel(String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", table,
                'd', List.of(new Column("id", 23, -1, true), new Column("v", 25, -1, false)));
    }

    /** 构造一行两列文本元组 (id, v) 的**断言侧** record（与解码产物做值相等比较）。 */
    private static TupleData row(String id, String v) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(v)));
    }

    /** 构造流式块外的 Insert 字节（回放单元 payload 用）。 */
    private static byte[] insert(String id, String v) {
        return PgWire.insert(OID, PgWire.tuple(id, v));
    }

    /** 构造顶层（非流式块内）单元：payload 原样、无 xid 前缀（decodeSingle 以 inStream=false 消费）。 */
    private static PayloadUnit unit(byte[] payload, long seq) {
        return new PayloadUnit(payload, seq, OptionalLong.empty());
    }

    /** 构造流式块内单元：payload 经 {@link PgWire#streamed} 加 Int32 前缀且 streamXid 同值（回放契约）。 */
    private static PayloadUnit streamedUnit(long xid, byte[] payload, long seq) {
        return new PayloadUnit(PgWire.streamed(xid, payload), seq, OptionalLong.of(xid));
    }

    /** 预置单版本 registry：rel 于 seq 时刻到达（此后任意 asOf ≥ seq 的 require 均命中该版本）。 */
    private static VersionedRelationRegistry registryAt(long seq, PgOutputMessage.Relation rel) {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(seq, rel);
        return registry;
    }

    /**
     * 正常 I/U/D 三单元回放：产出三条 RowChange（顺序与单元一致），before/after 按 DML 语义
     * （INSERT 仅 after、UPDATE 无旧镜像时 before 空、DELETE 仅 before），Relation 为 registry
     * 命中版本的原样 record（assertSame 证快照嵌入），decodedObserver 逐单元回调且顺序一致。
     */
    @Test
    void replaysInsertUpdateDeleteWithRelationSnapshot() {
        PgOutputMessage.Relation rel = rel("t");
        List<PgOutputMessage> observed = new ArrayList<>();
        BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, registryAt(10L, rel), observed::add);

        List<TxChange> changes = replayer.replay(List.of(
                unit(PgWire.insert(OID, PgWire.tuple("1", "a")), 20L),
                unit(PgWire.update(OID, null, null, PgWire.tuple("1", "b")), 30L),   // 无旧镜像
                unit(PgWire.delete(OID, 'O', PgWire.tuple("1", "b")), 40L)),
                Set.of());

        assertEquals(3, changes.size());
        RowChange c0 = (RowChange) changes.get(0);
        assertEquals(DmlKind.INSERT, c0.dml());
        assertSame(rel, c0.relation());               // require(oid, seq=20) 命中 seq=10 的版本
        assertTrue(c0.before().isEmpty());
        assertEquals(row("1", "a"), c0.after().orElseThrow());
        RowChange c1 = (RowChange) changes.get(1);
        assertEquals(DmlKind.UPDATE, c1.dml());
        assertSame(rel, c1.relation());
        assertTrue(c1.before().isEmpty());            // REPLICA IDENTITY DEFAULT：无旧镜像
        assertEquals(row("1", "b"), c1.after().orElseThrow());
        RowChange c2 = (RowChange) changes.get(2);
        assertEquals(DmlKind.DELETE, c2.dml());
        assertSame(rel, c2.relation());
        assertEquals(row("1", "b"), c2.before().orElseThrow());
        assertTrue(c2.after().isEmpty());
        assertTrue(changes.stream().allMatch(c -> c.streamXid().isEmpty()));
        assertEquals(List.of("Insert", "Update", "Delete"),   // observer 逐单元回调且保序
                observed.stream().map(m -> m.getClass().getSimpleName()).toList());
    }

    /**
     * abortedSubxids 过滤：streamXid 命中被回滚子事务（SUB）的单元被剔除，其余（TOP 的流式单元
     * 与块外单元）保留且顺序不变；被剔单元**不解码**——decodedObserver 只收到保留单元的回调。
     */
    @Test
    void abortedSubxidUnitsAreSkippedAndNotDecoded() {
        List<PgOutputMessage> observed = new ArrayList<>();
        BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, registryAt(1L, rel("t")), observed::add);

        List<TxChange> changes = replayer.replay(List.of(
                streamedUnit(TOP, insert("1", "a"), 10L),
                streamedUnit(SUB, insert("2", "b"), 11L),
                streamedUnit(SUB, insert("3", "c"), 12L),
                unit(insert("4", "d"), 13L)),          // 块外单元：不参与子事务过滤
                Set.of(SUB));

        assertEquals(2, changes.size());
        assertEquals(OptionalLong.of(TOP), changes.get(0).streamXid());
        assertTrue(changes.get(1).streamXid().isEmpty());
        assertEquals(2, observed.size());              // 两条 SUB 单元被跳过，未发生解码
    }

    /**
     * asOf 版本正确性（spec §4.4 机制级验证）：registry 预置同 oid 两版本（seq 10/50，表名
     * t_v1/t_v2），单元 seq=30 取 v1、seq=60 取 v2——DDL 中途换版后旧单元仍按当时 schema 渲染。
     */
    @Test
    void asOfSeqPicksRelationVersionAtChangeTime() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, rel("t_v1"));
        registry.accept(50L, rel("t_v2"));
        BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, registry, m -> { });

        List<TxChange> changes = replayer.replay(List.of(
                unit(insert("1", "a"), 30L),           // 版本切换前 → v1
                unit(insert("2", "b"), 60L)),          // 版本切换后 → v2
                Set.of());

        assertEquals(List.of("t_v1", "t_v2"),
                changes.stream().map(c -> ((RowChange) c).relation().table()).toList());
    }

    /** 单元类型非 I/U/D/T/M（'B' 控制消息混入桶——组装器路由不可达，防御路径）→ IllegalStateException fail-fast。 */
    @Test
    void rejectsNonDataMessageTypeUnit() {
        BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, new VersionedRelationRegistry(), m -> { });

        assertThrows(IllegalStateException.class,
                () -> replayer.replay(List.of(unit(PgWire.begin(1L), 10L)), Set.of()));
    }

    /** 空单元列表回放产出空 List（空桶提交路径：Begin 后无变更即 Commit，协议合法）。 */
    @Test
    void emptyUnitListReplaysToEmptyChanges() {
        BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, new VersionedRelationRegistry(), m -> { });

        assertTrue(replayer.replay(List.of(), Set.of()).isEmpty());
    }
}
