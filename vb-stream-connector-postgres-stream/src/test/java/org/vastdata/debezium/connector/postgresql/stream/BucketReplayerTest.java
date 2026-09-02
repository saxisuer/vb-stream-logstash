package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleData;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link BucketReplayer} 专项单测:<b>绕过组装器</b>直接以"MessagePipe 追加 + TxBuffer 段记账"
 * 驱动回放,逐机制验证——I/U/D 三单元的 RowChange 语义与 Relation 快照嵌入、aborted 子事务
 * 过滤(被剔单元不解码不回调)、同 oid 多版本下按单元 seq 的 asOf 渲染(DDL 中途换版不串位)、
 * 非数据消息类型的 fail-fast、空桶回放零交付(空桶提交路径;回放为 sink 交付——各用例以
 * 列表收件驱动)。引擎 {@code BucketReplayerTest}(245 行)的逐用例翻译。
 *
 * <p>夹具约定(回放契约 = 冻结桶段 × 管道):单元 payload 字节全部经 {@link PgWire} 构造
 * (流式桶的单元用 {@link PgWire#streamed} 加 Int32 xid 前缀——回放按桶级 hasPrefix 重窥前缀作
 * streamXid),逐单元 {@code pipe.append} 后以返回 index 给桶记一段 [index,index](index 即单元
 * seq,作 asOf 查询入参与 seq 偏差组件的期望值);registry 预置版本用 wire Relation 直接构造,
 * 经 {@link #resolved} 包成 {@link ResolvedRelation}(wire + 最小 Debezium Table,与
 * StreamedTransactionAssemblerTest 的假 RESOLVER 同款),版本的 accept seq 取自相关单元的实际
 * index(绝对值随建队列时刻漂移,不可字面硬编码),随后经 {@link #freeze} 以 oidSet 圈定、
 * 截止 lastIndex 拷出快照冻结进桶(组装器 handoff 的同款语义)——回放器不收 resolver,
 * 表定义解析只走桶内快照。前缀不变量按桶组织:流式单元与块外单元分属两个桶(混现是组装器的
 * fail-fast 路径,不在此测)。回放器以 {@link StreamingMode#ON} 构造(decodeSingle 对白名单
 * 类型不分支于模式档位,仅保持与既有测试夹具一致);OID 固定 16384,Relation 为两列
 * (id int, v text) 与元组对齐。
 */
class BucketReplayerTest {

    private static final int OID = 16384;
    /** 顶层流式事务 xid——aborted 过滤用例中应保留的流式单元归属。 */
    private static final long TOP = 7001L;
    /** 被回滚的子事务 xid——aborted 过滤用例的剔除目标。 */
    private static final long SUB = 7003L;

    /** 每用例独立的管道目录(@TempDir):用例间零残留,无需依赖 wipe-on-open 顺序。 */
    @TempDir
    Path pipeDir;

    /** 构造两列 (id int 键列, v text) 的 Relation 样本,仅表名随参数变化(断言读 {@code table()} 区分版本)。 */
    private static PgOutputMessage.Relation rel(String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", table,
                'd', List.of(new RelationColumn("id", 23, -1, true), new RelationColumn("v", 25, -1, false)));
    }

    /**
     * 责任:把 wire Relation 包成版本日志载荷(双形态)——Table 用 {@code Table.editor()} 造
     * 最小形态(TableId 取 wire 的 schema/table,列沿 wire 列序全 text),不连库;
     * JDBC enrich 的真实现属 Task 7 的 RelationTableFactory(与组装器测试的假 RESOLVER 同款)。
     */
    private static ResolvedRelation resolved(PgOutputMessage.Relation wire) {
        var editor = Table.editor().tableId(new TableId(null, wire.schema(), wire.table()));
        for (var col : wire.columns()) {
            editor.addColumn(io.debezium.relational.Column.editor()
                    .name(col.name()).jdbcType(java.sql.Types.VARCHAR).type("text").create());
        }
        return new ResolvedRelation(wire, editor.create());
    }

    /** 构造一行两列文本元组 (id, v) 的断言侧 record(与解码产物做值相等比较)。 */
    private static TupleData row(String id, String v) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(v)));
    }

    /** 构造流式块外的 Insert 字节(回放单元 payload 用)。 */
    private static byte[] insert(String id, String v) {
        return PgWire.insert(OID, PgWire.tuple(id, v));
    }

    /** 构造块外桶(无前缀形态):hasPrefix=false 且已定型(与组装器首单元定型后的状态一致)。 */
    private static TxBuffer plainBucket(long xid) {
        TxBuffer bucket = new TxBuffer(xid);
        bucket.hasPrefix = false;
        bucket.prefixKnown = true;
        return bucket;
    }

    /** 构造流式桶(带前缀形态):hasPrefix=true 且已定型——回放据此重窥 raw[1..4] 作 streamXid。 */
    private static TxBuffer streamedBucket(long xid) {
        TxBuffer bucket = new TxBuffer(xid);
        bucket.hasPrefix = true;
        bucket.prefixKnown = true;
        return bucket;
    }

    /**
     * 把一条单元字节追加进管道并给桶记一段 [index,index](每单元独立段——回放语义与段合并无关,
     * 组装器的连续段合并由 StreamedTransactionAssemblerTest 经真实路径覆盖)。同时维护桶端点,
     * 返回 index(即该单元 seq,调用方作 registry 的 accept/require 锚点用)。
     */
    private static long appendUnit(MessagePipe pipe, TxBuffer bucket, byte[] payload) {
        long index = pipe.append(payload);
        bucket.segments.addLast(new long[]{ index, index });
        if (bucket.firstIndex < 0) {
            bucket.firstIndex = index;
        }
        bucket.lastIndex = index;
        return index;
    }

    /**
     * 交接辅助(测试面,按组装器 handoff 的冻结语义):圈定 oidSet(本夹具全部单元指向 OID)
     * 后以截止 lastIndex 从 registry 拷出 RelationSnapshot 冻结进桶——回放契约要求桶已交接
     * (快照随行),直接驱动回放器必须补上这一步。纯函数,测试线程调用。
     */
    private static void freeze(TxBuffer bucket, VersionedRelationRegistry registry) {
        bucket.oidSet.add(OID);
        bucket.relationSnapshot = registry.snapshot(bucket.oidSet, bucket.lastIndex);
    }

    /**
     * 正常 I/U/D 三单元回放:产出三条 RowChange(顺序与单元一致),before/after 按 DML 语义
     * (INSERT 仅 after、UPDATE 无旧镜像时 before 空、DELETE 仅 before),Relation 为 registry
     * 命中版本的原样 record(assertSame 证快照嵌入),decodedObserver 逐单元回调且顺序一致;
     * seq 偏差组件逐变更等于该单元自己的 CQ index。
     * 版本 accept seq 取首单元 index-10('R' 恒先于 DML 到达的相对化表达,绝对值不依赖队列时刻)。
     */
    @Test
    void replaysInsertUpdateDeleteWithRelationSnapshot() {
        PgOutputMessage.Relation rel = rel("t");
        List<PgOutputMessage> observed = new ArrayList<>();
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            TxBuffer bucket = plainBucket(1L);
            long i0 = appendUnit(pipe, bucket, PgWire.insert(OID, PgWire.tuple("1", "a")));
            long i1 = appendUnit(pipe, bucket, PgWire.update(OID, null, null, PgWire.tuple("1", "b")));   // 无旧镜像
            long i2 = appendUnit(pipe, bucket, PgWire.delete(OID, 'O', PgWire.tuple("1", "b")));
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            registry.accept(i0 - 10L, resolved(rel));
            freeze(bucket, registry);
            BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, (m, v) -> observed.add(m));

            List<TxChange> changes = new ArrayList<>();          // sink 驱动——断言对着该列表
            replayer.replay(bucket, pipe, changes::add);

            assertEquals(3, changes.size());
            RowChange c0 = (RowChange) changes.get(0);
            assertEquals(DmlKind.INSERT, c0.dml());
            assertSame(rel, c0.relation());               // require(oid, seq=i0) 命中 i0-10 的版本(wire 形态原样嵌入)
            assertTrue(c0.before().isEmpty());
            assertEquals(row("1", "a"), c0.after().orElseThrow());
            RowChange c1 = (RowChange) changes.get(1);
            assertEquals(DmlKind.UPDATE, c1.dml());
            assertSame(rel, c1.relation());
            assertTrue(c1.before().isEmpty());            // REPLICA IDENTITY DEFAULT:无旧镜像
            assertEquals(row("1", "b"), c1.after().orElseThrow());
            RowChange c2 = (RowChange) changes.get(2);
            assertEquals(DmlKind.DELETE, c2.dml());
            assertSame(rel, c2.relation());
            assertEquals(row("1", "b"), c2.before().orElseThrow());
            assertTrue(c2.after().isEmpty());
            assertTrue(changes.stream().allMatch(c -> c.streamXid().isEmpty()));
            assertEquals(i0, c0.seq());                   // seq 偏差组件:逐变更等于自己单元的 CQ index
            assertEquals(i1, c1.seq());
            assertEquals(i2, c2.seq());
            assertEquals(List.of("Insert", "Update", "Delete"),   // observer 逐单元回调且保序
                    observed.stream().map(m -> m.getClass().getSimpleName()).toList());
        }
    }

    /**
     * abortedSubxids 过滤:流式桶内 streamXid(重窥前缀)命中被回滚子事务(SUB)的单元被剔除,
     * 其余(TOP 的流式单元)保留且顺序不变;块外桶的单元无前缀、不参与子事务过滤。被剔单元
     * <b>不解码</b>——decodedObserver 只收到保留单元的回调。同一回放器实例先后回放带前缀与
     * 不带前缀两桶,兼证 decodeSingle 不在实例上残留流块状态(与组装器 live 解码器互不干扰的
     * 机制级切片——两解码器各自独立实例之外,同实例跨形态连续回放也不串位)。
     */
    @Test
    void abortedSubxidUnitsAreSkippedAndNotDecoded() {
        List<PgOutputMessage> observed = new ArrayList<>();
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            TxBuffer streamed = streamedBucket(TOP);
            appendUnit(pipe, streamed, PgWire.streamed(TOP, insert("1", "a")));
            appendUnit(pipe, streamed, PgWire.streamed(SUB, insert("2", "b")));
            appendUnit(pipe, streamed, PgWire.streamed(SUB, insert("3", "c")));
            streamed.abortedSubxids.add(SUB);
            TxBuffer plain = plainBucket(2L);
            appendUnit(pipe, plain, insert("4", "d"));          // 块外单元:不参与子事务过滤

            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            PgOutputMessage.Relation rel = rel("t");
            registry.accept(streamed.firstIndex - 10L, resolved(rel));
            freeze(streamed, registry);
            freeze(plain, registry);
            BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, (m, v) -> observed.add(m));

            List<TxChange> streamedChanges = new ArrayList<>();
            replayer.replay(streamed, pipe, streamedChanges::add);
            assertEquals(1, streamedChanges.size());
            assertEquals(OptionalLong.of(TOP), streamedChanges.get(0).streamXid());
            List<TxChange> plainChanges = new ArrayList<>();
            replayer.replay(plain, pipe, plainChanges::add);
            assertEquals(1, plainChanges.size());
            assertTrue(plainChanges.get(0).streamXid().isEmpty());
            assertEquals(2, observed.size());              // 两条 SUB 单元被跳过,未发生解码
        }
    }

    /**
     * asOf 版本正确性(机制级验证):两单元之间经管道追加一条 Relation 字节制造真实间隔
     * ('R' 到达于两单元之间——线上形态),registry 据两个单元的实际 index 预置同 oid 两版本
     * (v1 于首单元 index、v2 于间隔 R 的 index),回放取 v1/v2 各一次——DDL 中途换版后旧单元
     * 仍按当时 schema 渲染。
     */
    @Test
    void asOfSeqPicksRelationVersionAtChangeTime() {
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            TxBuffer bucket = plainBucket(1L);
            long u1 = appendUnit(pipe, bucket, insert("1", "a"));
            long r2 = pipe.append(PgWire.relation(OID, "t_v2", "id", "v"));   // 间隔:v2 版本的 R
            long u2 = appendUnit(pipe, bucket, insert("2", "b"));
            assertTrue(u1 < r2 && r2 < u2, "CQ index 应随追加单调递增");
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            registry.accept(u1, resolved(rel("t_v1")));
            registry.accept(r2, resolved(rel("t_v2")));
            freeze(bucket, registry);
            BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, (m, v) -> { });

            List<TxChange> changes = new ArrayList<>();
            replayer.replay(bucket, pipe, changes::add);

            assertEquals(List.of("t_v1", "t_v2"),
                    changes.stream().map(c -> ((RowChange) c).relation().table()).toList());
        }
    }

    /** 单元类型非 I/U/D/T/M('B' 控制消息混入桶——组装器路由不可达,防御路径)→ IllegalStateException fail-fast。 */
    @Test
    void rejectsNonDataMessageTypeUnit() {
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            TxBuffer bucket = plainBucket(1L);
            appendUnit(pipe, bucket, PgWire.begin(1L));
            freeze(bucket, new VersionedRelationRegistry());
            BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, (m, v) -> { });

            assertThrows(IllegalStateException.class, () -> replayer.replay(bucket, pipe, c -> { }));
        }
    }

    /** 无段空桶回放零交付(空桶提交路径:Begin 后无变更即 Commit,协议合法)。 */
    @Test
    void emptyUnitListReplaysToEmptyChanges() {
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            TxBuffer bucket = new TxBuffer(1L);
            freeze(bucket, new VersionedRelationRegistry());
            BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, (m, v) -> { });

            List<TxChange> delivered = new ArrayList<>();
            assertEquals(0L, replayer.replay(bucket, pipe, delivered::add));
            assertTrue(delivered.isEmpty());
        }
    }

    /** 桶未交接(relationSnapshot 缺失,快照未随行)→ IllegalStateException fail-fast——回放的前置条件违反。 */
    @Test
    void rejectsBucketWithoutHandedOffSnapshot() {
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            TxBuffer bucket = plainBucket(1L);   // 未 freeze:relationSnapshot 为 null
            BucketReplayer replayer = new BucketReplayer(StreamingMode.ON, (m, v) -> { });

            assertThrows(IllegalStateException.class, () -> replayer.replay(bucket, pipe, c -> { }));
        }
    }
}
