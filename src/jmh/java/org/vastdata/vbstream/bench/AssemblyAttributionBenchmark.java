package org.vastdata.vbstream.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 组装开销归因基准（1.7.1 设计 §3 腿 2）：把组装路径上"窥探与 append 之外"的**每次交接级**
 * 开销逐项隔离计量——每交接 RelationSnapshot 拷贝、交接队列 add+poll 一对。
 * 与 RoutePeekBenchmark（窥探 ns 级已入档，≈9 ns/条）、PipePathBenchmark（append 两口径）
 * 拼出 assembleWholeCorpus 总成本的分解视图，是 1.7.1 归因表的数据源。
 * 口径注意：两项预期都在百 ns 级——若实测如此，即从嫌疑清单排除（归因表的"排除法"同样入档）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AssemblyAttributionBenchmark {

    /**
     * 快照 state：预灌 2 oid × 各 2 版本的版本日志（对齐语料每轮约 4 条 'R' 的量级，含一次
     * 同 oid 重发模拟 DDL）——snapshot 的成本形状由（oid 数 × 版本数）决定，与语料真实
     * 'R' 内容无关，手工构造即可复现成本形态。
     */
    @State(Scope.Thread)
    public static class SnapshotState {

        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        Set<Integer> oidSet = Set.of(16384, 16390);
        long maxSeq = 40L;

        /**
         * 责任：灌 4 个版本（2 oid × 2 版本，seq 递增）。
         * 边界：Relation record 组件形态以 protocol 包实际定义为准（已核对：
         * (OptionalLong streamXid, int relationOid, String schema, String table,
         * char replicaIdentity, List&lt;Column&gt; columns)）。
         */
        @Setup(Level.Trial)
        public void setup() {
            registry.accept(10L, relation(16384, "t1_v1"));
            registry.accept(20L, relation(16390, "t2_v1"));
            registry.accept(30L, relation(16384, "t1_v2"));
            registry.accept(40L, relation(16390, "t2_v2"));
        }

        /**
         * 责任：手工构造一条最小 Relation 版本（schema/table 区分版本、空列——snapshot 只按
         * (oid, seq) 拷引用，列集空不改变拷贝成本形状）。
         * 边界：组件顺序与 PgOutputMessage.Relation record 定义逐一对齐。
         *
         * @param oid   表 oid（与 SnapshotState.oidSet 对应）
         * @param table 表名（区分 v1/v2 版本，仅助读）
         * @return 可安全共享的不可变 Relation record
         */
        private static PgOutputMessage.Relation relation(int oid, String table) {
            return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table, 'd', java.util.List.of());
        }
    }

    /**
     * 队列 state：与组装器交接队列同型（LinkedBlockingQueue）+ 一个冻结负载引用。
     * 负载用 Object 而非 TxBuffer（包私有不可及）：队列机制成本与负载类型无关。
     */
    @State(Scope.Thread)
    public static class QueueState {
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        Object payload = new Object();
    }

    /**
     * 计时体：一次交接快照拷贝（reader 提交路径的 snapshot(oidSet, lastIndex)）。
     * 返回快照对象防死码消除。
     */
    @Benchmark
    public Object snapshotCopyPerHandoff(SnapshotState s) {
        return s.registry.snapshot(s.oidSet, s.maxSeq);
    }

    /**
     * 计时体：交接一对（reader 侧 add + consumer 侧 poll）。
     * 返回 polled 对象防死码消除。
     */
    @Benchmark
    public Object handoffQueueOfferPoll(QueueState s) {
        s.queue.add(s.payload);
        return s.queue.poll();
    }
}
