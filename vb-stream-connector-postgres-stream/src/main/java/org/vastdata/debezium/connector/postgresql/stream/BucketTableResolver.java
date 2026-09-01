package org.vastdata.debezium.connector.postgresql.stream;

/**
 * listener 侧按 (oid, seq) 从桶快照解析 asOf 表定义的接缝(MS2 设计新增,引擎无对应物)。
 * 存在动机:connector 的 {@link RowChange} 只嵌入 wire 形态 Relation(协议列序真源),
 * Debezium 渲染视图的 {@code Table} 由下游监听器(Task 7 的 {@code DispatcherTransactionListener})
 * 在 TxChange 回调内按需解析——而事件本身不携带桶快照,本接缝就是"当前正在回放的桶快照"
 * 送达 listener 侧的唯一通道:{@code TransactionConsumer.processBucket} 在发 Begin 前
 * {@link #bind(RelationSnapshot)} 该桶的快照,listener 随后经 {@link #resolve(int, long)}
 * 以变更自带的 seq({@link TxChange#seq()})取<b>变更时刻</b>的 {@link ResolvedRelation}
 * (wire + Table 双形态,asOf 二分见 {@link RelationSnapshot#require(int, long)})。
 *
 * <p>两个实现:默认的 {@link #snapshotBacked()}(直接透传 ResolvedRelation——快照 require 的
 * 原样包装,测试与 Task 5 形态用);Task 7 起可注入真实现(如解析时顺带做 schema 版本安装/
 * 缓存等 listener 侧策略)。接缝方法面刻意最小——bind/resolve 两步,不暴露快照本体。
 *
 * <p>线程约束:bind 由 consumer 线程在 processBucket 内调用,resolve 由 listener 在事件回调内
 * 调用(同一 consumer 线程——同步测试形态即调用线程);单写者单读者同线程,实现无需并发原语,
 * 跨线程持有(resolve 前另一线程 bind)属使用违约。
 */
interface BucketTableResolver {

    /**
     * 责任:绑定当前正在回放的桶快照(consumer 在每个桶的 Begin 发出前调用一次)。
     * 边界:snapshot 为 null 抛 NPE(空桶的快照是空快照,不是 null——handoff 恒置非 null);
     * 绑定新快照即取代旧绑定(上一桶的解析窗口随 End 结束,之后不应再 resolve)。
     * 线程:consumer 线程。
     *
     * @param bucketSnapshot 刚交接冻结的桶内 Relation 版本快照
     */
    void bind(RelationSnapshot bucketSnapshot);

    /**
     * 责任:按 (oid, asOfSeq) 解析变更时刻的表定义(listener 在 TxChange 回调内调用)。
     * 关键步骤:在当前绑定的桶快照上取 ≤ asOfSeq 的最新版本。
     * 边界:oid 未先行到达(快照省略/全部晚于 asOfSeq)时按实现方语义 fail-fast
     * (默认实现即 {@link RelationSnapshot#require(int, long)} 的 ISE——协议流异常);
     * End 之后、下一次 bind 之前调用读到的是上一桶的绑定(调用序违约,属使用方错误)。
     * 线程:consumer 线程(与 bind 同线程)。
     *
     * @param relationOid 变更目标表 oid({@code RowChange.relation().relationOid()})
     * @param asOfSeq     变更消息序号({@link TxChange#seq()})
     * @return 变更时刻生效的表定义(wire + Table 双形态)
     */
    ResolvedRelation resolve(int relationOid, long asOfSeq);

    /**
     * 责任:构造默认实现——直接透传 ResolvedRelation 的桶快照包装(bind 记住快照,
     * resolve 即 {@code snapshot.require(oid, asOfSeq)},零附加策略)。
     * 边界:实现无状态校验、不复制快照(快照自身不可变,共享引用安全)。
     * 线程:工厂方法任意线程;产物的线程约束见接口级 javadoc。
     *
     * @return 快照透传解析器(测试与 Task 5 形态的默认接缝实现)
     */
    static BucketTableResolver snapshotBacked() {
        return new SnapshotBacked();
    }

    /** 默认实现:持有当前绑定快照,resolve 直通 require——"直接透传 ResolvedRelation"的最小形态。 */
    final class SnapshotBacked implements BucketTableResolver {

        private RelationSnapshot bound;

        /** 责任:记录当前桶快照(require 的解析目标)。边界:null 抛 NPE(接缝契约)。线程:consumer 线程。 */
        @Override
        public void bind(RelationSnapshot bucketSnapshot) {
            this.bound = java.util.Objects.requireNonNull(bucketSnapshot, "bucketSnapshot");
        }

        /** 责任:在绑定快照上按 asOf 二分取版。边界:未 bind 过即 resolve 属调用序违约,抛 ISE;oid miss 的 ISE 由快照 require 抛出。线程:consumer 线程。 */
        @Override
        public ResolvedRelation resolve(int relationOid, long asOfSeq) {
            if (bound == null) {
                throw new IllegalStateException("BucketTableResolver 尚未绑定桶快照(bind 先于 resolve)");
            }
            return bound.require(relationOid, asOfSeq);
        }
    }
}
