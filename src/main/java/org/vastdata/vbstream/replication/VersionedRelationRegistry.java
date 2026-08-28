package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * oid → Relation 版本日志：按消息 seq（组装器对原始流分配的单调序号，从 1 起）为每个 oid 维护一条
 * 升序版本序列，支持以 asOfSeq 二分取"变更发生时刻"的定义。存在动机：DDL 会让服务端在流中重发
 * Relation（同 oid 新定义），溢出（spill）回放旧单元时必须按当时版本渲染，取"最新版"会把旧行按新
 * schema 错误解释。
 *
 * <p>语义要点：
 * <ul>
 *   <li>{@link #accept(long, PgOutputMessage.Relation)}：按 oid 追加版本并维持 seq 升序；同 seq 重复接受幂等跳过</li>
 *   <li>{@link #require(int, long)}：二分取 ≤ asOfSeq 的最新版；oid 完全无版本或全部版本晚于 asOfSeq 时
 *       抛 {@link IllegalStateException}（沿用父类"Relation 未先行到达"的 fail-fast 语义）</li>
 *   <li>{@link #pruneBelow(long)}：以"最低仍会被查询的 seq"为低水位——各 oid 保留 asOf=minSeq
 *       时刻**生效**的版本（floor，其自身 seq 可能早于 minSeq——'R' 恒先于首个 DML 到达）与其后
 *       全部，仅丢弃更早版本；minSeq 之后无任何版本时整列保留（每 oid 至少保留最新一条由此
 *       自然成立，永不掏空）</li>
 *   <li>继承的 {@link #accept(PgOutputMessage)} / {@link #find(int)} / {@link #require(int)} 委托最新版本，
 *       旧接缝（渲染路径）行为不变</li>
 * </ul>
 *
 * <p>线程约束：非线程安全——单写者假设，accept/pruneBelow/require 全部由同一组装器（run 循环）线程串行
 * 调用（溢出回放也发生在该线程内），故用 HashMap 而非父类的 ConcurrentHashMap；需要跨线程查询的场景
 * 请改用父类 {@link RelationRegistry}。另注意旧接缝 {@link #accept(PgOutputMessage)} 无 seq 可用，
 * 以内部水位合成递增 seq 记入时间线末尾，仅为维持"最新视图 = 最后到达"；seq 接缝与旧接缝不应混用
 * （合成 seq 可能与真实流序号冲突，冲突时后到者被幂等跳过）。
 */
public class VersionedRelationRegistry extends RelationRegistry {

    /**
     * 单个版本条目。
     *
     * @param seq 该 Relation 到达时所处的消息序号（组装器按原始流顺序分配，单调递增、从 1 起）
     * @param rel 该时刻的表元数据（不可变 record，同一引用可在多版本间安全复用）
     */
    private record Version(long seq, PgOutputMessage.Relation rel) {}

    /** oid → 该 oid 的版本序列（按 seq 升序，由 accept 的插入点保证）。HashMap 即可：单写者假设（见类 javadoc）。 */
    private final Map<Integer, List<Version>> versions = new HashMap<>();

    /** 已记入的最大 seq（含旧接缝 accept 的合成推进）。供旧接缝合成"末尾 + 1"的序号，维持最新视图语义。 */
    private long maxSeq = 0;

    /**
     * 责任：把 Relation 按 oid 追加为下一个版本，维持该 oid 序列按 seq 升序。
     * 步骤：二分定位 ≤ seq 的最新版本下标——若该版本 seq 恰相等，视为同条消息重复投递，幂等跳过；
     * 否则插到其后的正确位置（真实流 seq 单调、天然尾插，插入点逻辑仅防御乱序到达破坏升序）；
     * 最后前移水位 maxSeq。
     * 边界：rel 为 null 抛 NPE（协议层保证非空）；同 oid 不同 seq 正常追加为多版本；
     * 同 oid 同 seq 不同内容也跳过（seq 与消息位置一一对应，真实流不会出现）。
     * 线程：单写者调用（组装器线程）。
     *
     * @param seq 消息序号（组装器分配）
     * @param rel 到达的 Relation 定义
     */
    public void accept(long seq, PgOutputMessage.Relation rel) {
        List<Version> list = versions.computeIfAbsent(rel.relationOid(), k -> new ArrayList<>());
        int idx = floorIndex(list, seq);
        if (idx >= 0 && list.get(idx).seq() == seq) {
            return; // 同 seq 重复接受幂等跳过
        }
        list.add(idx + 1, new Version(seq, rel));
        if (seq > maxSeq) {
            maxSeq = seq;
        }
    }

    /**
     * 责任：旧消息接缝兼容入口——只认 Relation 消息、其余忽略（与父类同语义），改记入版本日志。
     * 步骤：Relation 以合成 seq = ++maxSeq 落在时间线末尾，使"最新视图 = 最后到达"与父类覆盖式 put 等价。
     * 边界：非 Relation 消息直接丢弃；见类 javadoc——本接缝与带 seq 的 accept 不应混用。
     * 线程：单写者调用。
     */
    @Override
    public void accept(PgOutputMessage message) {
        if (message instanceof PgOutputMessage.Relation relation) {
            accept(++maxSeq, relation);
        }
    }

    /**
     * 责任：旧接缝"最新视图"查询——oid 最后到达的 Relation。
     * 步骤：取该 oid 版本序列末位（升序保证末位即最新）。
     * 边界：oid 无任何版本返回 {@link Optional#empty()}；子类完全接管读写，父类并发缓存不再被填充。
     * 线程：单写者调用。
     */
    @Override
    public Optional<PgOutputMessage.Relation> find(int relationOid) {
        List<Version> list = versions.get(relationOid);
        return (list == null || list.isEmpty()) ? Optional.empty()
                : Optional.of(list.get(list.size() - 1).rel());
    }

    /**
     * 责任：旧接缝"最新视图" fail-fast 查询——oid 最后到达的 Relation。
     * 步骤：取版本序列末位。
     * 边界：oid 无任何版本抛 {@link IllegalStateException}（消息同父类，缓存 miss 即协议流异常）。
     * 线程：单写者调用。
     */
    @Override
    public PgOutputMessage.Relation require(int relationOid) {
        List<Version> list = versions.get(relationOid);
        if (list == null || list.isEmpty()) {
            throw new IllegalStateException("Relation oid=" + relationOid + " 未先行到达，协议流异常");
        }
        return list.get(list.size() - 1).rel();
    }

    /**
     * 责任：取 oid 在 asOfSeq 时刻生效的 Relation 定义（≤ asOfSeq 的最新版本）。
     * 步骤：查该 oid 版本序列 → 手写二分求 floor（seq ≤ asOfSeq 的最大下标）→ 返回命中版本的 rel。
     * 边界：oid 完全无版本、或全部版本都晚于 asOfSeq（含已被 pruneBelow 剪掉的更早区间）→
     * {@link IllegalStateException}，"Relation 未先行到达"语义（消息附 oid 与 asOf 便于定位）。
     * 线程：单写者调用（溢出回放渲染发生在组装器线程内）。
     *
     * @param relationOid 表 oid
     * @param asOfSeq     查询时刻的消息序号（取该时刻已到达的最新版本）
     * @return asOfSeq 时刻生效的表定义
     */
    public PgOutputMessage.Relation require(int relationOid, long asOfSeq) {
        List<Version> list = versions.get(relationOid);
        int idx = (list == null) ? -1 : floorIndex(list, asOfSeq);
        if (idx < 0) {
            throw new IllegalStateException(
                    "Relation oid=" + relationOid + " 未先行到达（asOf seq=" + asOfSeq + "），协议流异常");
        }
        return list.get(idx).rel();
    }

    /**
     * 责任：按"最低仍会被查询的 seq"收缩各 oid 的版本日志（防长期运行内存膨胀）。
     * 步骤：对每个 oid 二分定位 asOf=minSeq 时刻**生效**的版本（floor——seq ≤ minSeq 的最新一条，
     * 其自身 seq 允许早于 minSeq：'R' 恒先于同表首个 DML 到达，存活桶的旧单元会解析到低水位之前
     * 记入的版本），随后一次性删除该版本之前的全部前缀（子视图 clear 避免逐个 remove 的 O(n²)）。
     * 边界：floor 不存在（该 oid 全部版本都晚于 minSeq——未来查询仍需它们）时整列保留；
     * floor 恒为保留区间首元素，"每 oid 至少保留最新一条"自然成立（minSeq 超过全部版本 seq 时
     * floor 即末位，只留最新）；空序列为无操作。
     * 线程：单写者调用（组装器在桶完结点驱动，见 TransactionAssembler.retireBucket）。
     *
     * @param minSeq 最低仍需可查询的消息序号（此前的 asOf 查询不再发生）
     */
    public void pruneBelow(long minSeq) {
        for (List<Version> list : versions.values()) {
            int keepFrom = floorIndex(list, minSeq);
            if (keepFrom > 0) {
                list.subList(0, keepFrom).clear();
            }
        }
    }

    /**
     * 责任：手写二分求 floor——"seq ≤ target 的最新版本"下标。
     * 步骤：标准两端收缩——中位 seq ≤ target 记为候选并右移下界，否则左移上界。
     * 边界：序列为空或全部版本 seq &gt; target 时返回 -1（由调用方按"未先行到达"fail-fast 处理）。
     * 前置：list 按 seq 升序（accept 的插入点保证）；静态无状态、线程任意。
     */
    private static int floorIndex(List<Version> list, long target) {
        int lo = 0;
        int hi = list.size() - 1;
        int ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (list.get(mid).seq() <= target) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return ans;
    }
}
