package org.vastdata.debezium.connector.postgresql.stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * oid → {@link ResolvedRelation} 的版本日志:为每个 oid 维护一条按 seq 升序的版本序列,
 * 支持按 asOfSeq 二分取"某个时刻正在生效"的表定义(seq 是消息的 CQ index,≡
 * {@code MessagePipe.append} 的返回值,起点随建队列时刻漂移,勿硬编码)。引擎
 * {@code org.vastdata.vbstream.replication.VersionedRelationRegistry}(227 行)的 1:1 重写
 * (文字参照,非依赖),唯一泛化:版本载荷由 wire Relation 扩为 wire + Debezium Table 双形态。
 *
 * <p>为什么需要它:DDL 会让服务端在流里重发 Relation(同一个 oid 换了新定义)。回放旧单元
 * 时必须按"当时"的版本渲染,如果取最新版,旧行会被按新表结构错误解释——回放在 consumer
 * 线程以桶内不可变快照({@link RelationSnapshot})渲染,快照内容仍是本类按 seq 记的版本时间线。
 *
 * <p>语义要点:
 * <ul>
 *   <li>{@link #accept(long, ResolvedRelation)}:按 oid 追加版本,维持 seq 升序;
 *       同一 seq 重复接受会幂等跳过</li>
 *   <li>{@link #require(int, long)}:二分取 ≤ asOfSeq 的最新版(其自身 seq 可早于 asOfSeq——
 *       Relation 总是先于同表第一个 DML 到达);oid 完全没有版本、或所有版本都晚于 asOfSeq 时抛
 *       {@link IllegalStateException}("Relation 未先行到达"语义:缓存 miss 即协议流异常)</li>
 *   <li>{@link #pruneBelow(long)}:以"最低仍会被查询的 seq"为低水位做剪枝——每个 oid 保留
 *       asOf=minSeq 时刻<b>正在生效</b>的那个版本(它自身的 seq 可以早于 minSeq)及其之后的
 *       全部,更早的丢弃;minSeq 之后一个版本都没有时整列保留("每个 oid 至少留最新一条"由
 *       这一规则自然保证,永远不会剪空)</li>
 *   <li>{@link #snapshot(Set, long)}:把指定 oid 集合在 maxSeq 时刻已生效的版本前缀拷成不可变
 *       {@link RelationSnapshot},供桶交接给 consumer 线程回放渲染(发布语义见该方法 javadoc)</li>
 *   <li>{@link #find(int)}:最新视图(live 解码点的渲染形态,实现 {@link RelationLookup})</li>
 * </ul>
 *
 * <p><b>取舍(与引擎的形态差异)</b>:引擎侧本类继承覆盖式父类 {@code RelationRegistry}
 * (ConcurrentHashMap 缓存,旧接缝 {@code accept(PgOutputMessage)} 的载体,跨线程查询安全)。
 * connector 不译该父类——它的运行期消费者是引擎测试侧 {@code DecodedMessageBridge} 的消息录制
 * 桥,connector 无此桥;"最新视图"语义由本类 {@link #find(int)} 直答(单写者下无需并发容器)。
 *
 * <p>线程约束:非线程安全——<b>单写者假设</b>:回放在 consumer 线程用桶内不可变快照
 * ({@link RelationSnapshot})渲染、不查本类,本类全部方法仅由 reader 线程(跑组装器 run 循环的
 * 线程)串行调用,所以用 HashMap 而不是 ConcurrentHashMap;需要跨线程查询的场景请改用
 * {@link RelationSnapshot}(经 {@link #snapshot} 拷出)。
 */
public class VersionedRelationRegistry implements RelationLookup {

    /**
     * 单个版本条目。
     *
     * @param seq 该版本记入时的消息序号(≡ 消息的 Chronicle Queue index,起点随建队列
     *            时刻漂移,勿硬编码)
     * @param rel 该时刻的表定义(wire + Table 双形态,不可变 record,同一引用可在多版本间
     *            安全复用)
     */
    private record Version(long seq, ResolvedRelation rel) {}

    /** oid → 该 oid 的版本序列(按 seq 升序,由 accept 的插入点保证)。HashMap 即可:单写者假设(见类 javadoc)。 */
    private final Map<Integer, List<Version>> versions = new HashMap<>();

    /**
     * 把 {@link ResolvedRelation} 追加为该 oid 的下一个版本,并保持序列按 seq 升序。
     *
     * <p>步骤:二分找到 ≤ seq 的最新版本——它的 seq 恰好相等就视为同一条消息重复投递,
     * 幂等跳过(先到者胜);否则插到它后面的正确位置(真实流的 seq 本来就单调,插入点逻辑
     * 只是防御乱序到达破坏升序)。
     *
     * <p>边界:rel 为 null 抛 NPE({@link ResolvedRelation} 构造期已拒 null 组件);同 oid 不同
     * seq 正常追加为多版本;同 oid 同 seq 但内容不同也跳过(seq 与消息位置一一对应,真实流里
     * 不会出现)。由组装器在 reader 线程单写者调用。
     *
     * @param seq 消息序号(组装器自 pipe.append 取得)
     * @param rel 到达的表定义('R' 经 Task 7 enrich 后的双形态)
     */
    public void accept(long seq, ResolvedRelation rel) {
        List<Version> list = versions.computeIfAbsent(rel.wire().relationOid(), k -> new ArrayList<>());
        int idx = floorIndex(list, seq);
        if (idx >= 0 && list.get(idx).seq() == seq) {
            return; // 同 seq 重复接受幂等跳过
        }
        list.add(idx + 1, new Version(seq, rel));
    }

    /**
     * 责任:取 oid 在 asOfSeq 时刻生效的 {@link ResolvedRelation}(≤ asOfSeq 的最新版本)。
     * 步骤:查该 oid 版本序列 → 手写二分求 floor(seq ≤ asOfSeq 的最大下标)→ 返回命中版本的 rel。
     * 边界:oid 完全无版本、或全部版本都晚于 asOfSeq(含已被 {@link #pruneBelow} 剪掉的更早
     * 区间)→ {@link IllegalStateException},"Relation 未先行到达"语义(消息附 oid 与 asOf
     * 便于定位)。
     * 线程:单写者调用(回放在 consumer 线程用桶内不可变快照 {@link RelationSnapshot} 渲染、
     * 不查本类,本方法仍仅由 reader 线程调用)。
     *
     * @param relationOid 表 oid
     * @param asOfSeq     查询时刻的消息序号(取该时刻已到达的最新版本)
     * @return asOfSeq 时刻生效的表定义(wire + Table 双形态)
     */
    public ResolvedRelation require(int relationOid, long asOfSeq) {
        List<Version> list = versions.get(relationOid);
        int idx = (list == null) ? -1 : floorIndex(list, asOfSeq);
        if (idx < 0) {
            throw new IllegalStateException(
                    "Relation oid=" + relationOid + " 未先行到达（asOf seq=" + asOfSeq + "），协议流异常");
        }
        return list.get(idx).rel();
    }

    /**
     * 按"最低仍会被查询的 seq"收缩各 oid 的版本日志,防止长期运行内存膨胀。
     *
     * <p>步骤:对每个 oid 二分找到 asOf=minSeq 时刻<b>正在生效</b>的版本(即 seq ≤ minSeq 的
     * 最新一条;注意它自身的 seq 允许早于 minSeq——Relation 总是先于同表第一个 DML 到达,
     * 存活桶里的旧单元会解析到低水位之前记入的版本,"字面丢弃 seq &lt; minSeq"的实现会在
     * 并发 DDL 流形下误剪崩回放),然后把这条之前的整段前缀一次性删掉(用子视图 clear,
     * 避免逐个 remove 的 O(n²))。
     *
     * <p>边界:找不到这条生效版本(该 oid 的所有版本都晚于 minSeq,未来的查询还用得着)时
     * 整列保留;生效版本就是要保留区间的第一个元素,所以"每个 oid 至少保留最新一条"自然
     * 成立——minSeq 超过全部版本 seq 时它就是末位,只剩最新;空序列什么都不做。由组装器在
     * 桶完结点以全部存活桶(含 2PC 挂起)firstIndex 低水位调用,单写者。
     *
     * @param minSeq 最低仍需可查询的消息序号(比它更早的 asOf 查询不会再发生)
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
     * 责任:把指定 oid 集合在 maxSeq 时刻已生效的版本前缀拷成不可变 {@link RelationSnapshot},
     * 供桶在交接(handoff)瞬间随行冻结——这是"快照在 handoff 瞬间冻结、consumer 不共享
     * registry"的关键拼图。
     *
     * <p><b>发布语义</b>:快照交出即与 registry 脱钩——此后 reader 继续对 registry 追加新版本
     * (accept)或剪枝(pruneBelow)都不影响已发快照的查询结果:逐 oid 二分找 ≤ maxSeq 的最新
     * 版本下标,把 [0..idx] 的 (seq, rel) <b>拷入全新列表</b>(浅拷——{@link ResolvedRelation}
     * 不可变,引用可安全共享;列表本身独立,registry 侧 subList.clear 够不着快照自己的列表),
     * consumer 线程只读消费、与 reader 的 HashMap 零共享可变状态。
     *
     * <p>边界:oid 无版本或全部版本晚于 maxSeq 时省略——回放期
     * {@link RelationSnapshot#require} 会以"未先行到达"fail-fast,报错时机与直查 registry
     * 一致;oids 为 null 抛 NPE;maxSeq ≤ 0 时所有 oid 都省略(空桶无渲染需求)。
     * 单写者(reader)调用。
     *
     * @param oids   需要快照的表 oid 集合(通常是桶内出现过的全部 oid)
     * @param maxSeq 快照截止序号(该时刻之后到达的版本不进快照)
     * @return 各 oid 版本前缀的不可变快照(省略的 oid 不含 key)
     */
    public RelationSnapshot snapshot(Set<Integer> oids, long maxSeq) {
        Map<Integer, List<RelationSnapshot.Entry>> out = new HashMap<>();
        for (Integer oid : oids) {
            List<Version> list = versions.get(oid);
            if (list == null) {
                continue;
            }
            int idx = floorIndex(list, maxSeq);
            if (idx < 0) {
                continue;
            }
            List<RelationSnapshot.Entry> copied = new ArrayList<>(idx + 1);
            for (int i = 0; i <= idx; i++) {
                copied.add(new RelationSnapshot.Entry(list.get(i).seq(), list.get(i).rel()));
            }
            out.put(oid, List.copyOf(copied));
        }
        return new RelationSnapshot(out);
    }

    /**
     * 责任:最新视图宽松查询——oid 最后到达的 {@link ResolvedRelation}(live 解码点的渲染
     * 形态,{@link RelationLookup} 实现)。
     * 步骤:取该 oid 版本序列末位(升序保证末位即最新)。
     * 边界:oid 无任何版本返回 {@link Optional#empty()}——调用方降级渲染,不 fail-fast。
     * 线程:单写者调用(live 解码点在 reader 线程)。
     */
    @Override
    public Optional<ResolvedRelation> find(int relationOid) {
        List<Version> list = versions.get(relationOid);
        return (list == null || list.isEmpty()) ? Optional.empty()
                : Optional.of(list.get(list.size() - 1).rel());
    }

    /**
     * 责任:手写二分求 floor——"seq ≤ target 的最新版本"下标。
     * 步骤:标准两端收缩——中位 seq ≤ target 记为候选并右移下界,否则左移上界。
     * 边界:序列为空或全部版本 seq &gt; target 时返回 -1(由调用方按"未先行到达"fail-fast 处理)。
     * 前置:list 按 seq 升序(accept 的插入点保证);静态无状态、线程任意。
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
