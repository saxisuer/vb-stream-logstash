package org.vastdata.vbstream.walsource.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.PhysicalSlotManager;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.receive.WalStreamReceiver;
import org.vastdata.vbstream.walsource.state.StateConfig;
import org.vastdata.vbstream.walsource.state.StateStore;
import org.vastdata.vbstream.walsource.state.StoredState;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.StringJoiner;

/**
 * 伪备库 catalog 同步器（spec §5 组件四）：JDBC 一致性引导 + 物理流接收 + ctid 重放
 * 的 v1 组装——一次 {@link #start} 完成槽确保、REPEATABLE READ 全行种子、起流（sink
 * 委派本类 {@link #apply}），此后字典随消费增量维护并经 {@link #snapshot()} 提供
 * {@link CatalogSnapshot} as-of 查询。</p>
 *
 * <p><strong>检查点生命周期（Task 15）</strong>：state 启用（{@link StateConfig} dir
 * 非 null）时——启动按"检查点存在且 lsn 在槽保留窗口内 → {@link StoredState#restoreInto}
 * 续传（跳过引导）；否则全新引导"双路决策；接收线程按 interval/events 双节拍周期落盘
 * （{@link StateStore#checkpoint}，单线程天然一致点）；落盘成功后经
 * {@code pg_replication_slot_advance} 推槽（严格持久化之后）；{@link #stop()} 最终
 * best-effort 检查点。state 禁用时行为与 Task 13 前完全一致。</p>
 *
 * <p><strong>装配序（spec §6①）</strong>：① {@link PhysicalSlotManager#ensureSlot} 取
 * P₀（建槽自身的 WAL 先于种子快照落盘）→ ② {@link CatalogBootstrap#bootstrap} 在
 * REPEATABLE READ 单事务内灌满两目录并返回<strong>事务首句读取</strong>的 flush LSN
 * （B ≤ 种子快照时点 S，终审 C1）→ ③ 流起点 max(P₀, B)（页对齐由接收器内部下取整）
 * ——引导路径的过滤线种子取<strong>页对齐流起点</strong>：流交付的重叠段（(B,S] 窗口
 * + 在途事务跨 B 的记录）按 spec §6① 原意重放消化（ctid 键控 upsert 幂等 + 截断更新
 * 末态采纳收敛）；续传/forced 路径过滤线仍 = stored/forced lsn（已施加记录二次施加
 * 非幂等，见 {@link #apply} javadoc）。</p>
 *
 * <p><strong>凭据移交（控制器裁定）</strong>：接收器自建物理复制连接，而 JDBC
 * {@link Connection} 不暴露密码。带凭据重载
 * {@link #start(Connection, String, WalLayout, String, String, long...)} 显式传
 * user/password（密码认证/scram 环境的推荐档，Task 15 WalSource 接线用）；原四参
 * 签名为无凭据回落——凭据取引导连接 URL 的 query 参数（{@code ?user=..&amp;password=..}，
 * 缺省 user 取 metadata 用户名、password 空串即 trust 认证）并 WARN 一行提示。</p>
 *
 * <p>线程约束：{@link #apply} 在接收线程（wal-receiver）单写者执行；stores 行字典
 * /tail 存储为 ConcurrentHashMap（跨线程快照迭代弱一致、不抛 CME，复合搬迁仍按单
 * 写者假设，见 {@link CatalogStores}）；{@link #snapshot()}/{@link #consumedLsn()}/
 * {@link #metrics()} 为只读观测面——跨线程读为尽力一致：apply 先写字典后经 volatile
 * 发布前沿，happens-before 保证读方<strong>见新前沿必见新字典</strong>，良性的交错是
 * 旧前沿 + 新字典（重放推进中的瞬时窗口；spec §7 检查点取单线程天然一致点）。
 * {@link #stop()} 幂等，可从任意线程调用。</p>
 */
public final class CatalogSynchronizer {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogSynchronizer.class);

    private final CatalogStores stores;
    private final CatalogReplay replay;

    /** 接收器（start 组装路径注入；纯逻辑测试缝为 null）。 */
    private final WalStreamReceiver receiver;

    /** 槽名（槽推进 SQL 的目标；纯逻辑缝为 null）。 */
    private final String slotName;

    /** 检查点配置（null = 禁用——无落盘/续传/槽推进，Task 13 前的无状态形态）。 */
    private final StateConfig stateConfig;

    /** 检查点存储器（stateConfig 启用时非 null）。 */
    private final StateStore stateStore;

    /** 槽推进复用的 SQL 会话（引导会话；接收线程单写者上下文与 stop 调用线程串行使用）。 */
    private final Connection advanceConnection;

    /** 最近一次检查点落盘时点（毫秒墙钟）——周期判定基准，接收线程单写者。 */
    private long lastCheckpointWallMs;

    /** 最近一次检查点落盘时的 catalog 行事件计数——事件阈值判定基准，接收线程单写者。 */
    private long replayedAtCheckpoint;

    /** 最近一次成功落盘的检查点 lsn（volatile 跨线程观测，未落盘为 0）。 */
    private volatile long lastCheckpointLsn;

    /** 最近一次槽推进成功的目标 lsn（volatile 跨线程观测，未推过为 0）。 */
    private volatile long lastSlotAdvanceLsn;

    /** 本次 start 是否自检查点续传（true = restoreInto 路径、false = 全新引导）。 */
    private volatile boolean resumedFromState;

    /** 已施加前沿镜像：最近一次 apply 完成的记录 MAXALIGN 末尾（volatile 跨线程读）。 */
    private volatile long appliedLsn;

    /**
     * 纯逻辑构造缝（测试/离线组装用：直接持 stores + 重放引擎，无接收器——apply 面
     * 与 start 组装路径完全同源）。
     *
     * @param stores 重放状态容器（调用方持有引导/种子责任）
     * @param replay 重放引擎（与 stores 配对的 layout/decoder 组合）
     */
    CatalogSynchronizer(CatalogStores stores, CatalogReplay replay) {
        this(stores, replay, null, null, null, null, null);
    }

    /**
     * 带前沿种子的纯逻辑缝（离线测试用）：在双参缝之上镜像 {@code startInternal} 的
     * {@code sync.appliedLsn = start} 种子化——引导路径 start = max(P₀, B)（bootstrap
     * 返回的首句 flush LSN）、续传路径 start = stored lsn。
     *
     * <p>测试锚定语义（终审 C1 离线判定）：种子 lsn 充当 apply 过滤线——记录末尾
     * ≤ 种子被跳过（重投递窗口过滤），紧邻其后（末尾 &gt; 种子）的首条记录必须施加
     * ——引导窗口内的记录不被过滤线吞掉。线程约束：构造线程单次调用。</p>
     *
     * @param stores         重放状态容器（调用方持有引导/种子责任）
     * @param replay         重放引擎
     * @param seedAppliedLsn 已施加前沿种子（start 返回的流起点）
     */
    CatalogSynchronizer(CatalogStores stores, CatalogReplay replay, long seedAppliedLsn) {
        this(stores, replay, null, null, null, null, null);
        this.appliedLsn = seedAppliedLsn;
    }

    /**
     * 全参构造（start 组装路径）。
     *
     * @param stores           重放状态容器（已引导或已自检查点恢复）
     * @param replay           重放引擎
     * @param receiver         接收器（null = 无流面，consumedLsn 回落 appliedLsn）
     * @param slotName         槽名（槽推进目标；null = 纯逻辑缝）
     * @param stateConfig      检查点配置（null = 禁用落盘/续传/槽推进）
     * @param stateStore       检查点存储器（stateConfig 禁用时 null）
     * @param advanceConnection 槽推进复用会话（null = 不推进）
     */
    private CatalogSynchronizer(CatalogStores stores, CatalogReplay replay, WalStreamReceiver receiver,
            String slotName, StateConfig stateConfig, StateStore stateStore, Connection advanceConnection) {
        this.stores = stores;
        this.replay = replay;
        this.receiver = receiver;
        this.slotName = slotName;
        this.stateConfig = stateConfig;
        this.stateStore = stateStore;
        this.advanceConnection = advanceConnection;
    }

    /**
     * v1 组装入口（无凭据回落档）：凭据从引导连接 URL 派生（query 参数优先，缺省
     * user 取 metadata 用户名、password 空串——trust 认证），并 WARN 一行提示密码
     * 认证环境改用带凭据重载。组装序与 {@link #start(Connection, String, WalLayout,
     * String, String, long...)} 完全一致，见其 javadoc。
     *
     * @param sql            引导用 SQL 会话（普通连接；凭据派生见类 javadoc）
     * @param slotName       物理槽名
     * @param layout         版本布局描述符（重放引擎与接收器共用）
     * @param interestRelOid 需要列字典/toast 跟踪的关系 oid（v1 tracked 面单表，可空）
     * @return 已运行的同步器（apply 消费面 + snapshot 查询面）
     * @throws SQLException 槽管理或引导查询失败
     */
    public static CatalogSynchronizer start(Connection sql, String slotName, WalLayout layout,
            long... interestRelOid) throws SQLException {
        ConnInfo ci = ConnInfo.from(sql);
        LOG.warn("CatalogSynchronizer 未显式传凭据——复制连接按派生凭据建立（URL 参数优先、"
                + "否则 user=metadata 用户名/password 空）；密码认证环境请用带凭据重载 start(..., user, password, ...)");
        return startInternal(sql, slotName, layout, ci.user(), ci.pass(), null, null, 0L, interestRelOid);
    }

    /**
     * v1 组装入口（带凭据档，密码认证环境的推荐形态）：槽确保 → 一致性引导 → 起接收
     * 流（sink = 本实例 apply）。
     *
     * <p>关键步骤：① interest oid 注册进 stores（引导据此定位 tracked 双 ctid；v1
     * 注册 0 个合法——纯字典同步形态）；② ensureSlot 得 P₀；③ bootstrap 得种子 +
     * 同事务 flush LSN（B）；④ 接收器连接参数 host/port/db 取引导连接 metadata URL
     * 派生 + 显式 user/password 覆盖；⑤ {@code receiver.start(this::apply, max(P0, B))}
     * ——页对齐由接收器内取整，sink 在接收线程同步执行；⑥ 已施加前沿种子化为
     * <strong>页对齐流起点</strong>（引导路径——过滤线不吞流交付的重叠段，终审 C1；
     * 续传路径 = stored lsn）——引导后首条记录施加前 snapshot().lsn() 即引导一致点
     * 而非 0。
     * <strong>自愈接线（Task 13）</strong>：本档构造 {@code new SelfHealer(new JdbcProbeImpl(sql))}
     * 注入重放引擎——截断更新未知 oldCtid 走 ctid 寻址精确采纳（class/attr 两面：
     * 按记录 new 位探测 JDBC 末态行整行采纳，末态回填语义）；
     * probe 复用引导会话，故 <strong>sql 会话自此归同步器独占</strong>（接收线程单写者
     * 上下文调用，调用方不得再并发使用；无凭据四参档 healer=null 保留 skip 行为）。
     * 边界与异常语义：SQLException/ISE 原样上抛（半建资源由接收器自有生命周期兜底，
     * 未 start 的接收器无需停机）；重复/前置失败由各组件自身 fail-fast。</p>
     *
     * @param sql            引导用 SQL 会话（普通连接；host/port/db 取其 URL 派生）
     * @param slotName       物理槽名
     * @param layout         版本布局描述符（重放引擎与接收器共用）
     * @param user           复制连接用户（覆盖 URL 派生值）
     * @param password       复制连接密码（trust 认证环境可传空串）
     * @param interestRelOid 需要列字典/toast 跟踪的关系 oid（v1 tracked 面单表，可空）
     * @return 已运行的同步器（apply 消费面 + snapshot 查询面）
     * @throws SQLException 槽管理或引导查询失败
     */
    public static CatalogSynchronizer start(Connection sql, String slotName, WalLayout layout,
            String user, String password, long... interestRelOid) throws SQLException {
        // SelfHealer 真接线（Task 13 装配裁定）：带凭据档 = 生产推荐形态，探测复用引导会话
        // ——probe 在接收线程单写者上下文调用，引导会话自此归同步器独占（调用方不得并发使用）
        return startInternal(sql, slotName, layout, user, password,
                new SelfHealer(new JdbcProbeImpl(sql)), null, 0L, interestRelOid);
    }

    /**
     * v1 组装入口（带凭据 + 检查点档，Task 15 全参版）：带凭据档之上接入
     * {@link StateStore} 生命周期 + 显式流起点覆盖。
     *
     * <p><strong>启动序（state.dir 有文件且 load 成功 → 续传）</strong>：① ensureSlot 取
     * P₀（槽必须存在——槽推进的目标）；② {@link StateStore#load()} 成功且 stored lsn
     * &ge; P₀ → {@link StoredState#restoreInto} 恢复字典 + 流起点 = stored lsn（页对齐
     * 由接收器下取整），<strong>跳过引导</strong>——已施加前沿种子化为 stored lsn，
     * <strong>apply 面跳过末尾 &le; 已施加前沿的重投递记录</strong>（页对齐多收/重连
     * 重发的窗口不重施加——部分记录类二次施加非幂等，见 {@link #apply} javadoc），
     * 此后 WAL 增量重放。stored lsn &lt; P₀ = 槽保留窗口已越前（槽被越程推进/重建），字典无法覆盖
     * 窗口 → WARN + 回落全新引导（安全侧：宁可重引导，不可错位窗口重放，spec §7）。</p>
     *
     * <p><strong>load 失败 / 无文件 → 全新引导</strong>（现路径：bootstrap + max(P₀, B)）。
     * <strong>forcedStartLsn &gt; 0</strong> 时无视上述计算直接以其为流起点（页对齐仍由
     * 接收器下取整）——诊断/丢页注入接缝（IT 模拟跳变用），字典恢复/引导决策不受其
     * 影响；<strong>须 ≥ 上述决策起点</strong>（低于 = 对已恢复字典二次施加的错位重放，
     * 启动期 ISE，终审 M1 守卫）；须落在服务端有效 WAL 区间内（未来位点被
     * START_REPLICATION 拒绝）。</p>
     *
     * <p><strong>运行中检查点</strong>：接收线程 apply 后按"距上次落盘 &ge;
     * {@code intervalMs} 或新施加 catalog 行事件 &ge; {@code eventsThreshold}（先到为准）"
     * 落一次检查点（单线程天然一致点，spec §7）；每次落盘成功后经 SQL 会话
     * {@code pg_replication_slot_advance(slot, 检查点 lsn)} 推进物理槽 restart_lsn
     * （Task 8 裁定方案 a：<strong>严格持久化之后</strong>——崩溃时最坏重复重放已落盘
     * 检查点之后的窗口，不丢状态）。检查点 IOException WARN 不中断接收；推进失败
     * （槽不存在/参数错）WARN 不中断。{@link #stop()} 在接收线程 join 后做最终
     * best-effort 检查点 + 推进（冻结态一致点）。</p>
     *
     * @param sql             引导用 SQL 会话（probe 与槽推进复用，归同步器独占）
     * @param slotName        物理槽名
     * @param layout          版本布局描述符
     * @param user            复制连接用户
     * @param password        复制连接密码
     * @param state           检查点配置（null 或 dir=null = 禁用）
     * @param forcedStartLsn  显式流起点（0 = 按续传/引导决策；&gt;0 = 覆盖，诊断接缝）
     * @param interestRelOid  tracked 面 oid
     * @return 已运行的同步器
     * @throws SQLException 槽管理或引导查询失败
     */
    public static CatalogSynchronizer start(Connection sql, String slotName, WalLayout layout,
            String user, String password, StateConfig state, long forcedStartLsn,
            long... interestRelOid) throws SQLException {
        return startInternal(sql, slotName, layout, user, password,
                new SelfHealer(new JdbcProbeImpl(sql)), state, forcedStartLsn, interestRelOid);
    }

    /**
     * 组装共核（全部公开 start 重载的唯一实现）：healer 与 state 注入是各档差异——带凭据
     * 档注入 {@link SelfHealer}（probe 复用引导会话），无凭据回落档 healer=null（截断
     * 未知 oldCtid 保留 skip + 计数行为）；state 启用时按续传/引导双路决策（见全参档
     * javadoc）。组装序见带凭据重载 javadoc。
     *
     * @param sql            引导用 SQL 会话（healer 非 null 时生命周期须覆盖同步器全程）
     * @param slotName       物理槽名
     * @param layout         版本布局描述符（须覆盖服务端版本，否则启动期 ISE）
     * @param user           复制连接用户
     * @param password       复制连接密码
     * @param healer         截断自愈校验器（null = 禁用）
     * @param state          检查点配置（null = 禁用）
     * @param forcedStartLsn 显式流起点（0 = 按决策；&gt;0 = 覆盖）
     * @param interestRelOid tracked 面 oid
     * @return 已运行的同步器
     * @throws SQLException 槽管理或引导查询失败
     * @throws IllegalStateException 布局与服务端 server_version_num 错配（跨版本注入）
     */
    private static CatalogSynchronizer startInternal(Connection sql, String slotName, WalLayout layout,
            String user, String password, SelfHealer healer, StateConfig state, long forcedStartLsn,
            long... interestRelOid) throws SQLException {
        // 版本交叉校验（审查 Med-3）：连接端 server_version_num 与 layout 双向核对——
        // 错配的常量会让页遍历/记录解析/投影槽位整体错位（如 V18 layout 读 V17 的
        // reltoastrelid@112 会读进 relallvisible），必须在任何槽/流副作用之前 fail-fast
        int serverVersionNum = queryServerVersionNum(sql);
        if (!layout.supports(serverVersionNum)) {
            throw new IllegalStateException("layout mismatch: descriptor PG" + layout.majorVersion()
                    + " does not support server_version_num " + serverVersionNum
                    + "（跨版本注入——请经 WalLayouts.forServerVersion 分发）");
        }
        CatalogStores stores = new CatalogStores();
        for (long oid : interestRelOid) {
            stores.interestRelOids().add(oid);
        }
        long p0 = new PhysicalSlotManager(sql).ensureSlot(slotName);

        StateStore stateStore = null;
        long start;
        boolean resumed = false;
        long storedLsn = 0;
        if (state != null && state.enabled()) {
            stateStore = new StateStore(state.dir(), layout.majorVersion());
            Optional<StoredState> loaded = stateStore.load();
            if (loaded.isPresent() && loaded.get().lsn() >= p0) {
                loaded.get().restoreInto(stores);
                storedLsn = loaded.get().lsn();
                start = storedLsn;
                resumed = true;
                LOG.info("CatalogSynchronizer 自检查点续传: stored lsn={}（attr {} / class {} 行, 跳过引导）",
                        Lsn.format(start), stores.attrRows().size(), stores.classRows().size());
            } else if (loaded.isPresent()) {
                LOG.warn("检查点 lsn {} 落在槽保留窗口 P₀ {} 之前——字典无法覆盖窗口, 回落全新引导（安全侧）",
                        Lsn.format(loaded.get().lsn()), Lsn.format(p0));
                start = Long.MIN_VALUE;    // 哨兵：下方引导路径覆盖
            } else {
                start = Long.MIN_VALUE;    // 无文件/拒载：全新引导
            }
        } else {
            start = Long.MIN_VALUE;
        }
        if (!resumed && start == Long.MIN_VALUE) {
            long bootstrapLsn = new CatalogBootstrap(sql, layout).bootstrap(stores);
            start = Math.max(p0, bootstrapLsn);
        }
        boolean forced = false;
        if (forcedStartLsn > 0) {
            if (forcedStartLsn < start) {
                // 终审 M1 守卫：forcedStartLsn 低于字典一致起点 = 起流点落回字典覆盖窗口
                // 之前——已恢复（续传/引导）字典会被窗口内记录二次施加，且部分记录类
                // 二次施加非幂等（见 apply javadoc），宁拒启不错位重放
                throw new IllegalStateException("forcedStartLsn " + Lsn.format(forcedStartLsn)
                        + " 低于字典一致起点 " + Lsn.format(start) + "——对已恢复字典二次施加风险，拒绝启动");
            }
            start = forcedStartLsn;
            forced = true;
        }

        ConnInfo ci = ConnInfo.from(sql).withCredentials(user, password);
        WalStreamReceiver receiver = new WalStreamReceiver(
                ci.host(), ci.port(), ci.database(), ci.user(), ci.pass(), layout, slotName);
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout), healer), receiver,
                slotName, state, stateStore, stateStore == null ? null : sql);
        // 前沿种子化（终审 C1 两级语义）——前沿兼作 apply 的重投递过滤线，种子按字典来源分级：
        // ① 续传/forced：字典已<strong>施加</strong>到 stored/forced lsn——部分记录类二次施加
        //    非幂等，须在未对齐的 stored/forced lsn 处严格过滤（Low-5 泛化：续传页对齐
        //    多收 + 断流重连重发 + 丢页注入的 gap 尾页）；② 全新引导：字典来自快照（无任何
        //    "已施加"记录），过滤线取<strong>页对齐流起点</strong>——流实际交付的重叠段
        //    （含在途事务跨 B 的记录：INSERT 已 flush ≤ B 而 commit 晚于快照）按 spec §6①
        //    原意重放消化（INS/UPD ctid upsert 幂等、截断更新走 ctid 寻址末态采纳收敛），
        //    实测该段不过滤会在引导并发 DDL 下偶发丢 attr 行（终审复测实证）
        sync.appliedLsn = resumed || forced ? start
                : start & ~((long) layout.walBlockSize() - 1);
        sync.resumedFromState = resumed;
        if (state != null && state.enabled()) {
            sync.lastCheckpointWallMs = System.currentTimeMillis();
            sync.replayedAtCheckpoint = stores.metrics().get(CatalogStores.CatalogMetrics.REPLAYED);
        }
        receiver.start(sync::apply, start);
        LOG.info("CatalogSynchronizer 启动: slot={} 流起点 {}（interest {} 个, selfHeal={}, state={}, resumed={}）",
                slotName, Lsn.format(start), interestRelOid.length, healer != null,
                state != null && state.enabled(), resumed);
        return sync;
    }

    /**
     * 查询连接端 {@code server_version_num}（版本交叉校验的输入，审查 Med-3）。
     *
     * <p>关键步骤：单行单列设置查询；边界与异常语义：连接失效/查询失败由 SQLException
     * 上抛（调用方 startInternal 尚无槽/流副作用，失败即净退出）；线程约束：静态纯
     * 查询，startInternal 调用线程执行。</p>
     *
     * @param sql 引导用 SQL 会话
     * @return 十进制版本号（如 170011 / 180006）
     * @throws SQLException 查询失败
     */
    private static int queryServerVersionNum(Connection sql) throws SQLException {
        try (PreparedStatement ps = sql.prepareStatement("SELECT current_setting('server_version_num')::int");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /**
     * 流消费面（接收线程回调）：施加一条 WAL 记录到 catalog 字典并推进已施加前沿，
     * state 启用时顺带判定周期检查点。
     *
     * <p>关键步骤：{@link CatalogReplay#applyCatalogRecord}（prune → inplace → attr
     * events → class events + tracked 跟随，施加次序由引擎固定）→ 前沿推进到本记录
     * MAXALIGN 末尾（与 walker 记录对齐边界同式）→ {@link #maybeCheckpoint}（先到为
     * 准的节拍判定，未达阈值为 no-op）。边界与异常语义：非 catalog 记录（其它
     * rmgr/关系）为引擎内 no-op，前沿照常推进——位点语义是"已走读"而非"已改字典"；
     * <strong>重复窗口过滤（Low-5 泛化 + 终审 C1 两级种子）</strong>——记录末尾 &le;
     * 已施加前沿的直接跳过。前沿种子按字典来源分级：<strong>续传/forced 路径</strong>
     * 种子 = stored/forced lsn（未对齐）——字典已"施加"到该点，二次施加非幂等（截断
     * 更新对已被 INPLACE 失效 tail 的旧行做值编码重建曾产出部分零字段的行——Task 15
     * IT 实证），须严格过滤页对齐多收/重连重发/gap 尾页三类重投递；<strong>全新引导
     * 路径</strong>种子 = 页对齐流起点——快照字典无"已施加"记录，流交付的重叠段
     * （含在途事务跨 B 的记录）按 spec §6① 原意重放消化而非过滤。跳过保证前沿单调
     * 不回退。线程约束：接收线程单写者。</p>
     *
     * @param r 走读完成的记录
     */
    public void apply(WalRecord r) {
        long end = (r.lsn() + r.totLen() + 7) & ~7L;
        if (end <= appliedLsn) {
            return;    // 重投递窗口内已施加（续传页对齐多收/重连重发）——跳过, 前沿不回退
        }
        replay.applyCatalogRecord(r, stores);
        appliedLsn = end;
        maybeCheckpoint();
    }

    /**
     * 周期检查点判定（接收线程）：距上次落盘 &ge; {@code intervalMs} 或距上次落盘点
     * 新施加 catalog 行事件 &ge; {@code eventsThreshold}（先到为准）即落一次检查点。
     *
     * <p>关键步骤：state 禁用直接返回；节拍基准（墙钟/事件计数）在每次落盘后重置。
     * 边界与异常语义：{@link #persistCheckpoint} 内部吞 IOException（WARN 不中断接收
     * ——检查点失败不损既有正名文件，下次节拍重试）。线程约束：接收线程单写者
     * （节拍基准为普通 long 字段）。</p>
     */
    private void maybeCheckpoint() {
        StateConfig cfg = stateConfig;
        if (cfg == null || !cfg.enabled()) {
            return;
        }
        long replayed = stores.metrics().get(CatalogStores.CatalogMetrics.REPLAYED);
        if (System.currentTimeMillis() - lastCheckpointWallMs < cfg.intervalMs()
                && replayed - replayedAtCheckpoint < cfg.eventsThreshold()) {
            return;
        }
        lastCheckpointWallMs = System.currentTimeMillis();
        replayedAtCheckpoint = replayed;
        persistCheckpoint(appliedLsn);
    }

    /**
     * 落一次检查点（best-effort）+ 持久化成功后推槽（Task 8 裁定方案 a）。
     *
     * <p>关键步骤：{@link StateStore#checkpoint}（全量序列化 + fsync + 原子换名）成功
     * 才记 {@link #lastCheckpointLsn} 并 {@link #advanceSlot}——推进严格在持久化之后
     * （崩溃时最坏重复重放已落盘之后的窗口，不丢状态）。边界与异常语义：IOException
     * WARN 吞掉（旧检查点不受损）；推进失败仅 WARN。线程约束：接收线程（周期路径）与
     * stop 调用线程（最终路径）串行——stop 先 join 接收线程。</p>
     *
     * @param lsn 检查点 lsn（调用时点的已施加前沿）
     */
    private void persistCheckpoint(long lsn) {
        StateStore store = stateStore;
        if (store == null) {
            return;
        }
        try {
            store.checkpoint(stores, lsn);
            lastCheckpointLsn = lsn;
            advanceSlot(lsn);
        } catch (IOException e) {
            LOG.warn("检查点落盘失败（保留旧检查点, 接收不中断）: lsn={} 原因: {}",
                    Lsn.format(lsn), e.getMessage(), e);
        }
    }

    /**
     * 推进物理槽 restart_lsn 到检查点 lsn（SQL：{@code pg_replication_slot_advance}）。
     *
     * <p>关键步骤：参数绑定槽名与 lsn 文本（{@code ?::pg_lsn} 服务端转换）；成功记
     * {@link #lastSlotAdvanceLsn}。边界与异常语义：槽不存在/参数错等 SQLException 一律
     * WARN 不中断（推进是保留窗口的优化而非正确性前提——WAL 保留兜底由
     * {@code max_slot_wal_keep_size} 承担）；会话失效同样 WARN（下次检查点重试）。
     * 线程约束：与 persistCheckpoint 同线程串行。</p>
     *
     * @param lsn 推进目标（刚落盘的检查点 lsn）
     */
    private void advanceSlot(long lsn) {
        Connection c = advanceConnection;
        String slot = slotName;
        if (c == null || slot == null) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_replication_slot_advance(?, ?::pg_lsn)")) {
            ps.setString(1, slot);
            ps.setString(2, Lsn.format(lsn));
            ps.executeQuery();
            lastSlotAdvanceLsn = lsn;
            LOG.debug("槽推进完成: {} -> {}", slot, Lsn.format(lsn));
        } catch (SQLException e) {
            LOG.warn("槽推进失败（不中断, 下次检查点重试）: slot={} 目标={} 原因: {}",
                    slot, Lsn.format(lsn), e.getMessage());
        }
    }

    /**
     * 字典 as-of 快照（{@link CatalogSnapshot} 契约实现——内部视图类，活引用不冻结）。
     *
     * @return 快照视图（lsn = 已施加前沿；查询随字典推进反映最新状态）
     */
    public CatalogSnapshot snapshot() {
        return new SnapshotView();
    }

    /**
     * 接收消费前沿（透传 {@link WalStreamReceiver#consumedLsn()}——含已走读但 sink
     * 尚在执行的批内记录；纯逻辑缝无接收器时回落已施加前沿）。
     *
     * @return 下一个未消费字节的 LSN；未 start 为 0
     */
    public long consumedLsn() {
        WalStreamReceiver r = receiver;
        return r == null ? appliedLsn : r.consumedLsn();
    }

    /**
     * 重放指标快照（防御拷贝——跨线程只读面）。
     *
     * @return 计数键值不可变拷贝（replayed/skippedTruncated/pruneRedirects/...）
     */
    public Map<String, Long> metrics() {
        return stores.metrics().snapshot();
    }

    /**
     * 重放状态容器直读面（活引用，非拷贝）——诊断/对拍（Task 13 IT 的 ctid 键控逐行
     * 对拍）与 Task 14 StateStore 持久化的种子来源。读方语义：停流（{@link #stop()}
     * 后）为冻结视图；流运行期为尽力一致（见类 javadoc 线程约束节）。
     *
     * @return 内部 stores（与本同步器同寿）
     */
    public CatalogStores stores() {
        return stores;
    }

    /**
     * 接收器指标观测面（resyncs/reconnects/census 等锚定协议计数）。
     *
     * @return 接收器内建计数器；纯逻辑缝实例（无接收器）null
     */
    public WalStreamMetrics streamMetrics() {
        WalStreamReceiver r = receiver;
        return r == null ? null : r.metrics();
    }

    /**
     * 接收器终态失败透传面（终审 I2）：接收线程因确定性失败（重连耗尽/解析 ISE）自行
     * 退出时返回根因，Main/宿主据此 fail-fast；正常停机与运行中/纯逻辑缝为 empty。
     *
     * @return 终态根因；无接收器或运行中为 empty
     */
    public Optional<Throwable> terminalFailure() {
        WalStreamReceiver r = receiver;
        return r == null ? Optional.empty() : r.terminalFailure();
    }

    /**
     * 最近一次成功落盘的检查点 lsn（Main 周期行 / 生命周期 IT 观测面）。
     *
     * @return 检查点 lsn；state 禁用或尚未落盘为 0
     */
    public long lastCheckpointLsn() {
        return lastCheckpointLsn;
    }

    /**
     * 最近一次槽推进成功的目标 lsn（观测面——与 {@link #lastCheckpointLsn()} 相等即
     * "落盘后必推过"，落后即推进失败被 WARN 吞掉的形态）。
     *
     * @return 推进目标 lsn；未推过为 0
     */
    public long lastSlotAdvanceLsn() {
        return lastSlotAdvanceLsn;
    }

    /**
     * 本次 start 是否自检查点续传（生命周期 IT 的路径断言面：true = restoreInto 跳过
     * 引导，false = 全新引导或拒载回落）。
     *
     * @return 续传为 true；纯逻辑缝/引导路径为 false
     */
    public boolean resumedFromState() {
        return resumedFromState;
    }

    /**
     * 幂等停机：停接收线程（join 3s 上限，委派 {@link WalStreamReceiver#stop()}）→
     * state 启用时做最终 best-effort 检查点 + 槽推进（接收线程已 join，冻结态即单
     * 线程天然一致点）；引导 SQL 会话归调用方持有生命周期，本类不越权关闭。
     *
     * <p>重复调用安全：appliedLsn 已冻结，重复落盘内容幂等。</p>
     */
    public void stop() {
        WalStreamReceiver r = receiver;
        if (r != null) {
            r.stop();
        }
        StateConfig cfg = stateConfig;
        if (cfg != null && cfg.enabled()) {
            persistCheckpoint(appliedLsn);
        }
        LOG.info("CatalogSynchronizer 停机: 已施加前沿 {}（检查点 {} / 槽推进 {}）",
                Lsn.format(appliedLsn), Lsn.format(lastCheckpointLsn), Lsn.format(lastSlotAdvanceLsn));
    }

    /**
     * 引导连接的 metadata URL 解析产物（接收器复制连接的四件 + 凭据）。包可见以便
     * 离线单测（{@link #parseUrl(String, String)}）。
     *
     * @param host     主机（多宿主取首个，v1 限制）
     * @param port     端口（缺省 5432）
     * @param database 库名（缺省回落用户名——pgjdbc 同语义）
     * @param user     用户（URL 参数优先，回落 metadata 用户名）
     * @param pass     密码（仅取 URL 参数；缺省空串——trust 认证，见类 javadoc）
     */
    record ConnInfo(String host, int port, String database, String user, String pass) {

        /**
         * 显式凭据覆盖（带凭据重载用）：host/port/db 保持 URL 派生值。
         *
         * @param user 覆盖用户
         * @param pass 覆盖密码
         * @return 覆盖后的解析产物
         */
        ConnInfo withCredentials(String user, String pass) {
            return new ConnInfo(host, port, database, user, pass);
        }

        /**
         * 从 JDBC Connection 的 metadata 解析连接参数（URL 即 pgjdbc 建连原串，
         * {@code PgConnection.getURL()} 返回 creatingURL），回落用户取
         * {@code getMetaData().getUserName()}——纯解析逻辑委派 {@link #parseUrl}。
         *
         * @param c 引导连接
         * @return 解析产物
         * @throws SQLException metadata 读取失败
         */
        static ConnInfo from(Connection c) throws SQLException {
            return parseUrl(c.getMetaData().getURL(), c.getMetaData().getUserName());
        }
    }

    /**
     * pgjdbc URL 解析（纯函数，包可见供离线单测）：形如
     * {@code jdbc:postgresql://host[:port][/db][?k=v&..]} 或
     * {@code jdbc:postgresql:dbname}（全默认主机形态）。
     *
     * <p>关键步骤：前缀校验 → query 参数切出（凭据面：user/password）→ 主机段
     * <strong>先切多宿主逗号再切端口冒号</strong>（{@code //h1:5432,h2:5433} 形态
     * 若先切冒号会把端口解析成 "5432,h2:5433"——审查修复锚）→ 缺省回落（host
     * localhost、port 5432、db=用户名、password 空串）。边界与异常语义：非
     * postgresql URL / 端口非数字抛 ISE（fail-fast 优于静默连错目标）；IPv6 字面量
     * 不支持（v1 限制）；URL 参数值不做百分号解码（pgjdbc URL 参数即明文
     * properties，官方语义）。线程约束：纯函数。</p>
     *
     * @param url          pgjdbc 建连 URL
     * @param fallbackUser URL 无 user 参数时的回落用户（metadata 用户名）
     * @return 解析产物
     */
    static ConnInfo parseUrl(String url, String fallbackUser) {
        String prefix = "jdbc:postgresql:";
        if (!url.startsWith(prefix)) {
            throw new IllegalStateException("非 postgresql URL，无法派生复制连接: " + url);
        }
        String rest = url.substring(prefix.length());
        Map<String, String> params = new HashMap<>();
        int q = rest.indexOf('?');
        if (q >= 0) {
            for (String kv : rest.substring(q + 1).split("&")) {
                int eq = kv.indexOf('=');
                if (eq > 0) {
                    params.put(kv.substring(0, eq), kv.substring(eq + 1));
                }
            }
            rest = rest.substring(0, q);
        }
        String host = "localhost";
        int port = 5432;
        String db = null;
        if (rest.startsWith("//")) {
            rest = rest.substring(2);
            int slash = rest.indexOf('/');
            String hostPort = slash >= 0 ? rest.substring(0, slash) : rest;
            db = slash >= 0 ? rest.substring(slash + 1) : null;
            int comma = hostPort.indexOf(',');
            if (comma >= 0) {
                hostPort = hostPort.substring(0, comma);    // 多宿主取首个（先于端口切分）
            }
            int colon = hostPort.indexOf(':');
            if (colon >= 0) {
                host = hostPort.substring(0, colon);
                String portText = hostPort.substring(colon + 1);
                try {
                    port = Integer.parseInt(portText);
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("URL 端口非数字: " + url);
                }
            } else {
                host = hostPort;
            }
        } else if (!rest.isEmpty()) {
            db = rest;    // jdbc:postgresql:dbname 形态（全默认主机）
        }
        String user = params.getOrDefault("user", fallbackUser);
        if (db == null || db.isEmpty()) {
            db = user;    // pgjdbc 缺省库 = 用户名
        }
        return new ConnInfo(host, port, db, user, params.getOrDefault("password", ""));
    }

    /**
     * {@link CatalogSnapshot} 的内部视图实现——活引用 stores（不冻结拷贝；行字典为
     * ConcurrentHashMap，跨线程迭代弱一致不抛 CME——与重放线程并发的尽力一致语义，
     * 见接口 javadoc 与类 javadoc 线程约束节）。
     */
    private final class SnapshotView implements CatalogSnapshot {

        /**
         * 已施加前沿（start 种子化：引导路径 = 页对齐流起点（终审 C1）、续传/forced
         * 路径 = stored/forced lsn，此后 = 最近一次 apply 完成的记录末尾）——字典
         * 内容的 as-of 位点。
         *
         * @return 打包 LSN（未 start 的纯逻辑缝实例为 0）
         */
        @Override
        public long lsn() {
            return appliedLsn;
        }

        /**
         * 列字典查询：attrRows 值域过滤 attrelid 后按 attnum 升序投影——含 dropped
         * 占位行（发现 26），未知关系空表。
         *
         * @param relOid 关系 oid
         * @return 列列表（attnum 升序）
         */
        @Override
        public List<Column> columnsOf(long relOid) {
            List<Column> out = new ArrayList<>();
            for (CatalogRow.AttrRow row : stores.attrRows().values()) {
                if (row.attrelid() == relOid) {
                    out.add(new Column(row.attnum(), row.attname(), row.atttypid(), row.attisdropped()));
                }
            }
            out.sort(Comparator.comparingInt(Column::attnum));
            return out;
        }

        /**
         * relfilenode 查询：classRows 值域首配 relOid（oid 目录内唯一，首配即全配）。
         *
         * @param relOid 关系 oid
         * @return 当前 relfilenode；不在字典 empty
         */
        @Override
        public OptionalLong relfilenodeOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    return OptionalLong.of(row.relfilenode());
                }
            }
            return OptionalLong.empty();
        }

        /**
         * toast 关系查询：与 {@link #relfilenodeOf(long)} 同源行取 reltoastrelid。
         *
         * @param relOid 关系 oid
         * @return toast 关系 oid（无 toast 为 of(0)）；不在字典 empty
         */
        @Override
        public OptionalLong toastOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    return OptionalLong.of(row.reltoastrelid());
                }
            }
            return OptionalLong.empty();
        }

        /**
         * 诊断用字符串形态（前沿 + 两字典行数 + tracked 双 ctid）。
         *
         * @return 概要文本
         */
        @Override
        public String toString() {
            return new StringJoiner(", ", "CatalogSnapshot[", "]")
                    .add("lsn=" + Lsn.format(appliedLsn))
                    .add("attrRows=" + stores.attrRows().size())
                    .add("classRows=" + stores.classRows().size())
                    .add("trackedTable=" + stores.trackedTableCtid())
                    .add("trackedToast=" + stores.trackedToastCtid())
                    .toString();
        }
    }
}
