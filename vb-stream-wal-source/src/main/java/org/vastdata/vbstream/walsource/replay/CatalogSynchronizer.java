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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.StringJoiner;

/**
 * 伪备库 catalog 同步器（spec §5 组件四）：JDBC 一致性引导 + 物理流接收 + ctid 重放
 * 的 v1 组装——一次 {@link #start} 完成槽确保、REPEATABLE READ 全行种子、起流（sink
 * 委派本类 {@link #apply}），此后字典随消费增量维护并经 {@link #snapshot()} 提供
 * {@link CatalogSnapshot} as-of 查询。
 *
 * <p><strong>装配序（spec §6①）</strong>：① {@link PhysicalSlotManager#ensureSlot} 取
 * P₀（建槽自身的 WAL 先于种子快照落盘）→ ② {@link CatalogBootstrap#bootstrap} 在
 * REPEATABLE READ 单事务内灌满两目录并返回同事务 flush LSN（B）→ ③ 流起点
 * max(P₀, B)（页对齐由接收器内部下取整）——窗口重叠由 ctid 键控 upsert 幂等消化。</p>
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
        this(stores, replay, null);
    }

    /**
     * 全参构造（start 组装路径）。
     *
     * @param stores   重放状态容器（已引导）
     * @param replay   重放引擎
     * @param receiver 接收器（null = 无流面，consumedLsn 回落 appliedLsn）
     */
    private CatalogSynchronizer(CatalogStores stores, CatalogReplay replay, WalStreamReceiver receiver) {
        this.stores = stores;
        this.replay = replay;
        this.receiver = receiver;
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
        return startInternal(sql, slotName, layout, ci.user(), ci.pass(), null, interestRelOid);
    }

    /**
     * v1 组装入口（带凭据档，密码认证环境的推荐形态）：槽确保 → 一致性引导 → 起接收
     * 流（sink = 本实例 apply）。
     *
     * <p>关键步骤：① interest oid 注册进 stores（引导据此定位 tracked 双 ctid；v1
     * 注册 0 个合法——纯字典同步形态）；② ensureSlot 得 P₀；③ bootstrap 得种子 +
     * 同事务 flush LSN（B）；④ 接收器连接参数 host/port/db 取引导连接 metadata URL
     * 派生 + 显式 user/password 覆盖；⑤ {@code receiver.start(this::apply, max(P0, B))}
     * ——页对齐由接收器内取整，sink 在接收线程同步执行；⑥ 已施加前沿种子化为流起点
     * max(P₀, B)——引导后首条记录施加前 snapshot().lsn() 即引导一致点而非 0。
     * <strong>自愈接线（Task 13）</strong>：本档构造 {@code new SelfHealer(new JdbcProbeImpl(sql))}
     * 注入重放引擎——pg_class 截断未知 oldCtid 走候选枚举 + 中段新值对 JDBC 末态校验；
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
                new SelfHealer(new JdbcProbeImpl(sql)), interestRelOid);
    }

    /**
     * 组装共核（两个公开 start 重载的唯一实现）：healer 注入与否是两档唯一差异——带凭据
     * 档注入 {@link SelfHealer}（probe 复用引导会话），无凭据回落档 healer=null（截断
     * 未知 oldCtid 保留 skip + 计数行为）。组装序见带凭据重载 javadoc。
     *
     * @param sql            引导用 SQL 会话（healer 非 null 时生命周期须覆盖同步器全程）
     * @param slotName       物理槽名
     * @param layout         版本布局描述符
     * @param user           复制连接用户
     * @param password       复制连接密码
     * @param healer         截断自愈校验器（null = 禁用）
     * @param interestRelOid tracked 面 oid
     * @return 已运行的同步器
     * @throws SQLException 槽管理或引导查询失败
     */
    private static CatalogSynchronizer startInternal(Connection sql, String slotName, WalLayout layout,
            String user, String password, SelfHealer healer, long... interestRelOid) throws SQLException {
        CatalogStores stores = new CatalogStores();
        for (long oid : interestRelOid) {
            stores.interestRelOids().add(oid);
        }
        long p0 = new PhysicalSlotManager(sql).ensureSlot(slotName);
        long bootstrapLsn = new CatalogBootstrap(sql, layout).bootstrap(stores);
        long start = Math.max(p0, bootstrapLsn);

        ConnInfo ci = ConnInfo.from(sql).withCredentials(user, password);
        WalStreamReceiver receiver = new WalStreamReceiver(
                ci.host(), ci.port(), ci.database(), ci.user(), ci.pass(), layout, slotName);
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout), healer), receiver);
        sync.appliedLsn = start;    // 前沿种子化：引导一致点而非 0（首条 apply 前的 as-of）
        receiver.start(sync::apply, start);
        LOG.info("CatalogSynchronizer 启动: slot={} 流起点 max(P0={}, bootstrap={}) = {}（interest {} 个, selfHeal={}）",
                slotName, Lsn.format(p0), Lsn.format(bootstrapLsn), Lsn.format(start),
                interestRelOid.length, healer != null);
        return sync;
    }

    /**
     * 流消费面（接收线程回调）：施加一条 WAL 记录到 catalog 字典并推进已施加前沿。
     *
     * <p>关键步骤：{@link CatalogReplay#applyCatalogRecord}（prune → inplace → attr
     * events → class events + tracked 跟随，施加次序由引擎固定）→ 前沿推进到本记录
     * MAXALIGN 末尾（与 walker 记录对齐边界同式）。边界与异常语义：非 catalog 记录
     * （其它 rmgr/关系）为引擎内 no-op，前沿照常推进——位点语义是"已走读"而非"已改
     * 字典"。线程约束：接收线程单写者。</p>
     *
     * @param r 走读完成的记录
     */
    public void apply(WalRecord r) {
        replay.applyCatalogRecord(r, stores);
        appliedLsn = (r.lsn() + r.totLen() + 7) & ~7L;
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
     * 幂等停机：停接收线程（join 3s 上限，委派 {@link WalStreamReceiver#stop()}）；
     * 引导 SQL 会话归调用方持有生命周期，本类不越权关闭。
     */
    public void stop() {
        WalStreamReceiver r = receiver;
        if (r != null) {
            r.stop();
        }
        LOG.info("CatalogSynchronizer 停机: 已施加前沿 {}", Lsn.format(appliedLsn));
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
         * 已施加前沿（start 种子化为流起点 max(P₀, 引导 LSN)，此后 = 最近一次 apply
         * 完成的记录末尾）——字典内容的 as-of 位点。
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
