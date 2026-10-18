package org.vastdata.vbstream.walsource.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.changes.ChangeStream;
import org.vastdata.vbstream.walsource.changes.RenderOptions;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.receive.FileWalSource;
import org.vastdata.vbstream.walsource.receive.ReceivexlogProcess;
import org.vastdata.vbstream.walsource.receive.WalByteSource;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * WAL 直解源全装配门面（Task 15 升级：接收面 → catalog 同步全管线）——把槽管理、
 * 版本布局分发、JDBC 一致性引导、物理流接收、ctid 重放、检查点持久化与槽推进装配
 * 为一次 {@link #start()} 调用，供冒烟 {@code Main} 与后续宿主（reader/连接器）复用。
 *
 * <p>装配次序（委派 {@link CatalogSynchronizer#start(Connection, String, WalLayout,
 * String, String, StateConfig, long...)}）：① 普通 JDBC URL 建 SQL 会话；② 同会话查
 * {@code current_setting('server_version_num')} 经 {@link WalLayouts#forServer}（9 万段
 * 补充 version() 文本；且无 openGauss branding 时再补 {@code pg_class.relname} attlen
 * 探测——vastbase T4 判别第二面，128 分发 VastBase 档、64/不可得落既有拒绝面，建槽
 * 之前、17/18 档零触碰）分发布局——错配版本启动期 ISE fail-fast（绝不近似回退）；
 * ③ 带凭据档 + 检查点配置起同步器（自愈接线；state.dir 有有效检查点 → 续传跳过引导，
 * 否则全新引导——决策见同步器 javadoc）。v1 门面无 tracked 面（interest 0 个，纯字典
 * 同步形态）。<strong>openGauss 5.0.0 档（M1 Task 6 + M2 Task 1）</strong>：判别命中
 * {@code WalLayoutVOpenGauss}（instanceof 门控——vastbase T4 起 VastBase 子类
 * {@code WalLayoutVVastbase} 同走本装配）时转 {@code startOpenGauss}——槽走 ensureSlotOg（dummy_
 * standby 建槽 + xlog 位点函数）、收流按 {@code vb.wal.og.source} 走 WalByteSource
 * 双通道（file=sidecar tail / direct=裸 v3 直连）、DML 面与 PG 17/18 路同构装配
 * （M2 Task 1 撤销 M1 的 WARN 降级——{@code ogCatalogOnly} 位退役；listener 在场且
 * dml 开着时 postApply/pendingFloor 经 startOg 注入 sink 惰性串接 ChangeStream）+
 * <strong>库级兼容模式护栏</strong>（DML 面请求在场时探测 {@code pg_database.
 * datcompatibility}，白名单 {'PG','A'}——M3 起 'A' 档[Oracle 风格]放行；可判定的
 * 白名单外档位['B'/'C']启动期 ISE 附档位与 M4 指引；探测不可判定
 * WARN 放行——catalog 面与兼容模式无关，M1 OG IT 即在 'A' 档缺省库全绿）；
 * 观测面（consumedLsn/metrics/terminalFailure/检查点/槽推进）经同步器同源透传。</p>
 *
 * <p><strong>离线回放档（offline replay，2026-10-15 spec）</strong>：数据库不可访问时
 * 经 {@link #startOffline()} 凭本地检查点 + WAL 段目录快照离线续传——不建 SQL 会话/
 * 不建槽/无引导无护栏探测（{@code sqlConnection} 本路径恒 null，close 的 null 安全是
 * 既有现状），委派 {@link CatalogSynchronizer#startOffline(StateConfig, Path, long, Set,
 * ChangeOutputListener)} 复用 OG 档运行期管线（ctid 重放/检查点节拍/快照视图）而无一
 * SQL 面；检查点是唯一状态源与入口凭证（state 未启用/检查点缺失损坏或档位不符由同步器
 * 启动期 ISE）。读尽终态经 {@link #eofReached()} 透传（在线档恒 false——EOF 是静态
 * 目录档专属概念），宿主据此 close 落最终检查点（同目录续写，离线/在线双向互通）。</p>
 *
 * <p>配置面：构造入参 {@link Properties} 读 {@code vb.wal.host/port/db/user/pass/slot}
 * 六键 + {@code vb.wal.state.dir/interval.ms/events} 三键 + v2 两键
 * （{@code vb.wal.tables} 白名单 / {@code vb.wal.dml} DML 输出开关，Task 7）+ openGauss
 * 档五键（M1 Task 6，spec §7：{@code vb.wal.og.source} file|direct / {@code
 * vb.wal.og.rx.mode} managed|external / {@code vb.wal.og.rx.dir} / {@code
 * vb.wal.og.rx.bin} / {@code vb.wal.og.jdbc.prefix}——前缀分流 opengauss-jdbc 与
 * pgjdbc，缺省 jdbc:postgresql: 保 PG 档零感知）+ 离线回放两键（{@code
 * vb.wal.offline.dir} 段快照目录——缺省 null = 在线档 / {@code vb.wal.offline.endLsn}
 * 可选交付上界——缺省 0 = 无上界），默认 {@code localhost:5432/postgres}、
 * {@code postgres/postgres}、槽 {@code wal_source}、state 禁用（三键语义/默认值见
 * {@link StateConfig}，单一来源不双写）、白名单空 = 全放行、dml 缺省 true。port/interval/
 * events 非数字启动构造期抛 {@link NumberFormatException}；{@code vb.wal.dml} 非
 * true/false、OG 五键取值域外 / 显式空白 rx.dir/rx.bin/offline.dir / offline.endLsn
 * 非法 LSN 文本构造期 IAE（fail-fast 优于静默连错端口/吞掉 DML 面）。</p>
 *
 * <p>生命周期：start/close 单程（重复 start 抛 ISE；close 幂等，实现 AutoCloseable；
 * close 含最终 best-effort 检查点 + 槽推进——委派 {@link CatalogSynchronizer#stop()}；
 * {@link #startOffline()} 与 start 共用同一单程闸门，两路互斥）；start/startOffline
 * 中途失败（SQLException/ISE）时内部资源由本类持有，调用方 close 释放。
 * <strong>契约（Med-3）：一个检查点目录同一时刻仅一个活实例</strong>——旧实例后
 * stop 会以陈旧状态覆盖新实例的检查点（文件锁后续任务）。线程约束：
 * start/close 仅装配线程调用；{@link #consumedLsn()} 与 {@link #metrics()} 快照面任意
 * 线程可读（前者 volatile 镜像，后者 {@link WalStreamMetrics#censusSnapshot()} 不可变副本）。</p>
 */
public final class WalSource implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WalSource.class);

    /**
     * OG 档 direct 通道的反馈心跳 statusInterval（毫秒）——与接收器路 5s flush 反馈同
     * 量级（DirectWalSource 心跳周期 = 其一半）。
     */
    private static final long OG_DIRECT_STATUS_INTERVAL_MS = 10_000;

    /**
     * VastBase 判别第二面的探测 SQL（vastbase T4，spec §4.1）：查 {@code pg_class}
     * 自身 {@code relname} 列的 {@code attlen}——NAMEDATALEN 128 是 VastBase G100
     * 相对 OG/vanilla 9.2（64）的唯一稳定区分面（其 version() 报 vanilla 形态无
     * branding，branding 只在 v$version）。public static final 供离线测试锚 SQL 形状
     * （{@code VastProbeDiscriminatorTest}）。
     */
    public static final String RELNAME_PROBE_SQL =
            "SELECT attlen FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relname'";

    /** PG 主机键（默认 localhost）。 */
    public static final String KEY_HOST = "vb.wal.host";

    /** PG 端口键（默认 5432）。 */
    public static final String KEY_PORT = "vb.wal.port";

    /** 库名键（默认 postgres）。 */
    public static final String KEY_DB = "vb.wal.db";

    /** 用户键（默认 postgres，须有 REPLICATION 权限）。 */
    public static final String KEY_USER = "vb.wal.user";

    /** 密码键（默认 postgres）。 */
    public static final String KEY_PASS = "vb.wal.pass";

    /** 物理复制槽名键（默认 wal_source）。 */
    public static final String KEY_SLOT = "vb.wal.slot";

    /** 检查点目录键（透传 {@link StateConfig#KEY_DIR}——缺省/空 = 检查点禁用）。 */
    public static final String KEY_STATE_DIR = StateConfig.KEY_DIR;

    /** 检查点间隔键（透传 {@link StateConfig#KEY_INTERVAL_MS}）。 */
    public static final String KEY_STATE_INTERVAL_MS = StateConfig.KEY_INTERVAL_MS;

    /** 检查点事件阈值键（透传 {@link StateConfig#KEY_EVENTS}）。 */
    public static final String KEY_STATE_EVENTS = StateConfig.KEY_EVENTS;

    /** DML 输出面表白名单键（逗号分隔 {@code schema.table}；缺省/空 = 用户表全放行）。 */
    public static final String KEY_TABLES = "vb.wal.tables";

    /** DML 输出开关键（默认 true；false 回纯 v1 catalog 形态——ChangeStream 不装配）。 */
    public static final String KEY_DML = "vb.wal.dml";

    /**
     * openGauss 档收流通道键（M1，spec §7）：{@code file}=sidecar pg_receivexlog 落目录
     * + FileWalSource tail（缺省）；{@code direct}=DirectWalSource 裸 v3 协议直连。非法值
     * 构造期 IAE。PG 17/18 档不读本键。
     */
    public static final String KEY_OG_SOURCE = "vb.wal.og.source";

    /**
     * openGauss 档 sidecar 管理键（仅 file 通道有意义）：{@code managed}=自拉起
     * pg_receivexlog 子进程（缺省）；{@code external}=只读既有落盘目录（进程运维自担）。
     * 非法值构造期 IAE。
     */
    public static final String KEY_OG_RX_MODE = "vb.wal.og.rx.mode";

    /**
     * openGauss 档段落盘目录键（file 通道的 tail 目录 / managed 的 {@code -D} 参数值；
     * 缺省 {@code wal-og-queue}，相对工作目录）。显式空白构造期 IAE（spec §4）。
     */
    public static final String KEY_OG_RX_DIR = "vb.wal.og.rx.dir";

    /**
     * openGauss 档 pg_receivexlog 可执行文件键（managed 模式；缺省 {@code pg_receivexlog}
     * 走 PATH）。显式空白构造期 IAE（spec §4）。
     */
    public static final String KEY_OG_RX_BIN = "vb.wal.og.rx.bin";

    /**
     * openGauss 档 SQL 会话驱动前缀键（M1 Task 6 增设，spec §0 的 URL 前缀分流面）：
     * {@code jdbc:opengauss:}=opengauss-jdbc（sha256 认证，OG 部署推荐）；缺省
     * {@code jdbc:postgresql:}=vanilla pgjdbc（+md5，须服务端 pg_hba 预埋 md5 行——运维
     * 自担）。<strong>缺省保持 jdbc:postgresql: 的理由</strong>：是否 OG 档在建连后才知道，
     * 缺省换前缀会让 PG 17/18 档在零配置变化下改走 opengauss-jdbc（违反"PG 档零回归"
     * 硬约束）——OG 部署显式设本键选驱动。非法值构造期 IAE。
     */
    public static final String KEY_OG_JDBC_PREFIX = "vb.wal.og.jdbc.prefix";

    /**
     * 离线回放档段快照目录键（offline replay，2026-10-15 spec）：指向 WAL 段文件的
     * 静态快照目录（拷 pg_xlog / recorder 落段），{@link #startOffline()} 的字节源。
     * 缺省 null = 在线档；显式空白构造期 IAE（沿 {@code vb.wal.og.rx.dir} 的空白纪律）。
     * 注意：{@code vb.wal.mode} 分派键是 Main 层直读，不属门面配置面。
     */
    public static final String KEY_OFFLINE_DIR = "vb.wal.offline.dir";

    /**
     * 离线回放档可选交付上界键：十六进制 LSN 文本（如 {@code 2/071BF288}），经
     * {@link Lsn#parse} 解析；缺省 0 = 无上界（静态目录清单读尽即 EOF）。非法值
     * 构造期 IAE（fail-fast 优于运行期跳错位）。
     */
    public static final String KEY_OFFLINE_END_LSN = "vb.wal.offline.endLsn";

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String pass;
    private final String slotName;
    private final StateConfig state;

    /** DML 白名单（{@code schema.table} 集；空 = 全放行——透传 ChangeStream/TableFilter）。 */
    private final Set<String> tables;

    /** OG 档收流通道（file/direct——构造期已校验，存档内合法值）。 */
    private final String ogSource;

    /** OG 档 sidecar 管理形态（managed/external——仅 file 通道消费）。 */
    private final String ogRxMode;

    /** OG 档段落盘目录（file 通道 tail 目录 / managed 的 -D 参数值）。 */
    private final Path ogRxDir;

    /** OG 档 pg_receivexlog 可执行文件（managed 模式）。 */
    private final String ogRxBin;

    /** 离线回放段快照目录（null = 在线档——startOffline 的前置判据）。 */
    private final Path offlineDir;

    /** 离线回放可选交付上界（0 = 无上界，清单读尽即 EOF）。 */
    private final long offlineEndLsn;

    /** SQL 会话 URL 驱动前缀（jdbc:postgresql: / jdbc:opengauss:——DriverManager 分流面）。 */
    private final String ogJdbcPrefix;

    /** DML 输出开关与输出 listener（listener 为 null 时 DML 面恒关——纯 catalog 形态）。 */
    private final ChangeOutputListener dmlOut;

    /**
     * 变更流门面（start 后接收线程首条记录到达时惰性建——需要同步器的活快照视图；
     * 离线档由 {@link #startOffline()} 装配后自同步器接线——离线无惰性路径，终审 F1：
     * 不接线则 {@code dml*} 发射计数观测面离线恒 0，Main 周期行误导操作员）。
     */
    private volatile ChangeStream changeStream;

    /** 同步器全管线装配用 SQL 会话（start 建，close 关；probe 与槽推进复用）。 */
    private Connection sqlConnection;

    /**
     * 同步器（start 建；null = 未 start）。<b>volatile（Task 8 JMM 修复）</b>：接收
     * 线程在 {@code CatalogSynchronizer.start} 内部启动、而 {@code sync} 字段赋值在
     * start 返回路径上——原声明无 HB 边可依（线程启动序只覆盖 start 调用<b>前</b>的
     * 写），接收线程首条记录到达时理论上可见 null/陈旧引用；volatile 补齐可见性，
     * {@link #awaitSync()} 的有界自旋再收窄时间窗（赋值在 start 返回前完成，正常
     * 路径自旋零次通过）。
     */
    private volatile CatalogSynchronizer sync;

    /** start/close 的单程闸门。 */
    private boolean started;
    private boolean closed;

    /** 渲染面四键（vb.wal.render.*）的解析产物——ChangeStream 装配与宿主 OutputRenderer 共用。 */
    private final RenderOptions renderOptions;

    /**
     * v1 单参构造（纯 catalog 形态）：无 DML 输出 listener——{@code vb.wal.dml} 配置
     * 不复活 DML 面（无输出契约消费者，装配即浪费）。委派
     * {@link #WalSource(Properties, ChangeOutputListener)} 传 null。
     *
     * @param cfg 配置源
     */
    public WalSource(Properties cfg) {
        this(cfg, null);
    }

    /**
     * 从配置装配方面（v2 全参：DML 输出 listener 注入）。
     *
     * <p>关键步骤：六键各自 {@code getProperty(key, 默认值)}——缺键取默认、显式空串保留
     * （pgjdbc 自行报错，不在此猜测补值）；port 经 {@link Integer#parseInt}、state 三键
     * 经 {@link StateConfig#fromProperties} 解析，非数字构造期抛 NumberFormatException
     * （fail-fast）；白名单 {@code vb.wal.tables} 逗号切分（逐项 trim、空项跳过，空集 =
     * 全放行）；DML 开关 {@code vb.wal.dml} <b>严格布尔解析</b>（true/false 之外构造期
     * IAE——静默当 false 会吞掉整个 DML 面）；离线两键——{@code vb.wal.offline.dir}
     * 缺省 null（在线档）显式空白 IAE、{@code vb.wal.offline.endLsn} 缺省 0（无上界）
     * present 即 {@link Lsn#parse}（IAE 转译附键名）。本方法不建任何连接（纯字段装配，
     * 连接推迟到 {@link #start()}——失败配置不应留半开资源）。</p>
     *
     * @param cfg     配置源（典型 {@code System.getProperty} 收集产物；null 键直接走默认）
     * @param dmlOut  DML 变更事件输出契约实现（null = 纯 v1 catalog 形态）
     * @throws IllegalArgumentException {@code vb.wal.dml} 非 true/false 字面 / OG 五键
     *                                  取值域外 / 离线键显式空白或非法 LSN 文本
     */
    public WalSource(Properties cfg, ChangeOutputListener dmlOut) {
        this.host = cfg.getProperty(KEY_HOST, "localhost");
        this.port = Integer.parseInt(cfg.getProperty(KEY_PORT, "5432"));
        this.database = cfg.getProperty(KEY_DB, "postgres");
        this.user = cfg.getProperty(KEY_USER, "postgres");
        this.pass = cfg.getProperty(KEY_PASS, "postgres");
        this.slotName = cfg.getProperty(KEY_SLOT, "wal_source");
        this.state = StateConfig.fromProperties(cfg);
        this.tables = parseWhitelist(cfg.getProperty(KEY_TABLES, ""));
        this.dmlOut = dmlOut;
        String dmlCfg = cfg.getProperty(KEY_DML, "true").trim();
        switch (dmlCfg) {
            case "true" -> {
                // dmlOut 为 null 时下方 dmlEnabled() 恒 false（v1 单参构造路径）
            }
            case "false" -> this.dmlOff = true;
            default -> throw new IllegalArgumentException(
                    KEY_DML + " 须为 true/false: " + dmlCfg + "（静默当 false 会吞掉 DML 输出面）");
        }
        // openGauss 档五键（M1，spec §7）：缺省值对 PG 17/18 档零感知（前缀缺省即现状拼接）
        this.ogSource = parseChoice(cfg, KEY_OG_SOURCE, "file", "file", "direct");
        this.ogRxMode = parseChoice(cfg, KEY_OG_RX_MODE, "managed", "managed", "external");
        this.ogJdbcPrefix = parseChoice(cfg, KEY_OG_JDBC_PREFIX, "jdbc:postgresql:",
                "jdbc:postgresql:", "jdbc:opengauss:");
        this.ogRxDir = parseNonBlankPath(cfg, KEY_OG_RX_DIR, "wal-og-queue");
        this.ogRxBin = parseNonBlank(cfg, KEY_OG_RX_BIN, "pg_receivexlog");
        // 离线回放两键（offline replay）：dir 缺省 null = 在线档（显式空白 IAE 沿空白纪律）；
        // endLsn 缺省 0 = 无上界，present 即 Lsn.parse（IAE 家族异常转译附键名便于归因）
        this.offlineDir = parseOptionalPath(cfg, KEY_OFFLINE_DIR);
        String endLsnCfg = cfg.getProperty(KEY_OFFLINE_END_LSN);
        if (endLsnCfg == null) {
            this.offlineEndLsn = 0L;
        } else {
            try {
                this.offlineEndLsn = Lsn.parse(endLsnCfg.trim());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(KEY_OFFLINE_END_LSN
                        + " 须为十六进制 LSN 文本（如 2/071BF288）: " + endLsnCfg, e);
            }
        }
        // 渲染面四键（vb.wal.render.*，2026-10-17 配置化）：构造期解析 fail-fast，
        // 穿线 ChangeStream → XactGrouper → DiskValueRenderer（值面）；OutputRenderer
        // 的截断面由宿主经 renderOptions() 自行构造（Main 冒烟路径已接）
        this.renderOptions = RenderOptions.fromProperties(cfg);
    }

    /**
     * 渲染选项观测面（{@code vb.wal.render.*} 四键的解析产物——宿主构造
     * {@code OutputRenderer} 复用同一实例，保证截断阈值与门面值面档位一致）。
     *
     * @return 渲染选项（缺省档 = {@link RenderOptions#PG}）
     */
    public RenderOptions renderOptions() {
        return renderOptions;
    }

    /**
     * 枚举式配置选择：取键值（缺省 default 值）后必须命中允许值之一，否则构造期 IAE。
     *
     * @param cfg          配置源
     * @param key          键
     * @param defaultValue 键缺席时的取值（本身须是合法值）
     * @param allowed      允许值全集
     * @return 已校验的取值
     * @throws IllegalArgumentException 值不在允许集（fail-fast 优于静默跑错通道）
     */
    private static String parseChoice(Properties cfg, String key, String defaultValue, String... allowed) {
        String value = cfg.getProperty(key, defaultValue).trim();
        for (String candidate : allowed) {
            if (candidate.equals(value)) {
                return value;
            }
        }
        throw new IllegalArgumentException(key + " 须为 " + String.join("|", allowed) + ": " + value);
    }

    /**
     * 非空文本配置：键缺席取默认；显式设置但空白抛 IAE（spec §4——显式空白属配置面
     * 错误，不得静默回落默认值跑错目录/二进制）。
     *
     * @param cfg          配置源
     * @param key          键
     * @param defaultValue 键缺席时的取值
     * @return 非空取值
     * @throws IllegalArgumentException 显式空白
     */
    private static String parseNonBlank(Properties cfg, String key, String defaultValue) {
        String value = cfg.getProperty(key, defaultValue);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " 显式设置时不得为空白（键缺席才有默认值 "
                    + defaultValue + "）");
        }
        return value.trim();
    }

    /**
     * 非空路径配置（{@link #parseNonBlank} 的路径面）。
     *
     * @param cfg          配置源
     * @param key          键
     * @param defaultValue 键缺席时的路径文本
     * @return 路径
     * @throws IllegalArgumentException 显式空白
     */
    private static Path parseNonBlankPath(Properties cfg, String key, String defaultValue) {
        return Path.of(parseNonBlank(cfg, key, defaultValue));
    }

    /**
     * 可选路径配置（离线回放 {@code vb.wal.offline.dir} 面）：键缺席取 null（在线档——
     * "未设"与"设了空串"语义分立）；显式设置但空白抛 IAE（沿 {@link #parseNonBlank} 的
     * 空白纪律——显式空白属配置面错误，不得静默当在线档跑）。
     *
     * @param cfg 配置源
     * @param key 键
     * @return 路径；键缺席为 null
     * @throws IllegalArgumentException 显式空白
     */
    private static Path parseOptionalPath(Properties cfg, String key) {
        String value = cfg.getProperty(key);
        if (value == null) {
            return null;
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException(key + " 显式设置时不得为空白（键缺席才表示在线档）");
        }
        return Path.of(value.trim());
    }

    /** 显式关闸（vb.wal.dml=false）；缺省 null = 由 listener 有无决定。 */
    private Boolean dmlOff;

    /**
     * 白名单文本解析：逗号切分 + 逐项 trim + 空项跳过（"public.a, public.b,," 形态）。
     *
     * @param text 配置原文（null 安全按空串）
     * @return 不可变白名单集（空 = 全放行）
     */
    private static Set<String> parseWhitelist(String text) {
        if (text == null || text.isBlank()) {
            return Set.of();
        }
        Set<String> out = new HashSet<>();
        for (String item : text.split(",")) {
            String t = item.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return Set.copyOf(out);
    }

    /**
     * DML 输出面是否装配（构造期裁定，三档统一语义——M2 T1 起 OG 档不再降级）：
     * listener 缺席（v1 单参构造）或 {@code vb.wal.dml=false} 时 false；否则 true——
     * OG 档与 PG 17/18 档同构装配（postApply/pendingFloor 接缝 + 惰性 ChangeStream），
     * M1 的 {@code ogCatalogOnly} WARN 降级位已退役（spec 2026-10-12 §1 变化①）；OG
     * 档 DML 面另有库级兼容模式护栏（{@link #startOpenGauss}——非 PG 档启动期 ISE）。
     *
     * @return true = start 后 sink 串接 ChangeStream
     */
    public boolean dmlEnabled() {
        return dmlOut != null && dmlOff == null;
    }

    /**
     * DML 白名单观测面（配置直读，Main/诊断用）。
     *
     * @return 白名单集（空 = 用户表全放行）
     */
    public Set<String> tables() {
        return tables;
    }

    /**
     * OG 档收流通道观测面（配置直读——file/direct，spec §7）。
     *
     * @return 通道名（缺省 file）
     */
    public String ogSource() {
        return ogSource;
    }

    /**
     * OG 档 sidecar 管理形态观测面（配置直读——managed/external）。
     *
     * @return 管理形态（缺省 managed）
     */
    public String ogRxMode() {
        return ogRxMode;
    }

    /**
     * OG 档段落盘目录观测面（配置直读）。
     *
     * @return 目录（缺省 wal-og-queue）
     */
    public Path ogRxDir() {
        return ogRxDir;
    }

    /**
     * OG 档 pg_receivexlog 可执行文件观测面（配置直读）。
     *
     * @return 二进制（缺省 pg_receivexlog）
     */
    public String ogRxBin() {
        return ogRxBin;
    }

    /**
     * SQL 会话驱动前缀观测面（配置直读——jdbc:postgresql: / jdbc:opengauss:）。
     *
     * @return 前缀（缺省 jdbc:postgresql:——PG 档零感知前提）
     */
    public String ogJdbcPrefix() {
        return ogJdbcPrefix;
    }

    /**
     * 离线回放段快照目录观测面（配置直读——{@code vb.wal.offline.dir}）。
     *
     * @return 目录；缺省（在线档）为 null
     */
    public Path offlineDir() {
        return offlineDir;
    }

    /**
     * 启动全管线：建 SQL 会话 → 版本分发布局 → 起同步器（续传/引导决策 + 接收线程 +
     * 检查点/槽推进生命周期）；DML 面启用时 sink 串接 {@link ChangeStream}（post-apply
     * 分发：先 catalog 施加后变更组装，as-of 次序保证）。
     *
     * <p>关键步骤：① 普通 JDBC URL（不带 replication 参数，前缀按
     * {@code vb.wal.og.jdbc.prefix} 分流——缺省 jdbc:postgresql: 与既有拼接相同）建会话
     * （此后归同步器独占——probe/槽推进复用；DML 面的 TOAST 回查 probe 同线程串行共享，
     * 单写者契约不破）；② {@code current_setting('server_version_num')}（文本数字，如
     * 180002）+ 9 万段补充的 {@code version()} 文本经 {@link WalLayouts#forServer} 分发
     * （17/18 区间返回与旧 {@code forServerVersion} 同实例——PG 档行为零变化），未注册
     * 版本 ISE fail-fast；<strong>9 万段且无 openGauss branding 时经 {@code
     * pg_class.relname} attlen 探测（vastbase T4，spec §4——128 分发 VastBase 档；
     * 探测在建槽之前，失败/64 落既有 vanilla 9.2 ISE 面消息附探测值，17/18 档
     * 零触碰）</strong>；<strong>判别命中 openGauss 档（instanceof 门控——VastBase
     * 子类同走）时转入 {@link #startOpenGauss}（M1 Task 6：槽 OG 变体 +
     * WalByteSource 双通道装配，与 PG 档互斥）</strong>；
     * ③ {@link CatalogSynchronizer#start} 带凭据 + state 配置（interest 0 个）+ 可选
     * post-apply 接缝——ChangeStream 惰性建（快照视图需同步器实例，而 sink 在
     * startInternal 内接线先于 start 返回；接收线程单写者初始化、volatile 发布）。
     * 边界与异常语义：已 start 再调抛 ISE、已 close 再调抛 ISE；SQLException（连不上/
     * 权限不足）原样上抛，已建资源由本类持有、调用方经 {@link #close()} 释放。</p>
     *
     * @throws SQLException SQL 会话建立、槽管理或版本查询失败
     */
    public synchronized void start() throws SQLException {
        if (closed) {
            throw new IllegalStateException("WalSource already closed");
        }
        if (started) {
            throw new IllegalStateException("WalSource already started");
        }
        sqlConnection = DriverManager.getConnection(sqlJdbcUrl(), user, pass);
        int versionNum = queryServerVersionNum(sqlConnection);
        // version() 文本只在 9 万段（openGauss 撞号区间）查询——判别 openGauss 的唯一面是
        // 版本文本（90204 与 vanilla 9.2 撞号，WalLayouts.forServer）；17/18 区间不触碰
        // 该查询（PG 档除分发调用形态外行为零变化）
        String versionText = versionNum >= 90_000 && versionNum < 100_000
                ? queryServerVersionText(sqlConnection) : null;
        // VastBase 判别第二面(spec §4):9 万段无 openGauss branding 时探测 pg_class.relname
        // 的 attlen——VastBase G100 的 version() 报 vanilla 形态无 branding,唯一稳定区分面是
        // NAMEDATALEN 加宽(128);64/不可得落 vanilla 9.2 拒绝面(真 vanilla 零扰动)。
        // 探测在建槽之前(ISE 先于槽副作用,M2 护栏纪律)。
        int relnameAttlen = -1;
        if (versionNum >= 90_000 && versionNum < 100_000
                && (versionText == null || !versionText.contains("openGauss"))) {
            relnameAttlen = queryRelnameAttlen(sqlConnection);
        }
        WalLayout layout = WalLayouts.forServer(versionNum, versionText, relnameAttlen);
        if (layout instanceof WalLayoutVOpenGauss) {   // 原实例相等改 instanceof——VastBase 子类同走 OG 装配
            startOpenGauss(layout);
            return;
        }
        Consumer<WalRecord> postApply = dmlEnabled() ? rec -> changeStreamOrInit(layout).onRecord(rec) : null;
        // 待决桶下界供应者（Task 11 挂起桶跨检查点修复）：检查点落盘/槽推进封顶/续传流起点
        // 回退三面经同步器消费；changeStream 惰性建（首条记录前为 null → 0，与"无待决桶"同面）
        LongSupplier pendingFloor = dmlEnabled() ? this::dmlPendingFloorLsn : null;
        sync = CatalogSynchronizer.start(sqlConnection, slotName, layout, user, pass, state, 0L,
                postApply, pendingFloor);
        started = true;
        LOG.info("WalSource 启动: server_version_num={} slot={} state={} dml={} tables={} 续传起点 {}",
                versionNum, slotName, state.enabled() ? state.dir() : "禁用", dmlEnabled(),
                tables.isEmpty() ? "全放行" : tables, Lsn.format(sync.consumedLsn()));
    }

    /**
     * 启动离线回放档（offline replay，2026-10-15 spec）：数据库不可访问时凭本地检查点 +
     * WAL 段快照目录离线续传——与 {@link #start()} 互斥（共用 started/closed 单程闸门，
     * 两路任一已走即 ISE）。
     *
     * <p>关键步骤：① {@code vb.wal.offline.dir} 缺失（null）启动期 ISE（消息附键名——
     * {@code vb.wal.mode=offline} 而目录未设是配置面残缺，spec §5）；② 委派
     * {@link CatalogSynchronizer#startOffline(StateConfig, Path, long, Set, ChangeOutputListener)}
     * ——不建 SQL 会话、不建槽（{@code sqlConnection} 本路径恒 null，{@link #close()} 的
     * null 安全面是既有现状）；DML 面按 {@link #dmlEnabled()} 裁定：false（无 listener
     * 或 {@code vb.wal.dml=false}）时 listener 传 null——纯 catalog 形态，true 时传
     * {@code dmlOut} 原值，装配后离线 ChangeStream 自同步器接进 {@code changeStream}
     * 字段（{@code dml*} 发射计数观测面离线同源，终审 F1）。
     * 边界与异常语义：state 未启用/检查点缺失损坏或档位不符/静态
     * 目录审计失败由同步器 ISE 原样上抛（已建资源由本类持有、调用方 close 释放——离线
     * 档失败面即启动失败，无半开会话）；读尽终态经 {@link #eofReached()} 观测，宿主据此
     * close 落最终检查点（同目录续写，离线/在线双向互通）。线程约束：装配线程单次调用
     * （synchronized 与 start 同锁）。</p>
     */
    public synchronized void startOffline() {
        if (closed) {
            throw new IllegalStateException("WalSource already closed");
        }
        if (started) {
            throw new IllegalStateException("WalSource already started");
        }
        if (offlineDir == null) {
            throw new IllegalStateException("离线回放档必须设置 " + KEY_OFFLINE_DIR
                    + "（vb.wal.mode=offline 而 offline.dir 未设是配置面残缺，spec §5）");
        }
        sync = CatalogSynchronizer.startOffline(state, offlineDir, offlineEndLsn,
                tables, dmlEnabled() ? dmlOut : null, renderOptions);
        // 终审 F1：把离线装配的 ChangeStream 接进 changeStream 字段——dml* 发射计数观测面
        // （含 Main 周期行 dml=[buckets=.. rows=..]）零新观测面即离线复用（spec §3.4）。
        // DML 禁用档 getter 回 null，字段保持 null 与"面未装配"同语义；装配线程写先于
        // started=true，Main 线程读 volatile 字段安全。
        this.changeStream = sync.offlineChangeStream();
        started = true;
        LOG.info("WalSource 启动（离线回放档）: dir={} endLsn={} state={} dml={} tables={} 续传起点 {}",
                offlineDir, offlineEndLsn > 0 ? Lsn.format(offlineEndLsn) : "无上界",
                state.dir(), dmlEnabled(), tables.isEmpty() ? "全放行" : tables,
                Lsn.format(sync.consumedLsn()));
    }

    /**
     * openGauss 5.0.0 档装配分支（M1 Task 6 落地，M2 Task 1 增 DML 面装配 + 兼容模式
     * 护栏，M3 Task 3 白名单放宽 {'PG'} → {'PG','A'}，M4 Task 6 白名单按 layout 分档）：
     * 判别命中 OG 布局后由
     * {@link #start()} 转入——SQL 会话已建（URL 前缀按 {@code vb.wal.og.jdbc.prefix}
     * 分流），此后委派 {@link CatalogSynchronizer#startOg}（槽 OG 变体 → state 决策
     * → bootstrap → 字节源装配）。
     *
     * <p>关键步骤：①<strong>兼容模式护栏（M2 Task 1，M3 Task 3 放宽，M4 Task 6 分档）</strong>
     * ——DML 面请求在场（{@link #dmlEnabled()}：listener + vb.wal.dml=true）时先探测库级
     * {@code pg_database.datcompatibility}（{@link #queryDbCompatMode}），可判定的
     * 白名单外档位启动期 ISE（附档位值与分档指引——白名单 {@link #dmlCompatModes}：
     * VastBase 档 {'PG','A','B'}[B=MySQL 风格，M4 开门] / OG 5.0.0 档 {'PG','A'}[B 档
     * 未验证]，'C' 两档均拒）；不可判定（无行/空白值/探测
     * SQLException）WARN 放行。护栏只在 DML 面请求时探测的依据：不兼容面是 DML 解码
     * 语义（三档分区表实现不同，spec §0——'A' 档原生分区已随 M3 pg_partition 第四
     * 字典捕获），catalog 同步面与兼容模式无关——M1 全部 OG IT 即跑在 'A' 档的
     * postgres 缺省库上全绿（og-plan 实测 datcompatibility 值：postgres='A'、
     * DBCOMPATIBILITY 'PG' 库='PG'）；显式 {@code -Dvb.wal.dml=false}
     * 等价声明 catalog-only 意图，不 ISE。②DML 面与 PG 17/18 路同构装配（M1 的
     * WARN 降级形态已废）：{@code dmlEnabled()} 时 postApply（惰性 ChangeStream 的
     * onRecord）与 pendingFloor（待决桶下界供应者）经 startOg 注入 sink——先施加
     * 字典后解码行，as-of 次序与 PG 档同源。③字节源按 {@code vb.wal.og.source} 由
     * {@link #buildOgByteSource} 构造（工厂形态——流起点由同步器的续传/引导决策给出，
     * 构造需在此之后）。边界与异常语义：SQLException/ISE（含护栏）原样上抛（已建资源
     * 由本类持有、调用方 close 释放），护栏先于槽副作用。线程约束：装配线程（start
     * 调用线程）单次执行。</p>
     *
     * @param layout 判别出的 openGauss 布局描述符（VastBase 子类同走本分支——护栏白名单按其分档）
     * @throws SQLException 槽管理/引导查询失败
     * @throws IllegalStateException DML 面请求在场且库级兼容模式可判定为白名单外档位
     *         （VastBase 档仅 'C' / OG 5.0.0 档 'B'/'C'，M4 Task 6 分档）
     */
    private void startOpenGauss(WalLayout layout) throws SQLException {
        if (dmlEnabled()) {
            String compat = queryDbCompatMode(sqlConnection);
            boolean vastbase = layout instanceof WalLayoutVVastbase;
            if (compat != null && !dmlCompatModes(layout).contains(compat)) {
                throw new IllegalStateException((vastbase ? "VastBase" : "openGauss") + " 档 DML 面仅支持 PG/A"
                        + (vastbase ? "/B" : "") + " 兼容模式库: 当前库 datcompatibility=" + compat
                        + (vastbase
                                ? "（'C' 为 PG 兼容历史别名未验证——请以 DBCOMPATIBILITY 'PG'/'A'/'B' 建库）"
                                : "（B 档=MySQL 风格支持属 VastBase 档 M4——OG 5.0.0 未验证，请以 DBCOMPATIBILITY 'PG' 或 'A' 建库）")
                        + "；仅需 catalog 同步面可 -Dvb.wal.dml=false 显式声明，catalog 面与兼容模式无关）");
            }
            if (compat == null) {
                LOG.warn("openGauss 库级兼容模式探测不可判定（无行/空值/探测失败）——护栏按放行处理"
                        + "（白名单外档位 ISE 仅对可判定档位生效）");
            }
        }
        Consumer<WalRecord> postApply = dmlEnabled() ? rec -> changeStreamOrInit(layout).onRecord(rec) : null;
        LongSupplier pendingFloor = dmlEnabled() ? this::dmlPendingFloorLsn : null;
        sync = CatalogSynchronizer.startOg(sqlConnection, slotName, layout, state, 0L,
                startLsn -> buildOgByteSource(layout, startLsn), postApply, pendingFloor);
        started = true;
        LOG.info("WalSource 启动（openGauss 档）: slot={} source={} mode={} dir={} bin={} jdbcPrefix={} "
                        + "state={} dml={} 续传起点 {}",
                slotName, ogSource, ogRxMode, ogRxDir, ogRxBin, ogJdbcPrefix,
                state.enabled() ? state.dir() : "禁用", dmlEnabled(), Lsn.format(sync.consumedLsn()));
    }

    /**
     * 按 {@code vb.wal.og.source} 构造 OG 档字节源（同步器决策出流起点后经工厂回调）。
     *
     * <p>关键步骤：direct 通道 = {@link DirectWalSource}（首连起点注入 startLsn，反馈
     * 心跳 10s——与接收器路 5s 反馈同量级）；file 通道 = {@link FileWalSource}（段尺寸
     * 16MB 生产档、续传起点 startLsn 段内页对齐由源内取整），managed 模式附带
     * {@link ReceivexlogProcess}（拉起 {@code vb.wal.og.rx.bin} 落盘 {@code rx.dir}，
     * 密码只经 PGPASSWORD 环境变量），external 模式纯 tail。边界与异常语义：构造期不建
     * 连接/不拉进程（source.start 由同步器调用）；sidecar 终态失败经源的
     * terminalFailure 联动暴露（FileWalSource Task 6 接线）。</p>
     *
     * @param layout   openGauss 布局描述符（页大小 = 续传段内偏移的页对齐基准）
     * @param startLsn 同步器决策出的流起点（续传 = stored lsn / 引导 = max(P₀, B)）
     * @return 已构造未启动的字节源
     */
    private WalByteSource buildOgByteSource(WalLayout layout, long startLsn) {
        if ("direct".equals(ogSource)) {
            return new DirectWalSource(host, port, user, pass, slotName,
                    OG_DIRECT_STATUS_INTERVAL_MS, startLsn);
        }
        if ("managed".equals(ogRxMode)) {
            ReceivexlogProcess process = new ReceivexlogProcess(ogRxBin, ogRxDir,
                    host, port, user, pass, slotName);
            return new FileWalSource(ogRxDir, FileWalSource.DEFAULT_SEGMENT_SIZE, startLsn,
                    layout.walBlockSize(), process);
        }
        return new FileWalSource(ogRxDir, FileWalSource.DEFAULT_SEGMENT_SIZE, startLsn,
                layout.walBlockSize());
    }

    /**
     * SQL 会话 URL（包可见供离线测试钉 PG 档零回归锚）：前缀按
     * {@code vb.wal.og.jdbc.prefix}（缺省 {@code jdbc:postgresql:}——与既有拼接逐字节
     * 相同），DriverManager 按前缀分流驱动（代码零分支）。
     *
     * @return 形如 {@code <prefix>//host:port/db}
     */
    String sqlJdbcUrl() {
        return ogJdbcPrefix + "//" + host + ":" + port + "/" + database;
    }

    /**
     * 惰性建/取变更流门面（接收线程单写者调用；volatile 字段发布——Main 线程的
     * {@link #dmlEmittedBuckets()} 读到非 null 引用即见安全构造）。
     *
     * <p>惰性的原因：ChangeStream 需要 {@code sync.snapshot()} 活视图，而 sink 接线
     * 发生在 {@code CatalogSynchronizer.start} 内部（先于 start 返回、sync 字段赋值）。
     * <b>Task 8 JMM 修复</b>：原注释"happens-before 经线程启动序传递"不成立——接收
     * 线程在 start 内部启动，赋值发生在其后，线程启动 HB 边覆盖不到；现 sync 已
     * volatile（可见性）+ {@link #awaitSync()} 有界自旋（时间窗——赋值与接收线程首条
     * postApply 之间只有 start 的返回路径，正常路径零自旋通过）。</p>
     *
     * @param layout start 解析的版本布局（闭包捕获，避免字段化）
     * @return 变更流门面
     */
    private ChangeStream changeStreamOrInit(WalLayout layout) {
        ChangeStream cs = changeStream;
        if (cs == null) {
            cs = new ChangeStream(awaitSync().snapshot(), sqlConnection, layout, tables, dmlOut,
                    renderOptions);
            changeStream = cs;
        }
        return cs;
    }

    /**
     * 等待 {@code sync} 字段就绪（接收线程调用）：volatile 读 + 有界自旋（上限 10s）。
     *
     * <p>时间窗语义：{@code CatalogSynchronizer.start} 在返回前已完成全部装配且
     * {@code sync = start(...)} 赋值紧随其后——接收线程即便立即收到首条记录，等待
     * 也只是微秒级；上限 10s 防御 start 返回与首条记录之间的极端调度延迟，超时抛
     * ISE（装配序破坏的 fail-fast 信号而非重试面）。</p>
     *
     * @return 已就绪的同步器
     * @throws IllegalStateException 期限内 sync 仍未赋值（start 装配序被破坏）
     */
    private CatalogSynchronizer awaitSync() {
        CatalogSynchronizer s = sync;
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (s == null) {
            if (System.nanoTime() - deadline > 0) {
                throw new IllegalStateException("WalSource.start 装配序破坏：接收线程等待 sync 字段超时");
            }
            Thread.onSpinWait();
            s = sync;
        }
        return s;
    }

    /**
     * DML 观测面：已发射事务桶数（Main 周期 smoke 行数据源；DML 面未装配/尚无记录为 0）。
     *
     * @return 会话累计发射桶数
     */
    public long dmlEmittedBuckets() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.emittedBuckets();
    }

    /**
     * DML 观测面：已发射行数（过滤后实付口径；DML 面未装配/尚无记录为 0）。
     *
     * @return 会话累计发射行数
     */
    public long dmlEmittedRows() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.emittedRows();
    }

    /**
     * DML 观测面：截断 UPDATE 行级跳过计数（liveness guard 观测面，Task 8——对拍 IT
     * 的回归哨兵；停机后读安全：close 含接收线程 join，happens-before 成立）。
     *
     * @return 会话累计跳过的截断 UPDATE 行数（DML 面未装配/尚无记录为 0）
     */
    public long dmlSkippedTruncatedRows() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.skippedTruncatedRows();
    }

    /**
     * DML 观测面：截断 UPDATE 重建成功计数（Task 8.5，与
     * {@link #dmlSkippedTruncatedRows()} 互补的观测面）。
     *
     * @return 会话累计重建的截断 UPDATE 行数（DML 面未装配/尚无记录为 0）
     */
    public long dmlReconstructedTruncatedRows() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.reconstructedTruncatedRows();
    }

    /**
     * DML 观测面：OG 档分区身份链断裂计数（M3 T2 fail-safe 观测——透传
     * {@link ChangeStream#partitionChainMisses()}；Main 周期 smoke 行
     * {@code partMiss=} 数据源）。
     *
     * <p>链断两形态（第四字典命中 'p' 行但 parentid 悬空 / DROP PARTITION 后迟到行）
     * 行照常跳过不砖流；vanilla 档分区链不存在恒 0。changeStream 惰性建（接收线程
     * 首条记录前 null → 0，与"无链断"同面）。</p>
     *
     * @return 会话累计链断跳过的分区行数（DML 面未装配/尚无记录为 0）
     */
    public long dmlPartitionChainMisses() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.partitionChainMisses();
    }

    /**
     * DML 观测面：待决桶重放下界（Task 11 挂起桶跨检查点修复——透传
     * {@link ChangeStream#pendingFloorLsn()}；检查点落盘/槽推进封顶/续传流起点回退的
     * 数据源，亦是诊断面："挂起 2PC 存活期间 floor 停滞推进"属预期形态）。
     *
     * <p>changeStream 惰性建（接收线程首条记录前 null → 0，与"无待决桶"同面）；
     * 检查点调用与 onRecord 同在 wal-receiver 线程或停机 join 后——串行一致。</p>
     *
     * @return 待决桶首记录 lsn 最小值；无待决桶（或 DML 面未起）为 0
     */
    public long dmlPendingFloorLsn() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.pendingFloorLsn();
    }

    /**
     * 已消费 LSN 前沿（透传 {@link CatalogSynchronizer#consumedLsn()}）。
     *
     * @return 下一个未消费字节的 LSN；未 start 时为 0
     */
    public long consumedLsn() {
        CatalogSynchronizer s = sync;
        return s == null ? 0L : s.consumedLsn();
    }

    /**
     * 静态目录读尽终态透传面（离线回放档）：清单读尽或 endLsn 上界到达后 true——EOF
     * 是确定性的，宿主（冒烟 Main 周期循环）据此 close 落最终检查点优雅退出而非等待
     * 增长。在线档恒 false（接收器路字节源为 null、OG file 通道的 tail 档源无读尽
     * 概念——{@link CatalogSynchronizer#eofReached()} 的既有裁定），行为零变化。
     *
     * @return 离线档读尽为 true；在线档/未读尽/未 start 为 false
     */
    public boolean eofReached() {
        CatalogSynchronizer s = sync;
        return s != null && s.eofReached();
    }

    /**
     * 接收计数器观测面（透传 {@link CatalogSynchronizer#streamMetrics()}）。
     *
     * <p>计数实例与接收器同寿——start 前调用抛 ISE（尚无接收线程产出可观测）；
     * 返回引用稳定，调用方轮询同一实例即可。</p>
     *
     * @return 计数器（records/resyncs/contrecords/orphanSkips/carryDrops/reconnects + census）
     * @throws IllegalStateException 尚未 start
     */
    public WalStreamMetrics metrics() {
        CatalogSynchronizer s = sync;
        if (s == null) {
            throw new IllegalStateException("metrics available only after start()");
        }
        return s.streamMetrics();
    }

    /**
     * 接收器终态失败透传面（终审 I2——接收器死亡静默修复）：接收线程因重连耗尽/解析
     * ISE 自行退出后返回根因；Main 周期行检测到终态即 ERROR + exit(1)。运行中/正常
     * 停机/未 start 为 empty。
     *
     * @return 终态根因；运行中或无终态失败为 empty
     */
    public Optional<Throwable> receiverTerminalFailure() {
        CatalogSynchronizer s = sync;
        return s == null ? Optional.empty() : s.terminalFailure();
    }

    /**
     * 最近一次成功落盘的检查点 lsn（透传 {@link CatalogSynchronizer#lastCheckpointLsn()}，
     * Main 周期行的 checkpoint 观测面）。
     *
     * @return 检查点 lsn；state 禁用或尚未落盘为 0
     */
    public long lastCheckpointLsn() {
        CatalogSynchronizer s = sync;
        return s == null ? 0L : s.lastCheckpointLsn();
    }

    /**
     * 最近一次槽推进成功的目标 lsn（透传 {@link CatalogSynchronizer#lastSlotAdvanceLsn()}，
     * Main 周期行的槽推进观测面）。
     *
     * @return 推进目标 lsn；未推过为 0
     */
    public long lastSlotAdvanceLsn() {
        CatalogSynchronizer s = sync;
        return s == null ? 0L : s.lastSlotAdvanceLsn();
    }

    /**
     * 本次 start 是否自检查点续传（透传 {@link CatalogSynchronizer#resumedFromState()}）。
     *
     * @return 续传为 true；state 禁用/引导路径为 false
     */
    public boolean resumedFromState() {
        CatalogSynchronizer s = sync;
        return s != null && s.resumedFromState();
    }

    /**
     * 检查点目录（配置面直读——Main 判定"有 stateDir 时"增打 checkpoint/槽推进观测）。
     *
     * @return 目录；state 禁用为 null
     */
    public Path stateDir() {
        return state.dir();
    }

    /**
     * 幂等停机：先停同步器（含接收线程 join + 最终 best-effort 检查点 + 槽推进），再关
     * SQL 会话。
     *
     * <p>关键步骤：closed 单检幂等（重复调用 no-op）；未 start 时调用合法（仅关可能存在的
     * 半开 SQL 会话——start 中途失败的清理路径）；关会话异常 WARN 吞掉（清理路径不掩盖
     * 主流程）。</p>
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (sync != null) {
            sync.stop();
        }
        if (sqlConnection != null) {
            try {
                sqlConnection.close();
            } catch (SQLException e) {
                LOG.warn("关闭 SQL 会话失败（忽略）: {}", e.getMessage());
            }
        }
        LOG.info("WalSource 已关闭: slot={} 消费前沿 {}（检查点 {} / 槽推进 {}）", slotName,
                Lsn.format(consumedLsn()), Lsn.format(lastCheckpointLsn()), Lsn.format(lastSlotAdvanceLsn()));
    }

    /**
     * DML 面兼容模式白名单（M2 Task 1 落地 {'PG'}，M3 Task 3 放宽 {'PG','A'}，M4
     * Task 6 起按 layout 分档）：VastBase 档 {'PG','A','B'}（B=MySQL 风格，M4 开门）；
     * OG 5.0.0 档 {'PG','A'} 不变（B 档 typemap 未验证，spec §0 裁定 a）；'C' 两档均拒
     * （PG 兼容历史别名未验证——5.0.0 仍接受建库且存储值与 'PG' 相异）。档位值域
     * og-plan 容器 2026-10-12 实测钉死：{@code CREATE DATABASE ... DBCOMPATIBILITY
     * 'PG'} 库的 {@code pg_database.datcompatibility} 列恰回 {@code "PG"} 单值；
     * 'A'=Oracle 风格（postgres 缺省库即此档，M3 起支持——A 档原生分区的 DML 捕获经
     * pg_partition 第四字典落地）；'B'=MySQL 风格（M4 起 VastBase 档支持）。
     *
     * @param layout 判别出的布局描述符（null 视为非 VastBase 档——旧单参调用面的 OG 语义）
     * @return 该 layout 的 DML 面兼容模式白名单
     */
    static Set<String> dmlCompatModes(WalLayout layout) {
        return layout instanceof WalLayoutVVastbase ? Set.of("PG", "A", "B") : Set.of("PG", "A");
    }

    /**
     * 查当前库的 openGauss 兼容模式档位（护栏的探测面，M2 Task 1）。
     *
     * <p>关键步骤：单行查询 {@code pg_database.datcompatibility}（探测 SQL 以 og-plan
     * 实测钉死——见 {@link #dmlCompatModes} javadoc 的档位值域）；无行、null 或
     * 空白值归一化为 null（不可判定面——OG 对各档的表示形态已实测，空/NULL 不是
     * PG 档表示）；SQLException 捕获后 WARN 并归 null（护栏是兼容性预检不是正确性
     * 锚，探测瞬断静默砖死启动不如放行可观测）。边界：连接须已建立（start 装配线程
     * 内调用）；返回值仅作护栏判定，不缓存。</p>
     *
     * @param connection 已建立的 SQL 会话
     * @return 档位值（如 "PG"/"A"/"B"/"C"）；不可判定为 null
     */
    @SuppressWarnings("SqlResolve")
    static String queryDbCompatMode(Connection connection) {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT datcompatibility FROM pg_database WHERE datname = current_database()")) {
            if (!rs.next()) {
                return null;
            }
            String mode = rs.getString(1);
            return mode == null || mode.isBlank() ? null : mode.trim();
        } catch (SQLException e) {
            LOG.warn("openGauss 兼容模式探测查询失败（归不可判定处理）: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 当前库是否 DML 面支持的兼容档（{@link #queryDbCompatMode} 的判定面，离线测试
     * 消费——护栏本体内联 queryDbCompatMode + dmlCompatModes 直接判定不经本方法；
     * M2 Task 1 落地，M3 Task 3 白名单放宽，M4 Task 6 起按 layout 分档）：
     * 白名单 {@link #dmlCompatModes}（VastBase 档 {'PG','A','B'} / 其余 {'PG','A'}）
     * 放行；可判定的白名单外档位（VastBase 档仅 'C' / OG 档 'B'/'C'）false；不可判定
     * （探测失败/空值）true——fail-open 语义，与 startOpenGauss 护栏分支一致。
     *
     * @param connection 已建立的 SQL 会话
     * @param layout     判别出的布局描述符；null 视为非 VastBase 档（旧调用面的 OG 语义）
     * @return true = 白名单档或不可判定（放行）；false = 可判定的白名单外档位
     */
    static boolean isDmlSupportedCompatMode(Connection connection, WalLayout layout) {
        String mode = queryDbCompatMode(connection);
        return mode == null || dmlCompatModes(layout).contains(mode);
    }

    /**
     * 单参 OG 档委托（M4 前旧调用面）：layout 缺省 null 走非 VastBase 白名单
     * {'PG','A'}——既有离线测试断言的语义不变。
     *
     * @param connection 已建立的 SQL 会话
     * @return true = OG 白名单档（'PG'/'A'）或不可判定（放行）；false = 可判定的白名单外档位
     */
    static boolean isDmlSupportedCompatMode(Connection connection) {
        return isDmlSupportedCompatMode(connection, null);
    }

    /**
     * 查服务端版本号（决定 WAL 布局描述符的分发输入）。
     *
     * <p>{@code current_setting('server_version_num')} 返回文本形数字（如 "180002"），
     * {@link Integer#parseInt} 直取——非法值（不期望出现）NumberFormatException 上抛交
     * 启动失败面。</p>
     *
     * @param connection 已建立的 SQL 会话
     * @return 版本号（如 180002）
     * @throws SQLException 查询失败
     */
    private static int queryServerVersionNum(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_setting('server_version_num')")) {
            rs.next();
            return Integer.parseInt(rs.getString(1));
        }
    }

    /**
     * 查服务端版本文本（openGauss 判别的唯一面——90204 与 vanilla 9.2 撞号，仅版本文本
     * 含 "openGauss" 可区分，见 {@link WalLayouts#forServer}）。
     *
     * <p>只在 9 万段版本号时调用（PG 17/18 档零额外查询）；空值/null 不在此判——分发器
     * 对判别信息缺失走拒绝面。</p>
     *
     * @param connection 已建立的 SQL 会话
     * @return {@code version()} 文本（如 "(openGauss 5.0.0 build ...) compiled at ..."）
     * @throws SQLException 查询失败
     */
    private static String queryServerVersionText(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT version()")) {
            rs.next();
            return rs.getString(1);
        }
    }

    /**
     * 探测 {@code pg_class.relname} 的 attlen（VastBase 判别第二面的执行面，vastbase T4）。
     *
     * <p>关键步骤：{@link #RELNAME_PROBE_SQL} 单行查询经 {@link #parseRelnameAttlen}
     * 解析；无行或 SQLException 归 -1（不可得）记 DEBUG 不中断——探测失败落既有
     * vanilla 9.2 ISE 拒绝面（三参重载消息附探测值可诊断），静默砖死启动不如拒绝面
     * 可观测（探测是判别预检不是正确性锚，沿兼容模式护栏的 fail 语义收敛）。边界：
     * 连接须已建立（start 装配线程内调用、建槽之前——ISE 先于槽副作用）；返回值
     * 不缓存（单次探测，9 万段无 branding 才触达）。线程约束：静态，装配线程单次。</p>
     *
     * @param connection 已建立的 SQL 会话
     * @return attlen 探测值（如 128/64）；不可得为 -1
     */
    private static int queryRelnameAttlen(Connection connection) {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(RELNAME_PROBE_SQL)) {
            return parseRelnameAttlen(rs).orElse(-1);
        } catch (SQLException e) {
            LOG.debug("VastBase relname attlen 探测失败（归 -1 不可得——落 vanilla 9.2 拒绝面）: {}",
                    e.getMessage());
            return -1;
        }
    }

    /**
     * 解析探测结果集（{@link #queryRelnameAttlen} 的纯函数面，包可见供离线测试锚
     * 128/64/无行三分支——{@code VastProbeDiscriminatorTest}）。
     *
     * <p>关键步骤：无行（next false）回 empty（不可得）；有行取首列 int。边界与
     * 异常语义：SQLException 原样上抛由调用面归 -1；多行不期望出现（pg_class 的
     * relname 列唯一），首行即答案；attlen 为 NULL 的形态不存在于该列（int2 非空），
     * 不做 wasNull 分支。线程约束：静态纯函数。</p>
     *
     * @param rs 探测查询结果集
     * @return attlen；无行为 empty
     * @throws SQLException 游标推进/取值失败
     */
    static OptionalInt parseRelnameAttlen(ResultSet rs) throws SQLException {
        if (!rs.next()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(rs.getInt(1));
    }
}
