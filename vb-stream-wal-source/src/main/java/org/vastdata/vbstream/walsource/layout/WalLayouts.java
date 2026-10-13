package org.vastdata.vbstream.walsource.layout;

/**
 * WAL 布局描述符的版本分发点：按连接端 {@code server_version_num}（如
 * {@code SELECT current_setting('server_version_num')}）选出对应的
 * {@link WalLayout} 实现。布局随 PG 大版本漂移，错配的常量会让页遍历/
 * 记录解析整体错位——因此对未知或未注册的大版本一律启动期
 * {@link IllegalStateException} fail-fast，绝不静默回退到相近版本。
 */
public final class WalLayouts {

    /** 私有构造器：静态分发点，不可实例。 */
    private WalLayouts() {
    }

    /**
     * 按 server_version_num 分发布局描述符。
     *
     * <p>关键步骤：大版本全区间匹配（小版本与发行版位不影响布局）——
     * 180000-189999 返回 V18、170000-179999 返回 V17（Task 16 注册）。边界与
     * 异常语义：区间外（如 160000 及以下、190000 及以上）均抛 ISE（消息携带
     * 版本号，便于排障），无 null 返回——跨版本近似回退被结构性排除。</p>
     *
     * @param pgVersionNum 服务端版本号（如 180000）
     * @return 覆盖该版本的布局描述符（线程安全单例）
     * @throws IllegalStateException 版本未注册或不受支持
     */
    public static WalLayout forServerVersion(int pgVersionNum) {
        if (pgVersionNum >= 180000 && pgVersionNum < 190000) {
            return WalLayoutV18.INSTANCE;
        }
        if (pgVersionNum >= 170000 && pgVersionNum < 180000) {
            return WalLayoutV17.INSTANCE;
        }
        throw new IllegalStateException("unsupported server version " + pgVersionNum);
    }

    /**
     * 带版本文本的分发（openGauss 判别）。
     *
     * <p>关键步骤：先按既有大版本区间路由 V18/V17（与 {@link #forServerVersion(int)}
     * 同判据，带文本不改变这两档行为）；9 万段（90000-99999）进入 openGauss 判别——
     * openGauss 全系版本报 90204（与 vanilla 9.2 撞号），唯一区分面是 version() 文本
     * （实测 5.0.0 形如 "(openGauss 5.0.0 build ...) compiled at ..."）。边界与异常
     * 语义：90204 且文本含 "openGauss 5.0." 返回 OG 档；含 "openGauss" 但非 5.0.x
     * 抛 ISE（消息附版本文本）；无 openGauss 标记＝vanilla 9.2 抛 ISE 指明非目标
     * （无复制槽/无 flush LSN 函数/pgjdbc 拒 replication 参数，四重硬阻塞，
     * 见 docs/wal-source-opengauss-adaptation-research.md）；versionText 为 null 的
     * 9 万段同落拒绝面（判别信息缺失不得静默猜测）。其余版本号 ISE 附版本号与文本。
     * 线程约束：静态纯函数，并发安全。vastbase T4 起委派三参重载
     * {@link #forServer(int, String, int)}（探测值传 -1——无探测面时 VastBase 分支
     * 不可达，分发语义与旧实现一致(仅 vanilla 9.2 拒绝消息文案附探测值后缀)，离线测试锚不破）。</p>
     *
     * @param pgVersionNum 服务端版本号（如 90204/180000）
     * @param versionText  服务端 {@code version()} 返回文本（判别 openGauss 的唯一面）
     * @return 覆盖该版本的布局描述符（线程安全单例）
     * @throws IllegalStateException 版本未注册、vanilla 9.2 非目标或 openGauss 版本不在支持面
     */
    public static WalLayout forServer(int pgVersionNum, String versionText) {
        return forServer(pgVersionNum, versionText, -1);
    }

    /**
     * 带版本文本与 name 宽度探测的分发(VastBase 判别)。
     *
     * <p>关键步骤:与两参版同判据处理 17/18 区间与 openGauss branding;9 万段无
     * openGauss 字样时按 pgClassRelnameAttlen 分支——128 返回 VastBase 档
     * (VastBase G100 的 version() 报 vanilla 形态,branding 只在 v$version,唯一
     * 稳定区分面是 NAMEDATALEN 探测,spec §4);64/其他/不可得(-1)落 vanilla 9.2
     * ISE(消息附探测值)。边界与异常语义:探测值不影响 branding 命中路径(优先)。
     * 线程约束:静态纯函数。</p>
     *
     * @param pgVersionNum 服务端版本号（如 90204/180000）
     * @param versionText  服务端 {@code version()} 返回文本（判别 openGauss 的唯一面）
     * @param pgClassRelnameAttlen {@code pg_class.relname} 的 attlen 探测值
     *                             （128 = VastBase NAMEDATALEN 加宽；-1 = 探测不可得）
     * @return 覆盖该版本的布局描述符（线程安全单例）
     * @throws IllegalStateException 版本未注册、vanilla 9.2 非目标或 openGauss 版本不在支持面
     */
    public static WalLayout forServer(int pgVersionNum, String versionText, int pgClassRelnameAttlen) {
        if (pgVersionNum >= 180000 && pgVersionNum < 190000) {
            return WalLayoutV18.INSTANCE;
        }
        if (pgVersionNum >= 170000 && pgVersionNum < 180000) {
            return WalLayoutV17.INSTANCE;
        }
        if (pgVersionNum / 10000 == 9) {
            if (versionText != null && versionText.contains("openGauss")) {
                if (versionText.contains("openGauss 5.0.")) {
                    return WalLayoutVOpenGauss.INSTANCE;
                }
                throw new IllegalStateException("openGauss 版本不在支持面(当前仅 5.0.x): " + versionText);
            }
            if (pgClassRelnameAttlen == 128) {
                return WalLayoutVVastbase.INSTANCE;
            }
            throw new IllegalStateException("vanilla PG 9.2 非目标版本(无复制槽/无逻辑日志,见 "
                    + "docs/wal-source-opengauss-adaptation-research.md;VastBase 探测 relname attlen="
                    + pgClassRelnameAttlen + "): " + versionText);
        }
        throw new IllegalStateException("unsupported server version " + pgVersionNum + ": " + versionText);
    }
}
