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
     * 180000-189999 返回 V18；170000-179999 属 PG 17 区间但 V17 描述符
     * 尚未注册（Task 16 落地），当前同未知版本处理。边界与异常语义：区间外
     * 或未注册均抛 ISE（消息携带版本号，便于排障），无 null 返回。</p>
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
            // PG 17 布局描述符由后续任务注册；注册前显式拒绝而非近似回退
            throw new IllegalStateException(
                    "unsupported server version " + pgVersionNum + ": PG 17 layout not yet registered");
        }
        throw new IllegalStateException("unsupported server version " + pgVersionNum);
    }
}
