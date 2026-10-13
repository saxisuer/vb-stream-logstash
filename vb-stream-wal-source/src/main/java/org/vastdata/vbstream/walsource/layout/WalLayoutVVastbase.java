package org.vastdata.vbstream.walsource.layout;

/**
 * VastBase G100 V3.0（基于 openGauss 6.0.0 内核）第四布局档——<b>骨架</b>
 * （vastbase 适配 Task 2 预埋类声明与档位身份；完整词典几何与读区偏移归 Task 3
 * 落地，届时替换本骨架的覆写面）。
 *
 * <p><b>定位</b>：记录层与 OG 5.0.0 全同构（页魔数/记录头/heap 尺寸/块引用——
 * 132019 条真实 WAL 记录零 resync 实证，vastbase spec §0.2），差异集中在 catalog
 * 词典几何：① NAMEDATALEN 64→128（name 类型全 catalog 加宽，读区偏移统一 +64B，
 * 经 {@link WalLayout#nameDatumWidth()} 供数——骨架期暂承接口缺省 64，Task 3 覆写
 * 128）；② pg_class 42 列（OG 40）；③ pg_attribute 29 列（OG 24）——列序/attlen
 * 锚定 2026-10-13 真库实查（spec 附录 A）。继承 {@link WalLayoutVOpenGauss} 取
 * OG 侧缺省（{@code openGaussDialect()=true}、9.2 式元组头 oid、记录层全部常量），
 * 本骨架仅覆写档位身份两方法（{@link #supports(int)} / {@link #majorVersion()}）。
 * 判别经 relname attlen 探测（{@code WalLayouts} 三参重载，Task 4 落地前本档不经
 * 分发器触达，{@link #INSTANCE} 仅由 nsp/partition 词典的版本感知分派
 * {@code instanceof} 消费）。majorVersion=6 使 v5 检查点在本档拒载回落全新引导。</p>
 *
 * <p>线程约束：无状态单例，并发安全。</p>
 */
public final class WalLayoutVVastbase extends WalLayoutVOpenGauss {

    /** 唯一实例：零状态描述符，无并发约束。 */
    public static final WalLayoutVVastbase INSTANCE = new WalLayoutVVastbase();

    /** 私有构造器：单例描述符，经 {@link #INSTANCE} 访问。 */
    private WalLayoutVVastbase() {
    }

    /**
     * {@inheritDoc}——90204（VastBase 与 OG/vanilla 9.2 撞号；档位判别在
     * {@code WalLayouts} 三参重载的 relname attlen 探测，Task 4 落地前不经
     * 分发器触达——返回值仅声明档位覆盖区间，不构成分发入口）。
     */
    @Override
    public boolean supports(int pgVersionNum) {
        return pgVersionNum == 90204;
    }

    /** {@inheritDoc}——6（档号延续：OG=5；v5 检查点在本档拒载回落全新引导）。 */
    @Override
    public int majorVersion() {
        return 6;
    }
}
