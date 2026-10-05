package org.vastdata.vbstream.walsource.replay;

/**
 * 截断自愈探测面（spec §6② 的 Task 13 收敛形态）——未知 oldCtid 的截断更新经
 * <strong>ctid 寻址精确采纳</strong>恢复：按记录 new 位探测目录末态行，行仍居该位
 * 即为本记录施加后的精确状态（更新移位、INPLACE 不移位但重放收敛），整行采纳。
 *
 * <p><strong>对 Task 12"候选枚举 + 中段新值对末态校验"形态的裁定（对抗性 IT 实证）</strong>：
 * 候选前缀源是断链窗口内的历史快照，与记录中段拼装会产出时代错位的混合行（实测：
 * pre-RENAME 的 relname 混入 toast 关系的 relfilenode）；且"末态 ctid == new 位"可被
 * 恰巧途经该位的他关系行满足——中段值校验的鉴别力在高频迁移风暴下不足，已废。本类
 * 收敛为薄探测透传（null-probe = 禁用档恒 null，真禁用）。</p>
 *
 * <p>线程约束：按重放线程单写者假设（与 {@link CatalogReplay}/{@link CatalogStores}
 * 同缝），不得跨线程并发调用。</p>
 */
public final class SelfHealer {

    private final JdbcProbe probe;

    /**
     * 构造探测面。
     *
     * @param probe 目录末态探测接缝（null = 禁用档：两探测恒 null，调用方
     *              {@link CatalogReplay} 保留 skip + 计数行为）
     */
    public SelfHealer(JdbcProbe probe) {
        this.probe = probe;
    }

    /**
     * 是否处于启用态（probe 已注入）。
     *
     * @return true = 可用
     */
    public boolean enabled() {
        return probe != null;
    }

    /**
     * 按物理 ctid 探测 pg_class 末态行（透传 probe）——class 面<strong>精确采纳</strong>
     * 面（Task 13）：更新必移行位，"末态仍居记录 new 位"的行即该记录施加后的精确
     * 状态，整行采纳（行值全部来自探测行自身，无拼装时序问题）。
     *
     * <p>边界与异常语义：禁用档（probe null）恒 null；该位无行 null（行已再迁移，
     * 记录形态过时——采纳拒绝，调用方走 skip 计数）；基础设施失败由实现方抛 ISE
     * （fail-fast）。线程约束：单写者（与重放同缝）。</p>
     *
     * @param ctidText ctid 文本形态（"(block,off)"）
     * @return 末态行（ctid 键 + 行模型）；禁用档/该位无行 null
     */
    public JdbcProbe.ProbedRow probeClassByCtid(String ctidText) {
        return probe == null ? null : probe.currentClassRowByCtid(ctidText);
    }

    /**
     * 按物理 ctid 探测 pg_attribute 末态行（透传 probe）——attr 面<strong>精确采纳</strong>
     * 面（Task 13）：语义同 {@link #probeClassByCtid}（更新必移行位，末态仍居记录
     * new 位的行即精确状态）。
     *
     * <p>边界与异常语义：禁用档恒 null；该位无行 null（采纳拒绝）；基础设施失败由
     * 实现方抛 ISE。线程约束：单写者（与重放同缝）。</p>
     *
     * @param ctidText ctid 文本形态（"(block,off)"）
     * @return 末态行值模型；禁用档/该位无行 null
     */
    public CatalogRow.AttrRow probeAttrByCtid(String ctidText) {
        return probe == null ? null : probe.currentAttrRowByCtid(ctidText);
    }
}
