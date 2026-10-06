package org.vastdata.vbstream.walsource.state;

import org.vastdata.vbstream.walsource.replay.CatalogRow.AttrRow;
import org.vastdata.vbstream.walsource.replay.CatalogRow.ClassRow;
import org.vastdata.vbstream.walsource.replay.CatalogRow.NspRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.util.Map;
import java.util.Set;

/**
 * {@link StateStore#load()} 的产物——一次成功检查点解出的完整可重建态：attr/class/
 * nsp 三行字典（ctid 键控）、三 raw tail 存储、tracked 双 ctid、pg_attribute/
 * pg_class/pg_namespace 三 relfilenode、interest 与 stale 两 oid 集，加检查点 LSN
 * （v2 扩链：nsp 行字典 + nsp tail + pgNspRelnode，formatVersion bump 1→2）。
 *
 * <p>控制器裁定：持久化面 = 完整 CatalogStores 可重建态<strong>除 metrics 外全部
 * 字段</strong>（任务书基础 record 的 attr/class 表 + tracked 双 ctid 之上，扩展
 * 三 tail 存储、三 relfilenode 与 interest/stale 两集）；行字典以 ctid 为键，故
 * 组件类型为 ctid 键控 map 而非任务书的裸行列表——键不落盘则回填后键面断裂，
 * 灌回契约（{@link #restoreInto}）无法成立。边界与线程约束：容器为 load 方新建、
 * 归调用方所有（不做防御拷贝）；byte[] tail 值为引用共享，灌回后调用方不得再改。
 * record 的 equals 对 byte[] 值是恒不等（数组一性），逐值比较由调用方自理。</p>
 *
 * @param lsn                 检查点 LSN（已施加前沿——重放起点语义由 Task 15 接线）
 * @param attrs               pg_attribute 行字典（ctid 键 → 行模型）
 * @param classes             pg_class 行字典（ctid 键 → 行模型，v2 行含 relkind）
 * @param nspRows             pg_namespace 行字典（ctid 键 → 行模型，v2 扩链）
 * @param rawAttrTails        pg_attribute raw tail 存储（ctid 键 → tuple offset 23 起字节）
 * @param rawClassTails       pg_class raw tail 存储（语义同 rawAttrTails）
 * @param rawNspTails         pg_namespace raw tail 存储（语义同 rawAttrTails，v2 扩链）
 * @param trackedTableCtid    用户表在 pg_class 中的行位（0 = 无）
 * @param trackedToastCtid    toast 关系行位（0 = 无）
 * @param pgAttrRelfilenode   pg_attribute relfilenode（块匹配面）
 * @param pgClassRelfilenode  pg_class relfilenode
 * @param pgNspRelnode        pg_namespace relfilenode（v2 扩链）
 * @param interestRelOids     兴趣关系 oid 集
 * @param staleOids           自愈失败 stale oid 集
 */
public record StoredState(long lsn,
                          Map<Long, AttrRow> attrs,
                          Map<Long, ClassRow> classes,
                          Map<Long, NspRow> nspRows,
                          Map<Long, byte[]> rawAttrTails,
                          Map<Long, byte[]> rawClassTails,
                          Map<Long, byte[]> rawNspTails,
                          long trackedTableCtid,
                          long trackedToastCtid,
                          long pgAttrRelfilenode,
                          long pgClassRelfilenode,
                          long pgNspRelnode,
                          Set<Long> interestRelOids,
                          Set<Long> staleOids) {

    /**
     * 把本状态灌回目标 stores（Task 15 续传路径）：先清空六 map 与 interest/stale
     * 两集（确定性重建——目标 stores 既有内容不保留），再逐面回填行字典/tail 存储/
     * tracked 双 ctid/三 relfilenode/两 oid 集。
     *
     * <p>边界与异常语义：metrics 不在持久化面、保持目标 stores 原值（重建会话的
     * 计数从 0 起）；tail 的 byte[] 为引用共享（load 产物归调用方独享，灌回后两处
     * 指向同数组——stores 后续 splice 只读不改写既有 tail 内容）。线程约束：意图上
     * 在停流/装配期单线程调用；灌回瞬间跨线程读目标 stores 为弱一致。</p>
     *
     * @param stores 灌回目标（既有六 map/两集内容被清除替换）
     */
    public void restoreInto(CatalogStores stores) {
        stores.attrRows().clear();
        stores.attrRows().putAll(attrs);
        stores.classRows().clear();
        stores.classRows().putAll(classes);
        stores.nspRows().clear();
        stores.nspRows().putAll(nspRows);
        stores.rawAttrTails().clear();
        stores.rawAttrTails().putAll(rawAttrTails);
        stores.rawClassTails().clear();
        stores.rawClassTails().putAll(rawClassTails);
        stores.rawNspTails().clear();
        stores.rawNspTails().putAll(rawNspTails);
        stores.trackedTableCtid(trackedTableCtid);
        stores.trackedToastCtid(trackedToastCtid);
        stores.pgAttrRelfilenode(pgAttrRelfilenode);
        stores.pgClassRelfilenode(pgClassRelfilenode);
        stores.pgNspRelnode(pgNspRelnode);
        stores.interestRelOids().clear();
        stores.interestRelOids().addAll(interestRelOids);
        stores.staleOids().clear();
        stores.staleOids().addAll(staleOids);
    }
}
