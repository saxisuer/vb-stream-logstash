package org.vastdata.vbstream.walsource.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 截断自愈校验器（spec §6②）——未知 oldCtid 的截断更新按候选行重建后，以
 * <strong>中段新值对 JDBC 末态</strong>校验决定采纳与否。
 *
 * <p>鉴别力来源（spike 发现 23a）：重建行的 OID 列来自候选前缀（值编码），拿它对
 * 候选 oid 自证恒真无鉴别力——已废；真正的对照面是记录<strong>中段</strong>携带的
 * relfilenode/reltoastrelid 新值（候选无关、直录自 WAL）与 {@link JdbcProbe} 末态
 * 行的逐项比较，叠加"末态 ctid == 记录 new 位"的行位现势性检查。</p>
 *
 * <p>失败语义：校验判否一律返回 {@link Optional#empty()} <strong>不抛异常</strong>
 * （连续失败不升级），按 oid 维护连续失败计数——第二次连续失败起调用方可据
 * {@link #repeatedFailure(long)} 标 staleOids 待下轮引导；采纳将计数清零。</p>
 *
 * <p>线程约束：连续失败计数为普通 HashMap——按重放线程单写者假设（与
 * {@link CatalogReplay}/{@link CatalogStores} 同缝），不得跨线程并发调用。</p>
 */
public final class SelfHealer {

    private static final Logger LOG = LoggerFactory.getLogger(SelfHealer.class);

    private final JdbcProbe probe;

    /** 每 oid 连续校验失败计数（采纳清零；第二次起达 stale 门槛）。 */
    private final Map<Long, Integer> consecutiveRejections = new HashMap<>();

    /**
     * 构造校验器。
     *
     * @param probe pg_class 末态探测接缝（null = 禁用档：validate 恒 empty，
     *              调用方 {@link CatalogReplay} 保留 skip + 计数行为）
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
     * 校验一个候选重建产物（spec §6② 三级规则）：
     * <ol>
     *   <li>probe 查 candidateOf.relOid() 的末态行——行不存在（关系已删）拒绝；</li>
     *   <li>末态行 ctid != newCtid 拒绝（该行其后又动过，记录形态已过时——重建值
     *       即使吻合也只是历史快照，不得用于修 tracked）；</li>
     *   <li>中段新值对末态校验：重建行（candidate.row()）的 relfilenode/reltoastrelid
     *       与末态行逐项比较——<strong>至少一项相等且该项非零</strong>才采纳（双零
     *       无鉴别力，防假阳性；两项都等最优），都不等拒绝。</li>
     * </ol>
     * 采纳返回候选本身（调用方据此返回事件、修 tracked、打 WARN）；每次判否按 oid
     * 递增连续失败计数并 WARN 一行，连续失败不升级异常。
     *
     * <p>边界与异常语义：probe 为 null（禁用档）直接 empty；候选行投影/数值错配由
     * {@link CatalogRow.ClassRow#fromDecoded(Object[])} 裸抛（上游词典与重建行同源，
     * 自洽前提）。线程约束：单写者。</p>
     *
     * @param candidate  候选重建产物（tail + 解码值行——值行的中段列是校验面）
     * @param candidateOf 候选旧行值模型（oid 即探测键；prefix 来源）
     * @param newCtid    记录新行物理 ctid 键（末态行位对照面）
     * @param rec        源 WAL 记录（日志定位用）
     * @return 采纳时候选产物；拒绝 empty
     */
    public Optional<Reconstruction> validate(Reconstruction candidate, CatalogRow.ClassRow candidateOf,
            long newCtid, WalRecord rec) {
        if (probe == null) {
            return Optional.empty();
        }
        long oid = candidateOf.relOid();
        JdbcProbe.ProbedRow end = probe.currentClassRow(oid);
        if (end == null) {
            return reject(oid, "末态行不存在（关系已删）", rec);
        }
        if (end.ctidKey() != newCtid) {
            return reject(oid, "末态 ctid=" + end.ctidKey() + " != 记录 new 位 " + newCtid
                    + "（该行其后又动过，记录形态已过时）", rec);
        }
        CatalogRow.ClassRow mid = CatalogRow.ClassRow.fromDecoded(candidate.row());
        CatalogRow.ClassRow endRow = end.row();
        boolean filenodeMatch = mid.relfilenode() != 0 && mid.relfilenode() == endRow.relfilenode();
        boolean toastMatch = mid.reltoastrelid() != 0 && mid.reltoastrelid() == endRow.reltoastrelid();
        if (filenodeMatch || toastMatch) {
            consecutiveRejections.remove(oid);    // 采纳清零连续失败计数
            LOG.debug("截断自愈校验采纳: oid={} lsn={} 中段 relfilenode {}/{} reltoastrelid {}/{}",
                    oid, Lsn.format(rec.lsn()), mid.relfilenode(), endRow.relfilenode(),
                    mid.reltoastrelid(), endRow.reltoastrelid());
            return Optional.of(candidate);
        }
        return reject(oid, "中段新值与末态不一致: relfilenode " + mid.relfilenode() + "/"
                + endRow.relfilenode() + " reltoastrelid " + mid.reltoastrelid() + "/" + endRow.reltoastrelid(), rec);
    }

    /**
     * 指定 oid 是否处于"连续失败达 stale 门槛"状态——本 oid 已连续判否 ≥ 2 次；
     * 调用方（CatalogReplay）在 validate 返回 empty 后查询，为真则向
     * {@link CatalogStores#staleOids()} 登记（Set 去重）待下轮引导。
     *
     * @param relOid 关系 oid
     * @return true = 连续失败 ≥ 2 次
     */
    public boolean repeatedFailure(long relOid) {
        return consecutiveRejections.getOrDefault(relOid, 0) >= 2;
    }

    /**
     * 判否记账：连续失败计数 +1 并 WARN 一行（含原因与次数——第二次起即为 stale
     * 门槛，日志面可直接观测）；恒返回 {@link Optional#empty()} 供调用点单式返回。
     *
     * @param oid 候选关系 oid
     * @param why 拒绝原因（日志面）
     * @param rec 源记录（LSN 定位）
     * @return 恒 empty
     */
    private Optional<Reconstruction> reject(long oid, String why, WalRecord rec) {
        int times = consecutiveRejections.merge(oid, 1, Integer::sum);
        LOG.warn("截断自愈校验失败: oid={} 第 {} 次连续失败 lsn={} 原因: {}", oid, times, Lsn.format(rec.lsn()), why);
        return Optional.empty();
    }
}
