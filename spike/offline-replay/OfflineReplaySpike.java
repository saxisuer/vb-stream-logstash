import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.changes.ChangeStream;
import org.vastdata.vbstream.walsource.changes.OutputRenderer;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.FileWalSource;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.receive.WalStreamWalker;
import org.vastdata.vbstream.walsource.replay.CatalogReplay;
import org.vastdata.vbstream.walsource.replay.CatalogRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;
import org.vastdata.vbstream.walsource.state.StateStore;
import org.vastdata.vbstream.walsource.state.StoredState;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 离线 WAL 重放 spike harness（throwaway——验证"库死亡后：本地检查点 + 拷出的 WAL 段
 * 目录 → 无库续传解析 DML"链路）。绕过 WalSource/CatalogSynchronizer 门面（它们对活库
 * 有硬依赖：SQL 会话/版本判别/ensureSlot），直接手工装配全部公开组件：
 *
 * StateStore.load（检查点恢复）→ StoredState.restoreInto（字典灌回）→
 * CatalogReplay（healer=null 纯回放档）+ appliedLsn 过滤线（=stored lsn 严格档）→
 * ChangeStream（sqlConn=null probe 禁用档）→ FileWalSource（external 4 参构造，纯 tail
 * 目录）→ WalStreamWalker → EOF（前沿 15s 停滞判读尽，库死文件不再生长）→
 * 最终检查点落盘（验证离线推进检查点、后续可衔接在线）。
 *
 * 布局档从检查点恢复是正式特性的改造点（StoredState 不带 pgVersion，header 校验面）；
 * 原型按本 spike 场景硬编码 WalLayoutVOpenGauss（OG=5 检查点）。
 *
 * args: <walDir> <stateDir> <table1,table2...>
 */
public class OfflineReplaySpike {

    private static final long STALL_TIMEOUT_MS = 15_000;
    private static final long HARD_TIMEOUT_MS = 300_000;

    public static void main(String[] args) throws Exception {
        Path walDir = Path.of(args[0]);
        Path stateDir = Path.of(args[1]);
        Set<String> tables = Set.of(args[2].split(","));

        // ① 检查点恢复（离线模式的前提：必须有一份有效检查点——失效回落是引导，而引导需要活库）
        StateStore store = new StateStore(stateDir, 5);   // OG majorVersion=5
        StoredState st = store.load().orElseThrow(() ->
                new IllegalStateException("检查点缺失/损坏——离线模式无法引导（需要活库）"));
        long storedLsn = st.lsn();
        long storedFloor = st.dmlFloorLsn();
        System.out.println("[spike] 检查点恢复: lsn=" + Lsn.format(storedLsn)
                + " floor=" + Lsn.format(storedFloor)
                + " attrRows=" + st.attrs().size() + " classRows=" + st.classes().size());

        // ② 字典灌回 + 组件装配（全部公开 API，零门面）
        CatalogStores stores = new CatalogStores();
        st.restoreInto(stores);
        // 布局经工厂分发（OG 档单例）——正式特性的"从检查点 pgVersion 恢复档位"改造点
        // 应同样收敛到本工厂面（pgVersion 5 → 90204+openGauss 文本）
        WalLayout layout = WalLayouts.forServer(90204, "(openGauss 5.0.0 spike offline)", -1);
        AtomicLong appliedLsn = new AtomicLong(storedLsn);   // 续传严格过滤线（非页对齐）
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout));   // healer=null 档
        AtomicLong floor = new AtomicLong(storedFloor);
        ChangeStream cs = new ChangeStream(new SpikeSnapshot(stores, appliedLsn),
                null, layout, tables, new OutputRenderer());   // sqlConn=null → TOAST probe 禁用

        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, new Consumer<>() {
            @Override public void accept(WalRecord rec) {
                long end = (rec.lsn() + rec.totLen() + 7) & ~7L;
                if (end > appliedLsn.get()) {
                    replay.applyCatalogRecord(rec, stores);
                    appliedLsn.set(end);
                }
                cs.onRecord(rec);          // DML 面（重放段事务由 XactGrouper 幂等重建，at-least-once）
                floor.set(cs.pendingFloorLsn());
            }
        });

        // ③ 流起点：min(stored, floor)（Task 11 语义复刻——挂起桶回退）；FileWalSource 段对齐下取整
        long streamStart = storedFloor > 0 && storedFloor < storedLsn ? storedFloor : storedLsn;
        FileWalSource source = new FileWalSource(walDir,
                FileWalSource.DEFAULT_SEGMENT_SIZE, streamStart, layout.walBlockSize());
        System.out.println("[spike] 离线源启动: walDir=" + walDir + " 起点=" + Lsn.format(streamStart));
        source.start(lsn -> { }, (endLsn, bytes) -> walker.feed(endLsn, bytes));

        // ④ EOF 判定：前沿 15s 停滞（库已死、文件不生长）或 5min 硬超时
        long startMs = System.currentTimeMillis();
        long lastFrontier = -1;
        long lastMoveMs = System.currentTimeMillis();
        while (true) {
            Thread.sleep(500);
            long frontier = walker.consumedLsn();
            long records = metrics.records.sum();
            if (frontier != lastFrontier || records != 0 && records != lastRecords) {
                lastFrontier = frontier;
                lastRecords = records;
                lastMoveMs = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - lastMoveMs > STALL_TIMEOUT_MS) {
                System.out.println("[spike] 前沿停滞 15s——判定 EOF");
                break;
            }
            if (System.currentTimeMillis() - startMs > HARD_TIMEOUT_MS) {
                System.out.println("[spike] 5min 硬超时退出");
                break;
            }
        }
        source.stop();

        // ⑤ 最终检查点落盘（离线推进检查点——验证后续衔接在线的位点面）
        store.checkpoint(stores, appliedLsn.get(), floor.get());
        System.out.println("[spike] 离线重放完成: consumed=" + Lsn.format(walker.consumedLsn())
                + " applied=" + Lsn.format(appliedLsn.get())
                + " records=" + metrics.records.sum() + " resyncs=" + metrics.resyncs.sum()
                + " lossyResyncs=" + metrics.lossyResyncs.sum()
                + " zeroPageSkips=" + metrics.zeroPageSkips.sum()
                + " carryDrops=" + metrics.carryDrops.sum()
                + " dmlBuckets=" + cs.emittedBuckets() + " dmlRows=" + cs.emittedRows()
                + " 检查点推进至 " + Lsn.format(appliedLsn.get()));
    }

    private static long lastRecords;

    /**
     * CatalogSnapshot 的离线 adapter——照 CatalogSynchronizer.SnapshotView 的实现逐方法
     * 复刻（该视图全部基于 CatalogStores 公开 getter，无门面依赖）；lsn() 前沿由 harness
     * 维护的 AtomicLong 供给（apply 先写字典后 set 前沿——与同步器同发布次序）。
     */
    static final class SpikeSnapshot implements CatalogSnapshot {
        private static final long PG_ATTRIBUTE_OID = 1249;
        private static final long PG_CLASS_OID = 1259;
        private static final long PG_PARTITION_OID = 9016;

        private final CatalogStores stores;
        private final AtomicLong appliedLsn;

        SpikeSnapshot(CatalogStores stores, AtomicLong appliedLsn) {
            this.stores = stores;
            this.appliedLsn = appliedLsn;
        }

        @Override public long lsn() {
            return appliedLsn.get();
        }

        @Override public List<Column> columnsOf(long relOid) {
            List<Column> out = new ArrayList<>();
            for (CatalogRow.AttrRow row : stores.attrRows().values()) {
                if (row.attrelid() == relOid) {
                    out.add(new Column(row.attnum(), row.attname(), row.atttypid(), row.attisdropped()));
                }
            }
            out.sort(Comparator.comparingInt(Column::attnum));
            return out;
        }

        @Override public OptionalLong relfilenodeOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    return OptionalLong.of(row.relfilenode());
                }
            }
            return OptionalLong.empty();
        }

        @Override public OptionalLong toastOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    return OptionalLong.of(row.reltoastrelid());
                }
            }
            return OptionalLong.empty();
        }

        @Override public Optional<String> schemaOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    for (CatalogRow.NspRow nsp : stores.nspRows().values()) {
                        if (nsp.nspOid() == row.relnamespace()) {
                            return Optional.of(nsp.nspname());
                        }
                    }
                    return Optional.empty();
                }
            }
            return Optional.empty();
        }

        @Override public Optional<String> relkindOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    return Optional.ofNullable(row.relkind());
                }
            }
            return Optional.empty();
        }

        @Override public Optional<String> nameOf(long relOid) {
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relOid) {
                    return Optional.of(row.relname());
                }
            }
            return Optional.empty();
        }

        @Override public OptionalLong relOidOf(long relNodeOrOid) {
            if (relNodeOrOid != 0) {
                if (relNodeOrOid == stores.pgAttrRelfilenode()) {
                    return OptionalLong.of(PG_ATTRIBUTE_OID);
                }
                if (relNodeOrOid == stores.pgClassRelfilenode()) {
                    return OptionalLong.of(PG_CLASS_OID);
                }
                if (relNodeOrOid == stores.pgPartitionRelfilenode()) {
                    return OptionalLong.of(PG_PARTITION_OID);
                }
            }
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relfilenode() == relNodeOrOid) {
                    return OptionalLong.of(row.relOid());
                }
            }
            for (CatalogRow.ClassRow row : stores.classRows().values()) {
                if (row.relOid() == relNodeOrOid) {
                    return OptionalLong.of(relNodeOrOid);
                }
            }
            return OptionalLong.empty();
        }

        @Override public Optional<PartitionIdentity> partitionOf(long relfilenode) {
            for (CatalogRow.PartitionRow row : stores.partitionRows().values()) {
                if (row.relfilenode() == relfilenode && "p".equals(row.parttype())) {
                    return Optional.of(new PartitionIdentity(row.relname(), row.parentid()));
                }
            }
            return Optional.empty();
        }
    }
}
