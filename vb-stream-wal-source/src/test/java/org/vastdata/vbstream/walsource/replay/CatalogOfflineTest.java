package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.state.StateConfig;
import org.vastdata.vbstream.walsource.state.StateStore;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CatalogSynchronizer 无库装配档（离线回放，spec §6 装配路径单测——零 Docker）：
 * 检查点凭证两面（缺失 ISE 附采集指引 / 非 VastBase 档 ISE 附档位指引）+ 手造 6 档
 * 检查点与手造全零段 16MB 稀疏文件的全链冒烟（装配成功 → EOF 确定性到达 → stop 后
 * 检查点同目录续写）+ 离线 DML 发射计数接线（终审 F1——同步器供给面与门面 dml*
 * 观测面）。
 *
 * <p>段几何对齐生产档（16MB 段/8KB 页——与 FileWalSource.DEFAULT_SEGMENT_SIZE
 * 同值）；全零段经 OG 系整零页容忍消化（walker 零页跳过、零记录产出，EOF 仍确定性
 * 到达——静态清单读尽与记录产出无关）。临时目录用 Files.createTempDirectory
 * （brief 原文形态）。</p>
 */
class CatalogOfflineTest {

    /** 生产档段尺寸（16MB，与 FileWalSource.DEFAULT_SEGMENT_SIZE 同值——稀疏全零段的建段基准）。 */
    private static final long SEG = 16L << 20;

    /** 终态位轮询间隔（毫秒）——静态线程读尽秒级收敛，20ms 轮询即够。 */
    private static final long AWAIT_POLL_MS = 20;

    /**
     * 无检查点 ISE 附采集指引（spec §5：离线无法引导——冷启动非目标，入口凭证就是
     * 一份合法检查点）。
     */
    @Test
    void offlineWithoutCheckpointFailsWithGuidance() throws Exception {
        Path stateDir = Files.createTempDirectory("off-nostate");
        Path walDir = Files.createTempDirectory("off-wal");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CatalogSynchronizer.startOffline(cfg(stateDir), walDir, 0L, Set.of(), null));
        assertTrue(e.getMessage().contains("先在线运行"), "附采集指引: " + e.getMessage());
    }

    /**
     * 检查点 pgVersion=5（OG 档）在离线档 ISE 附档位指引（spec §3.2 第 1 步——本版仅
     * VastBase=6）。
     */
    @Test
    void offlineRejectsNonVastbaseCheckpoint() throws Exception {
        Path stateDir = Files.createTempDirectory("off-og");
        new StateStore(stateDir, 5).checkpoint(new CatalogStores(), 0x100L, 0L);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CatalogSynchronizer.startOffline(cfg(stateDir), Files.createTempDirectory("off-wal"), 0L, Set.of(), null));
        assertTrue(e.getMessage().contains("仅支持 VastBase"), "附档位指引: " + e.getMessage());
    }

    /**
     * 全链冒烟：手造 6 档检查点 + 手造全零段（OG 系零页容忍消化）——装配成功、EOF 到达、
     * 最终检查点落盘（spec §6 装配路径单测）。
     */
    @Test
    void offlineAssemblyReachesEofAndPersistsCheckpoint() throws Exception {
        Path stateDir = Files.createTempDirectory("off-ok");
        Path walDir = Files.createTempDirectory("off-wal");
        seedOfflineCorpus(stateDir, walDir);
        CatalogSynchronizer sync = CatalogSynchronizer.startOffline(
                cfg(stateDir), walDir, 0L, Set.of(), null);
        awaitTrue(sync::eofReached, 15_000);
        sync.stop();                                                   // 最终检查点落盘
        // 真推进断言（lastCheckpointLsn ∈ (0, bound]——离线回放窗口内的检查点前移）由
        // VastOfflineIT 用例 3/4 在真库承担；本用例只钉"同目录续写可再载"的落盘事实
        assertTrue(new StateStore(stateDir, 6).load().isPresent(), "离线检查点续写同目录");
    }

    /**
     * 离线 DML 发射计数接线（终审 F1）：离线装配的 ChangeStream 由同步器字段供给、门面
     * 接进 {@code changeStream}——{@code dml*} 观测面在线/离线同源（在线档同字段由接收
     * 线程惰性建，离线档无惰性路径；不接线则 Main 周期行 {@code dml=[buckets=0 rows=0]}
     * 离线恒 0 误导操作员，spec §3.4）。
     *
     * <p>三层断言：① 供给面——DML 启用档 {@code startOffline} 返回的同步器
     * {@code offlineChangeStream()} 非 null（计数随 EOF 仍为 0 属实）；② 禁用档
     * （listener null）供给面 null——门面字段保持"面未装配"同语义；③ 门面全路径——
     * {@code WalSource.startOffline} 后 dml* 观测面不抛 NPE。非零计数需手造完整
     * VastBase 页头+记录头+字典供数的用户表 heap 记录（成本记档不强制），真计数对拍
     * 由 VastOfflineIT 在线/离线逐块全等承担。</p>
     */
    @Test
    void offlineAssemblyWiresDmlCountersThroughFacade() throws Exception {
        Path stateDir = Files.createTempDirectory("off-dml");
        Path walDir = Files.createTempDirectory("off-wal");
        seedOfflineCorpus(stateDir, walDir);
        CatalogSynchronizer sync = CatalogSynchronizer.startOffline(
                cfg(stateDir), walDir, 0L, Set.of(), noopListener());
        assertTrue(sync.offlineChangeStream() != null, "DML 启用档供给门面的 ChangeStream 在场");
        awaitTrue(sync::eofReached, 15_000);
        assertEquals(0L, sync.offlineChangeStream().emittedBuckets(), "全零段无 DML——0 属实");
        sync.stop();

        Path stateDirNoDml = Files.createTempDirectory("off-nodml");
        Path walDirNoDml = Files.createTempDirectory("off-wal2");
        seedOfflineCorpus(stateDirNoDml, walDirNoDml);
        CatalogSynchronizer syncNoDml = CatalogSynchronizer.startOffline(
                cfg(stateDirNoDml), walDirNoDml, 0L, Set.of(), null);
        assertNull(syncNoDml.offlineChangeStream(), "DML 禁用档无 ChangeStream 供给");
        syncNoDml.stop();

        Path stateDirFacade = Files.createTempDirectory("off-facade");
        seedOfflineCorpus(stateDirFacade, walDir);
        Properties p = new Properties();
        p.setProperty(WalSource.KEY_OFFLINE_DIR, walDir.toString());
        p.setProperty(WalSource.KEY_STATE_DIR, stateDirFacade.toString());
        try (WalSource source = new WalSource(p, noopListener())) {
            source.startOffline();
            awaitTrue(source::eofReached, 15_000);
            assertEquals(0L, source.dmlEmittedBuckets(), "门面 dml 观测面离线可达（计数 0 属实）");
            assertEquals(0L, source.dmlEmittedRows());
            assertEquals(0L, source.dmlPendingFloorLsn());
        }
    }

    /**
     * 构造启用检查点的测试配置（目录即可达性由 StateStore 惰性处理）。
     *
     * @param dir 检查点目录
     * @return 30s / 1000 条默认节拍的启用配置
     */
    private static StateConfig cfg(Path dir) {
        return new StateConfig(dir, 30_000, 1_000);
    }

    /**
     * 手造离线语料（6 档检查点 + 段 5 全零稀疏段）：检查点 lsn 落段 5 内（起点段在场的
     * 审计前提），全零段经 OG 系零页容忍消化——EOF 确定性到达而零记录产出。
     *
     * @param stateDir 检查点目录（写入 pgVersion=6 检查点）
     * @param walDir   段快照目录（写入 000000010000000000000005 全零稀疏段）
     */
    private static void seedOfflineCorpus(Path stateDir, Path walDir) throws Exception {
        long lsn = 5L * SEG + 0x1234;                                  // 段 5 内
        new StateStore(stateDir, 6).checkpoint(new CatalogStores(), lsn, 0L);
        try (RandomAccessFile f = new RandomAccessFile(
                walDir.resolve("000000010000000000000005").toFile(), "rw")) {
            f.setLength(SEG);                                          // 稀疏全零段
        }
    }

    /**
     * 全空操作的输出 listener（接线测试只关心装配开关，不消费事件）。
     *
     * @return 四回调全 no-op 的 {@link ChangeOutputListener}
     */
    private static ChangeOutputListener noopListener() {
        return new ChangeOutputListener() {
            @Override
            public void onBegin(BatchBegin begin) {
            }

            @Override
            public void onRow(RowChange row) {
            }

            @Override
            public void onEnd(BatchEnd end) {
            }

            @Override
            public void onAborted(BatchAborted aborted) {
            }
        };
    }

    /**
     * 轮询等待条件成立（离线静态线程终态位的观察辅助）。
     *
     * <p>关键步骤：每 {@value #AWAIT_POLL_MS}ms 轮询一次条件，成立即返回。边界与异常
     * 语义：超时抛 AssertionError（调用点按"必达"语义断言，不返回假值）；条件须线程
     * 安全可轮询（本类仅用于 volatile 终态位）；中断恢复中断位后按失败路径抛出。
     * 线程约束：测试主线程调用。</p>
     *
     * @param cond      终态条件（如 eofReached 透传面）
     * @param timeoutMs 超时毫秒
     */
    private static void awaitTrue(BooleanSupplier cond, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError("条件在 " + timeoutMs + "ms 内未成立（终态位未到达）");
            }
            try {
                Thread.sleep(AWAIT_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待被中断", e);
            }
        }
    }
}
