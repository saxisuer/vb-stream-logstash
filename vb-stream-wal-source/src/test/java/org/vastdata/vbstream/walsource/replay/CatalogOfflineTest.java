package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.state.StateConfig;
import org.vastdata.vbstream.walsource.state.StateStore;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CatalogSynchronizer 无库装配档（离线回放，spec §6 装配路径单测——零 Docker）：
 * 检查点凭证两面（缺失 ISE 附采集指引 / 非 VastBase 档 ISE 附档位指引）+ 手造 6 档
 * 检查点与手造全零段 16MB 稀疏文件的全链冒烟（装配成功 → EOF 确定性到达 → stop 后
 * 检查点同目录续写）。
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
        long lsn = 5L * SEG + 0x1234;                                  // 段 5 内
        new StateStore(stateDir, 6).checkpoint(new CatalogStores(), lsn, 0L);
        try (RandomAccessFile f = new RandomAccessFile(
                walDir.resolve("000000010000000000000005").toFile(), "rw")) {
            f.setLength(SEG);                                          // 稀疏全零段
        }
        CatalogSynchronizer sync = CatalogSynchronizer.startOffline(
                cfg(stateDir), walDir, 0L, Set.of(), null);
        awaitTrue(sync::eofReached, 15_000);
        sync.stop();                                                   // 最终检查点落盘
        assertTrue(new StateStore(stateDir, 6).load().isPresent(), "离线检查点续写同目录");
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
