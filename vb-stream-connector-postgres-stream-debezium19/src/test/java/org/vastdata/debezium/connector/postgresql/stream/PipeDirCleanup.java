package org.vastdata.debezium.connector.postgresql.stream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * 管道目录的 Windows 句柄兜底清理(测试侧共享工具,引擎 {@code replication.PipeDirCleanup}
 * 的 1:1 搬运——两侧测试代码不共享依赖,同构双份是既有惯例)。存在动机:Chronicle Queue 的
 * mmap 句柄在 {@code close()} 后由 GC/cleaner **异步**释放,Windows 不允许删除仍被映射占用的
 * 文件(POSIX 的 unlink 语义无此约束——macOS/Linux/WSL 天然免疫),紧随 close 的两种目录操作
 * 都会撞句柄:① 类级共享 {@code @TempDir} 的下一用例构造管道({@code MessagePipe} 的
 * wipe-on-open 删上一用例的队列文件失败);② {@code @TempDir} 默认 ALWAYS 清理的用例收尾
 * 删除。测试因此在 Windows 上系统性判红(2026-09-08 本机实测:组装器大类 40/50 用例翻车),
 * macOS/WSL 全绿从未暴露。本侧用例级目录类已于 2026-09-07 以 {@code CleanupMode.NEVER}
 * 规避收尾删除,但类级共享类的用例间 wipe 竞态仍在,故仍需本工具。
 *
 * <p>用法:受影响测试类挂 {@code @AfterEach} 调 {@link #wipeWithGcRetry(Path)}——在
 * @TempDir 收尾与下一用例构造<b>之前</b>清空目录,两者此后只见空目录(收尾删空目录、
 * wipe-on-open 无事可做,均不再碰被占用文件)。尽力而为语义:删除失败 System.gc 提示
 * cleaner 释放后重试(≤5 次 × 100ms),重试耗尽**静默放弃不抛**——残留只占临时目录空间,
 * 真出问题时下一用例的 wipe-on-open 会抛带路径的 UncheckedIOException 兜底定位。
 * 注意:用例<b>内部</b>多管道实例顺序复用同一目录的竞态本工具救不了(@AfterEach 时点太晚),
 * 该形态应让每个实例各用独立子目录。
 */
final class PipeDirCleanup {

    private PipeDirCleanup() {
    }

    /**
     * 责任:递归清空目录内容(保留根目录本身,含子目录树——多实例独立子目录夹具同样覆盖)。
     * 关键步骤:{@code Files.walk} 逆序遍历(深度优先、子项先于父目录)逐项 deleteIfExists,
     * 本轮全部删成即返回;任一项失败(IOException)即整轮作废——System.gc 提示 mmap cleaner
     * 释放 + 100ms 后重试,至多 5 轮;耗尽仍失败则静默返回(清理属尽力而为,见类 javadoc)。
     * 边界:目录不存在或非目录直接返回;中断恢复中断标志即返回(测试线程被中断时清理让路)。
     * 线程:测试线程(@AfterEach 调用点)。
     *
     * @param dir 待清空的目录(保留自身)
     */
    static void wipeWithGcRetry(Path dir) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        for (int attempt = 0; attempt < 5; attempt++) {
            try (Stream<Path> entries = Files.walk(dir)) {
                entries.sorted(Comparator.reverseOrder())     // 深度优先逆序:子文件先于父目录
                        .filter(p -> !p.equals(dir))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                return;   // 本轮全部删成即清空
            } catch (IOException | UncheckedIOException e) {
                System.gc();   // 提示 cleaner 回收 native mmap,句柄释放后下一轮重试可删
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
