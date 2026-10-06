package org.vastdata.vbstream.walsource.it;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.ConsoleRenderer;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.replication.StreamingTransactionListener;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.TransactionEvent;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 双路对拍的 engine 路执行器（Task 8）——<b>in-process</b> 跑 engine 逻辑解码管线：
 * {@link PgReplicationSession}（open/ensureSlot/start）+ 异步 {@link TransactionAssembler}
 * + {@link ConsoleRenderer} 直挂流式事件契约（engine {@code Main} 的装配减去 JVM/系统
 * 属性/hook 面——组件与接线次序逐项同源）。
 *
 * <p><b>输出捕获</b>：{@link ParityEnv#capture(String)} 挂 logback ListAppender 到
 * engine 的 CDC 专用 logger {@code org.vastdata.vbstream.cdc}——事务块行
 * （TXN-BEGIN/逐行/TXN-END）唯一出口；{@code streaming=OFF + twoPhase=false} 下无
 * 生命周期控制行（它们才是 INFO，行级 BEGIN/COMMIT 是 DEBUG），捕获面天然干净。</p>
 *
 * <p><b>前沿接线</b>（照 engine Main）：输出前沿 AtomicLong 由 consumer 在 End 处理
 * 完毕后单调累加，{@code session.run(assembler, frontier::get)} 据此对 LSN 确认封顶
 * ——at-least-once 语义与生产路径一致；{@code onFailure} 收敛到 failure latch（测试
 * 轮询面）。启停契约：{@link #start()} 建 session 与 reader 线程（parity-pgoutput-reader，
 * 组装器 try-with-resources 在线程内——run 退出即毒丸排干 consumer），{@link #stopAndDrain()}
 * 关 session → join reader → 摘 appender 取捕获行（join 后读，无并发追加）。</p>
 *
 * <p><b>管道目录</b>：每次实例化建独立临时目录（MessagePipe 构造 wipe-on-open 清空后
 * 建队列；同 JVM 多实例不可共享目录——进程内独占契约）。</p>
 */
final class EnginePathRunner {

    private static final Logger LOG = LoggerFactory.getLogger(EnginePathRunner.class);

    /** reader 线程 join 上限（对齐组装器 close 的 consumer join 60s）。 */
    private static final long JOIN_MS = 60_000;

    private final ReplicationConfig config;

    private final PgReplicationSession session;

    /** CDC logger 捕获器（构造即挂——先挂后起流，头部行不漏）。 */
    private final ParityEnv.CdcCapture capture;

    /** 计数包装的 ConsoleRenderer：End 事件计数暴露给测试轮询（volatile 跨线程读）。 */
    private final CountingRenderer renderer = new CountingRenderer(new ConsoleRenderer());

    /** 输出前沿（consumer 单调累加、run 循环读取封顶——照 engine Main 接线）。 */
    private final AtomicLong outputFrontier = new AtomicLong();

    /** consumer 回放失败收敛面（onFailure countDown——测试轮询的 fail-fast 信号）。 */
    private final CountDownLatch failure = new CountDownLatch(1);

    /** 管道临时目录（实例独占；wipe-on-open 由 MessagePipe 构造承担）。 */
    private final Path pipeDir;

    /** reader 线程（start 建；stopAndDrain join）。 */
    private Thread reader;

    /**
     * 装配 runner（不建连接——连接推迟到 {@link #start()}，失败配置不留半开资源）。
     *
     * @param config engine 路复制配置（ParityEnv.engineConfig 形态）
     * @throws IOException 管道临时目录创建失败
     */
    EnginePathRunner(ReplicationConfig config) throws IOException {
        this.config = config;
        this.session = new PgReplicationSession(config);
        this.capture = ParityEnv.capture("org.vastdata.vbstream.cdc");
        this.pipeDir = Files.createTempDirectory("parity-pipe");
    }

    /**
     * 启动 engine 路：session open → ensureSlot（幂等建逻辑槽）→ start（建复制流）→
     * reader 线程内 try-with-resources 建异步组装器后进入 run 消息循环。
     *
     * <p>装配次序逐项照 engine Main：ConsoleRenderer 一个实例三角色（流式 listener +
     * 解码点 observer），registry 独享组装器，LSN 反馈按输出前沿封顶。管道用独立临时
     * 目录 + MINUTELY 滚动（Main 默认形态）。</p>
     *
     * @throws Exception session 生命周期任一步失败（连接/建槽/建流）
     */
    void start() throws Exception {
        session.open();
        session.ensureSlot();
        session.start();
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        PipeConfig pipe = new PipeConfig(pipeDir, LegacyRollCycles.MINUTELY);
        reader = new Thread(() -> {
            // 组装器随 session 生命周期关闭：session.close → run 经 isClosed 守卫退出 →
            // 此处毒丸排干 consumer（已提交未输出的事务不丢）后关管道
            try (TransactionAssembler assembler = new TransactionAssembler(renderer, config.streamingMode(),
                    registry, pipe, (msg, view) -> renderer.console.onMessage(msg, view),
                    outputFrontier, failure::countDown)) {
                session.run(assembler, outputFrontier::get);
            } catch (Exception e) {
                // run 退出含正常停机路径（session.close 使 isClosed 检查抛出）——DEBUG 留痕即可
                LOG.debug("engine reader 退出: {}", e.toString());
            }
        }, "parity-pgoutput-reader");
        reader.start();
    }

    /**
     * 观测面：已完整输出的事务数（End 事件计数——测试的追平轮询锚）。
     *
     * @return 会话累计 End 事件数（volatile 读，任意线程安全）
     */
    long emittedTxns() {
        return renderer.txns;
    }

    /**
     * 观测面：consumer 回放是否已失败（onFailure 收敛面）。
     *
     * @return true = 组装器报告了回放失败（fail-fast）
     */
    boolean failed() {
        return failure.getCount() == 0;
    }

    /**
     * 停机并取捕获行：session close（run 循环经 isClosed 守卫退出）→ join reader
     * （退出路径内组装器 close 毒丸排干 consumer——捕获行成确定性终态）→ 摘 appender
     * → best-effort 递归删管道临时目录（测试工作区不留残）。
     *
     * @return CDC logger 捕获行（输出序保持；join 后读，无并发追加）
     * @throws InterruptedException join 被中断
     */
    List<String> stopAndDrain() throws InterruptedException {
        session.close();
        if (reader != null) {
            reader.join(JOIN_MS);
        }
        List<String> lines = capture.closeAndDrain();
        deleteRecursively(pipeDir);
        return lines;
    }

    /**
     * best-effort 递归删除目录树（管道临时工作区清理）——任一文件删除失败 WARN 吞掉
     * （残留只占临时目录磁盘，不影响正确性）。
     *
     * @param dir 根目录
     */
    private static void deleteRecursively(Path dir) {
        try (var walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    LOG.warn("清理管道临时文件失败（忽略）: {}", p, e);
                }
            });
        } catch (IOException e) {
            LOG.warn("清理管道临时目录失败（忽略）: {}", dir, e);
        }
    }

    /**
     * 流式事件计数包装：逐事件委派 {@link ConsoleRenderer}，{@link TransactionEvent.End}
     * 计数（End 返回 = 完整消费，计数点即完整事务输出点）。
     *
     * <p>线程约束：onEvent 由 transaction-consumer 线程回调；txns 为 volatile——测试
     * 线程轮询安全（计数读为最终一致语义，追平判定足够）。</p>
     */
    private static final class CountingRenderer implements StreamingTransactionListener {

        final ConsoleRenderer console;

        volatile long txns;

        /**
         * 包装目标渲染器。
         *
         * @param console engine 的 ConsoleRenderer（三角色实例——observer 面经公开字段透出）
         */
        CountingRenderer(ConsoleRenderer console) {
            this.console = console;
        }

        /**
         * 逐事件委派 + End 计数（渲染在先、计数在后——渲染抛出时该事务不计入）。
         *
         * @param event 流式事件（Begin / TxChange / End）
         */
        @Override
        public void onEvent(TransactionEvent event) {
            console.onEvent(event);
            if (event instanceof TransactionEvent.End) {
                txns++;
            }
        }
    }
}
