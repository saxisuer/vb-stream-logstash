package org.vastdata.vbstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.replication.BlockOutputAdapter;
import org.vastdata.vbstream.replication.OutputMode;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.TransactionListener;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/** 冒烟入口（2.0 流式输出形态）：reader 记账 + CQ 管道 + consumer 流式回放——连上复制流后，pgoutput 原始字节经 raw 接缝喂给异步 {@link TransactionAssembler}（数据消息 append 进管道、桶只记 CQ index 段），提交事务交接冻结桶由 transaction-consumer 线程回放成 {@link org.vastdata.vbstream.replication.TransactionEvent} 事件流打印到控制台（默认 STREAMING 直渲染；{@code vb.output.mode=block} 经 {@link BlockOutputAdapter} 重组 1.7 整块语义）；LSN 反馈按输出前沿封顶，Ctrl+C 优雅退出。 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * 装配并启动复制会话（2.0 双线程流式形态）：ConsoleListener 一个实例三角色——组装器解码点
     * observer（reader 线程的控制消息/'R' live 解码 + consumer 线程的回放解码，逐消息
     * DEBUG/INFO）、流式事件渲染（STREAMING 默认，onEvent 直渲染）与整块渲染（BLOCK 经
     * {@link BlockOutputAdapter} 回调 onTransaction）。关键步骤：校验配置（含 pipe 与输出形态
     * 解析，非法值启动期 fail-fast）
     * → 会话 open/ensureSlot/start → reader 线程（pgoutput-reader）内 try-with-resources 建**异步**
     * 组装器（独享 {@link VersionedRelationRegistry} 与 pipe 配置；构造即建管道并起非守护的
     * transaction-consumer 线程开始消费交接队列）→ {@code session.run(assembler, outputFrontier::get)}
     * 把组装器作为 raw 消费者，LSN 确认按输出前沿封顶——consumer 每输出一个事务以 endLsn 单调
     * 累加前沿，crash 时未输出事务必然被重发（1.7 设计 §5）→ 主线程 await 停机信号（Ctrl+C 触发
     * shutdown hook）。关闭次序：会话 → 组装器（排干）→ 管道——会话 close 使 run 经 isClosed 守卫
     * 退出，组装器 try-with-resources 收尾走毒丸排干协议（已提交未输出的事务不丢），管道随组装器
     * close 内部释放。**信号停机的排干闸门**（spec §4.6）：JVM 在 shutdown hook 完成后才 halt、
     * 不等 non-daemon 线程，故 hook 除 countDown 外还 join pgoutput-reader（上限 60s，对齐组装器
     * close 的 consumer join；超时 WARN 放行防卡死兜底，正常路径远快于此）——hook 与 main 并行
     * 推进、无互相等待（worker 退出依赖的 session.close 由 main 醒来后在 try-with-resources
     * 执行，worker 的组装器 close 内部自会 join consumer），Ctrl+C 的排干承诺由此成立。
     * consumer 回放失败经 onFailure（stop::countDown）触发停机（fail-fast，等价 1.6"异常上抛
     * 终止会话"）。配置缺失 exit 2，启动失败 exit 1，复制流中断保持槽位并倒计时停机（重启续传）。
     */
    public static void main(String[] args) throws Exception {
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        if (config.host().isBlank() || config.slotName().isBlank() || config.publicationNames().isBlank()) {
            LOG.error("用法: java -Dvb.pg.host=... -Dvb.pg.port=... -Dvb.pg.slot=... "
                    + "-Dvb.pg.publication=... org.vastdata.vbstream.Main");
            System.exit(2);
        }
        LOG.info("vb-stream-logstash → {}:{}/{} 槽={} publication={} proto=v{} streaming={} twoPhase={}（Ctrl+C 退出）",
                config.host(), config.port(), config.database(), config.slotName(), config.publicationNames(),
                config.protoVersion(), config.streamingMode(), config.twoPhase());
        PipeConfig pipe = PipeConfig.fromSystemProperties();
        LOG.info("pipe 配置: dir={} rollCycle={}", pipe.dir(), pipe.rollCycle());
        // 输出形态解析前置（非法值启动期 fail-fast，早于连接建立）：STREAMING=流式事件直渲染
        // （默认，回放期堆 O(单条)）；BLOCK=适配器重组整块（1.7 原子交付语义逃生门，堆 O(事务)）
        OutputMode outputMode = OutputMode.fromSystemProperties();
        LOG.info("输出形态: mode={}", outputMode);

        CountDownLatch stop = new CountDownLatch(1);
        // 输出前沿（consumer 已输出事务的最大 endLsn，跨线程 AtomicLong）：consumer 线程单调累加、
        // reader 线程的 run 循环每轮读取并对 LSN 确认做 min 封顶——确认锚定输出进度而非读取进度。
        AtomicLong outputFrontier = new AtomicLong();

        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            ConsoleListener console = new ConsoleListener();
            // vb.output.mode 接线（2.0 spec §1.1）：STREAMING（默认）——console 直接作为流式
            // listener（onEvent 逐事件渲染，O(单条) 堆）；BLOCK——BlockOutputAdapter 把事件流攒齐
            // 整块再回调 console.onTransaction（1.7 原子交付语义逃生门，O(事务) 堆）。
            TransactionListener output = outputMode == OutputMode.BLOCK
                    ? new BlockOutputAdapter(console)
                    : console;
            // 组装器独享的 Relation 版本日志：'R' live 解码入版本序列（seq 戳），交接时按桶圈定拷快照；
            // console 逐消息渲染的第二参（RelationLookup）由组装器分流——live 解码点传 registry
            // （最新版视图），回放解码点传桶快照（Task 6 起，1.7 设计 §4.3）。
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            Thread worker = new Thread(() -> {
                // 组装器随会话生命周期关闭（关闭次序 session → assembler → pipe）：会话 close → run
                // 经 isClosed 守卫退出 → 此处毒丸排干 consumer（已提交未输出的事务不丢）后关管道。
                // consumer 回放失败经 onFailure（stop::countDown）触发停机，与复制流中断同一收敛路径。
                try (TransactionAssembler assembler = new TransactionAssembler(output, config.streamingMode(),
                        registry, pipe, (msg, view) -> console.onMessage(msg, view),
                        outputFrontier, stop::countDown)) {
                    session.run(assembler, outputFrontier::get);
                } catch (Exception e) {
                    LOG.error("复制流中断: {}（槽 {} 已保留，重启续传）", e.toString(), config.slotName(), e);
                    stop.countDown();
                }
            }, "pgoutput-reader");
            worker.start();
            // 信号停机的排干闸门（spec §4.6，须在 worker 建好并启动后注册）：SIGINT/SIGTERM 下
            // JVM 跑完 shutdown hook 即 halt、不等 non-daemon 线程——hook 必须亲自等 worker 走完
            // "run 退出 → 组装器 close（毒丸排干 consumer）"，已提交未输出的事务才不丢。join
            // 上限 60s 对齐组装器 close 的 consumer join；超时/被中断 WARN 放行（防关闭序列卡死
            // 拖住停机的兜底）。无死锁链：hook 等 worker；worker 的退出依赖 session.close——由
            // main 线程 countDown 醒来后在 try-with-resources 收尾执行，与 hook 并行不互等。
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                stop.countDown();
                try {
                    worker.join(60_000L);
                    if (worker.isAlive()) {
                        LOG.warn("pgoutput-reader 60s 内未退出（关闭序列卡死），放弃等待交还 JVM halt");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("等待 pgoutput-reader 退出被中断，放弃等待交还 JVM halt");
                }
            }, "shutdown-hook"));
            stop.await();
            LOG.info("正在关闭复制流...");
        } catch (Exception e) {
            LOG.error("启动失败: {}", e.getMessage());
            System.exit(1);
        }
    }
}
