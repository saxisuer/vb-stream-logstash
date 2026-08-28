package org.vastdata.vbstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/** 冒烟入口（1.7 解耦形态）：reader 记账 + CQ 管道 + consumer 回放输出——连上复制流后，pgoutput 原始字节经 raw 接缝喂给异步 {@link TransactionAssembler}（数据消息 append 进管道、桶只记 CQ index 段），提交事务交接冻结桶由 transaction-consumer 线程回放成原子事务块打印到控制台；LSN 反馈按输出前沿封顶，Ctrl+C 优雅退出。 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * 装配并启动复制会话（1.7 双线程形态）：ConsoleListener 一个实例双角色——组装器解码点 observer
     * （reader 线程的控制消息/'R' live 解码 + consumer 线程的回放解码，逐消息 DEBUG/INFO）与事务回调
     * （TXN 块 INFO，consumer 线程）。关键步骤：校验配置（含 pipe 配置解析，非法值启动期 fail-fast）
     * → 会话 open/ensureSlot/start → reader 线程（pgoutput-reader）内 try-with-resources 建**异步**
     * 组装器（独享 {@link VersionedRelationRegistry} 与 pipe 配置；构造即建管道并起非守护的
     * transaction-consumer 线程开始消费交接队列）→ {@code session.run(assembler, outputFrontier::get)}
     * 把组装器作为 raw 消费者，LSN 确认按输出前沿封顶——consumer 每输出一个事务以 endLsn 单调
     * 累加前沿，crash 时未输出事务必然被重发（1.7 设计 §5）→ 主线程 await 停机信号（Ctrl+C 触发
     * shutdown hook）。关闭次序：会话 → 组装器（排干）→ 管道——会话 close 使 run 经 isClosed 守卫
     * 退出，组装器 try-with-resources 收尾走毒丸排干协议（已提交未输出的事务不丢），管道随组装器
     * close 内部释放。consumer 回放失败经 onFailure（stop::countDown）触发停机（fail-fast，等价
     * 1.6"异常上抛终止会话"）。配置缺失 exit 2，启动失败 exit 1，复制流中断保持槽位并倒计时停机
     * （重启续传）。
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

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "shutdown-hook"));
        // 输出前沿（consumer 已输出事务的最大 endLsn，跨线程 AtomicLong）：consumer 线程单调累加、
        // reader 线程的 run 循环每轮读取并对 LSN 确认做 min 封顶——确认锚定输出进度而非读取进度。
        AtomicLong outputFrontier = new AtomicLong();

        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            ConsoleListener console = new ConsoleListener();
            // 组装器独享的 Relation 版本日志：'R' live 解码入版本序列（seq 戳），交接时按桶圈定拷快照；
            // console 逐消息渲染的第二参（RelationLookup）由组装器分流——live 解码点传 registry
            // （最新版视图），回放解码点传桶快照（Task 6 起，1.7 设计 §4.3）。
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            Thread worker = new Thread(() -> {
                // 组装器随会话生命周期关闭（关闭次序 session → assembler → pipe）：会话 close → run
                // 经 isClosed 守卫退出 → 此处毒丸排干 consumer（已提交未输出的事务不丢）后关管道。
                // consumer 回放失败经 onFailure（stop::countDown）触发停机，与复制流中断同一收敛路径。
                try (TransactionAssembler assembler = new TransactionAssembler(console, config.streamingMode(),
                        registry, pipe, (msg, view) -> console.onMessage(msg, view),
                        outputFrontier, stop::countDown)) {
                    session.run(assembler, outputFrontier::get);
                } catch (Exception e) {
                    LOG.error("复制流中断: {}（槽 {} 已保留，重启续传）", e.toString(), config.slotName(), e);
                    stop.countDown();
                }
            }, "pgoutput-reader");
            worker.start();
            stop.await();
            LOG.info("正在关闭复制流...");
        } catch (Exception e) {
            LOG.error("启动失败: {}", e.getMessage());
            System.exit(1);
        }
    }
}
