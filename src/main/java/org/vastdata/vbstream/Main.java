package org.vastdata.vbstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.replication.DecodedMessageBridge;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.replication.SpillConfig;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.util.concurrent.CountDownLatch;

/** 里程碑 1 入口：连上复制流，把 pgoutput 消息组装为事务块打印到控制台，Ctrl+C 优雅退出。 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * 装配并启动复制会话：ConsoleListener 一个实例同时充当逐消息消费者（DEBUG）与事务回调（INFO），
     * raw 驱动的 TransactionAssembler 在 reader 线程内把消息流组装为原子事务。
     * 关键步骤：校验配置 → 会话 open/ensureSlot/start → reader 线程把每条 raw 字节一分为二：
     * DecodedMessageBridge 解码（含 Relation 元数据缓存）后供 console 逐消息 DEBUG 渲染，
     * 组装器另走 raw 接缝自解码路由（临时接线，Task 11 正式化）
     * → 主线程 await 停机信号（Ctrl+C 触发 shutdown hook）→ try-with-resources 关闭会话。
     * 配置缺失 exit 2，启动失败 exit 1，复制流中断保持槽位并倒计时停机（重启续传）。
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

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "shutdown-hook"));

        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            ConsoleListener console = new ConsoleListener();
            // 临时接线（Task 11 正式化装配）：raw 字节一分为二——桥继续供 console 逐消息 DEBUG
            // （含 Y/O 等组装器不解码的类型，渲染行为与改造前一致）；raw 驱动的新组装器自解码路由，
            // decodedObserver 留空避免与桥双份逐消息 DEBUG。
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            TransactionAssembler assembler = new TransactionAssembler(console, config.streamingMode(),
                    registry, SpillConfig.fromSystemProperties(), msg -> { });
            Thread worker = new Thread(() -> {
                try {
                    DecodedMessageBridge bridge = new DecodedMessageBridge(console::onMessage,
                            config.streamingMode());
                    session.run(raw -> {
                        bridge.onRaw(raw);
                        assembler.onRaw(raw);
                    });
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
