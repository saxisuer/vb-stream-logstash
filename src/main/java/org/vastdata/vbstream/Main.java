package org.vastdata.vbstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
     * 装配并启动复制会话：ConsoleListener 一个实例同时充当组装器解码点消费者（逐消息 DEBUG/INFO）与
     * 事务回调（TXN 块 INFO），raw 驱动的 TransactionAssembler 在 reader 线程内把消息流组装为原子事务。
     * 关键步骤：校验配置（含 spill 配置解析，非法值启动期 fail-fast）→ 会话 open/ensureSlot/start →
     * reader 线程内 try-with-resources 建组装器（独享 {@link VersionedRelationRegistry} 与 spill 配置；
     * 解码点 observer 把控制消息/Relation 的 live 解码与提交回放期的 payload 解码透传 console
     * 逐消息渲染——组装器是唯一解码者，无第二套解码链路）并把组装器直接作为 run 的 raw 消费者
     * → 主线程 await 停机信号（Ctrl+C 触发 shutdown hook）→ 会话关闭使 run 退出，组装器随之
     * try-with-resources 收尾（spill 池释放）。
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
        SpillConfig spill = SpillConfig.fromSystemProperties();
        LOG.info("spill 配置: enabled={} threshold={}B dir={} rollCycle={}",
                spill.spillEnabled(), spill.thresholdBytes(), spill.dir(), spill.rollCycle());

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "shutdown-hook"));

        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            ConsoleListener console = new ConsoleListener();
            // 组装器独享的 Relation 版本日志：'R' live 解码入版本序列（seq 戳），回放按单元 seq 取
            // asOf 版本；console 逐消息渲染经同一实例的 find 走最新版视图（闭包持引用，随 'R' 到达演进）。
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            Thread worker = new Thread(() -> {
                // 组装器随会话生命周期关闭：会话 close → run 经 isClosed 守卫退出 → 此处收尾 spill 池。
                try (TransactionAssembler assembler = new TransactionAssembler(console, config.streamingMode(),
                        registry, spill, msg -> console.onMessage(msg, registry))) {
                    session.run(assembler);
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
