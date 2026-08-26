package org.vastdata.vbstream;

import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.util.concurrent.CountDownLatch;

/** 里程碑 1 入口：连上复制流并把解析出的 pgoutput 消息打印到控制台，Ctrl+C 优雅退出。 */
public final class Main {

    public static void main(String[] args) throws Exception {
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        if (config.host().isBlank() || config.slotName().isBlank() || config.publicationNames().isBlank()) {
            System.err.println("用法: java -Dvb.pg.host=... -Dvb.pg.port=... -Dvb.pg.slot=... "
                    + "-Dvb.pg.publication=... org.vastdata.vbstream.Main");
            System.exit(2);
        }
        System.out.printf("vb-stream-logstash → %s:%d/%s 槽=%s publication=%s proto=v%d streaming=%s twoPhase=%s（Ctrl+C 退出）%n",
                config.host(), config.port(), config.database(), config.slotName(), config.publicationNames(),
                config.protoVersion(), config.streamingMode(), config.twoPhase());

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "shutdown-hook"));

        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            Thread worker = new Thread(() -> {
                try {
                    session.run(new ConsoleListener());
                } catch (Exception e) {
                    System.err.println("复制流中断: " + e + "（槽 " + config.slotName()
                            + " 已保留，重启续传）");
                    stop.countDown();
                }
            }, "pgoutput-reader");
            worker.start();
            stop.await();
            System.out.println("正在关闭复制流...");
        } catch (Exception e) {
            System.err.println("启动失败: " + e.getMessage());
            System.exit(1);
        }
    }
}
