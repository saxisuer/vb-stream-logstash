package org.vastdata.vbstream.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * vb-stream-reader 冒烟入口:系统属性 {@code -Dvb.*} 剥前缀透传为 Debezium 配置,经
 * {@link EngineLifecycle} 起 async 引擎加载 {@code PostgresStreamConnector},
 * {@link LogChangeConsumer} 逐条 SourceRecord 渲染 INFO;Ctrl+C 优雅停机(offset 排干落盘)。
 * 生命周期模板对齐 vb-stream-engine 的 Main(系统属性 + latch + hook + 退出码)。
 *
 * <p>用法:{@code java ... -Dvb.database.hostname=... -Dvb.topic.prefix=... org.vastdata.vbstream.reader.Main}
 * ——连接器/engine 的全部配置项加 {@code vb.} 前缀即透传(语义真源见连接器模块 README
 * 配置表与 Debezium EmbeddedEngineConfig)。
 *
 * <p>线程约束:main 编排;engine.run() 在 engine-run 线程;记录回调在 engine 任务轮询线程;
 * 停机 hook 由 JVM 触发(只 countDown + join,close 归本线程的 shutdown)。
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /** 用法错误退出码(必填配置缺失,对齐 vb-stream-engine Main 的 exit 2)。 */
    private static final int EXIT_USAGE = 2;

    /** 失败退出码(build 失败或引擎回调 failed)。 */
    private static final int EXIT_FAILED = 1;

    private Main() {
    }

    /**
     * 责任:装配、监督与收敛的编排主线。关键步骤:①组装 props 并校验必填,缺失打印用法
     * exit 2;②INFO 生效配置(password 打码);③{@link EngineLifecycle#start} 建 engine 并起
     * run 线程(build 同步抛——必填缺失/连接器类不可加载——exit 1);④注册停机 hook;
     * ⑤awaitStop 阻塞至停机信号(hook/失败回调/正常完成);⑥shutdown 收敛(close + join)
     * 返回失败态,失败 exit 1、正常 exit 0。
     * 边界:awaitStop 被中断恢复中断位后照常走收敛(停机意图优先,不吞);
     * shutdown 的失败态含 fail-stop 与 run 线程兜底两侧。
     *
     * @param args 命令行参数(不使用——配置一律走系统属性)
     */
    public static void main(String[] args) {
        Properties props = ReaderProperties.fromSystemProperties();
        List<String> missing = ReaderProperties.missingRequired(props);
        if (!missing.isEmpty()) {
            LOG.error("缺少必填配置: {}。用法: java ... -Dvb.<debezium 键>=<值> org.vastdata.vbstream.reader.Main"
                    + "(必填: -Dvb.database.hostname= -Dvb.database.dbname= -Dvb.database.user="
                    + " -Dvb.database.password= -Dvb.topic.prefix=;连接器/engine 其余配置加 vb. 前缀透传)", missing);
            System.exit(EXIT_USAGE);
        }
        LOG.info("vb-stream-reader 启动,生效配置: {}", ReaderProperties.masked(props));

        EngineLifecycle lifecycle;
        try {
            lifecycle = EngineLifecycle.start(props, new LogChangeConsumer());
        }
        catch (Throwable t) {
            LOG.error("引擎启动失败(engine build 阶段)", t);
            System.exit(EXIT_FAILED);
            return;   // 不可达——满足 lifecycle 的明确赋值分析(exit 后编译器不视为终止)
        }
        lifecycle.installShutdownHook();

        try {
            lifecycle.awaitStop();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("main 等待停机信号被中断,直接进入收敛");
        }
        boolean failed = lifecycle.shutdown(60_000L);
        LOG.info("vb-stream-reader 已退出{}", failed ? "(失败)" : "");
        System.exit(failed ? EXIT_FAILED : 0);
    }
}
