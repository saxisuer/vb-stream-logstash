package org.vastdata.vbstream.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * vb-stream-reader 冒烟入口:配置三层合并(classpath 的 {@code dbconfig.properties} 基础值
 * → 系统属性 {@code -Dvb.*} 覆盖 → 默认兜底,见 {@link ReaderProperties#resolve}),经
 * {@link EngineLifecycle} 起 async 引擎加载 {@code PostgresStreamConnector},
 * {@link LogChangeConsumer} 逐条 SourceRecord 渲染 INFO;Ctrl+C 优雅停机(offset 排干落盘)。
 * 生命周期模板对齐 vb-stream-engine 的 Main(系统属性 + latch + hook + 退出码)。
 *
 * <p>用法:{@code java ... org.vastdata.vbstream.reader.Main}——默认读 classpath 的
 * dbconfig.properties(src/docker 本地 PG 模板,零参数即可起);临时覆盖单项加
 * {@code -Dvb.<键>=<值>},整体换文件加 {@code -Dvb.config=<路径>}(连接器/engine 的全部
 * 配置项语义真源见连接器模块 README 配置表与 Debezium EmbeddedEngineConfig)。
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
     * 责任:装配、监督与收敛的编排主线。关键步骤:①{@link ReaderProperties#resolve} 三层合并
     * (classpath 的 dbconfig.properties 基础值 → -Dvb.* 覆盖 → 默认兜底)组装 props 并校验必填,
     * 缺失打印用法 exit 2;②INFO 生效配置(password 打码);③{@link EngineLifecycle#start} 建
     * engine 并起 run 线程(build 同步抛——必填缺失/连接器类不可加载——exit 1);④注册停机 hook;
     * ⑤awaitStop 阻塞至停机信号(hook/失败回调/正常完成);⑥shutdown 收敛(close + join)
     * 返回失败态,失败 exit 1、正常 exit 0。
     * 边界:awaitStop 被中断恢复中断位后照常走收敛(停机意图优先,不吞);
     * shutdown 的失败态含 fail-stop 与 run 线程兜底两侧。
     *
     * @param args 命令行参数(不使用——配置走配置文件与系统属性)
     */
    public static void main(String[] args) {
        Properties props = ReaderProperties.resolve();
        List<String> missing = ReaderProperties.missingRequired(props);
        if (!missing.isEmpty()) {
            LOG.error("缺少必填配置: {}。配置来源三层合并: classpath dbconfig.properties(基础值)"
                    + " → -Dvb.<debezium 键>=<值>(覆盖) → 默认值(兜底);整体换文件用 -Dvb.config=<路径>"
                    + "(必填五项: database.hostname/database.dbname/database.user/database.password/topic.prefix,"
                    + "写在 dbconfig.properties 或以 -Dvb.* 传入均可)", missing);
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
