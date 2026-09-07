package org.vastdata.vbstream.reader;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.embedded.Connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 引擎装配与生命周期闸门:{@code DebeziumEngine.create(Connect.class)} 工厂建 async 引擎
 * (3.x 唯一实现,Connect 格式直通无序列化——{@code event.value()} 即原始 {@link SourceRecord}),
 * 起名 {@code engine-run} 的线程跑阻塞的 {@code run()},停机经 latch/hook/completed 三守卫收敛。
 * Main 与 IT 共用同一装配路径(生产 consumer 与收集 consumer 只在 notifying 参数分叉)。
 *
 * <p>生命周期语义(async 引擎,3.6.1 源码核实):{@code run()} 阻塞至引擎终止且从不向调用者
 * 抛异常——失败经 CompletionCallback 上报,回调恰在 run() 返回前触发一次;{@code close()}
 * 必须由另一线程调用,引擎已自行结束(STOPPED)时再 close 抛 IllegalStateException——本类以
 * {@code completed} 守卫主动跳过、ISE catch 兜底两层处理;引擎内部线程池自建自关,宿主只提供
 * run 线程。{@code build()} 本身会同步抛(必填缺失/连接器类不可加载),由 start 原样上抛调用方。
 *
 * <p>线程约束:start 由装配方线程调用;run 在 {@code engine-run} 线程;consumer 的 handleBatch
 * 在 engine 任务轮询线程;CompletionCallback 在 run 线程;awaitStop/shutdown 由 main(或测试)
 * 线程;shutdown hook 由 JVM 停机触发——hook 只 countDown + join,不调 close(close 归 main,
 * 避免 hook 与 main 双重收敛竞态)。
 */
public final class EngineLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(EngineLifecycle.class);

    /** 停机时 join engine-run 线程的上限(ms,与 vb-stream-engine Main 的 60s 排干闸门同款)。 */
    private static final long RUN_JOIN_TIMEOUT_MILLIS = 60_000L;

    /** close 撞引擎启动窗口(STARTING_TASKS)时的重试上限(每轮 1s)。 */
    private static final int MAX_CLOSE_ATTEMPTS = 5;

    private final DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
    private final Thread runThread;
    private final CountDownLatch stop;
    private final AtomicBoolean completed;
    private final AtomicBoolean failed;

    /**
     * 私有构造(实例只经 {@link #start} 诞生,保证回调与字段的 happens-before)。
     *
     * @param engine    已 build 的引擎实例
     * @param runThread 已启动的 run 线程
     * @param stop      停机信号(hook 触发 / 失败回调触发 / 正常完成触发)
     * @param completed 完成标志——CompletionCallback 已触发(run 即将返回,close 需跳过)
     * @param failed    失败标志——回调 success=false 或 run 线程异常退出
     */
    private EngineLifecycle(DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine, Thread runThread,
                            CountDownLatch stop, AtomicBoolean completed, AtomicBoolean failed) {
        this.engine = engine;
        this.runThread = runThread;
        this.stop = stop;
        this.completed = completed;
        this.failed = failed;
    }

    /**
     * 责任:装配并启动引擎。关键步骤:三守卫容器(latch/completed/failed)先行创建供回调捕获 →
     * {@code DebeziumEngine.create(Connect.class)} 取 Builder(泛型
     * {@code ChangeEvent<SourceRecord,SourceRecord>},{@code event.value()} 即原始记录)→
     * using(props) + using(本类 classloader——连接器类的确定性解析,不依赖 TCCL)→
     * using(CompletionCallback:置 completed;失败置 failed + countDown(fail-stop);
     * 正常完成仅 INFO + countDown——streaming 源自行结束也该让 main 醒来收敛)→
     * notifying(consumer)→ build()(必填缺失/类加载失败在此同步抛,原样上抛调用方)→
     * 起非守护 {@code engine-run} 线程跑 run()(线程内 catch Throwable 兜底:run 契约不抛,
     * 兜底只是防御,触发即 failed + countDown)。
     * 边界:build 抛出的异常不带引擎副作用(引擎未建),调用方 exit/retry 均安全;
     * start 返回即 run 线程在途,后续必须走 {@link #shutdown} 收敛。
     *
     * @param props    完整引擎/连接器配置(含 connector.class/name/offset 三件)
     * @param consumer 变更消费回调(生产 LogChangeConsumer / 测试收集实现)
     * @return 已启动的生命周期实例
     */
    public static EngineLifecycle start(Properties props,
                                        ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> consumer) {
        CountDownLatch stop = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean failed = new AtomicBoolean();
        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .using(EngineLifecycle.class.getClassLoader())
                .using((DebeziumEngine.CompletionCallback) (success, message, error) -> {
                    completed.set(true);
                    if (success) {
                        LOG.info("引擎正常结束: {}", message);
                    }
                    else {
                        failed.set(true);
                        LOG.error("引擎失败,停机收敛: {}", message, error);
                    }
                    stop.countDown();
                })
                .notifying(consumer)
                .build();
        Thread runThread = new Thread(() -> {
            try {
                engine.run();
            }
            catch (Throwable t) {
                failed.set(true);
                LOG.error("engine-run 线程异常退出(契约上 run 不抛,此处为防御兜底): {}", t.getMessage(), t);
                stop.countDown();
            }
        }, "engine-run");
        runThread.setDaemon(false);
        runThread.start();
        return new EngineLifecycle(engine, runThread, stop, completed, failed);
    }

    /**
     * 责任:注册 Ctrl+C/SIGTERM 停机 hook——countDown 停机信号 + join run 线程(上限 60s)。
     * join 的理由:JVM 在全部 hook 跑完后 halt、不等非守护线程——run 线程走完引擎停机链
     * (源任务停前 final commitOffsets)offset 才落盘;hook 不调 close(close 归 main 醒来后的
     * {@link #shutdown},避免双线程同时触发引擎停机链)。
     * 边界:join 超时 WARN 放行(offset 可能未排干,槽确认位仍是权威——at-least-once);
     * join 被中断恢复中断位放行。
     */
    public void installShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.countDown();
            try {
                runThread.join(RUN_JOIN_TIMEOUT_MILLIS);
                if (runThread.isAlive()) {
                    LOG.warn("engine-run 线程 {}ms 内未退出(引擎停机链卡住),JVM halt 放行", RUN_JOIN_TIMEOUT_MILLIS);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("shutdown-hook 等 engine-run 被中断,放行");
            }
        }, "shutdown-hook"));
    }

    /**
     * 责任:阻塞至停机信号——三种触发之一:shutdown hook(Ctrl+C)、失败回调(fail-stop)、
     * 引擎正常完成(回调 success=true 也 countDown,streaming 源自行结束时 main 醒来收敛)。
     *
     * @throws InterruptedException main 线程被中断(调用方恢复中断位后照常收敛)
     */
    public void awaitStop() throws InterruptedException {
        stop.await();
    }

    /**
     * 责任:收敛引擎并返回失败态。关键步骤:completed <b>未</b>置位才调 close(已置位说明
     * run 已返回,再 close 抛 ISE——主动跳过);close 的 ISE 兜底分两类——completed 已置位
     * (与主守卫的竞态窗口,正常已停)INFO 跳过;未置位(撞 STARTING_TASKS 启动窗口)WARN +
     * 1s 重试上限 5 次;随后 join run 线程(上限入参,超时 WARN 放行)。
     * 边界:重复调用安全(join 对已终止线程立即返回,close 跳过);失败态以回调/run 兜底
     * 两侧任一置位为准。
     *
     * @param joinTimeoutMillis join run 线程的上限毫秒
     * @return 是否失败(true=回调 success=false 或 run 线程异常)
     */
    public boolean shutdown(long joinTimeoutMillis) {
        if (!completed.get()) {
            closeEngineWithRetry();
        }
        else {
            LOG.info("引擎已自行结束,跳过 close");
        }
        try {
            runThread.join(joinTimeoutMillis);
            if (runThread.isAlive()) {
                LOG.warn("engine-run 线程 {}ms 内未退出,放行(非守护线程随 JVM 收敛)", joinTimeoutMillis);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("等待 engine-run 退出被中断,放行");
        }
        return failed.get();
    }

    /**
     * 责任:close 的带兜底重试——直接 close;抛 ISE 时若 completed 已置位(引擎已自行完成,
     * 与 shutdown 主守卫的竞态窗口)INFO 跳过,否则视为撞启动窗口(STARTING_TASKS)1s 后重试,
     * {@link #MAX_CLOSE_ATTEMPTS} 次未成 WARN 放行(不抛——停机路径不被次生异常掩盖,
     * run 线程随后自行收敛或随 JVM halt)。
     * 边界:close 抛出的非 ISE 异常原样上抛(真实停机失败,调用方该看见);重试中被中断
     * 恢复中断位放弃(停机请求优先)。
     */
    private void closeEngineWithRetry() {
        for (int attempt = 1; attempt <= MAX_CLOSE_ATTEMPTS; attempt++) {
            try {
                engine.close();
                return;
            }
            catch (IllegalStateException e) {
                if (completed.get()) {
                    LOG.info("引擎已完成停机,close 无需重复: {}", e.getMessage());
                    return;
                }
                LOG.warn("close 撞上引擎启动窗口(第 {}/{} 次): {}", attempt, MAX_CLOSE_ATTEMPTS, e.getMessage());
                try {
                    Thread.sleep(1_000L);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            catch (IOException e) {
                LOG.warn("engine close IO 失败(忽略——槽确认位仍是 at-least-once 的权威面): {}", e.getMessage());
                return;
            }
        }
        LOG.warn("close 重试 {} 次未成,放行(engine-run 线程随后自行收敛)", MAX_CLOSE_ATTEMPTS);
    }
}
