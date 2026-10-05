package org.vastdata.vbstream.walsource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * WAL 直解源冒烟入口（接收形态，Task 9）：读 {@code -Dvb.wal.*} 配置起 {@link WalSource}，
 * 主线程每 10s 打一行周期统计 INFO（消费前沿 + 计数器 + census 前 3 形态），Ctrl-C 经
 * shutdown hook 优雅 close。
 *
 * <p>配置面即 {@link WalSource} 六键（{@code vb.wal.host/port/db/user/pass/slot}，默认值见
 * 其 javadoc）——本类仅做 {@code System.getProperty} 收集进 Properties，不重复解析。
 * 周期行经 slf4j（logger {@code org.vastdata.vbstream.walsource.Main}）而非 System.out
 * （项目规约），形态：{@code smoke: lsn=0/XXXX records=N resyncs=N reconnects=N
 * censusTop3=[rmid/XX=count, ...]}——censusTop3 取 census 快照按条数降序前 3
 * （形态面收敛到最热 rmgr，冒烟时一眼看出 DDL/写入落在哪个资源管理器）。</p>
 *
 * <p>生命周期：启动失败 ERROR + close 清理 + {@code System.exit(1)}（对齐引擎 Main 约定）；
 * 正常路径主线程以 {@link CountDownLatch#await(long, TimeUnit)} 分片睡 10s（hook countDown
 * 即时打断，停机不等当前周期耗尽）；hook 内 close 幂等，先于 JVM halt 排干接收线程
 * （已收数据至少留痕 census）。</p>
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /** 周期统计行间隔（毫秒）。 */
    private static final long STATS_INTERVAL_MS = 10_000;

    /** censusTop3 展示的形态桶数。 */
    private static final int CENSUS_TOP_N = 3;

    /** 配置六键（与 {@link WalSource} 配置面同形；其 KEY_ 常量在 api 包内不可见，此处同值复刻）。 */
    private static final String KEY_HOST = "vb.wal.host";
    private static final String KEY_PORT = "vb.wal.port";
    private static final String KEY_DB = "vb.wal.db";
    private static final String KEY_USER = "vb.wal.user";
    private static final String KEY_PASS = "vb.wal.pass";
    private static final String KEY_SLOT = "vb.wal.slot";

    /** 私有构造器：入口类仅静态 main。 */
    private Main() {
    }

    /**
     * 冒烟入口：收集配置 → 起门面 → 周期统计行循环 → hook 优雅停。
     *
     * <p>关键步骤：① 六键逐一 {@code System.getProperty} 非 null 才入 Properties（null 不覆盖
     * 门面默认值）；② {@link WalSource#start()} 失败（连不上/版本不支持/权限不足）ERROR 记
     * 录后 close 释放半开资源并 {@code System.exit(1)}；③ shutdown hook（名 wal-source-shutdown）
     * 调 close + countDown——主线程 await 以 10s 分片睡，被打断即退出循环由 hook 收尾；
     * 每个分片醒来打一行 {@link #logSmokeLine(WalSource)}。边界：主线程被中断（非 hook 路径）
     * 恢复中断位后退出循环（hook 兜底关资源）。</p>
     *
     * @param args 未用（配置全部走 -D 系统属性）
     */
    public static void main(String[] args) {
        WalSource source = new WalSource(collectConfig());
        try {
            source.start();
        } catch (Exception e) {
            LOG.error("WalSource 启动失败，退出（exit 1）", e);
            source.close();
            System.exit(1);
            return;
        }
        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            source.close();
            shutdown.countDown();
        }, "wal-source-shutdown"));
        LOG.info("WAL 直解源冒烟运行中（{}ms 周期统计行，Ctrl-C 优雅退出）", STATS_INTERVAL_MS);
        try {
            while (!shutdown.await(STATS_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
                logSmokeLine(source);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 收集 {@code vb.wal.*} 六键系统属性为门面配置。
     *
     * <p>关键步骤：逐键 {@code System.getProperty}，仅非 null 值入 Properties——缺键留给
     * {@link WalSource} 构造器取默认值（单一默认值来源，Main 不复刻）。</p>
     *
     * @return 配置载体（可能只含部分键）
     */
    private static Properties collectConfig() {
        Properties cfg = new Properties();
        copySysProp(KEY_HOST, cfg);
        copySysProp(KEY_PORT, cfg);
        copySysProp(KEY_DB, cfg);
        copySysProp(KEY_USER, cfg);
        copySysProp(KEY_PASS, cfg);
        copySysProp(KEY_SLOT, cfg);
        return cfg;
    }

    /**
     * 单键拷贝：系统属性存在才写入配置载体。
     *
     * @param key 系统属性键（与门面配置键同形，{@code vb.wal.*}）
     * @param cfg 目标配置载体
     */
    private static void copySysProp(String key, Properties cfg) {
        String value = System.getProperty(key);
        if (value != null) {
            cfg.setProperty(key, value);
        }
    }

    /**
     * 打一行周期统计：消费前沿 LSN + 三计数 + census 前 3 形态。
     *
     * <p>关键步骤：census 快照（不可变副本）按值降序取前 3，{@code 键=条数} 逗号拼接——
     * 快照为空（尚无记录交付）时 censusTop3 渲染为空串。数据面均只读（volatile 镜像 +
     * {@code sum()} 快照），任意时点打行安全。</p>
     *
     * @param source 运行中的门面
     */
    private static void logSmokeLine(WalSource source) {
        WalStreamMetrics m = source.metrics();
        String censusTop3 = m.censusSnapshot().entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(CENSUS_TOP_N)
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "));
        LOG.info("smoke: lsn={} records={} resyncs={} reconnects={} censusTop3=[{}]",
                Lsn.format(source.consumedLsn()),
                m.records.sum(),
                m.resyncs.sum(),
                m.reconnects.sum(),
                censusTop3);
    }
}
