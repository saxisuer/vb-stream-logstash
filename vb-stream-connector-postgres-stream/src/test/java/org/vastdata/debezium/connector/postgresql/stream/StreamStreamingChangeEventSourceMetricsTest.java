package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * {@link StreamStreamingChangeEventSourceMetrics} + {@link StreamMetricsBridge} +
 * {@link StreamChangeEventSourceMetricsFactory} 单测(MS5 Task 4 的 MBean 面):
 * <ul>
 *   <li>未装配槽的缺省语义——五项速率与 lag/pending 恒 0、管道磁盘占用恒 -1(brief 契约
 *       "未装配返回 0/-1"),tick 驱动安全不抛</li>
 *   <li>装配后生效——{@code setSuppliers} 注入假 suppliers,受控时钟两 tick 驱动窗口差分:
 *       五项速率 = Totals 增量 ÷ 窗口秒数;lag/挂起 prepared/管道磁盘占用在 tick 内采样</li>
 *   <li>metrics 类八 getter 全量委派 bridge(MBean 属性面与 bridge 观测面同值)</li>
 *   <li>工厂换装——{@code getStreamingMetrics} 产出本连接器 metrics 实例且委派同一 bridge</li>
 *   <li>JMX 注册面——{@code register()} 后平台 MBeanServer 可按属性名读到扩展属性
 *       (MXBean 子接口被框架自动识别,扩展 getter 真正进入 MBean 面)</li>
 * </ul>
 * 刻意不连库/不起管道——suppliers 全部为手写假件,Debezium 侧四参构造链走最小真件。
 */
class StreamStreamingChangeEventSourceMetricsTest {

    /** 受控时钟(nanoTime 时域,可手动推进)——速率窗口差分时间分母的确定性驱动。 */
    private static final class MutableClock implements LongSupplier {
        long nanos;

        @Override
        public long getAsLong() {
            return nanos;
        }
    }

    /** 本测试专属 server 逻辑名——JMX ObjectName 的定位过滤键,避免与其余测试的注册互扰。 */
    private static final String SERVER_NAME = "ms5metrics";

    private MutableClock clock;
    private StreamMetricsBridge bridge;

    /** 可变的 Totals 持有者——假 totals supplier 背后的读源(两 tick 之间被测试改写)。 */
    private StreamThroughputMetrics.Totals totals;

    /** lag 假供应商的最新值(装配后由 tick 采样,测试在 tick 间改写)。 */
    private long lagValue;

    /** 管道磁盘占用假供应商的最新值。 */
    private long diskValue;

    /** 挂起 prepared 假供应商的最新值。 */
    private int preparedValue;

    /** JMX 用例注册的 metrics(tearDown 统一注销,防跨用例污染平台 MBeanServer)。 */
    private StreamStreamingChangeEventSourceMetrics<Partition> registered;

    /**
     * 每用例重建受控时钟与 bridge,并重置假供应商读源——速率窗口差分跨用例不得串值。
     */
    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        bridge = new StreamMetricsBridge(clock);
        totals = new StreamThroughputMetrics.Totals(0, 0, 0, 0, 0, 0);
        lagValue = 0L;
        diskValue = -1L;
        preparedValue = 0;
    }

    /**
     * 注销 JMX 用例注册过的 MBean(未注册用例为 null 直过)——残留会让同 server 名的
     * 后续注册撞 InstanceAlreadyExists。
     */
    @AfterEach
    void tearDown() {
        if (registered != null) {
            registered.unregister();
            registered = null;
        }
    }

    /**
     * 用例①未装配缺省:五项速率 0.0、lagBytes 0、pendingPreparedCount 0、
     * pipeDiskUsageBytes -1;tick 驱动(totals 槽仍空)安全不抛且缺省不变。
     */
    @Test
    void unwiredBridgeReturnsZeroOrMinusOneDefaults() {
        assertEquals(0.0, bridge.getSlotReadBytesPerSecond(), "未装配:slot 读取字节速率应恒 0");
        assertEquals(0.0, bridge.getSlotReadMessagesPerSecond(), "未装配:slot 读取消息速率应恒 0");
        assertEquals(0.0, bridge.getAssembledTxsPerSecond(), "未装配:组装速率应恒 0");
        assertEquals(0.0, bridge.getOutputRecordsPerSecond(), "未装配:输出记录速率应恒 0");
        assertEquals(0.0, bridge.getOutputBytesPerSecond(), "未装配:输出字节速率应恒 0");
        assertEquals(0L, bridge.getLagBytes(), "未装配:滞后字节数应恒 0");
        assertEquals(0, bridge.getPendingPreparedCount(), "未装配:挂起 prepared 数应恒 0");
        assertEquals(-1L, bridge.getPipeDiskUsageBytes(), "未装配:管道磁盘占用应为 -1(未知哨兵)");
        assertDoesNotThrow(bridge::onStatsTick, "未装配时 tick 应是安全 no-op(不得向 consumer 路径抛)");
        assertEquals(0.0, bridge.getSlotReadBytesPerSecond(), "no-op tick 后速率仍恒 0");
    }

    /**
     * 用例②装配后速率预计算:setSuppliers 注入假件 → 首 tick(时钟 t=0)立基线 →
     * Totals 推进(slot 字节 +900/消息 +90/组装 +9/输出字节 +1800/输出记录 +180)→
     * 时钟推进 10s 再 tick——五项速率应恰为 90.0/9.0/0.9/180.0/18.0(增量 ÷ 窗口秒);
     * lag/pending/disk 同 tick 采样自各自假供应商。
     */
    @Test
    void tickPrecomputesRatesAndSamplesStateSuppliers() {
        bridge.setSuppliers(() -> totals, () -> lagValue, () -> diskValue, () -> preparedValue);

        clock.nanos = 0L;
        bridge.onStatsTick();   // 首 tick:只立基线,速率不动

        totals = new StreamThroughputMetrics.Totals(900, 90, 9, 1800, 180, 9);
        lagValue = 42L;
        diskValue = 4_242L;
        preparedValue = 3;
        clock.nanos = 10_000_000_000L;
        bridge.onStatsTick();

        assertEquals(90.0, bridge.getSlotReadBytesPerSecond(), 1e-9, "slot 字节速率 = 900B / 10s");
        assertEquals(9.0, bridge.getSlotReadMessagesPerSecond(), 1e-9, "slot 消息速率 = 90msg / 10s");
        assertEquals(0.9, bridge.getAssembledTxsPerSecond(), 1e-9, "组装速率 = 9tx / 10s");
        assertEquals(18.0, bridge.getOutputRecordsPerSecond(), 1e-9, "输出记录速率 = 180rec / 10s");
        assertEquals(180.0, bridge.getOutputBytesPerSecond(), 1e-9, "输出字节速率 = 1800B / 10s");
        assertEquals(42L, bridge.getLagBytes(), "lagBytes 应在 tick 内采样自假供应商");
        assertEquals(3, bridge.getPendingPreparedCount(), "挂起 prepared 数应在 tick 内采样");
        assertEquals(4_242L, bridge.getPipeDiskUsageBytes(), "管道磁盘占用应在 tick 内采样(目录遍历不进 JMX 读路径)");
    }

    /**
     * 用例③空窗归零:第二个窗口 Totals 无增量 → 再 tick 后五项速率回到 0
     * (观测面诚实——窗口差分语义,不是累计平均)。
     */
    @Test
    void idleWindowRatesFallBackToZero() {
        bridge.setSuppliers(() -> totals, () -> lagValue, () -> diskValue, () -> preparedValue);
        clock.nanos = 0L;
        bridge.onStatsTick();
        clock.nanos = 10_000_000_000L;
        bridge.onStatsTick();
        assertEquals(0.0, bridge.getSlotReadBytesPerSecond(), 1e-9, "零增量窗口的速率应为 0");
        assertEquals(0.0, bridge.getAssembledTxsPerSecond(), 1e-9, "零增量窗口的组装速率应为 0");
    }

    /**
     * 用例④metrics 类八 getter 全量委派:装配 + 两 tick 后,StreamStreamingChangeEvent
     * SourceMetrics 的每个 MBean getter 与 bridge 同值(委派而非独立状态机)。
     */
    @Test
    void metricsClassDelegatesAllGettersToBridge() {
        bridge.setSuppliers(() -> totals, () -> lagValue, () -> diskValue, () -> preparedValue);
        clock.nanos = 0L;
        bridge.onStatsTick();
        totals = new StreamThroughputMetrics.Totals(1_000, 100, 10, 2_000, 200, 10);
        lagValue = 77L;
        diskValue = 999L;
        preparedValue = 5;
        clock.nanos = 5_000_000_000L;
        bridge.onStatsTick();

        StreamStreamingChangeEventSourceMetrics<Partition> metrics = newStreamMetrics(bridge);

        assertEquals(bridge.getSlotReadBytesPerSecond(), metrics.getSlotReadBytesPerSecond(), 1e-9, "slot 字节速率应委派 bridge");
        assertEquals(bridge.getSlotReadMessagesPerSecond(), metrics.getSlotReadMessagesPerSecond(), 1e-9, "slot 消息速率应委派 bridge");
        assertEquals(bridge.getAssembledTxsPerSecond(), metrics.getAssembledTxsPerSecond(), 1e-9, "组装速率应委派 bridge");
        assertEquals(bridge.getOutputRecordsPerSecond(), metrics.getOutputRecordsPerSecond(), 1e-9, "输出记录速率应委派 bridge");
        assertEquals(bridge.getOutputBytesPerSecond(), metrics.getOutputBytesPerSecond(), 1e-9, "输出字节速率应委派 bridge");
        assertEquals(bridge.getLagBytes(), metrics.getLagBytes(), "lagBytes 应委派 bridge");
        assertEquals(bridge.getPendingPreparedCount(), metrics.getPendingPreparedCount(), "挂起 prepared 数应委派 bridge");
        assertEquals(bridge.getPipeDiskUsageBytes(), metrics.getPipeDiskUsageBytes(), "管道磁盘占用应委派 bridge");
    }

    /**
     * 用例⑤工厂换装:getStreamingMetrics 产出 {@link StreamStreamingChangeEventSourceMetrics}
     * 实例(Task.start 的 Default 工厂换装点),且其 getter 委派构造时传入的同一 bridge。
     */
    @Test
    void factoryProducesStreamMetricsWiredToBridge() {
        bridge.setSuppliers(() -> totals, () -> lagValue, () -> diskValue, () -> preparedValue);
        clock.nanos = 0L;
        bridge.onStatsTick();
        lagValue = 123L;
        clock.nanos = 10_000_000_000L;
        bridge.onStatsTick();

        StreamChangeEventSourceMetricsFactory<Partition> factory = new StreamChangeEventSourceMetricsFactory<>(bridge);
        StreamingChangeEventSourceMetrics<Partition> produced =
                factory.getStreamingMetrics(taskContext(), queueMetrics(), metadataProvider(), () -> List.of());

        StreamStreamingChangeEventSourceMetrics<Partition> streamMetrics =
                assertInstanceOf(StreamStreamingChangeEventSourceMetrics.class, produced,
                        "工厂应产出本连接器的流式 metrics(换装点契约)");
        assertEquals(123L, streamMetrics.getLagBytes(), "工厂产物应委派构造传入的同一 bridge");
    }

    /**
     * 用例⑥JMX 注册面:metrics.register() 后,平台 MBeanServer 上按本测试专属 server 名
     * 定位到 streaming MBean,扩展属性(LagBytes / SlotReadBytesPerSecond / PendingPrepared
     * Count / PipeDiskUsageBytes)可按属性名读到——MXBean 子接口被 JMX 运行时识别,新增
     * getter 真正进入 MBean 面(仅类上有 getter 而接口缺失时属性不可见,本用例即此回归闸)。
     * 边界:注册/读值失败即测试失败;tearDown 注销防跨用例残留。
     */
    @Test
    void mbeanRegistrationExposesExtendedAttributes() throws Exception {
        bridge.setSuppliers(() -> totals, () -> lagValue, () -> diskValue, () -> preparedValue);
        clock.nanos = 0L;
        bridge.onStatsTick();
        lagValue = 555L;
        diskValue = 6_400L;
        preparedValue = 2;
        clock.nanos = 10_000_000_000L;
        bridge.onStatsTick();

        registered = newStreamMetrics(bridge);
        registered.register();

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName found = null;
        Set<ObjectName> names = server.queryNames(null, null);
        for (ObjectName name : names) {
            if (name.toString().contains("context=streaming") && name.toString().contains("server=" + SERVER_NAME)) {
                found = name;
                break;
            }
        }
        assertEquals(true, found != null, "注册后应能在平台 MBeanServer 定位到 streaming MBean");
        assertEquals(555L, ((Number) server.getAttribute(found, "LagBytes")).longValue(), "JMX 属性 LagBytes 应可读");
        assertEquals(2, ((Number) server.getAttribute(found, "PendingPreparedCount")).intValue(),
                "JMX 属性 PendingPreparedCount 应可读");
        assertEquals(6_400L, ((Number) server.getAttribute(found, "PipeDiskUsageBytes")).longValue(),
                "JMX 属性 PipeDiskUsageBytes 应可读");
        assertEquals(0.0, ((Number) server.getAttribute(found, "SlotReadBytesPerSecond")).doubleValue(),
                "JMX 属性 SlotReadBytesPerSecond 应可读(本窗口零增量)");
    }

    /**
     * 组装最小可用的 Debezium 四件套之 CdcSourceTaskContext:最小 PG 配置 + topic.prefix
     * (JMX ObjectName 的 server 键来源)+ 空自定义指标标签——metrics 构造链走真件,
     * 与 PostgresStreamConnectorTask.preStart 同形。
     *
     * @return 可直接交给 metrics 构造/工厂的任务上下文
     */
    private static CdcSourceTaskContext<PostgresConnectorConfig> taskContext() {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "no_data");
        props.put("topic.prefix", SERVER_NAME);
        Configuration config = Configuration.from(props);
        return new CdcSourceTaskContext<>(config, new PostgresStreamConnectorConfig(config), Map.of());
    }

    /**
     * 队列指标假件(四方法全 0)——metrics 基类的队列容量面与本测试断言无关。
     *
     * @return 全 0 的 ChangeEventQueueMetrics
     */
    private static ChangeEventQueueMetrics queueMetrics() {
        return new ChangeEventQueueMetrics() {
            @Override
            public int totalCapacity() {
                return 0;
            }

            @Override
            public int remainingCapacity() {
                return 0;
            }

            @Override
            public long maxQueueSizeInBytes() {
                return 0L;
            }

            @Override
            public long currentQueueSizeInBytes() {
                return 0L;
            }
        };
    }

    /**
     * 事件元数据假件(三方法返回空值)——metrics 基类的位置/事务 id 面与本测试断言无关。
     *
     * @return 空 EventMetadataProvider
     */
    private static EventMetadataProvider metadataProvider() {
        return new EventMetadataProvider() {
            @Override
            public java.time.Instant getEventTimestamp(DataCollectionId source, io.debezium.pipeline.spi.OffsetContext offset,
                    Object key, org.apache.kafka.connect.data.Struct value) {
                return null;
            }

            @Override
            public Map<String, String> getEventSourcePosition(DataCollectionId source, io.debezium.pipeline.spi.OffsetContext offset,
                    Object key, org.apache.kafka.connect.data.Struct value) {
                return Map.of();
            }

            @Override
            public String getTransactionId(DataCollectionId source, io.debezium.pipeline.spi.OffsetContext offset,
                    Object key, org.apache.kafka.connect.data.Struct value) {
                return null;
            }
        };
    }

    /**
     * 用最小真件四参 + 指定 bridge 构造本连接器的流式 metrics(MXBean 面)。
     *
     * @param metricsBridge 已装配/已 tick 驱动的 bridge 实例
     * @return 已就绪的 StreamStreamingChangeEventSourceMetrics
     */
    private static StreamStreamingChangeEventSourceMetrics<Partition> newStreamMetrics(StreamMetricsBridge metricsBridge) {
        return new StreamStreamingChangeEventSourceMetrics<>(taskContext(), queueMetrics(), metadataProvider(),
                () -> List.of(), metricsBridge);
    }
}
