package org.vastdata.vbstream.reader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ReaderProperties} 三层合并与文件加载的离线单测(零 PG):合并次序(文件基础值 →
 * -Dvb.* 覆盖 → 默认兜底)、classpath 默认文件与 vb.config 外部文件两条加载路径、外部路径
 * 缺失的 fail-fast、必填校验与 password 打码。系统属性是进程级全局态——本类对用到的键
 * 在 {@code @AfterEach} 统一清理,且各用例用互不重叠的键名避免相互干扰。
 */
class ReaderPropertiesTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path tempDir;

    /**
     * 清理本类用过的系统属性键(进程级全局态,不清会串到同 JVM 的后续用例/其他测试类)。
     */
    @AfterEach
    void clearSystemProps() {
        System.clearProperty(ReaderProperties.CONFIG_FILE_KEY);
        System.clearProperty("vb.topic.prefix");
        System.clearProperty("vb.slot.name");
        System.clearProperty("vb.sink.mode");
        System.clearProperty("vb.sink.data-dir");
    }

    /**
     * 场景:classpath 默认文件参与合并——resolve() 的结果必含模板文件写入的必填五项
     * (src/docker 本地 PG 默认值),即"零 -D 参数可起"的验收面;且 connector.class 被
     * 固定值覆盖、三项默认(name/offset 文件/flush 间隔)注入。
     */
    @Test
    void classpathDefaultFileFeedsResolve() {
        Properties props = ReaderProperties.resolve();

        assertEquals("localhost", props.getProperty("database.hostname"), "模板文件的 hostname 基础值生效");
        assertEquals("postgres", props.getProperty("database.dbname"), "模板文件的 dbname 基础值生效");
        assertTrue(ReaderProperties.missingRequired(props).isEmpty(), "模板文件齐备必填五项");
        assertEquals(ReaderProperties.CONNECTOR_CLASS, props.getProperty("connector.class"),
                "connector.class 固定注入");
        assertEquals("vb-stream-reader", props.getProperty("name"), "name 默认注入");
        assertEquals("data/reader-offsets.dat", props.getProperty("offset.storage.file.filename"),
                "offset 文件默认注入");
        assertEquals("1000", props.getProperty("offset.flush.interval.ms"), "flush 间隔默认注入");
    }

    /**
     * 场景:合并次序——同一键文件给基础值、-Dvb.* 覆盖之;-D 项同时可引入文件没有的新键。
     */
    @Test
    void systemPropertyOverridesFileValue() {
        System.setProperty("vb.topic.prefix", "override-prefix");
        System.setProperty("vb.slot.name", "extra-slot");

        Properties props = ReaderProperties.resolve();

        assertEquals("override-prefix", props.getProperty("topic.prefix"),
                "-Dvb.* 覆盖文件基础值(次序:文件 → -D)");
        assertEquals("extra-slot", props.getProperty("slot.name"), "-Dvb.* 可引入文件外的新键");
        assertEquals("localhost", props.getProperty("database.hostname"), "未覆盖的文件基础值原样保留");
    }

    /**
     * 场景:vb.config 指定外部文件——整份替换 classpath 模板(外部文件的 hostname 生效,
     * 模板的 dbname 不再出现——替换语义而非合并语义);文件键即 Debezium 裸键。
     */
    @Test
    void externalConfigFileReplacesClasspathTemplate() throws Exception {
        Path external = Files.writeString(tempDir.resolve("external.properties"),
                "database.hostname=other-host\ntopic.prefix=extprefix\n"
                        + "database.dbname=otherdb\ndatabase.user=u\ndatabase.password=p\n");
        System.setProperty(ReaderProperties.CONFIG_FILE_KEY, external.toString());

        Properties props = ReaderProperties.resolve();

        assertEquals("other-host", props.getProperty("database.hostname"), "外部文件的值生效");
        assertEquals("extprefix", props.getProperty("topic.prefix"), "外部文件的值生效");
        assertEquals("otherdb", props.getProperty("database.dbname"), "替换语义:模板值不再出现");
    }

    /**
     * 场景:vb.config 指向不存在的路径——fail-fast 抛 IllegalStateException(用户明确指定的
     * 路径错误必须立刻报,拒绝静默回落成"看似没配")。
     */
    @Test
    void missingExternalConfigFileFailsFast() {
        System.setProperty(ReaderProperties.CONFIG_FILE_KEY, tempDir.resolve("no-such-file.properties").toString());

        IllegalStateException e = assertThrowsISE();
        assertTrue(e.getMessage().contains("vb.config") || e.getMessage().contains("不存在"),
                "异常文案指向 vb.config 路径错误: " + e.getMessage());
    }

    /**
     * 场景:必填校验——空 Properties 下五项全缺失(清单有序、可作用法提示);补齐任意一项
     * 即从清单消失;masked 对 password 打码且不打码其他键。
     */
    @Test
    void missingRequiredAndMasked() {
        Properties empty = new Properties();
        assertEquals(5, ReaderProperties.missingRequired(empty).size(), "空配置下必填五项全缺失");

        empty.setProperty("database.hostname", "h");
        assertEquals(4, ReaderProperties.missingRequired(empty).size(), "补齐一项即少报一项");

        empty.setProperty("database.password", "secret");
        String masked = ReaderProperties.masked(empty);
        assertFalse(masked.contains("secret"), "password 值不打码不得外泄");
        assertTrue(masked.contains("database.password=********"), "password 以打码占位呈现");
        assertTrue(masked.contains("database.hostname=h"), "其余键原样呈现");
    }

    /**
     * 场景:sink 配置默认——mode 缺省 log、task 缺省取 topic.prefix、目录/滚动参数带默认;
     * 且 -Dvb.sink.* 系统属性不透传进 Debezium props(命名空间剥离)。
     */
    @Test
    void sinkDefaultsToLogWithTaskFromTopicPrefix() {
        System.setProperty("vb.sink.mode", "file");   // 覆盖默认形态供剥离断言,resolveSink 另行断言
        System.setProperty("vb.sink.data-dir", "should-not-leak");

        Properties props = ReaderProperties.resolve();
        assertTrue(props.stringPropertyNames().stream().noneMatch(k -> k.startsWith("sink.")),
                "vb.sink.* 不透传 Debezium props(sink 命名空间归 reader 自用)");

        SinkConfig sink = ReaderProperties.resolveSink(props);
        assertEquals(SinkConfig.Mode.FILE, sink.mode(), "-Dvb.sink.mode 覆盖默认 log");
        assertEquals(Path.of("should-not-leak"), sink.dataDir(), "-Dvb.sink.data-dir 生效");
        assertEquals("vbread", sink.task(), "task 缺省取 topic.prefix(模板文件的值)");
        assertEquals(SinkConfig.DEFAULT_ROLL_MAX_RECORDS, sink.rollMaxRecords(), "滚动条数默认");
        assertEquals(SinkConfig.DEFAULT_ROLL_INTERVAL_MS, sink.rollIntervalMs(), "滚动间隔默认");
    }

    /**
     * 场景:外部文件的 sink.* 裸键作基础值、-Dvb.sink.* 覆盖(两层合并次序);非法 mode
     * 在解析期 fail-fast 抛 IAE。
     */
    @Test
    void externalFileSinkKeysMergedWithSystemOverride() throws Exception {
        Path external = Files.writeString(tempDir.resolve("sink.properties"),
                "database.hostname=h\ndatabase.dbname=d\ndatabase.user=u\ndatabase.password=p\n"
                        + "topic.prefix=fileprefix\n"
                        + "sink.mode=file\nsink.data-dir=file-dir\nsink.task=file-task\n");
        System.setProperty(ReaderProperties.CONFIG_FILE_KEY, external.toString());
        System.setProperty("vb.sink.data-dir", "sys-dir");   // 覆盖文件基础值

        Properties props = ReaderProperties.resolve();
        SinkConfig sink = ReaderProperties.resolveSink(props);
        assertEquals(SinkConfig.Mode.FILE, sink.mode(), "文件键 sink.mode 基础值生效");
        assertEquals(Path.of("sys-dir"), sink.dataDir(), "-Dvb.sink.* 覆盖文件基础值(次序:文件 → -D)");
        assertEquals("file-task", sink.task(), "文件显式 sink.task 优先于 topic.prefix 兜底");

        System.setProperty("vb.sink.mode", "bogus");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ReaderProperties.resolveSink(props));
        assertTrue(e.getMessage().contains("vb.sink.mode"), "非法 mode 的报错指向配置键: " + e.getMessage());
    }

    /**
     * 断言抛 IllegalStateException 的便捷包装(两处用例共用,失败信息带实际异常类型)。
     *
     * @return 捕获的异常
     */
    private static IllegalStateException assertThrowsISE() {
        try {
            ReaderProperties.fileProperties();
        }
        catch (IllegalStateException expected) {
            return expected;
        }
        throw new AssertionError("期望 IllegalStateException 未抛出");
    }
}
