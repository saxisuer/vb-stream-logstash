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
