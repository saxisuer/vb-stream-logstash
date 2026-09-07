package org.vastdata.vbstream.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * reader 配置面:<b>三层合并</b>——classpath 配置文件 {@code dbconfig.properties}(基础值)→
 * 系统属性 {@code -Dvb.*}(剥前缀,覆盖文件同名项)→ reader 自有默认值(兜底 putIfAbsent)。
 * 纯静态工具类,构造后不可变,无状态。
 *
 * <p>文件形态:键即 Debezium 键(不带 {@code vb.} 前缀——文件本身就是 reader 专属命名空间);
 * 系统属性经 {@code vb.} 前缀区分归属并可在不改文件的情况下临时覆盖单项(如
 * {@code -Dvb.slot.name=another_slot});要整体换文件时复制模板为外部文件并以
 * {@code -Dvb.config=<路径>} 指定(该键是 reader 的保留键,不进 Debezium props)。
 *
 * <p>透传红利:连接器六个专属项({@code slot.streaming}/{@code slot.two.phase}/{@code pipe.dir}/
 * {@code pipe.roll.cycle}/{@code slot.feedback.interval.ms}/{@code slot.messages})与 engine 高级项
 * ({@code record.processing.order}/{@code offset.commit.policy} 等)无论写在文件还是以 -D 传入,
 * 均零代码直达——本类不感知其语义。
 *
 * <p>线程约束:由 main 线程在启动期调用一次;返回的 Properties 随后交给引擎线程只读。
 */
public final class ReaderProperties {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderProperties.class);

    /** 透传前缀:系统属性 {@code vb.<debezium 键>} → props 键 {@code <debezium 键>}。 */
    private static final String PREFIX = "vb.";

    /** classpath 默认配置文件名(基础值来源;随包分发,模板即 src/docker 本地 PG 默认)。 */
    static final String DEFAULT_CONFIG_FILE = "dbconfig.properties";

    /** 外部配置文件的系统属性键(保留键:剥前缀语义不适用,不进 Debezium props)。 */
    static final String CONFIG_FILE_KEY = "vb.config";

    /** 固定的连接器类(本宿主只服务自研 PostgresStreamConnector,任何来源的同名项被覆盖)。 */
    public static final String CONNECTOR_CLASS =
            "org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector";

    /** 必填键清单:连接器 required 面(hostname/dbname/user/topic.prefix)+ password——
     * Field 层非 required,但冒烟环境必有口令,缺失提前报用法比运行期认证失败友好。 */
    private static final List<String> REQUIRED_KEYS = List.of(
            "database.hostname", "database.dbname", "database.user", "database.password", "topic.prefix");

    /** password 的日志打码值。 */
    private static final String MASK = "********";

    private ReaderProperties() {
    }

    /**
     * 责任:组装最终 Debezium props(三层合并,后者覆盖前者)。关键步骤:①{@link #fileProperties}
     * 读配置文件为基础值(默认 classpath 的 {@code dbconfig.properties},{@code vb.config} 指定
     * 外部文件时整份替换);②遍历系统属性取 {@code vb.} 前缀项剥前缀覆盖(值空串跳过——shell
     * 传空视为未设;保留键 {@code vb.config} 本身排除);③强制覆盖 {@code connector.class} 为固定值
     * → putIfAbsent 三项默认({@code name}/{@code offset.storage.file.filename}/
     * {@code offset.flush.interval.ms})。
     * 边界:配置文件缺失(classpath 无且未指定 vb.config)按空集处理——纯 -D 形态仍可运行;
     * 指定了 {@code vb.config} 但文件不存在/不可读抛 IllegalStateException(fail-fast,明确指向
     * 错误的路径);同键多来源以系统属性为准。
     *
     * @return 组装完成的 Debezium 引擎/连接器配置
     */
    public static Properties resolve() {
        Properties props = new Properties();
        props.putAll(fileProperties());
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith(PREFIX) && name.length() > PREFIX.length() && !CONFIG_FILE_KEY.equals(name)) {
                String value = System.getProperty(name, "");
                if (!value.isEmpty()) {
                    props.setProperty(name.substring(PREFIX.length()), value);
                }
            }
        }
        props.setProperty("connector.class", CONNECTOR_CLASS);
        props.putIfAbsent("name", "vb-stream-reader");
        props.putIfAbsent("offset.storage.file.filename", "data/reader-offsets.dat");
        props.putIfAbsent("offset.flush.interval.ms", "1000");
        return props;
    }

    /**
     * 责任:读配置文件的基础值。关键步骤:系统属性 {@code vb.config} 指定了外部文件就先
     * {@link Files#exists} 预检——不存在抛 IllegalStateException(用户明确指定的路径错了
     * 必须立刻报,不能静默回落成"看似没配"),存在则开它(整份<b>替换</b> classpath 模板);
     * 未指定则从 classpath 读 {@link #DEFAULT_CONFIG_FILE}(随包分发的默认模板,缺失按空集
     * ——classpath 由装配方保证,缺了也不排除用户走纯 -D 形态)→ Properties.load → INFO 一行
     * (来源与条数)。
     * 边界:文件读失败(IO)与内容非法(load 异常)均转 IllegalStateException(fail-fast);
     * 空文件合法(全靠 -D)。
     *
     * @return 文件提供的配置项(可能为空集,绝不返回 null)
     */
    static Properties fileProperties() {
        Properties fileProps = new Properties();
        String external = System.getProperty(CONFIG_FILE_KEY);
        if (external != null && !Files.exists(Path.of(external))) {
            throw new IllegalStateException("vb.config 指定的配置文件不存在: " + external
                    + "(路径错误 fail-fast,拒绝静默回落成'看似没配')");
        }
        try (InputStream in = external != null
                ? Files.newInputStream(Path.of(external))
                : ReaderProperties.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE)) {
            if (in != null) {
                fileProps.load(in);
                LOG.info("配置文件已加载({} 项): {}", fileProps.size(),
                        external != null ? external : "classpath:" + DEFAULT_CONFIG_FILE);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("配置文件读取失败: "
                    + (external != null ? external : "classpath:" + DEFAULT_CONFIG_FILE), e);
        }
        return fileProps;
    }

    /**
     * 责任:返回必填键中值缺失(null 或空白)的清单——Main 据此决定打印用法退出。
     * 边界:props 为 null 抛 NPE(调用方必持非空)。
     *
     * @param props 已组装的配置
     * @return 缺失键清单(必填序;全部齐备时为空)
     */
    public static List<String> missingRequired(Properties props) {
        List<String> missing = new ArrayList<>();
        for (String key : REQUIRED_KEYS) {
            String value = props.getProperty(key);
            if (value == null || value.isBlank()) {
                missing.add(key);
            }
        }
        return missing;
    }

    /**
     * 责任:配置的日志渲染面——{@code database.password} 的值替换为打码占位(配置打印不泄密),
     * 其余原样。关键步骤:TreeMap 按键排序拷贝(输出稳定可 diff)→ password 键值替换为
     * {@code ********} → {@code key=value} 逗号拼接。
     * 边界:只打码 {@code database.password} 精确键;URL 内嵌凭证等形态不在此职责面。
     *
     * @param props 已组装的配置
     * @return 打码后的单行渲染文本
     */
    public static String masked(Properties props) {
        Map<String, String> sorted = new TreeMap<>();
        props.forEach((k, v) -> sorted.put(String.valueOf(k), String.valueOf(v)));
        sorted.put("database.password", MASK);
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }
}
