package org.vastdata.vbstream.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * reader 配置面:{@code -Dvb.*} 系统属性剥前缀<b>零映射透传</b>为 Debezium 引擎/连接器配置,
 * 另注入 reader 自有默认值。纯静态工具类,构造后不可变,无状态。
 *
 * <p>透传红利:连接器六个专属项({@code slot.streaming}/{@code slot.two.phase}/{@code pipe.dir}/
 * {@code pipe.roll.cycle}/{@code slot.feedback.interval.ms}/{@code slot.messages})与 engine 高级项
 * ({@code record.processing.order}/{@code offset.commit.policy} 等)零代码可用——例如
 * {@code -Dvb.slot.streaming=parallel -Dvb.slot.two.phase=true} 直达连接器,本类不感知其语义。
 *
 * <p>线程约束:由 main 线程在启动期调用一次;返回的 Properties 随后交给引擎线程只读。
 */
public final class ReaderProperties {

    /** 透传前缀:系统属性 {@code vb.<debezium 键>} → props 键 {@code <debezium 键>}。 */
    private static final String PREFIX = "vb.";

    /** 固定的连接器类(本宿主只服务自研 PostgresStreamConnector,同名透传被覆盖;
     * public——IT 的 props 组装与此同源)。 */
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
     * 责任:从系统属性组装 Debezium props。关键步骤:遍历 {@link System#getProperties()} 取
     * {@code vb.} 前缀项剥前缀入结果(值空串跳过——shell 传空视为未设)→ 强制覆盖
     * {@code connector.class} 为本模块固定值(透传不可覆盖)→ putIfAbsent 三项 reader 默认
     * ({@code name}/{@code offset.storage.file.filename}/{@code offset.flush.interval.ms})。
     * 边界:非 {@code vb.} 前缀项忽略;同键多次出现以最后一次为准(Properties 语义);
     * {@code connector.class} 即使被透传也会被固定值替换。
     *
     * @return 组装完成的 Debezium 引擎/连接器配置
     */
    public static Properties fromSystemProperties() {
        Properties props = new Properties();
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith(PREFIX) && name.length() > PREFIX.length()) {
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
     * 责任:返回必填键中值缺失(null 或空白)的清单——Main 据此决定打印用法退出。
     * 边界:props 为 null 抛 NPE(调用方必持非空)。
     *
     * @param props 已组装的配置
     * @return 缺失键清单(有序,必填序;全部齐备时为空)
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
