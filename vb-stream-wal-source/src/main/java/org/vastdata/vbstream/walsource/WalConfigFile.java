package org.vastdata.vbstream.walsource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.RenderOptions;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

/**
 * 冒烟 Main 的配置文件基础值层（2026-10-12）：工作目录 {@code walconfig.properties}（或
 * {@code -Dvb.config=<路径>} 显式指定外部文件）为三层合并的文件层——系统属性
 * {@code -Dvb.wal.*} 同键覆盖（Main 层），缺键由 {@link WalSource} 构造器默认值兜底
 * （单一默认值来源不变）。文件键与 {@code -D} 参数完全同形（全 {@code vb.wal.*} 形态，
 * 文档键表直接照抄），按门面十八键 + 渲染面四键（{@code vb.wal.render.*}）白名单
 * 过滤——白名单外键 WARN 一行并忽略（wal-source
 * 键面封闭，未知键按拼写错处理不静默吞；{@code vb.wal.mode} 是 Main 层直读分派键，
 * 计划裁定不进白名单——文件里写它同按白名单外键忽略，离线档切换只走
 * {@code -Dvb.wal.mode=offline}）。
 *
 * <p>文件来源裁定：{@code vb.config} 显式指定时整份替换默认查找，文件不存在/不可读/解析
 * 失败抛 {@link IllegalStateException}（fail-fast——明确的路径错误不静默回落，对齐
 * vb-stream-reader 先例）；未指定时读工作目录默认名，默认名不存在按空集处理（纯
 * {@code -D} 形态，行为与文件支持前逐字节一致）。{@code vb.config} 是保留键，不进
 * 门面配置（白名单天然排除）。</p>
 *
 * <p>空串语义：文件键值为空串视为未设（写入的键不生效，留给 {@code -D} 或默认值）。
 * 纯静态工具类，启动期 main 线程调用一次，无状态线程安全。</p>
 */
final class WalConfigFile {

    private static final Logger LOG = LoggerFactory.getLogger(WalConfigFile.class);

    /** 工作目录默认配置文件名（未指定 vb.config 时的查找目标；不存在即纯 -D 形态）。 */
    static final String DEFAULT_FILE_NAME = "walconfig.properties";

    /** 外部配置文件的系统属性键（保留键——不进门面配置，仅决定文件层来源）。 */
    static final String CONFIG_FILE_KEY = "vb.config";

    /**
     * 白名单：冒烟面全部合法键（{@link WalSource} 十八键 + {@link RenderOptions}
     * 渲染面四键 {@code vb.wal.render.*}——键常量单一来源，本类不复刻字面量；含
     * 离线回放两键 offline.dir/offline.endLsn，offline replay）。文件中出现在此
     * 集合外的键 WARN 并忽略。
     */
    private static final Set<String> KNOWN_KEYS = Set.of(
            WalSource.KEY_HOST,
            WalSource.KEY_PORT,
            WalSource.KEY_DB,
            WalSource.KEY_USER,
            WalSource.KEY_PASS,
            WalSource.KEY_SLOT,
            WalSource.KEY_STATE_DIR,
            WalSource.KEY_STATE_INTERVAL_MS,
            WalSource.KEY_STATE_EVENTS,
            WalSource.KEY_TABLES,
            WalSource.KEY_DML,
            WalSource.KEY_OG_SOURCE,
            WalSource.KEY_OG_RX_MODE,
            WalSource.KEY_OG_RX_DIR,
            WalSource.KEY_OG_RX_BIN,
            WalSource.KEY_OG_JDBC_PREFIX,
            WalSource.KEY_OFFLINE_DIR,
            WalSource.KEY_OFFLINE_END_LSN,
            RenderOptions.KEY_FLOAT_STYLE,
            RenderOptions.KEY_DATE_FORMAT,
            RenderOptions.KEY_TIME_FORMAT,
            RenderOptions.KEY_TRUNCATE);

    /** 私有构造器：纯静态工具类不可实例。 */
    private WalConfigFile() {
    }

    /**
     * 解析文件层基础值（生产入口）：{@code vb.config} 系统属性显式指定时读该文件
     * （缺失即 ISE），否则读工作目录默认名（缺失即空集）。
     *
     * @return 文件层键值（已过白名单；可能为空集）
     */
    static Properties loadBase() {
        return loadBase(Paths.get(DEFAULT_FILE_NAME));
    }

    /**
     * 解析文件层基础值（参数化核心，测试面注入默认路径）。
     *
     * <p>关键步骤：{@code vb.config} 系统属性非空时整份替换默认查找（文件不存在/读取
     * 失败抛 ISE——fail-fast）；否则默认路径存在才读（缺失 DEBUG 一行，返回空集——
     * 纯 {@code -D} 形态与文件支持前行为一致）。空串 {@code vb.config} 视为未指定。</p>
     *
     * @param defaultFile 默认查找路径（生产为工作目录 walconfig.properties）
     * @return 文件层键值（已过白名单；可能为空集）
     * @throws IllegalStateException vb.config 指定的文件不存在或读取/解析失败
     */
    static Properties loadBase(Path defaultFile) {
        String explicit = System.getProperty(CONFIG_FILE_KEY, "");
        if (!explicit.isEmpty()) {
            return loadFile(Path.of(explicit));
        }
        if (Files.exists(defaultFile)) {
            return loadFile(defaultFile);
        }
        LOG.debug("工作目录无 {}（纯 -D 形态运行）", DEFAULT_FILE_NAME);
        return new Properties();
    }

    /**
     * 读单个配置文件并按白名单过滤。
     *
     * <p>关键步骤：UTF-8 {@link Properties#load(Reader)}（ISO 流式读非 ASCII 会乱码）；
     * 白名单内且值非空串的键进结果（空串视为未设）；白名单外键 WARN 一行含键名与
     * 白名单提示。文件不存在/不可读/内容坏（损坏的 {@code \\u} 转义等——load 抛
     * IllegalArgumentException）抛 ISE（确定性错误，路径由调用方给出，消息带路径
     * 便于归因；注：行尾续行符残缺等多数"看似坏"的内容 Properties 均宽容吞下不抛）。</p>
     *
     * @param file 配置文件路径
     * @return 过滤后的键值（可能为空集——文件存在但无合法键）
     * @throws IllegalStateException 文件不存在/读取/解析失败
     */
    static Properties loadFile(Path file) {
        Properties raw = new Properties();
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            raw.load(r);
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalStateException("配置文件读取失败: " + file, e);
        }
        Properties out = new Properties();
        for (String key : raw.stringPropertyNames()) {
            if (!KNOWN_KEYS.contains(key)) {
                LOG.warn("配置文件忽略白名单外键 {}（合法键即 vb.wal.* 二十二键，拼写错不静默吞；vb.wal.mode 是 -D 专属分派键）", key);
                continue;
            }
            String value = raw.getProperty(key, "");
            if (!value.isEmpty()) {
                out.setProperty(key, value);
            }
        }
        return out;
    }
}
