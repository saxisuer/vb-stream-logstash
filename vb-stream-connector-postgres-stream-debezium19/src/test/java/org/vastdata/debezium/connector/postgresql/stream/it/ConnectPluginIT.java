package org.vastdata.debezium.connector.postgresql.stream.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 真连接器验收(1.9.7 线对齐 3.6.1 模块 MS6/R4):插件装进真 Kafka Connect 运行(非
 * embedded engine)——类加载面(plugin.path 隔离类加载器加载本插件与 lib/ 依赖)、REST
 * 配置暴露面(configDef/validate 三层防线)、序列化面、连接器生命周期全走真路径。
 * <p>关键步骤:{@code @BeforeAll} 核对 assembly 产物结构(根恰一连接器 jar 且带 ServiceLoader
 * 清单/lib 依赖齐/excluded 零命中,缺产物即 fail-fast 提示先 package)→ 起容器组
 * (cp-kafka 7.1.0 ZK 单容器 broker + cp-kafka-connect 7.1.0(叠 JDK 17)Connect,
 * plugin.path 绑定插件目录)→ await Connect REST 就绪 → PUT /connectors 建连接器
 * (database.* 指向 {@link StreamPgTestEnv}.PG,database.server.name=ms6connect——
 * 1.9.7 的逻辑名键,2.0+ 才改名 topic.prefix;快照/事务元数据两键不设——默认注入在真
 * Connect 生效的顺带验收)→ 夹具表 publication 预建 → INSERT → KafkaConsumer 轮询
 * (topic ms6connect.public.t_plug)→ 断言记录 op=c/值等/零 op=r(默认 never)
 * + 事务元数据 topic 有 BEGIN/END(默认 true)→ {@code @AfterEach} 删槽、
 * {@code @AfterAll} 删连接器与容器。
 * <p>选型注记:CP 7.1.0 = Apache Kafka 3.1.0,与 Debezium 1.9.7 官方 Kafka 矩阵及本模块
 * connect-api 3.1.0 编译面三方对齐。容器形态与 3.6.1 版的三处不同:①cp 7.1 镜像不支持
 * KRaft(启动脚本不处理 CLUSTER_ID,缺 KAFKA_ZOOKEEPER_CONNECT 即拒启),而
 * testcontainers 2.x 的 ConfluentKafkaContainer 恒以 KRaft 参数面起容器——broker 改用
 * 镜像唯一受支持的 ZK 形态,ZooKeeper 经镜像自带的 zookeeper-server-start 单容器内嵌
 * (经典 1.x KafkaContainer 形态);②cp 7.1 镜像 JVM 为 Zulu 11,而本模块字节码按 spec D3
 * 钉在 release 17,connect 容器以 eclipse-temurin:17-jre 覆盖层换 JVM——Kafka Connect
 * 3.1.0 runtime 原样保留,与嵌入式 IT(本模块 27 用例在 JDK 17 surefire 上跑
 * connect-runtime 3.1.0)同组合,spec §6.3 风险表口径;③本模块 test 面 commons-lang3 被
 * 1.9.7 传递依赖钉在 3.8.1,而 testcontainers 2.x 的 commons-compress 1.28 写路径需
 * 3.14+(ArrayFill)——一切经 tar 的传输(copyFileToContainer/ImageFromDockerfile 构建上下
 * 文)在 surefire 类路径上必炸,故镜像构建绕道 docker-java build API + 单文件 ustar 手写
 * 上下文、插件目录用文件系统 bind 而非 tar 拷贝、broker 启动脚本整体内联为 CMD(详见
 * 各自方法的 javadoc;约束:本任务不动模块 pom)。Kafka 3.1 已移除内部转换器配置
 * (3.6.1 版的 CONNECT_INTERNAL_*_CONVERTER 键在此形态下省略,运行时恒用内部
 * JsonConverter + schemas 禁用)。插件内 Chronicle Queue 经 KAFKA_OPTS 携带与引擎
 * surefire 同源的 --add-opens 清单。
 * 边界:cp 镜像大、首次拉取慢(JDK 覆盖层另含 eclipse-temurin:17-jre 拉取)——独立类可
 * {@code -Dtest} 单跑;断言超时口径 60s 级 await。
 */
class ConnectPluginIT {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectPluginIT.class);

    /** 本 IT 专用复制槽(PG 单例跨类共享,各 IT 独立槽名;前后清删)。 */
    private static final String SLOT = "ms6_plug";

    /** 本 IT 专用 publication(@BeforeAll 预建,流式源免 autocreate 权限面)。 */
    private static final String PUBLICATION = "pub_ms6_plug";

    /** 夹具表:id 主键 + 文本列,topic 路径 public.t_plug 由此得名。 */
    private static final String TABLE = "t_plug";

    /** 逻辑名前缀:数据 topic ms6connect.public.t_plug、事务元数据 topic ms6connect.transaction。 */
    private static final String TOPIC_PREFIX = "ms6connect";

    /** 数据 topic 全名(Debezium 默认命名:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String DATA_TOPIC = TOPIC_PREFIX + ".public." + TABLE;

    /** 事务元数据 topic 全名(TransactionMonitor 约定 &lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = TOPIC_PREFIX + ".transaction";

    /** Connect 侧连接器实例名(REST 路径段,亦作 Kafka Connect 连接器名)。 */
    private static final String CONNECTOR_NAME = "ms6-plug";

    /** Kafka broker 镜像:CP 7.1.0 = AK 3.1.0(与 Debezium 1.9.7 官方矩阵对齐;镜像仅支持 ZK 模式)。 */
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.1.0";

    /** Connect 运行时镜像 tag:cp-kafka-connect 7.1.0(= Kafka Connect 3.1.0)叠 eclipse-temurin:17-jre。 */
    private static final String CONNECT_IMAGE = "vb-stream/cp-kafka-connect:7.1.0-jdk17";

    /**
     * Connect 镜像的构建清单:cp-kafka-connect:7.1.0 基座 + 从 eclipse-temurin:17-jre 拷入
     * 的 JDK 17(镜像自带 Zulu 11 加载不了本模块 release 17 字节码)——多阶段 COPY 不引入
     * 额外下载源,Kafka Connect 3.1.0 runtime 及 Confluent 启动脚本(CONNECT_* env 转译面)
     * 原样保留;tag 固定,镜像存在即跳过重建,二次运行零开销。
     */
    private static final String CONNECT_DOCKERFILE = """
            FROM eclipse-temurin:17-jre AS jdk17
            FROM confluentinc/cp-kafka-connect:7.1.0
            COPY --from=jdk17 /opt/java/openjdk /opt/java/openjdk
            ENV JAVA_HOME=/opt/java/openjdk
            ENV PATH="/opt/java/openjdk/bin:${PATH}"
            """;

    /** assembly 产物目录(相对模块 basedir——surefire 工作目录即模块根)。 */
    private static final Path PLUGIN_DIR = Paths.get("target", "vb-stream-connector-postgres-stream-debezium19-plugin");

    /** assembly descriptor 显式排除族的 jar 文件名前缀(Connect runtime 已提供,零命中验收)。 */
    private static final Set<String> FORBIDDEN_JAR_PREFIXES = Set.of(
            "connect-api-", "kafka-clients-", "slf4j-api-",
            "zstd-jni-", "lz4-java-", "snappy-java-", "jakarta.ws.rs-api-");

    /** 连接器 jar 内的 ServiceLoader 清单路径:真 Connect 插件发现的必经登记处。 */
    private static final String CONNECTOR_SERVICE_ENTRY =
            "META-INF/services/org.apache.kafka.connect.source.SourceConnector";

    /** 常规等待口径(状态轮询/topic 出现/收数)——brief 钉死的 60s 级。 */
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /** Kafka Connect 容器内挂载插件的根目录(plugin.path 的值,每子目录一个隔离插件)。 */
    private static final String CONTAINER_PLUGIN_ROOT = "/plugins";

    /** 引擎 surefire argLine 同源的 --add-opens 清单(chronicle mmap 在模块系统下反射开放包)。 */
    private static final String CONNECT_ADD_OPENS = "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED "
            + "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
            + "--add-opens jdk.unsupported/sun.misc=ALL-UNNAMED "
            + "--add-opens java.base/sun.nio.fs=ALL-UNNAMED "
            + "--add-opens java.base/java.lang.reflect=ALL-UNNAMED";

    /** test 侧 JSON 构造/解析(jackson 随 connect-runtime 传递,test 域可达)。 */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** test 侧 REST 客户端(JDK 裸调,不引新依赖)。 */
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static Network network;
    private static ZkKafkaContainer kafka;
    private static GenericContainer<?> connect;

    /**
     * 起容器组并预建夹具:①断言 assembly 产物结构合规(缺产物/结构破坏即 fail-fast,
     * 文案指向可操作的补救命令);②PG 侧 DROP+CREATE 夹具表与 publication(自愈:重复跑
     * 或残留旧 schema 均收敛到已知形态);③确保 Connect 覆盖层镜像存在(缺则构建,见
     * {@link #ensureConnectImage()});④起 Kafka(ZK 单容器 broker,同网络别名 kafka:19092
     * 供 Connect 引导)与 Connect(plugin.path 绑插件目录,REST 就绪为放行条件)——两容器
     * 共享自定义网络,PG 走 host.docker.internal 回宿主映射端口(host-gateway 别名保证
     * Linux 亦可解析)。
     */
    @BeforeAll
    static void startCluster() throws Exception {
        assertPluginArtifact();
        StreamPgTestEnv.execSql(
                "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + " (id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS " + PUBLICATION,
                "CREATE PUBLICATION " + PUBLICATION + " FOR TABLE " + TABLE);

        ensureConnectImage();
        network = Network.newNetwork();
        kafka = new ZkKafkaContainer()
                .withNetwork(network)
                // 网络别名即 BROKER listener 的通告主机:Connect 容器经 kafka:19092 引导
                .withNetworkAliases("kafka");
        kafka.start();

        connect = new GenericContainer<>(DockerImageName.parse(CONNECT_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:19092")
                .withEnv("CONNECT_REST_PORT", "8083")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                .withEnv("CONNECT_GROUP_ID", "ms6-connect-it")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "_ms6_connect_config")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "_ms6_connect_offsets")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "_ms6_connect_status")
                // 单 broker:三大内部 topic 副本因子钉 1(默认 3 会卡在 ISR 不足)
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                // 数据面 JSON 直读(schemas.enable=false——payload 平铺,test 断言少一层包裹);
                // 内部转换器键省略——Kafka 3.0 起移除,运行时恒用内部 JsonConverter(schemas 禁用)
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_PLUGIN_PATH", CONTAINER_PLUGIN_ROOT)
                // 插件内 Chronicle Queue 的 mmap 需要(引擎 surefire argLine 同款开放包)
                .withEnv("KAFKA_OPTS", CONNECT_ADD_OPENS)
                // Connect 容器 → 宿主映射端口的 PG(Docker Desktop 原生解析,Linux 走 host-gateway)
                .withExtraHost("host.docker.internal", "host-gateway")
                // 宿主插件目录直 bind 进容器(不走 testcontainers 的 tar 拷贝——commons-compress
                // 写路径在本模块 test 类路径上损坏,见类 javadoc;只读,daemon 侧与 docker -v 同语义)
                .withFileSystemBind(PLUGIN_DIR.toAbsolutePath().toString(),
                        CONTAINER_PLUGIN_ROOT + "/vb-stream-connector-postgres-stream-debezium19", BindMode.READ_ONLY)
                // Connect REST 起来 = worker 已入组、配置 topic 读毕、插件扫描完成
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        connect.start();
        LOG.info("容器组就绪: kafka={} connect-rest=http://{}:{}",
                kafka.getBootstrapServers(), connect.getHost(), connect.getMappedPort(8083));
    }

    /**
     * 端到端主验收:REST 建连接器(不设 snapshot.mode/provide.transaction.metadata——
     * 默认注入在真 Connect 的顺带验收)→ 等 connector/task 双 RUNNING → walsender 挂上
     * (建槽+建流完成的可观测汇合点,防写入落 restart_lsn 之前的竞态)→ INSERT 两行 →
     * 消费断言:数据 topic 恰 op=c 且值等、零 op=r;事务元数据 topic 有 BEGIN/END。
     */
    @Test
    void pluginRunsInRealConnectAndStreamsToKafka() throws Exception {
        putConnectorConfig();
        awaitConnectorRunning();
        StreamPgTestEnv.awaitWalsender(SLOT, TIMEOUT.toMillis());

        Map<Integer, String> expected = Map.of(1, "ms6-r4-one", 2, "ms6-r4-two");
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (1, 'ms6-r4-one'), (2, 'ms6-r4-two')");
        consumeAndAssert(expected);
    }

    /**
     * 每用例后删槽(PG 单例跨 IT 类共享,槽残留会让下轮从旧 confirmed_flush 续传静默吞数据);
     * dropSlotQuietly 先杀 walsender 再删,幂等。连接器与容器的收敛在 {@link #teardown()}。
     */
    @AfterEach
    void dropSlot() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 类尾收敛:先 REST 删连接器(让任务优雅停,断开复制流)再二次删槽(防 DELETE 与
     * walsender 退出竞态漏删),最后停 Connect/Kafka 容器与网络——任一步失败不阻断后续
     * (逐项 catch,收敛链不短路的 best-effort)。
     */
    @AfterAll
    static void teardown() {
        if (connect != null && connect.isRunning()) {
            try {
                rest("DELETE", "/connectors/" + CONNECTOR_NAME, null);
            }
            catch (Exception e) {
                LOG.warn("删除连接器 {} 失败(容器即将销毁,忽略): {}", CONNECTOR_NAME, e.getMessage());
            }
        }
        StreamPgTestEnv.dropSlotQuietly(SLOT);
        if (connect != null) {
            connect.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    /**
     * 核对 assembly 产物结构(打包面的消费侧契约,结构破坏即打包回归):
     * ①目录存在(缺产物 fail-fast,文案指向先跑 package 的可操作命令);
     * ②根下恰一 jar 且是本连接器 jar;③该 jar 带 ServiceLoader 清单(真 Connect 插件
     * 发现的必经登记——embedded engine 直传 Class 不走此路径,是真容器验收独有的死法);
     * ④lib/ 非空;⑤排除族(connect-api/kafka-clients/slf4j-api 及其独占子件)在根+lib
     * 零命中。
     */
    private static void assertPluginArtifact() throws Exception {
        if (!Files.isDirectory(PLUGIN_DIR)) {
            throw new IllegalStateException("assembly 产物缺失: " + PLUGIN_DIR.toAbsolutePath()
                    + " —— 先运行 mvn -pl vb-stream-connector-postgres-stream-debezium19 package -DskipTests 再跑本 IT");
        }
        List<Path> rootJars = jarChildren(PLUGIN_DIR);
        assertThat(rootJars).as("插件根应恰一连接器 jar(两连接器并存的打包形态)").hasSize(1);
        Path connectorJar = rootJars.get(0);
        assertThat(connectorJar.getFileName().toString())
                .as("根 jar 应是本连接器 jar").startsWith("vb-stream-connector-postgres-stream-debezium19-");
        try (JarFile jar = new JarFile(connectorJar.toFile())) {
            assertThat(jar.getEntry(CONNECTOR_SERVICE_ENTRY)).as("连接器 jar 应带 ServiceLoader 清单 %s"
                    + "(真 Connect 据此发现插件,缺席即 connector class not found)", CONNECTOR_SERVICE_ENTRY).isNotNull();
        }
        Path lib = PLUGIN_DIR.resolve("lib");
        assertThat(jarChildren(lib)).as("lib/ 应非空(runtime 依赖集)").isNotEmpty();

        List<String> allJars = new ArrayList<>();
        jarChildren(PLUGIN_DIR).forEach(p -> allJars.add(p.getFileName().toString()));
        jarChildren(lib).forEach(p -> allJars.add(p.getFileName().toString()));
        List<String> forbidden = allJars.stream()
                .filter(n -> FORBIDDEN_JAR_PREFIXES.stream().anyMatch(n::startsWith))
                .toList();
        assertThat(forbidden).as("排除族 jar 应零命中(Connect runtime 已提供,重复类必炸)").isEmpty();
    }

    /**
     * 列目录下全部普通文件 jar(非递归;目录不存在即空列表,由调用方断言语义兜底)。
     *
     * @param dir 待列目录
     * @return jar 文件列表(目录序)
     */
    private static List<Path> jarChildren(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> Files.isRegularFile(p)
                            && p.getFileName().toString().endsWith(".jar"))
                    .toList();
        }
    }

    /**
     * 确保 Connect 覆盖层镜像({@link #CONNECT_IMAGE})存在,缺席则构建。不走
     * testcontainers 的 {@code ImageFromDockerfile}——其构建上下文经 commons-compress
     * 打 tar,在本模块 test 类路径上必炸(commons-lang3 3.8.1 缺 1.28 所需的 ArrayFill,
     * 见类 javadoc);改用 docker-java 的 build API 直接喂自构的 tar 字节(单文件
     * Dockerfile 上下文,{@link #tarOfSingleFile(String, byte[])} 手写 ustar)。基座与
     * JDK 源镜像的拉取由 daemon 在 build 内完成(本地已缓存则零拉取);镜像已存在时
     * 跳过构建(幂等,重跑零开销)。
     */
    private static void ensureConnectImage() {
        DockerClient docker = DockerClientFactory.instance().client();
        try {
            docker.inspectImageCmd(CONNECT_IMAGE).exec();
            LOG.info("Connect 覆盖层镜像已存在,跳过构建: {}", CONNECT_IMAGE);
            return;
        }
        catch (NotFoundException e) {
            // 镜像缺席——走下方构建
        }
        byte[] context = tarOfSingleFile("Dockerfile", CONNECT_DOCKERFILE.getBytes(StandardCharsets.UTF_8));
        String imageId = docker.buildImageCmd()
                .withTarInputStream(new ByteArrayInputStream(context))
                .withTags(Set.of(CONNECT_IMAGE))
                .start()
                .awaitImageId();
        LOG.info("Connect 覆盖层镜像构建完成: {} -> {}", CONNECT_IMAGE, imageId);
    }

    /**
     * 手写单文件 tar(ustar 格式):512B 头(name/mode/uid/gid/size/mtime/checksum/
     * typeflag/magic+version)+ 512 对齐的数据块 + 两块全零 EOF。Docker daemon 侧
     * (Go archive/tar)按 POSIX ustar 解析。存在意义:绕开本模块 test 类路径上损坏的
     * commons-compress 写路径(唯一入口是构建上下文这一份小文件,不值得也不允许动 pom)。
     *
     * @param name    归档内文件名(须短于 100 字节,无路径前缀需求)
     * @param content 文件字节
     * @return 完整 tar 字节流
     */
    private static byte[] tarOfSingleFile(String name, byte[] content) {
        byte[] header = new byte[512];
        writeAscii(header, 0, name);
        writeAscii(header, 100, "0000644");   // mode:rw-r--r--
        writeAscii(header, 108, "0000000");   // uid
        writeAscii(header, 116, "0000000");   // gid
        writeAscii(header, 124, String.format("%011o", content.length)); // size(11 八进制位,余位 NUL)
        writeAscii(header, 136, "00000000000");                            // mtime=0
        header[156] = '0';                    // typeflag:普通文件
        writeAscii(header, 257, "ustar");     // magic(NUL 由 writeAscii 的零填充保证)
        header[262] = 0;
        writeAscii(header, 263, "00");        // version:POSIX
        // checksum:以 8 空格占位求全头无符号和,回写 6 位八进制 + NUL + 空格(POSIX 约定)
        Arrays.fill(header, 148, 156, (byte) ' ');
        int checksum = 0;
        for (byte b : header) {
            checksum += b & 0xff;
        }
        writeAscii(header, 148, String.format("%06o", checksum));
        header[154] = 0;
        header[155] = ' ';
        int dataBlocks = (content.length + 511) / 512;
        byte[] tar = new byte[512 + dataBlocks * 512 + 1024];
        System.arraycopy(header, 0, tar, 0, 512);
        System.arraycopy(content, 0, tar, 512, content.length);
        return tar;
    }

    /**
     * 把 ASCII 文本写进字节数组指定偏移(不写结尾 NUL——目标数组已零初始化,NUL 由
     * 未写区域天然保证;越界即抛 ArrayIndexOutOfBoundsException,构建即错即修)。
     *
     * @param target 目标数组(tar 头)
     * @param offset 起始偏移
     * @param text   待写文本
     */
    private static void writeAscii(byte[] target, int offset, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(bytes, 0, target, offset, bytes.length);
    }

    /**
     * PUT 连接器配置(真 REST 生命周期入口:走 validate → configDef 暴露面 → 任务调度):
     * database 四件套指向 PG 容器的宿主映射端点(host.docker.internal + 映射端口),
     * 槽/publication/database.server.name 三件套钉本 IT 专用名(逻辑名键是 1.9.7 的
     * database.server.name——2.0+ 才改名 topic.prefix,1.9.7 对 topic.prefix 不识别),
     * pipe.dir 用容器内绝对路径(默认相对路径按 worker 工作目录解析,跨容器不确定);
     * snapshot.mode 与 provide.transaction.metadata 有意不设——默认注入(never/true)
     * 在真 Connect 生效。
     */
    private static void putConnectorConfig() throws Exception {
        ObjectNode config = JSON.createObjectNode();
        config.put("connector.class", PostgresStreamConnector.class.getName());
        config.put("database.hostname", "host.docker.internal");
        config.put("database.port", String.valueOf(
                StreamPgTestEnv.PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)));
        config.put("database.dbname", StreamPgTestEnv.PG.getDatabaseName());
        config.put("database.user", StreamPgTestEnv.PG.getUsername());
        config.put("database.password", StreamPgTestEnv.PG.getPassword());
        config.put("database.server.name", TOPIC_PREFIX);
        config.put("slot.name", SLOT);
        config.put("publication.name", PUBLICATION);
        config.put("pipe.dir", "/tmp/ms6-pipe-queue");
        // PUT /connectors/{name}/config 的请求体即扁平配置 map 本身(名字取路径段)——
        // {"name":..,"config":{..}} 包装是 POST /connectors 的形态,Connect 3.1 在本端点
        // 按 Map<String,String> 反序列化,包装体即 500(token START_OBJECT 串不进 String)
        HttpResponse<String> resp = rest("PUT", "/connectors/" + CONNECTOR_NAME + "/config", config.toString());
        assertThat(resp.statusCode()).as("PUT 连接器配置应成功(200 更新/201 新建), body=%s", resp.body())
                .isIn(200, 201);
    }

    /**
     * 轮询连接器与任务双 RUNNING(60s):PUT 后任务经调度→启动→建槽建流,状态短暂
     * UNASSIGNED/PROVISIONING 属正常;connector/task 任一 FAILED 即 fail-fast 并附
     * Connect 容器日志尾部(类加载/配置面问题的第一现场在 worker 日志)。
     */
    private static void awaitConnectorRunning() throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (true) {
            HttpResponse<String> resp = rest("GET", "/connectors/" + CONNECTOR_NAME + "/status", null);
            JsonNode root = JSON.readTree(resp.body());
            String connectorState = root.path("connector").path("state").asText("");
            JsonNode tasks = root.path("tasks");
            String taskState = tasks.isArray() && !tasks.isEmpty()
                    ? tasks.get(0).path("state").asText("") : "";
            if ("RUNNING".equals(connectorState) && "RUNNING".equals(taskState)) {
                LOG.info("连接器 {} 双 RUNNING", CONNECTOR_NAME);
                return;
            }
            if ("FAILED".equals(connectorState) || "FAILED".equals(taskState)) {
                throw new AssertionError("连接器 FAILED: connector=" + connectorState + " task=" + taskState
                        + " —— Connect 日志尾部:\n" + connectLogsTail());
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("连接器 60s 内未达双 RUNNING: connector=" + connectorState
                        + " task=" + taskState + " —— Connect 日志尾部:\n" + connectLogsTail());
            }
            Thread.sleep(500);
        }
    }

    /**
     * test 侧消费并断言(60s):assign+seekToBeginning 两 topic 全分区(免 group 管理的
     * 确定性读位),轮询至期望清空且事务元数据收齐 BEGIN/END。断言面:
     * ①数据 topic 每条 op=c 且 after.id/after.v 与写入值等(值完整性);②数据 topic
     * 的 op 集恰 {"c"}——零 op=r 证明默认注入的 snapshot.mode=never 在真 Connect 生效;
     * ③事务元数据 topic 有 BEGIN/END——证明默认注入的 provide.transaction.metadata=true 生效。
     *
     * @param expected id → 写入的 v 文本(值等断言的期望源)
     */
    private static void consumeAndAssert(Map<Integer, String> expected) throws Exception {
        Map<Integer, String> pending = new HashMap<>(expected);
        Set<String> dataOps = new HashSet<>();
        Set<String> txStatuses = new HashSet<>();
        List<String> mismatches = new ArrayList<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "ms6-plug-it-" + System.nanoTime());
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = awaitPartitions(consumer, DATA_TOPIC, TX_TOPIC);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline
                    && !(pending.isEmpty() && txStatuses.containsAll(Set.of("BEGIN", "END")))) {
                for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofSeconds(1))) {
                    JsonNode value = JSON.readTree(rec.value());
                    if (DATA_TOPIC.equals(rec.topic())) {
                        dataOps.add(value.path("op").asText(""));
                        JsonNode after = value.path("after");
                        if (after.isObject() && after.hasNonNull("id")) {
                            int id = after.path("id").asInt();
                            String v = after.path("v").asText("");
                            String want = pending.get(id);
                            if (want != null) {
                                if (!want.equals(v)) {
                                    mismatches.add("id=" + id + " v=" + v + " 期望 " + want);
                                }
                                pending.remove(id);
                            }
                        }
                    }
                    else if (TX_TOPIC.equals(rec.topic())) {
                        txStatuses.add(value.path("status").asText(""));
                    }
                }
            }
        }
        assertThat(mismatches).as("数据记录值应与写入值等").isEmpty();
        assertThat(pending).as("60s 内应收齐全部写入行(" + DATA_TOPIC + "), 缺失=" + pending.keySet() + ")").isEmpty();
        assertThat(dataOps).as("数据 topic 的 op 集应恰 {c}(零 op=r = 默认 never 注入生效)").containsExactly("c");
        assertThat(txStatuses).as("事务元数据 topic 应有 BEGIN/END(默认 provide.transaction.metadata=true 生效)")
                .contains("BEGIN", "END");
    }

    /**
     * 等 topic 出现并取全分区号(topic 由 Connect 首次 produce 时自动建,建前 partitionsFor
     * 为空):逐 topic 轮询(1s 周期,60s 兜底),全齐后聚合为 TopicPartition 列表。
     *
     * @param consumer 已建的消费者(仅元数据查询,不订阅)
     * @param topics   待等 topic 名
     * @return 全部 topic 的全分区(assign 的输入)
     */
    private static List<TopicPartition> awaitPartitions(KafkaConsumer<String, String> consumer,
                                                        String... topics) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<String, List<TopicPartition>> found = new HashMap<>();
        while (found.size() < topics.length) {
            for (String topic : topics) {
                if (found.containsKey(topic)) {
                    continue;
                }
                var parts = consumer.partitionsFor(topic);
                if (parts != null && !parts.isEmpty()) {
                    List<TopicPartition> tp = parts.stream()
                            .map(p -> new TopicPartition(topic, p.partition())).toList();
                    found.put(topic, tp);
                }
            }
            if (found.size() < topics.length) {
                if (System.nanoTime() > deadline) {
                    StringJoiner missing = new StringJoiner(", ");
                    for (String t : topics) {
                        if (!found.containsKey(t)) {
                            missing.add(t);
                        }
                    }
                    throw new AssertionError("60s 内 topic 未出现: " + missing
                            + " —— Connect 日志尾部:\n" + connectLogsTail());
                }
                Thread.sleep(1000);
            }
        }
        List<TopicPartition> all = new ArrayList<>();
        found.values().forEach(all::addAll);
        return all;
    }

    /**
     * Connect REST 裸调(JDK HttpClient,JSON 请求体/响应体均原文往返)。
     *
     * @param method HTTP 方法(GET/PUT/DELETE)
     * @param path   REST 路径(以 / 起)
     * @param body   JSON 请求体(null 即空体)
     * @return 响应(状态码 + 体原文),IO/超时异常原样上抛
     */
    private static HttpResponse<String> rest(String method, String path, String body) throws Exception {
        String url = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083) + path;
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(15));
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        else {
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Connect 容器日志尾部(失败诊断面:类加载/配置/连接器异常的第一现场)——全量日志可达
     * MB 级,只留末 8000 字符(含最近的 ERROR 堆栈)。
     *
     * @return 日志尾部文本
     */
    private static String connectLogsTail() {
        String logs = connect.getLogs();
        return logs.length() > 8000 ? logs.substring(logs.length() - 8000) : logs;
    }

    /**
     * cp-kafka 7.1.0 的 ZK 单容器 broker:cp 7.1 镜像不支持 KRaft(启动脚本不处理
     * CLUSTER_ID,缺 KAFKA_ZOOKEEPER_CONNECT 即拒启——testcontainers 2.x 的
     * ConfluentKafkaContainer 恒 KRaft 参数面,对 7.1 不可用),故用本类以镜像唯一受支持的
     * ZK 形态起 broker:CMD 整体内联启动脚本——先写最小 zookeeper.properties(clientPort
     * + dataDir,ZK 3.6 无 dataDir 默认值)并以后台进程起镜像自带的
     * zookeeper-server-start,等 2181 端口可连(bash /dev/tcp 轮询,免 nc)后再 exec
     * Confluent 官方启动链。
     * <p>双 listener 与 3.6.1 版同构:PLAINTEXT(0.0.0.0:9092,通告宿主 host:port——
     * test 侧 KafkaConsumer 用)与 BROKER(0.0.0.0:19092,通告 kafka:19092——同网络
     * Connect 容器引导用)。宿主侧端口在构造期预约并钉为固定映射(ServerSocket 探零关闭
     * → addFixedExposedPort):通告值因此可静态写进 env,免去 3.6.1 版"containerIsStarting
     * 拷启动脚本运行期注入"的手法——那个手法依赖 copyFileToContainer,其 tar 写路径在本
     * 模块 test 类路径上损坏(见类 javadoc)。预约与 daemon 绑定之间存在毫秒级竞态窗口,
     * IT 语境可接受(重跑即恢复)。
     * <p>线程约束:仅 testcontainers 启动线程触达;等待策略盯 ZK 模式 broker 的就绪日志行
     * "started (kafka.server.KafkaServer)"(120s 上限,含内嵌 ZK 的启动秒级开销)。
     */
    private static final class ZkKafkaContainer extends GenericContainer<ZkKafkaContainer> {

        /** broker 对外(宿主)listener 的容器内端口:固定映射到构造期预约的宿主端口。 */
        private static final int KAFKA_PORT = 9092;

        /** 宿主侧固定端口(构造期预约,test 侧消费者的 bootstrap 端口)。 */
        private final int hostPort;

        /**
         * 装配 ZK 形态 broker:预约宿主端口并钉固定映射,KAFKA_ZOOKEEPER_CONNECT 指向
         * 同容器内嵌 ZK,listener 三件套(监听/协议映射/inter-broker 名)与内部 topic
         * 副本因子钉 1(单 broker),通告 listener 按已知的宿主端点静态写 env(PLAINTEXT
         * 走 getHost()+预约端口,BROKER 走网络别名 kafka:19092)。
         */
        ZkKafkaContainer() {
            super(DockerImageName.parse(KAFKA_IMAGE));
            hostPort = reserveFreeHostPort();
            addFixedExposedPort(hostPort, KAFKA_PORT);
            withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181");
            withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:19092");
            withEnv("KAFKA_ADVERTISED_LISTENERS",
                    "PLAINTEXT://" + getHost() + ":" + hostPort + ",BROKER://kafka:19092");
            withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT");
            withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
            withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
            withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
            withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
            withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
            withCommand("bash", "-c",
                    "echo 'clientPort=2181' > /tmp/zookeeper.properties\n"
                            + "echo 'dataDir=/tmp/zookeeper' >> /tmp/zookeeper.properties\n"
                            + "zookeeper-server-start /tmp/zookeeper.properties &\n"
                            + "for i in $(seq 1 60); do (echo > /dev/tcp/127.0.0.1/2181) 2>/dev/null && break; sleep 0.5; done\n"
                            + "/etc/confluent/docker/run");
            waitingFor(Wait.forLogMessage(".*started \\(kafka.server.KafkaServer\\).*", 1)
                    .withStartupTimeout(Duration.ofSeconds(120)));
        }

        /**
         * 宿主侧 bootstrap 地址(PLAINTEXT listener 的宿主端点,test 侧消费者用)。
         *
         * @return host:port 文本(端口即构造期预约的固定映射端口)
         */
        String getBootstrapServers() {
            return getHost() + ":" + hostPort;
        }

        /**
         * 预约一个空闲宿主端口(开 ServerSocket 探零即关,取其端口号):端口在
         * 关闭后到 daemon 实际绑定间存在被他方抢占的毫秒级竞态窗口——被占时容器启动
         * 报 port is already allocated,fail-fast 语义清晰,重跑即恢复。
         *
         * @return 空闲端口号
         */
        private static int reserveFreeHostPort() {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
            catch (IOException e) {
                throw new IllegalStateException("无法预约空闲宿主端口给 broker 固定映射", e);
            }
        }
    }
}
