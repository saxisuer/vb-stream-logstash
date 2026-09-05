package org.vastdata.debezium.connector.postgresql.stream.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
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
 * MS6/R4 真连接器验收:插件装进真 Kafka Connect 运行(非 embedded engine)——类加载面
 * (plugin.path 隔离类加载器加载本插件与 lib/ 依赖)、REST 配置暴露面(configDef/validate
 * 三层防线)、序列化面、连接器生命周期全走真路径。
 * <p>关键步骤:{@code @BeforeAll} 核对 assembly 产物结构(根恰一连接器 jar 且带 ServiceLoader
 * 清单/lib 依赖齐/excluded 零命中,缺产物即 fail-fast 提示先 package)→ 起容器组
 * (cp-kafka 8.3.0 KRaft + cp-kafka-connect 8.3.0,plugin.path 挂载插件目录副本)→ await
 * Connect REST 就绪 → PUT /connectors 建连接器(database.* 指向 {@link StreamPgTestEnv}.PG,
 * topic.prefix ms6connect,快照/事务元数据两键不设——默认注入在真 Connect 生效的顺带验收)
 * → 夹具表 publication 预建 → INSERT → KafkaConsumer 轮询(topic ms6connect.public.t_plug)
 * → 断言记录 op=c/值等/零 op=r(默认 no_data)+ 事务元数据 topic 有 BEGIN/END(默认 true)
 * → {@code @AfterEach} 删槽、{@code @AfterAll} 删连接器与容器。
 * <p>选型注记:CP 8.3.0 = Apache Kafka 4.3.0,与 Debezium 3.6.1 的构建/测试目标
 * (Kafka Connect 4.3.0)及本模块 connect-api 编译面三方对齐;镜像 JVM 为 Java 25,
 * 插件内 Chronicle Queue 经 KAFKA_OPTS 携带与引擎 surefire 同源的 --add-opens 清单。
 * 边界:cp 镜像大、首次拉取慢——独立类可 {@code -Dtest} 单跑;断言超时口径 60s 级 await。
 */
class ConnectPluginIT {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectPluginIT.class);

    /** 本 IT 专用复制槽(PG 单例跨类共享,各 IT 独立槽名;前后清删)。 */
    private static final String SLOT = "ms6_plug";

    /** 本 IT 专用 publication(@BeforeAll 预建,流式源免 autocreate 权限面)。 */
    private static final String PUBLICATION = "pub_ms6_plug";

    /** 夹具表:id 主键 + 文本列,topic 路径 public.t_plug 由此得名。 */
    private static final String TABLE = "t_plug";

    /** topic 前缀:数据 topic ms6connect.public.t_plug、事务元数据 topic ms6connect.transaction。 */
    private static final String TOPIC_PREFIX = "ms6connect";

    /** 数据 topic 全名(Debezium 默认命名:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String DATA_TOPIC = TOPIC_PREFIX + ".public." + TABLE;

    /** 事务元数据 topic 全名(TransactionMonitor 约定 &lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = TOPIC_PREFIX + ".transaction";

    /** Connect 侧连接器实例名(REST 路径段,亦作 Kafka Connect 连接器名)。 */
    private static final String CONNECTOR_NAME = "ms6-plug";

    /** Kafka broker 镜像:CP 8.3.0 = AK 4.3.0(与 Debezium 3.6.1 构建/测试目标对齐)。 */
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:8.3.0";

    /** Kafka Connect 运行时镜像:与 broker 同版配对,内含 Kafka 4.3 + Temurin 25。 */
    private static final String CONNECT_IMAGE = "confluentinc/cp-kafka-connect:8.3.0";

    /** assembly 产物目录(相对模块 basedir——surefire 工作目录即模块根)。 */
    private static final Path PLUGIN_DIR = Paths.get("target", "vb-stream-connector-postgres-stream-plugin");

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

    /** test 侧 JSON 构造/解析(jackson 随 Debezium 传递,compile 域在 test 可达)。 */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** test 侧 REST 客户端(JDK 裸调,不引新依赖)。 */
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static Network network;
    private static ConfluentKafkaContainer kafka;
    private static GenericContainer<?> connect;

    /**
     * 起容器组并预建夹具:①断言 assembly 产物结构合规(缺产物/结构破坏即 fail-fast,
     * 文案指向可操作的补救命令);②PG 侧 DROP+CREATE 夹具表与 publication(自愈:重复跑
     * 或残留旧 schema 均收敛到已知形态);③起 Kafka(broker,同网络别名 kafka:19092)
     * 与 Connect(plugin.path 挂插件目录副本,REST 就绪为放行条件)——两容器共享自定义
     * 网络,PG 走 host.docker.internal 回宿主映射端口(host-gateway 别名保证 Linux 亦可解析)。
     */
    @BeforeAll
    static void startCluster() throws Exception {
        assertPluginArtifact();
        StreamPgTestEnv.execSql(
                "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + " (id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS " + PUBLICATION,
                "CREATE PUBLICATION " + PUBLICATION + " FOR TABLE " + TABLE);

        network = Network.newNetwork();
        kafka = new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetwork(network)
                // 网络别名 listener:Connect 容器经 kafka:19092 引导(与宿主映射端口解耦)
                .withListener("kafka:19092");
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
                // 数据面 JSON 直读(schemas.enable=false——payload 平铺,test 断言少一层包裹)
                .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("CONNECT_PLUGIN_PATH", CONTAINER_PLUGIN_ROOT)
                // 插件内 Chronicle Queue 的 mmap 需要(引擎 surefire argLine 同款开放包)
                .withEnv("KAFKA_OPTS", CONNECT_ADD_OPENS)
                // Connect 容器 → 宿主映射端口的 PG(Docker Desktop 原生解析,Linux 走 host-gateway)
                .withExtraHost("host.docker.internal", "host-gateway")
                // 整目录递归拷入(MountableFile 自动打 tar):/plugins/<插件名>/ 即一个隔离插件位
                .withCopyToContainer(MountableFile.forHostPath(PLUGIN_DIR.toString()),
                        CONTAINER_PLUGIN_ROOT + "/vb-stream-connector-postgres-stream")
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
     * 核对 assembly 产物结构(Task 1 的消费面契约,结构破坏即打包回归):
     * ①目录存在(缺产物 fail-fast,文案指向先跑 package 的可操作命令);
     * ②根下恰一 jar 且是本连接器 jar;③该 jar 带 ServiceLoader 清单(真 Connect 插件
     * 发现的必经登记——embedded engine 直传 Class 不走此路径,是真容器验收独有的死法);
     * ④lib/ 非空;⑤排除族(connect-api/kafka-clients/slf4j-api 及其独占子件)在根+lib
     * 零命中。
     */
    private static void assertPluginArtifact() throws Exception {
        if (!Files.isDirectory(PLUGIN_DIR)) {
            throw new IllegalStateException("assembly 产物缺失: " + PLUGIN_DIR.toAbsolutePath()
                    + " —— 先运行 mvn -pl vb-stream-connector-postgres-stream package -DskipTests 再跑本 IT");
        }
        List<Path> rootJars = jarChildren(PLUGIN_DIR);
        assertThat(rootJars).as("插件根应恰一连接器 jar(R4 两连接器并存的打包形态)").hasSize(1);
        Path connectorJar = rootJars.get(0);
        assertThat(connectorJar.getFileName().toString())
                .as("根 jar 应是本连接器 jar").startsWith("vb-stream-connector-postgres-stream-");
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
        assertThat(forbidden).as("排除族 jar 应零命中(Connect runtime 已提供,重复类在 R4 必炸)").isEmpty();
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
     * PUT 连接器配置(真 REST 生命周期入口:走 validate → configDef 暴露面 → 任务调度):
     * database 四件套指向 PG 容器的宿主映射端点(host.docker.internal + 映射端口),
     * 槽/publication/topic.prefix 三件套钉本 IT 专用名,pipe.dir 用容器内绝对路径
     * (默认相对路径按 worker 工作目录解析,跨容器不确定);snapshot.mode 与
     * provide.transaction.metadata 有意不设——默认注入(no_data/true)在真 Connect 生效。
     */
    private static void putConnectorConfig() throws Exception {
        ObjectNode config = JSON.createObjectNode();
        config.put("connector.class",
                org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector.class.getName());
        config.put("database.hostname", "host.docker.internal");
        config.put("database.port", String.valueOf(
                StreamPgTestEnv.PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)));
        config.put("database.dbname", StreamPgTestEnv.PG.getDatabaseName());
        config.put("database.user", StreamPgTestEnv.PG.getUsername());
        config.put("database.password", StreamPgTestEnv.PG.getPassword());
        config.put("topic.prefix", TOPIC_PREFIX);
        config.put("slot.name", SLOT);
        config.put("publication.name", PUBLICATION);
        config.put("pipe.dir", "/tmp/ms6-pipe-queue");
        // PUT /connectors/{name}/config 的请求体即扁平配置 map 本身(名字取路径段)——
        // {"name":..,"config":{..}} 包装是 POST /connectors 的形态,Connect 4.3 在本端点
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
     * 的 op 集恰 {"c"}——零 op=r 证明默认注入的 snapshot.mode=no_data 在真 Connect 生效;
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
        assertThat(dataOps).as("数据 topic 的 op 集应恰 {c}(零 op=r = 默认 no_data 注入生效)").containsExactly("c");
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
}
