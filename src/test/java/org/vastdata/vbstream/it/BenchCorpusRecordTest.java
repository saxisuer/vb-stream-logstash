package org.vastdata.vbstream.it;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;
import org.vastdata.vbstream.bench.BenchCorpus;
import org.vastdata.vbstream.bench.CorpusLoader;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JMH 基准语料的**生成器测试**（Task 13 Step 2）：对 {@link PgTestEnv} 的 Testcontainers PG
 * 执行 {@code src/main/resources/sql/} 全部 6 个场景脚本（psql 容器内执行，忠实保留脚本的
 * DO 块/多语句布局），经 {@link SessionHarness#rawMessages()} 录制真实 pgoutput 字节流，
 * 去掉收尾哨兵事务后 dump 到 {@code src/test/resources/bench-corpus/corpus.bin}（提交进库，
 * 几百 KB 二进制）。四个 JMH 基准类以该语料离线回放，免基准运行期依赖 Docker。
 *
 * <p>重录触发条件（生成器语义）：语料文件缺失，或场景脚本/建表 DDL 内容变化（SHA-256 指纹
 * 比对 {@code scripts.sha256} 边车文件）——指纹一致时本用例只做存量语料的健康断言，
 * **不启动容器不重录**（PgTestEnv 类加载即起容器，故其引用全部收缩在录制分支内，
 * 跳过路径不触类初始化），使常规 {@code mvn test} 维持秒级通过。
 *
 * <p>录制流内容（6 脚本叠加）：类型边界值 INSERT/UPDATE/DELETE、TOAST 未变标志、
 * REPLICA IDENTITY FULL 的全列 old tuple 与 Relation 重发、流式大事务提交（StreamStart/Stop/
 * StreamCommit 分段）与流式回滚（StreamAbort）——覆盖基准所需的全部消息形态。
 *
 * <p><b>生成器性质告警</b>：本用例在指纹失配（脚本/DDL 变化）或语料缺失时会**改写源码树**
 * （重录并覆盖 {@code src/test/resources/bench-corpus/} 下的语料与指纹边车），且该路径需要
 * Docker——只读 checkout 或无 Docker 环境下会失败，属预期行为（产物应提交回库，而非本地缓存）。
 */
class BenchCorpusRecordTest {

    private static final Logger LOG = LoggerFactory.getLogger(BenchCorpusRecordTest.class);

    /** 语料落盘位置（单一事实来源在 {@link BenchCorpus#CORPUS_FILE}，基准与录制两侧共用）。 */
    private static final Path CORPUS_FILE = BenchCorpus.CORPUS_FILE;

    /** 场景指纹边车：内容为当前 6 脚本 + 建表 DDL 的 SHA-256 十六进制串（换行结尾）。 */
    private static final Path DIGEST_FILE = Path.of("src/test/resources/bench-corpus/scripts.sha256");

    /** 场景脚本在 classpath 上的位置（src/main/resources/sql → target/classes/sql）。 */
    private static final List<String> SCRIPT_RESOURCES = List.of(
            "/sql/01-insert-types.sql",
            "/sql/02-update-cases.sql",
            "/sql/03-delete-cases.sql",
            "/sql/04-streaming-large-txn.sql",
            "/sql/05-stream-abort.sql",
            "/sql/06-replica-identity-full.sql");

    /** 录制用表（与 src/docker/initdb.d/02-init.sql 的 t_assembly_types 同构，脚本即为其编写目标）。 */
    private static final String TABLE_DDL =
            "CREATE TABLE t_assembly_types("
                    + "id int PRIMARY KEY, v_text text, v_bigint bigint, v_float double precision, "
                    + "v_numeric numeric, v_date date, v_time time, v_ts timestamp)";

    /** 录制专用复制槽/发布（不复用其它测试的名字，cleanup 幂等）。 */
    private static final String SLOT = "slot_bench_corpus";

    private static final String PUBLICATION = "pub_bench_corpus";

    /**
     * 收尾哨兵行的 id 值：全部脚本执行完后写入一个独立小事务作为"脚本全部送达"的确定信号，
     * 停条件在其 Commit 上触发；录制产物会整段剔除该哨兵事务（见 {@link #trimSentinel}）。
     */
    private static final String SENTINEL_ID = "999999";

    /** 脚本全部执行完 + 流式脚本（04/05 各 ~8.4s）解码送达的宽裕上限。 */
    private static final Duration RECORD_TIMEOUT = Duration.ofSeconds(120);

    /**
     * 生成器入口：指纹一致走存量健康断言（不触 Docker），否则起容器重录并落盘新语料+新指纹。
     * 两路径共同的出口断言：非空、类型字节 ≥ 6 种、含流式控制消息 'S'（场景 04/05 确已触发
     * 服务端驱逐——语料缺流式段会让基准失去最重的 16KB 流式 Insert 形态，宁可失败重录）。
     */
    @Test
    void recordsOrValidatesBenchCorpus() throws Exception {
        String digest = inputsDigest();
        if (Files.isRegularFile(CORPUS_FILE) && Files.isRegularFile(DIGEST_FILE)
                && Files.readString(DIGEST_FILE).trim().equals(digest)) {
            LOG.info("基准语料现行（指纹匹配），跳过录制，仅做健康断言: {}", CORPUS_FILE);
            assertCorpusHealthy(CorpusLoader.load(CORPUS_FILE));
            return;
        }
        LOG.info("基准语料缺失或场景脚本已变化，开始录制: {}", CORPUS_FILE);
        List<byte[]> corpus = recordFromDocker();
        CorpusLoader.dump(CORPUS_FILE, corpus);
        Files.createDirectories(DIGEST_FILE.getParent());
        Files.writeString(DIGEST_FILE, digest + "\n");
        LOG.info("基准语料已落盘: {} 条消息 / {} 字节", corpus.size(),
                corpus.stream().mapToLong(b -> b.length).sum());
        assertCorpusHealthy(corpus);
    }

    // ---- 录制路径（本节任何方法被触达前 PgTestEnv 已被引用并启动容器） ----

    /**
     * 责任：在 Testcontainers PG 上完成一次完整录制。
     * 关键步骤：建表/建发布（DROP..IF EXISTS 自愈）→ 清残留槽 → 启 harness（槽在启动时创建，
     * 其后的 WAL 才会被解码）→ 逐脚本拷进容器并经 psql -v ON_ERROR_STOP=1 执行（exit code 非 0
     * 即失败，附 stdout/stderr）→ 写哨兵行触发停条件 → awaitTermination → close 后取全量录制
     * → 剔除哨兵事务。
     * 边界与异常语义：槽清理放 finally（异常退出不留残槽拖垮下次录制）；psql 失败/超时/
     * 会话异常均直接上抛终结用例（生成器失败 = 产物不可信，不产出半截语料文件）。
     */
    private List<byte[]> recordFromDocker() throws Exception {
        PgTestEnv.execSql(
                "DROP PUBLICATION IF EXISTS " + PUBLICATION,
                "DROP TABLE IF EXISTS t_assembly_types",
                TABLE_DDL,
                "CREATE PUBLICATION " + PUBLICATION + " FOR TABLE t_assembly_types");
        PgTestEnv.dropSlotQuietly(SLOT);
        try {
            AtomicBoolean sentinelInserted = new AtomicBoolean(false);
            SessionHarness harness = SessionHarness.start(
                    PgTestEnv.newConfig(SLOT, PUBLICATION), sentinelStop(sentinelInserted));
            try {
                for (int i = 0; i < SCRIPT_RESOURCES.size(); i++) {
                    runScriptInContainer(SCRIPT_RESOURCES.get(i), "/tmp/bench-script-" + (i + 1) + ".sql");
                }
                // 哨兵事务：全部脚本送达的确定信号（独立小事务，录制产物中整段剔除）
                PgTestEnv.execSql("INSERT INTO t_assembly_types (id, v_text) VALUES ("
                        + SENTINEL_ID + ", 'bench-corpus-sentinel')");
                harness.awaitTermination(RECORD_TIMEOUT);
            } finally {
                harness.close();
            }
            return trimSentinel(harness);
        } finally {
            PgTestEnv.dropSlotQuietly(SLOT);
        }
    }

    /**
     * 停条件：先见到哨兵 Insert（after 元组首列 == 哨兵 id 文本），随后第一条 Commit 即停。
     * 哨兵是录制期唯一 id 为 999999 的行，误触率为零；脚本事务的 Commit 在哨兵出现前
     * 不满足第二个分支，不会提前停。
     */
    private static Predicate<PgOutputMessage> sentinelStop(AtomicBoolean sentinelInserted) {
        return msg -> {
            if (isSentinelInsert(msg)) {
                sentinelInserted.set(true);
            }
            return sentinelInserted.get() && msg instanceof PgOutputMessage.Commit;
        };
    }

    /** 解码消息是否为哨兵 Insert（after 元组首列——id 列——文本值等于哨兵 id）。 */
    private static boolean isSentinelInsert(PgOutputMessage msg) {
        return msg instanceof PgOutputMessage.Insert i
                && i.newTuple() != null
                && !i.newTuple().columns().isEmpty()
                && i.newTuple().columns().get(0) instanceof TupleValue.Text id
                && SENTINEL_ID.equals(id.value());
    }

    /**
     * 责任：把一个场景脚本拷进容器并经 psql 执行（忠实保留脚本的 DO 块/多语句/BEGIN..ROLLBACK
     * 布局——JDBC Statement 无法直接执行整文件）。
     * 关键步骤：{@code MountableFile.forClasspathResource} 从测试 classpath 取脚本（src/main/resources
     * 编译产物）→ 拷到容器 /tmp → 容器内 {@code psql -U <user> -d <db> -v ON_ERROR_STOP=1 -f}。
     * 边界与异常语义：exit code 非 0 抛 AssertionError（附脚本名与 psql stderr——
     * ON_ERROR_STOP 保证首错即停，不会半执行后静默续跑）；容器内无 psql/拷贝失败按
     * testcontainers 异常原样上抛。
     */
    private static void runScriptInContainer(String classpathResource, String containerPath) throws Exception {
        PgTestEnv.PG.copyFileToContainer(MountableFile.forClasspathResource(classpathResource), containerPath);
        Container.ExecResult result = PgTestEnv.PG.execInContainer(
                "psql", "-U", PgTestEnv.PG.getUsername(), "-d", PgTestEnv.PG.getDatabaseName(),
                "-v", "ON_ERROR_STOP=1", "-f", containerPath);
        if (result.getExitCode() != 0) {
            throw new AssertionError("场景脚本执行失败: " + classpathResource
                    + "\npsql stderr: " + result.getStderr()
                    + "\npsql stdout: " + result.getStdout());
        }
        LOG.info("场景脚本已执行: {}", classpathResource);
    }

    /**
     * 责任：从 close 后的全量录制中剔除哨兵事务（含其事务内可能重发的 Relation——
     * REPLICA IDENTITY 恢复 DEFAULT 后首条变更会触发重发，哨兵事务不恒为 [B,I,C] 三条）。
     * 关键步骤：在解码列表中倒序找最后一条哨兵 Insert → 从它向前回溯到最近的 Begin →
     * 断言 Begin 之前的消息不属于哨兵事务（回溯越过列表头即格式异常）→ 取 raw 前 beginIdx 条。
     * 边界与异常语义：找不到哨兵 Insert 或回溯不到 Begin 抛 AssertionError（录制流形态异常，
     * 不可猜测截断点）；raw 与 decoded 等长是 harness 的 close 后契约，先断言再按索引切片。
     */
    private static List<byte[]> trimSentinel(SessionHarness harness) {
        List<PgOutputMessage> decoded = harness.messages();
        List<byte[]> raw = harness.rawMessages();
        assertEquals(decoded.size(), raw.size(), "close 后 raw 与 decoded 应等长（harness 契约）");
        int insertIdx = -1;
        for (int i = decoded.size() - 1; i >= 0; i--) {
            if (isSentinelInsert(decoded.get(i))) {
                insertIdx = i;
                break;
            }
        }
        assertTrue(insertIdx >= 0, "录制流中应存在哨兵 Insert（停条件不可能由其它消息触发）");
        int beginIdx = insertIdx;
        while (!(decoded.get(beginIdx) instanceof PgOutputMessage.Begin)) {
            beginIdx--;
            assertTrue(beginIdx >= 0, "哨兵 Insert 之前应能回溯到其 Begin（录制流形态异常）");
        }
        LOG.info("录制 {} 条，剔除哨兵事务 {} 条（Begin@{}..Commit@{}）",
                raw.size(), decoded.size() - beginIdx, beginIdx, decoded.size() - 1);
        return new ArrayList<>(raw.subList(0, beginIdx));
    }

    // ---- 纯静态辅助（不触 PgTestEnv，跳过录制路径也可安全复用） ----

    /**
     * 健康断言：非空、每条 ≥1 字节、去重类型字节 ≥ 6 种、含 'S'（StreamStart——流式场景确已
     * 触发）且含 'A'（StreamAbort——回滚场景确已流式）。失败信息附类型直方图辅助定位
     * （哪个控制消息缺席一眼可见）。
     */
    private static void assertCorpusHealthy(List<byte[]> corpus) {
        assertTrue(!corpus.isEmpty(), "语料不应为空");
        for (byte[] raw : corpus) {
            assertTrue(raw.length >= 1, "每条消息至少含类型字节");
        }
        String histogram = typeHistogram(corpus);
        assertTrue(corpus.stream().map(raw -> raw[0]).distinct().count() >= 6,
                "语料应含至少 6 种类型字节: " + histogram);
        assertTrue(corpus.stream().anyMatch(raw -> raw[0] == 'S'),
                "语料应含 StreamStart（场景 04/05 需触发服务端流式驱逐；环境抖动时重跑本用例重建语料）: "
                        + histogram);
        assertTrue(corpus.stream().anyMatch(raw -> raw[0] == 'A'),
                "语料应含 StreamAbort（场景 05 的流式回滚）: " + histogram);
        LOG.info("基准语料健康: {} 条 / {} 字节, 类型分布 {}",
                corpus.size(), corpus.stream().mapToLong(b -> b.length).sum(), histogram);
    }

    /** 类型字节直方图（'B'=12 之类，TreeMap 排序）——失败诊断与录制对账用。 */
    private static String typeHistogram(List<byte[]> corpus) {
        return corpus.stream()
                .collect(Collectors.groupingBy(
                        raw -> "'" + (char) raw[0] + "'",
                        TreeMap::new,
                        Collectors.counting()))
                .toString();
    }

    /**
     * 场景指纹：对（建表 DDL + 6 个脚本内容）按序做 SHA-256。
     * 关键步骤：逐资源读 classpath 字节 → 以分隔符（资源名 + NUL）喂摘要——资源名入摘要使
     * 脚本改名/增删同样触发重录。
     * 边界与异常语义：资源缺失抛 IllegalStateException（classpath 不完整属构建损坏，fail-fast）；
     * 摘要算法不存在按 NoSuchAlgorithmException 上抛（JVM 规范保证 SHA-256 存在，理论路径）。
     */
    private static String inputsDigest() throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        update(digest, "ddl", TABLE_DDL.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (String resource : SCRIPT_RESOURCES) {
            update(digest, resource, readResource(resource));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** 以"名 + NUL + 内容 + NUL"的结构喂摘要（名入摘要把改名/增删也纳入重录触发）。 */
    private static void update(MessageDigest digest, String name, byte[] content) {
        digest.update(name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(content);
        digest.update((byte) 0);
    }

    /** 从测试 classpath 读资源字节；缺失抛 IllegalStateException（构建不完整，fail-fast）。 */
    private static byte[] readResource(String resource) {
        try (InputStream in = BenchCorpusRecordTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("classpath 上找不到场景脚本: " + resource);
            }
            return in.readAllBytes();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("读取场景脚本失败: " + resource, e);
        }
    }
}
