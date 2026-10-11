package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 黄金 wire 对拍 IT（M1 Task 8 Step 2）：把 {@code DirectWalSource} 的裸 v3
 * 协议客户端接到<strong>真容器 + 字节捕获代理</strong>上，伪断言其发出的命令序列与
 * {@code golden/receivexlog-wire.txt}（容器内 strace 实采的 pg_receivexlog 会话）结构
 * 等价。
 *
 * <p><strong>为什么走代理</strong>：客户端真实 socket 字节无法在进程内拦截（ssl 工厂/
 * 通道自持）；本地 {@link CaptureProxy} 双向泵——客户端→服务端方向 tee 进捕获缓冲，
 * 服务端→客户端方向 tee 留作 'k'/'w' 帧面核验（真 OG 流上 'k' 帧长度恰 33 的黄金冒烟，
 * Task 5 re-review 残留硬前置）。字节经真实网络栈往返，等价面含 TCP 分段无关性。</p>
 *
 * <p><strong>等价口径</strong>（fixture 头注释的恒定分叉表）：
 * startup 参数键集/值（application_name 值豁免）+ 协议 major==3（minor 参考端 51/本端 0，
 * Task 5 裁定）；IDENTIFY_SYSTEM 逐字节；START_REPLICATION 格式（{@code %X/%X} 非补零、
 * 无槽形态，起点时变豁免）；65B 'r' 反馈的恒定字段（peer_role=3/peer_state=7/总长/
 * replyRequested 位与 sendTime 微秒域 sanity，LSN 与 sendTime 值时变豁免——sendTime
 * 于 sendLock 前取值，写线程并发下线序可倒置，单调性不作比对面）。'p' 认证应答帧
 * 按宿主→容器命中码 11 的形态归一（startup 后允许至多一帧）。</p>
 *
 * <p>环境前提：本机 Docker（{@code OgTestEnv} 单例容器，类加载起库 60-90s）。</p>
 */
class OgGoldenWireIT {

    /** 会话观察窗（毫秒）——含 INSERT 驱动 'w' 流量与 ≥1 次心跳 'r'（1s 周期档）。 */
    private static final long SESSION_MS = 6_000;

    /** 直连源反馈心跳的 statusInterval（毫秒，心跳周期 = 一半——1s 一帧 'r'）。 */
    private static final long STATUS_INTERVAL_MS = 2_000;

    /** 捕获代理句柄（跨测试方法复用同一容器会话；类级清理）。 */
    private static CaptureProxy proxy;

    @AfterAll
    static void tearDown() {
        if (proxy != null) {
            proxy.close();
        }
    }

    /**
     * 主断言：DirectWalSource 经代理连真 OG 容器——实发命令序列与黄金 fixture 等价、
     * 真 'k' 帧长度恰 33、会话后实例存活（容器 SQL 可查 + 源无终态失败）。
     *
     * <p>关键步骤：起代理 → 起源（无槽、startLsn=0 采 IDENTIFY xlogpos）→ 等 'w' 数据
     * 到达（consumedLsn&gt;0）→ JDBC 写一行驱动流量 → 观察窗内收心跳 → 停源停代理 →
     * 捕获字节按 FE/BE 帧解析逐面断言。</p>
     *
     * @throws Exception 任一环节 IO/SQL 失败
     */
    @Test
    void directClientCommandSequenceMatchesReceivexlogGoldenWire() throws Exception {
        Map<String, String[]> fixture = loadFixture();
        OgTestEnv.execSql("CREATE TABLE IF NOT EXISTS og_wire_probe(id int, v text)");
        proxy = CaptureProxy.start(OgTestEnv.host(), OgTestEnv.port());
        DirectWalSource source = new DirectWalSource("127.0.0.1", proxy.port(),
                OgTestEnv.USER, OgTestEnv.PASSWORD, null, STATUS_INTERVAL_MS, 0L);
        try {
            source.start(lsn -> { }, (lsn, bytes) -> { });
            awaitDataFlow(source);
            try (Connection c = OgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("INSERT INTO og_wire_probe VALUES (999, 'golden')");
            }
            Thread.sleep(SESSION_MS);
        } finally {
            source.stop();
        }
        assertTrue(source.terminalFailure().isEmpty(),
                "黄金对拍会话不得有终态失败: " + source.terminalFailure());
        assertTrue(source.isTerminated(), "停机后工作线程须终止");

        byte[] captured = proxy.clientToServerBytes();
        ServerSideFrames server = proxy.serverFrames();
        assertWireEquivalence(fixture, captured);
        assertTrue(server.keepaliveCount >= 1,
                "真 OG 流上须观察到至少一帧 'k'（会话观察窗内无 keepalive——环境异常？）");
        assertTrue(server.keepaliveCount == server.keepalivesOfExactLength33,
                "真 'k' 帧长度必须恰 33（实得形态见断言值 " + server.keepaliveCount + " 帧）");
        assertTrue(server.dataFrameCount >= 1, "会话须收到 'w'/'d' 数据帧");
        assertInstanceAlive();
    }

    /**
     * 实例存活验收（brief Step 2 收尾锚）：会话后容器 SQL 可查 + 探针行可读——
     * 直连会话不得损伤服务端。
     *
     * @throws SQLException 存活查询失败（本身就是断言失败面）
     */
    private void assertInstanceAlive() throws Exception {
        try (Connection c = OgTestEnv.newSqlConnection(); Statement st = c.createStatement();
             var rs = st.executeQuery("SELECT count(*) FROM og_wire_probe WHERE id = 999")) {
            assertTrue(rs.next() && rs.getInt(1) == 1, "探针行须可查（实例存活面）");
        }
        OgTestEnv.execSql("DELETE FROM og_wire_probe WHERE id = 999");
    }

    /**
     * 等源头块数据到达（consumedLsn&gt;0；15s 上限——首连 + IDENTIFY + 首块的总预算）。
     *
     * @param source 直连源
     */
    private void awaitDataFlow(DirectWalSource source) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (source.consumedLsn() == 0) {
            assertTrue(source.terminalFailure().isEmpty(),
                    "直连首块前终态失败: " + source.terminalFailure());
            assertTrue(System.currentTimeMillis() < deadline, "15s 内未收到首块 'w' 数据");
            Thread.sleep(200);
        }
    }

    /**
     * 捕获字节 ↔ 黄金 fixture 的等价断言（口径见类注释恒定分叉表）。
     *
     * <p>关键步骤：捕获流按 [startup（无类型前缀）→ 类型帧序列] 解析；startup 断参数键集/
     * 值（application_name 豁免）与 major==3；可选单帧 'p' 归一；IDENTIFY_SYSTEM 与 fixture
     * 字节逐字节全等；START_REPLICATION 断命令形态（无 SLOT、%X/%X 非补零）；其余帧须全为
     * 65B 'r' 反馈——恒定字段（role/state/replyRequested 域/replyFlags 零）与 fixture 样本
     * 等值，sendTime 微秒域 sanity（写线程并发下线序可倒置，单调性不作比对面）。</p>
     *
     * @param fixture  黄金 fixture（名称 → 帧 hex 字节与注释）
     * @param captured 代理捕获的客户端→服务端全量字节
     */
    private void assertWireEquivalence(Map<String, String[]> fixture, byte[] captured) {
        ByteBuffer buf = ByteBuffer.wrap(captured);
        String[] fixtureStartup = fixture.get("startup");
        assertNotNull(fixtureStartup, "fixture 须含 startup 行");
        StartupMessage startup = parseStartup(buf);
        StartupMessage goldenStartup = parseStartup(ByteBuffer.wrap(hexToBytes(fixtureStartup[0])));
        assertEquals(Set.copyOf(goldenStartup.paramKeys()), Set.copyOf(startup.paramKeys()),
                "startup 参数键集与 pg_receivexlog 等价（顺序不作比对面——协议键序不敏感，"
                        + "参考客户端按 libpq 固定序、本端 HashMap 序）");
        for (int i = 0; i < goldenStartup.params.size(); i++) {
            String key = new ArrayList<>(goldenStartup.params.keySet()).get(i);
            if ("application_name".equals(key)) {
                continue;
            }
            assertEquals(goldenStartup.params.get(key), startup.params.get(key),
                    "startup 参数 " + key + " 须与参考客户端一致");
        }
        assertEquals(3, startup.major, "协议 major 须为 3（minor 参考端 51/本端 0 属裁定内分叉）");

        List<TypedFrame> frames = parseTypedFrames(buf);
        int idx = 0;
        if (!frames.isEmpty() && frames.get(0).type == 'p') {
            idx = 1;   // 码 11 认证应答（宿主→容器命中 md5 hba + sha256 存储）——归一豁免
        }
        TypedFrame identify = frameOfType(frames, idx, 'Q');
        assertArrayEqualsNote(hexToBytes(fixture.get("identify-system")[0]), identify.raw,
                "IDENTIFY_SYSTEM 须与参考客户端逐字节等同");
        TypedFrame startRep = frameOfType(frames, idx + 1, 'Q');
        // 'Q' 载荷是 SQL + 终止 NUL——剥尾后比对命令形态
        String sql = new String(startRep.payload, 0, startRep.payload.length - 1, StandardCharsets.UTF_8);
        assertTrue(sql.matches("START_REPLICATION [0-9A-F]{1,8}/[0-9A-F]+"),
                "START_REPLICATION 须为无槽 %X/%X 非补零形态: " + sql);
        int fixtureStartRepLen = hexToBytes(fixture.get("start-replication")[0]).length;
        assertTrue(Math.abs(fixtureStartRepLen - startRep.raw.length) <= 4,
                "START_REPLICATION 帧长量级与 fixture 等价（起点 LSN 位数波动 ±数字节属时变面）"
                        + " fixture=" + fixtureStartRepLen + " ours=" + startRep.raw.length);

        byte[] goldenReply = hexToBytes(fixture.get("standby-reply")[0]);
        ReplyMessage goldenReplyMsg = parseReply(goldenReply, 5);
        int replyCount = 0;
        for (int i = idx + 2; i < frames.size(); i++) {
            TypedFrame f = frames.get(i);
            assertEquals('d', f.type, "命令序列之后只允许 'd'('r') 反馈帧: " + f);
            ReplyMessage r = parseReply(f.payload, 0);
            assertEquals(65, f.payload.length, "'r' 反馈总长恒 65");
            assertEquals(goldenReplyMsg.peerRole, r.peerRole, "peer_role 恒 STANDBY_MODE=3");
            assertEquals(goldenReplyMsg.peerState, r.peerState, "peer_state 恒 BUILDING_STATE=7");
            assertTrue(r.replyRequested == 0 || r.replyRequested == 1, "replyRequested 位域 ∈{0,1}");
            assertEquals(0, r.replyFlags, "本端 replyFlags 恒 0（参考端该字段为栈垃圾不比对）");
            // sendTime 域 sanity：自 2000-01-01 的微秒（2026 年 ≈ 8.4e14；写线程并发时
            // sendTime 于 sendLock 前取值，线序可倒置——单调性不作为比对面）
            assertTrue(r.sendTime > 8e14 && r.sendTime < 9e14,
                    "sendTime 须为 2026 年邻域的微秒域值: " + r.sendTime);
            replyCount++;
        }
        assertTrue(replyCount >= 1, "观察窗内须捕获至少一帧 'r' 反馈（心跳 1s 档）");
    }

    /**
     * 取第 offset 起首个 'Q' 帧（越界/类型不符即失败——命令序列错位是硬断言面）。
     *
     * @param frames 已解析类型帧序列
     * @param offset 起始下标
     * @param expect 期望类型
     * @return 该帧
     */
    private static TypedFrame frameOfType(List<TypedFrame> frames, int offset, char expect) {
        assertTrue(offset < frames.size(), "命令帧缺失（期望 '" + expect + "' 于下标 " + offset
                + "，实得 " + frames.size() + " 帧）");
        TypedFrame f = frames.get(offset);
        assertEquals(expect, f.type, "命令序列下标 " + offset + " 须为 '" + expect + "'");
        return f;
    }

    /**
     * 载入黄金 fixture（classpath {@code golden/receivexlog-wire.txt}）。
     *
     * <p>行格式 {@code 名称 | hex 字节 | 注释}——# 注释行与空行跳过；本 IT 只消费
     * start-replication / standby-reply 的<strong>样本帧</strong>与 startup/identify-system
     * 的全量字节，ssl-probe 行仅文档化（libpq 专有探测帧，本客户端不发）。</p>
     *
     * @return 名称 → [hex, 注释]
     */
    private static Map<String, String[]> loadFixture() {
        Map<String, String[]> out = new LinkedHashMap<>();
        try (var in = OgGoldenWireIT.class.getResourceAsStream("/golden/receivexlog-wire.txt")) {
            assertNotNull(in, "黄金 fixture 缺失: src/test/resources/golden/receivexlog-wire.txt");
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                line = line.strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\|", 3);
                assertEquals(3, parts.length, "fixture 行格式（名称|hex|注释）破坏: " + line);
                out.put(parts[0].strip(), new String[] {parts[1].strip(), parts[2].strip()});
            }
        } catch (IOException e) {
            throw new UncheckedIOException("fixture 读取失败", e);
        }
        return out;
    }

    /**
     * 空格分隔大写 hex → 字节。
     *
     * @param hex hex 文本
     * @return 字节数组
     */
    private static byte[] hexToBytes(String hex) {
        String cleaned = hex.replaceAll("\\s+", "");
        byte[] out = new byte[cleaned.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(cleaned.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    /**
     * 逐字节全等断言（失败消息带双方 hex——排障可读）。
     *
     * @param expected 期望字节
     * @param actual   实得字节
     * @param message  失败前缀
     */
    private static void assertArrayEqualsNote(byte[] expected, byte[] actual, String message) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(message + "\nexpected: " + hexOf(expected) + "\nactual:   " + hexOf(actual));
        }
    }

    /**
     * 字节 → 紧凑 hex（诊断面）。
     *
     * @param bytes 字节
     * @return hex 文本
     */
    private static String hexOf(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    /**
     * 解析 startup 消息（FE/BE 唯一的无类型字节帧）：int32 总长 + int16 major + int16
     * minor + k\0v\0 参数对 + 终止 \0——游标停在消息末。
     *
     * @param buf 捕获缓冲（定位在消息首）
     * @return 解析产物
     */
    private static StartupMessage parseStartup(ByteBuffer buf) {
        int len = buf.getInt();
        int major = buf.getShort();
        int minor = buf.getShort();
        Map<String, String> params = new LinkedHashMap<>();
        while (true) {
            StringBuilder key = readCString(buf);
            if (key.length() == 0) {
                break;
            }
            params.put(key.toString(), readCString(buf).toString());
        }
        return new StartupMessage(major, minor, params);
    }

    /**
     * 读 NUL 结尾字符串（startup 参数面）。
     *
     * @param buf 缓冲
     * @return 文本（不含 NUL）
     */
    private static StringBuilder readCString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder();
        while (buf.get() != 0) {
            sb.append((char) buf.get(buf.position() - 1));
        }
        return sb;
    }

    /**
     * 解析剩余缓冲的全部类型帧（char + int32 大端长 + 载荷）。
     *
     * <p>边界与异常语义：<b>截断尾帧容错</b>——快照末尾的帧头（&lt;5B）或载荷（&lt;len-4）
     * 不足、帧长非法（&lt;4）时按"留待下轮"丢弃退出（与生产 walker 的 awaitMore 语义
     * 同构），不抛 BufferUnderflow——泵与快照的竞态窗（close join 已基本闭合，防御位）
     * 下截在帧中的尾部字节不构成假红。</p>
     *
     * @param buf 缓冲（定位在首帧类型字节）
     * @return 帧列表（含原始字节引用）
     */
    private static List<TypedFrame> parseTypedFrames(ByteBuffer buf) {
        List<TypedFrame> out = new ArrayList<>();
        while (buf.hasRemaining()) {
            if (buf.remaining() < 5) {
                break;   // 截断尾帧：帧头未到齐
            }
            int type = buf.get();
            int len = buf.getInt();
            if (len < 4 || buf.remaining() < len - 4) {
                break;   // 截断尾帧：载荷未到齐/帧长非法
            }
            byte[] payload = new byte[len - 4];
            buf.get(payload);
            byte[] raw = new byte[1 + 4 + payload.length];
            raw[0] = (byte) type;
            raw[1] = (byte) (len >>> 24);
            raw[2] = (byte) (len >>> 16);
            raw[3] = (byte) (len >>> 8);
            raw[4] = (byte) len;
            System.arraycopy(payload, 0, raw, 5, payload.length);
            out.add(new TypedFrame((char) type, payload, raw));
        }
        return out;
    }

    /**
     * 解析 65B StandbyReplyMessage（'r' 载荷自 base 偏移起——fixture 帧含 'd' 头 5 字节，
     * 捕获帧 raw 自 0 起）。
     *
     * @param frame 帧字节
     * @param base  'r' 载荷起点（fixture=5、捕获=0）
     * @return 恒定字段读出
     */
    private static ReplyMessage parseReply(byte[] frame, int base) {
        // 'r' 载荷是 C 结构体 memcpy——小端（与 V3ProtocolChannel.encodeStandbyReply 同布局）
        ByteBuffer p = ByteBuffer.wrap(frame, base, frame.length - base)
                .order(ByteOrder.LITTLE_ENDIAN);
        assertEquals('r', (char) p.get(), "'r' 反馈载荷首字节");
        p.getLong();
        p.getLong();
        p.getLong();
        p.getLong();
        p.getLong();                       // 五前沿 u64（时变豁免）
        return new ReplyMessage(p.getInt(), p.getInt(), p.getLong(), p.get(), p.getInt());
    }

    /**
     * startup 消息解析产物。
     *
     * @param major   协议大版本
     * @param minor   协议小版本
     * @param params  参数对（有序）
     */
    private record StartupMessage(int major, int minor, Map<String, String> params) {

        /**
         * 参数键集（有序——比对面）。
         *
         * @return 键列表
         */
        List<String> paramKeys() {
            return new ArrayList<>(params.keySet());
        }
    }

    /**
     * 一条已解析的类型帧。
     *
     * @param type    帧类型字符
     * @param payload 载荷（长前缀之后）
     * @param raw     帧全量字节（含类型与长度）
     */
    private record TypedFrame(char type, byte[] payload, byte[] raw) {
    }

    /**
     * 'r' 反馈恒定字段读出（时变的前沿五元组与 sendTime 值不进本 record——sendTime 仅
     * 单调性参与断言）。
     *
     * @param peerRole      STANDBY_MODE=3
     * @param peerState     BUILDING_STATE=7
     * @param sendTime      自 2000-01-01 微秒
     * @param replyRequested 即答请求位
     * @param replyFlags    反馈标志（本端恒 0）
     */
    private record ReplyMessage(int peerRole, int peerState, long sendTime, int replyRequested, int replyFlags) {
    }

    /**
     * 本地字节捕获代理：客户端 → 本代理 → 真 OG 容器，双向泵 + 双向 tee。
     *
     * <p>关键步骤：bind 随机回环端口即返回（accept 在守护线程——直连源的工作线程随后
     * 发起连接）；accept 单连接后建目标 socket，两泵线程逐 8KB 段复制（tee 进捕获缓冲
     * ——clientToServer 精确解析面 / serverToClient 会话后解析 'k'/'w' 面）；close 关三
     * socket，泵线程随阻塞读返回自行终止（守护位兜底 JVM 退出）。线程约束：捕获缓冲
     * 仅泵线程写（synchronized——主线程停机后快照读），serverFrames 在 close 之后调用
     * 保证 tee 不再增长。</p>
     */
    private static final class CaptureProxy {

        private final ServerSocket listen;
        private volatile Socket client;
        private volatile Socket server;
        private final ByteArrayOutputStream clientToServer = new ByteArrayOutputStream();
        private final ByteArrayOutputStream serverToClient = new ByteArrayOutputStream();
        private final AtomicReference<Throwable> pumpFailure = new AtomicReference<>();

        /** 已起泵线程登记表——close 时 join，建立"泵写尽 → 主线程快照"的 happens-before。 */
        private final List<Thread> pumps = new CopyOnWriteArrayList<>();

        /** close 对泵线程的 join 上限（毫秒）——socket 关闭使阻塞读立即返回，常理毫秒级。 */
        private static final long PUMP_JOIN_TIMEOUT_MS = 3_000;

        private CaptureProxy(ServerSocket listen) {
            this.listen = listen;
        }

        /**
         * 起代理：bind 随机回环端口即返回——accept + 目标连接在守护线程完成（不阻塞
         * 调用方起直连源）。
         *
         * @param targetHost 目标主机（OG 容器映射地址）
         * @param targetPort 目标端口
         * @return 就绪的代理
         * @throws IOException bind 失败
         */
        static CaptureProxy start(String targetHost, int targetPort) throws IOException {
            ServerSocket listen = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            CaptureProxy proxy = new CaptureProxy(listen);
            Thread setup = new Thread(() -> {
                try {
                    Socket client = listen.accept();   // 由 DirectWalSource 工作线程发起
                    Socket server = new Socket(targetHost, targetPort);
                    proxy.established(client, server);
                } catch (IOException e) {
                    proxy.pumpFailure.compareAndSet(null, e);
                }
            }, "og-wire-proxy-accept");
            setup.setDaemon(true);
            setup.start();
            return proxy;
        }

        /**
         * 连接建立完毕：记录双 socket 并起双向泵。
         *
         * @param client 客户端侧 socket
         * @param server 目标侧 socket
         * @throws IOException 流获取失败
         */
        private void established(Socket client, Socket server) throws IOException {
            this.client = client;
            this.server = server;
            pump(client.getInputStream(), server.getOutputStream(), clientToServer);
            pump(server.getInputStream(), client.getOutputStream(), serverToClient);
        }

        /**
         * 起一个泵线程（守护）：in → out 逐段复制 + tee 进捕获缓冲；IO 失败（对端关
         * 连接属正常收尾）静默终止并记录首个异常。线程登记进 {@link #pumps} 供
         * {@link #close()} join（快照静默握手）。
         *
         * @param in   源流
         * @param out  目标流
         * @param tee  捕获缓冲
         */
        private void pump(InputStream in, OutputStream out, ByteArrayOutputStream tee) {
            Thread t = new Thread(() -> {
                byte[] buf = new byte[8192];
                try {
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        synchronized (tee) {
                            tee.write(buf, 0, n);
                        }
                        out.write(buf, 0, n);
                        out.flush();
                    }
                } catch (IOException e) {
                    pumpFailure.compareAndSet(null, e);   // 会话收尾的连接复位属预期
                }
            }, "og-wire-proxy");
            t.setDaemon(true);
            pumps.add(t);
            t.start();
        }

        /**
         * 本代理监听端口。
         *
         * @return 端口
         */
        int port() {
            return listen.getLocalPort();
        }

        /**
         * 客户端→服务端捕获字节快照（会话停机后调用——泵已随连接关闭终止）。
         *
         * @return 全量字节
         */
        byte[] clientToServerBytes() {
            synchronized (clientToServer) {
                return clientToServer.toByteArray();
            }
        }

        /**
         * 服务端→客户端字节的服务面解析（'k' 帧长度黄金冒烟——Task 5 re-review 残留）。
         *
         * <p>关键步骤：全部按类型帧解析（服务端首应答即带类型的 'R'——无类型帧只有
         * 客户端 startup 与 SSLRequest 应答，本会话两者皆无）；'d' CopyData 载荷首字节
         * 'k' 即 keepalive（长度恰 33 断言面）、'd' 帧计数。</p>
         *
         * @return 帧面计数
         */
        ServerSideFrames serverFrames() {
            byte[] all;
            synchronized (serverToClient) {
                all = serverToClient.toByteArray();
            }
            ServerSideFrames out = new ServerSideFrames();
            ByteBuffer buf = ByteBuffer.wrap(all);
            while (buf.hasRemaining()) {
                if (buf.remaining() < 5) {
                    break;   // 截断尾帧（帧头未到齐——防御位，join 后不应出现）
                }
                int type = buf.get();
                int len = buf.getInt();
                if (len < 4 || buf.remaining() < len - 4) {
                    break;   // 载荷未到齐/帧长非法的截断尾帧——丢弃不比（awaitMore 同构）
                }
                byte[] payload = new byte[len - 4];
                buf.get(payload);
                if (type == 'd' && payload.length > 0) {
                    out.dataFrameCount++;
                    if (payload[0] == 'k') {
                        out.keepaliveCount++;
                        if (payload.length == 33) {
                            out.keepalivesOfExactLength33++;
                        }
                    }
                }
            }
            return out;
        }

        /**
         * 幂等收尾：关三 socket → join 泵线程（上限 {@value #PUMP_JOIN_TIMEOUT_MS}ms）
         * ——快照静默握手：join 建立 happens-before，此后 {@link #clientToServerBytes()}
         * /{@link #serverFrames()} 读到的 tee 必是泵的完整产出（不会截在帧中）；
         * 超时则守护位兜底退出，截断尾帧由解析面的防御分支兜住。
         */
        void close() {
            closeQuietly(client);
            closeQuietly(server);
            closeQuietly(listen);
            for (Thread t : pumps) {
                try {
                    t.join(PUMP_JOIN_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * 静默关 socket（清理路径）。
         *
         * @param s socket（可 null）
         */
        private static void closeQuietly(Closeable s) {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    // 收尾忽略
                }
            }
        }
    }

    /**
     * 服务端→客户端帧面计数（'k' 长度黄金冒烟的断言载体）。
     */
    private static final class ServerSideFrames {
        private int keepaliveCount;
        private int keepalivesOfExactLength33;
        private int dataFrameCount;
    }
}
