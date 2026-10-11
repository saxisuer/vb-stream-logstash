package org.vastdata.vbstream.walsource.receive.direct;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DirectWalSource 对伪服务器的流循环/反馈节奏/断线重连单测（Task 5 循环 ④⑤）：'w' 载荷
 * 交付与锚（dataStart+载荷长）、'k' 观测与 replyRequested 即时 'r' 回应、'm' 跳过、心跳
 * 线程按 statusIntervalMs/2 节奏发 replyRequested=true 的 'r'、断线后重连起点 = 消费前沿
 * 段对齐下取整且字节零丢（重叠段为 at-least-once 预期）、重连耗尽终态失败。段尺寸与退避
 * 走测试档参数（16MB/秒级退避对离线测试过大）。
 */
class DirectWalSourceStreamTest {

    /** 测试档段尺寸：32KB（生产 16MB——FileWalSource.DEFAULT_SEGMENT_SIZE 参数面）。 */
    private static final long SEG_SIZE = 32L * 1024;

    /** md5 黄金向量（循环 ① 同源——服务端校验客户端摘要真值）。 */
    private static final String MD5_VECTOR = "md584a71656325dc15bf06723e874a46f78";

    /** 异步等待上限（毫秒）。 */
    private static final long AWAIT_MS = 10_000;

    /** 脚本 join 上限（毫秒）。 */
    private static final long SCRIPT_TIMEOUT_MS = 15_000;

    /** 记录一次交付（锚 + 载荷），供锚语义与零丢断言。 */
    private static final class Chunk {
        final long anchor;
        final byte[] bytes;

        Chunk(long anchor, byte[] bytes) {
            this.anchor = anchor;
            this.bytes = bytes;
        }
    }

    /** 伪服务器握手至 ReadyForQuery 的公共前段（md5 黄金校验）。 */
    private static void handshake(OgPseudoServer.Session session) throws IOException {
        session.readStartup();
        session.writeAuthMd5(new byte[]{1, 2, 3, 4});
        String digest = session.readPasswordDigest();
        if (!MD5_VECTOR.equals(digest)) {
            throw new AssertionError("md5 摘要不符黄金向量: " + digest);
        }
        session.writeAuthOk();
        session.writeParameterStatus("integer_datetimes", "on");
        session.writeBackendKeyData();
        session.writeReadyForQuery();
    }

    /** IDENTIFY_SYSTEM 应答（xlogpos 可注入），返回收到的命令文本。 */
    private static String identify(OgPseudoServer.Session session, String xlogpos) throws IOException {
        String q = session.readQuery();
        if (!"IDENTIFY_SYSTEM".equals(q)) {
            throw new AssertionError("首命令须为 IDENTIFY_SYSTEM: " + q);
        }
        session.writeRowDescription(4);
        session.writeDataRow("6800000000000001", "1", xlogpos, "primary");
        session.writeCommandComplete("IDENTIFY_SYSTEM");
        session.writeReadyForQuery();
        return q;
    }

    /**
     * 循环 ④：CopyBoth 后推 'w'/'k'/'m'——sink 收到载荷与锚（dataStart+载荷长，非 walEnd）、
     * replyRequested 置位的 'k' 触发即时 'r'（replyRequested=false 档）、心跳线程按
     * statusIntervalMs/2 节奏发 replyRequested=true 档、全部 'r' 的 peer_role=3/peer_state=7
     * 与前沿字段正确、'm' 帧被跳过不断流。
     */
    @Test
    void streamLoopDeliversChunksAnchorsAndFeedbackRhythm() throws Exception {
        long segBase = 0x10000L;   // 65536 = 2 × SEG_SIZE
        try (OgPseudoServer server = OgPseudoServer.start()) {
            AtomicReference<List<OgPseudoServer.StandbyReply>> repliesSeen = new AtomicReference<>(List.of());
            server.script(session -> {
                handshake(session);
                identify(session, "0/10000");
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/10000", session.readQuery(),
                        "起点 = xlogpos 段对齐下取整（pg_receivexlog.cpp:265 同式）");
                session.writeCopyBothResponse();
                session.writeConfigFrame();   // 'm'：须跳过不断流
                session.writeWalData(segBase, segBase + 0x100000L, "abc".getBytes(StandardCharsets.UTF_8));
                session.writeKeepalive(segBase + 0x100000L, true);   // replyRequested → 即时 'r'（false 档）
                repliesSeen.set(session.collectStandbyReplies(1_100, 50));   // 心跳 300ms 节奏窗
                session.writeWalData(segBase + 3, segBase + 0x100000L, "de".getBytes(StandardCharsets.UTF_8));
                session.drainUntilClientClose();
            });
            List<Chunk> chunks = new ArrayList<>();
            List<Long> frontierSinks = new ArrayList<>();
            DirectWalSource source = new DirectWalSource("localhost", server.port(), "gaussdb",
                    "Gauss_12345", "wal_source", 600, SEG_SIZE, 10_000, 50, 3, 0L);
            source.start(l -> frontierSinks.add(l), (lsn, bytes) -> chunks.add(new Chunk(lsn, bytes)));
            try {
                await(() -> chunks.size() == 2, "两块 WAL 须交付");
            } finally {
                source.stop();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);

            assertEquals(2, chunks.size());
            assertEquals(segBase + 3, chunks.get(0).anchor, "首块锚 = dataStart+载荷长（非 walEnd）");
            assertEquals("abc", new String(chunks.get(0).bytes, StandardCharsets.UTF_8));
            assertEquals(segBase + 5, chunks.get(1).anchor, "次块锚接续前块锚");
            assertEquals("de", new String(chunks.get(1).bytes, StandardCharsets.UTF_8));
            assertEquals(segBase + 5, source.consumedLsn());
            assertFalse(frontierSinks.isEmpty(), "chunkEndLsnSink 须逐块通知");
            assertEquals(segBase + 5, frontierSinks.get(frontierSinks.size() - 1), "前沿末值 = 末块锚");

            List<OgPseudoServer.StandbyReply> replies = repliesSeen.get();
            assertTrue(replies.size() >= 3, "即答 1 + 心跳 ≥2（300ms 节奏 × 1100ms 窗），实得 " + replies.size());
            assertTrue(replies.stream().anyMatch(r -> r.replyRequested() == 0),
                    "keepalive 即时回应档 replyRequested=false 须在场");
            assertTrue(replies.stream().filter(r -> r.replyRequested() != 0).count() >= 2,
                    "心跳档 replyRequested=true 须 ≥2");
            for (OgPseudoServer.StandbyReply r : replies) {
                assertEquals(3, r.peerRole(), "peer_role 恒 STANDBY_MODE=3");
                assertEquals(7, r.peerState(), "peer_state 恒 BUILDING_STATE=7");
                assertTrue(r.receive() == 0 || r.receive() == segBase + 3,
                        "反馈前沿 = 已交付字节末尾（首块锚 " + (segBase + 3) + "）或首块前的 0（心跳竞态窗）: "
                                + r.receive());
            }
            assertTrue(replies.stream().anyMatch(r -> r.replyRequested() == 0 && r.receive() == segBase + 3),
                    "keepalive 即时回应（replyRequested=0）须带首块锚——服务端 'w' 先于 'k' 下发");
            assertTrue(source.terminalFailure().isEmpty(), "正常收流无终态失败");
        }
    }

    /**
     * 循环 ⑤：服务端首连接推一块后主动断开 → 客户端指数退避重连，重连起点 = 消费前沿
     * 段对齐下取整（次连接 IDENTIFY 的 xlogpos 更大也不采纳——consumedLsn 优先）、重叠段
     * 重发下字节零丢（[段基, 前沿) 两块并集覆盖、重发块前缀与首块逐字节相同）。
     */
    @Test
    void reconnectResumesFromSegmentAlignedFrontierWithNoByteLoss() throws Exception {
        long segBase = 0x10000L;                       // 65536，段尺寸 32KB 的段边界
        byte[] first = "0123456789".getBytes(StandardCharsets.UTF_8);   // [65536, 65546)
        byte[] replayed = ("0123456789ABCDEF").getBytes(StandardCharsets.UTF_8);   // [65536, 65552) 重发段
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                handshake(session);
                identify(session, "0/10000");
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/10000", session.readQuery(),
                        "首连起点 = xlogpos 段对齐下取整");
                session.writeCopyBothResponse();
                session.writeWalData(segBase, segBase + 0x100000L, first);
                session.writeKeepalive(segBase + 0x100000L, true);
                session.readStandbyReply();   // 即时 'r' 作同步点——首块已被客户端读走并交付
                session.closeClient();        // 主动断线
            });
            server.script(session -> {
                handshake(session);
                identify(session, "0/3000000");        // 次连 xlogpos 更大——不得采纳
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/10000", session.readQuery(),
                        "重连起点 = ⌊consumedLsn(65546)/32KB⌋×32KB = 65536（段对齐下取整）");
                session.writeCopyBothResponse();
                session.writeWalData(segBase, segBase + 0x100000L, replayed);
                session.drainUntilClientClose();
            });
            List<Chunk> chunks = new ArrayList<>();
            DirectWalSource source = new DirectWalSource("localhost", server.port(), "gaussdb",
                    "Gauss_12345", "wal_source", 600, SEG_SIZE, 10_000, 50, 3, 0L);
            source.start(l -> { }, (lsn, bytes) -> chunks.add(new Chunk(lsn, bytes)));
            try {
                await(() -> chunks.size() == 2, "重连后重发块须交付");
            } finally {
                source.stop();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);

            assertEquals(segBase + 10, chunks.get(0).anchor, "首连块锚");
            assertEquals(segBase + 16, chunks.get(1).anchor, "重发块锚（覆盖 [65536, 65552)）");
            assertEquals(segBase + 16, source.consumedLsn());
            assertEquals(new String(first, StandardCharsets.UTF_8),
                    new String(chunks.get(1).bytes, 0, 10, StandardCharsets.UTF_8),
                    "重发块前 10 字节与首块逐字节相同——重叠段 at-least-once 重发形态");
            assertTrue(source.terminalFailure().isEmpty());
        }
    }

    /**
     * 循环 ⑤ 补面：连接耗尽（无监听端口）→ 终态失败暴露于 WalByteSource.terminalFailure()
     * 且工作线程退出——宿主 fail-fast 轮询面。
     */
    @Test
    void connectExhaustionSurfacesTerminalFailure() throws Exception {
        int deadPort;
        try (ServerSocket probe = new ServerSocket(0)) {
            deadPort = probe.getLocalPort();   // 占用后即关——该端口无监听
        }
        DirectWalSource source = new DirectWalSource("localhost", deadPort, "gaussdb",
                "Gauss_12345", "wal_source", 600, SEG_SIZE, 500, 50, 2, 0L);
        source.start(l -> { }, (lsn, bytes) -> { });
        try {
            await(source::isTerminated, "两次连接失败后须终态退出");
        } finally {
            source.stop();
        }
        assertTrue(source.terminalFailure().isPresent(), "终态根因须在场");
        assertInstanceOf(IllegalStateException.class, source.terminalFailure().orElseThrow());
        assertEquals(0, source.consumedLsn());
    }

    /**
     * 审查修复 2 回归：畸形 keepalive 帧（'k' + 截断载荷）属协议字节错位——须按可重试
     * IOException 断流重连（重连后字节续传零丢），不得被 RuntimeException 面误分类成
     * 终态失败。
     */
    @Test
    void malformedKeepaliveFrameTriggersReconnectNotTerminalFailure() throws Exception {
        long segBase = 0x10000L;
        byte[] first = "0123456789".getBytes(StandardCharsets.UTF_8);       // [65536, 65546)
        byte[] second = "ABCDEF".getBytes(StandardCharsets.UTF_8);          // [65546, 65552)
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                handshake(session);
                identify(session, "0/10000");
                session.readQuery();
                session.writeCopyBothResponse();
                session.writeWalData(segBase, segBase + 0x100000L, first);
                session.writeMalformedKeepalive(5);   // 'k' + 5 字节——replyRequested 位越界
                session.drainUntilClientClose();
            });
            server.script(session -> {
                handshake(session);
                identify(session, "0/3000000");
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/10000", session.readQuery(),
                        "畸形帧断流后重连起点 = 前沿段对齐下取整");
                session.writeCopyBothResponse();
                session.writeWalData(segBase + 10, segBase + 0x100000L, second);
                session.drainUntilClientClose();
            });
            List<Chunk> chunks = new ArrayList<>();
            DirectWalSource source = new DirectWalSource("localhost", server.port(), "gaussdb",
                    "Gauss_12345", "wal_source", 600, SEG_SIZE, 10_000, 50, 3, 0L);
            source.start(l -> { }, (lsn, bytes) -> chunks.add(new Chunk(lsn, bytes)));
            try {
                await(() -> chunks.size() == 2, "重连后次块须交付");
            } finally {
                source.stop();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
            assertEquals(segBase + 10, chunks.get(0).anchor);
            assertEquals(segBase + 16, chunks.get(1).anchor, "重连续传锚接续");
            assertEquals(segBase + 16, source.consumedLsn());
            assertTrue(source.terminalFailure().isEmpty(), "畸形帧不得置终态——可重试面");
        }
    }

    /**
     * 审查修复 1 回归：服务端 CopyDone（'c' 帧正常结束 copy 流）后工作线程不得静默终止
     * ——须按可重试中断走重连（重连起点段对齐、字节续传零丢），源持续可用。
     */
    @Test
    void copyDoneFromServerReconnectsInsteadOfSilentTermination() throws Exception {
        long segBase = 0x10000L;
        byte[] first = "0123456789".getBytes(StandardCharsets.UTF_8);
        byte[] second = "ABCDEF".getBytes(StandardCharsets.UTF_8);
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                handshake(session);
                identify(session, "0/10000");
                session.readQuery();
                session.writeCopyBothResponse();
                session.writeWalData(segBase, segBase + 0x100000L, first);
                session.writeCopyDone();   // 服务端正常结束 copy 流
                session.drainUntilClientClose();
            });
            server.script(session -> {
                handshake(session);
                identify(session, "0/3000000");
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/10000", session.readQuery(),
                        "CopyDone 后重连起点 = 前沿段对齐下取整");
                session.writeCopyBothResponse();
                session.writeWalData(segBase + 10, segBase + 0x100000L, second);
                session.drainUntilClientClose();
            });
            List<Chunk> chunks = new ArrayList<>();
            DirectWalSource source = new DirectWalSource("localhost", server.port(), "gaussdb",
                    "Gauss_12345", "wal_source", 600, SEG_SIZE, 10_000, 50, 3, 0L);
            source.start(l -> { }, (lsn, bytes) -> chunks.add(new Chunk(lsn, bytes)));
            try {
                await(() -> chunks.size() == 2, "CopyDone 重连后次块须交付");
                assertFalse(source.isTerminated(),
                        "CopyDone 后工作线程不得静默终止——次块已交付即证其在重连后的连接上继续收流");
            } finally {
                source.stop();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
            assertEquals(segBase + 10, chunks.get(0).anchor);
            assertEquals(segBase + 16, chunks.get(1).anchor, "CopyDone 重连续传锚接续");
            assertEquals(segBase + 16, source.consumedLsn());
            assertTrue(source.terminalFailure().isEmpty(), "单次 CopyDone 属可重试面——无终态");
        }
    }

    /**
     * Task 6 装配缝：显式 startLsn&gt;0 时首连起点采纳注入值（段对齐下取整）而非
     * IDENTIFY_SYSTEM 的 xlogpos——WalSource openGauss 档把引导一致点（ensureSlot P₀ /
     * bootstrap B）传进字节源的通道；xlogpos（服务端当前末观测位点）晚于引导一致点时
     * 若被采纳会跳过 (B, xlogpos] 窗口记录（引导一致性 C1 的 OG 面）。
     */
    @Test
    void explicitStartLsnOverridesIdentifyXlogposOnFirstConnection() throws Exception {
        long segBase = 0x10000L;
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                handshake(session);
                identify(session, "0/3000000");   // 服务端观测位点远于注入起点——不得采纳
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/8000", session.readQuery(),
                        "首连起点 = 注入 startLsn(0x8000, 恰 32KB 段对齐) 而非 xlogpos");
                session.writeCopyBothResponse();
                session.writeWalData(segBase, segBase + 0x100000L, "ok".getBytes(StandardCharsets.UTF_8));
                session.drainUntilClientClose();
            });
            List<Chunk> chunks = new ArrayList<>();
            DirectWalSource source = new DirectWalSource("localhost", server.port(), "gaussdb",
                    "Gauss_12345", "wal_source", 600, SEG_SIZE, 10_000, 50, 3, 0x8000L);
            source.start(l -> { }, (lsn, bytes) -> chunks.add(new Chunk(lsn, bytes)));
            try {
                await(() -> chunks.size() == 1, "起点正确的流上须交付首块");
            } finally {
                source.stop();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);   // 起点断言在脚本线程内执行——不符即 AssertionError 上浮
        }
    }

    /** 轮询等待（100ms 节拍 + 上限断言）。 */
    private static void await(BooleanSupplier cond, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("等待超时: " + message);
    }

    /** 布尔供应商（避免与 java.util.function 同名 import 面的本地别名）。 */
    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
