package org.vastdata.vbstream.walsource.receive.direct;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V3ProtocolChannel 对伪服务器的握手/帧面单测（Task 5 循环 ①②）：startup 参数形态、
 * md5/sha256 认证挑战、'Q' IDENTIFY_SYSTEM DataRow 解析、START_REPLICATION 命令文本与
 * CopyBoth 进入、ErrorResponse 上抛。服务端脚本全部经 {@link OgPseudoServer} 手写 v3
 * 帧应答；md5/sha256 摘要断言用 opengauss-jdbc 参考实现生成的黄金向量（真值，非闭环）。
 */
class V3ProtocolChannelTest {

    /** md5 黄金向量（参考实现 /tmp/og-jdbc.jar MD5Digest.encode 实跑产物）。 */
    private static final String MD5_VECTOR = "md584a71656325dc15bf06723e874a46f78";

    /** sha256 RFC5802 挑战黄金向量成分（与参考实现实跑对齐，2048 迭代）。 */
    private static final String SALT_HEX64 =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String TOKEN_HEX8 = "deadbeef";

    /** RFC5802 黄金向量（参考实现实跑产物，2048 迭代）。 */
    private static final String RFC5802_VECTOR =
            "acdbf9f191301567f9029afe0a75419f2432a2cf696387ffbe94e9a206ef990c";

    /** RFC5802 黄金向量（参考实现实跑产物，10000 迭代 = 服务端缺省档）。 */
    private static final String PROOF_10000_VECTOR =
            "ad05dacca0f1f1fc459a2bfdc629921c47e4d83e4d71a0091043c44fa750d2f3";

    /** 脚本 join 上限（毫秒）。 */
    private static final long SCRIPT_TIMEOUT_MS = 10_000;

    /** 循环 ①：startup（replication=true + database=replication + user）→ md5 挑战黄金
     * 摘要 → AuthenticationOk → 'Q' IDENTIFY_SYSTEM DataRow 解析 → 'Q' START_REPLICATION
     * （含 SLOT 与 %X/%X 文本）→ CopyBothResponse 进入流面。 */
    @Test
    void md5HandshakeIdentifyAndCopyBothEntry() throws Exception {
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                Map<String, String> startup = session.readStartup();
                if (!"true".equals(startup.get("replication"))) {
                    throw new AssertionError("startup 须带 replication=true: " + startup);
                }
                if (!"replication".equals(startup.get("database"))) {
                    throw new AssertionError("startup 须带 database=replication（streamutil.cpp:175 形态）: " + startup);
                }
                if (!"gaussdb".equals(startup.get("user"))) {
                    throw new AssertionError("startup 须带 user: " + startup);
                }
                session.writeAuthMd5(new byte[]{1, 2, 3, 4});
                String digest = session.readPasswordDigest();
                if (!MD5_VECTOR.equals(digest)) {
                    throw new AssertionError("md5 摘要不符黄金向量: " + digest);
                }
                session.writeAuthOk();
                session.writeParameterStatus("integer_datetimes", "on");
                session.writeBackendKeyData();
                session.writeReadyForQuery();
                assertEquals("IDENTIFY_SYSTEM", session.readQuery(), "第一条命令须为 IDENTIFY_SYSTEM");
                session.writeRowDescription(4);
                session.writeDataRow("6800000000000001", "1", "0/1000000", "primary");
                session.writeCommandComplete("IDENTIFY_SYSTEM");
                session.writeReadyForQuery();
                assertEquals("START_REPLICATION SLOT \"wal_source\" 0/1000000", session.readQuery(),
                        "START_REPLICATION 命令文本须与 receivelog.cpp:632 形态一致");
                session.writeCopyBothResponse();
                session.writeWalData(0x1000000L, 0x1001000L, new byte[]{9, 9, 9});
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "Gauss_12345", "vb-wal-source", 3_000, 0)) {
                channel.connect();
                String[] row = channel.identifySystem();
                assertEquals("6800000000000001", row[0], "systemid 为 DataRow 首列");
                assertEquals("1", row[1], "timeline 为次列");
                assertEquals("0/1000000", row[2], "xlogpos 为第三列");
                channel.startPhysicalReplication("wal_source", Lsn.parse("0/1000000"));
                byte[] frame = channel.readCopyData();
                assertEquals('w', frame[0], "CopyBoth 后首帧须为 'w'");
                assertEquals(0x1000000L, V3ProtocolChannel.walDataStart(frame));
                assertArrayEquals(new byte[]{9, 9, 9}, V3ProtocolChannel.walPayload(frame),
                        "WAL 载荷 = 64B 头之后的字节");
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
    }

    /** 循环 ① 补面：认证失败（ErrorResponse）须上抛 IOException 且消息含服务端 M 字段。 */
    @Test
    void authenticationFailureSurfacesServerError() throws Exception {
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                session.readStartup();
                session.writeAuthMd5(new byte[]{1, 2, 3, 4});
                session.readPasswordDigest();
                session.writeError("28P01", "password authentication failed");
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "wrong", "vb-wal-source", 3_000, 0)) {
                IOException e = assertThrows(IOException.class, channel::connect,
                        "ErrorResponse 须上抛连接失败");
                assertTrue(e.getMessage().contains("password authentication failed"),
                        "异常消息须含服务端错误文本: " + e.getMessage());
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
    }

    /** 循环 ②：sha256 挑战（'R' 码 10 子码 0，64 字符盐 + 8 字符 token + 迭代数）——
     * 客户端 'p' 应答须命中 RFC5802 黄金向量（opengauss-jdbc 参考实现真值）。 */
    @Test
    void sha256ChallengeProducesReferenceVectorProof() throws Exception {
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                session.readStartup();
                session.writeAuthSha256(SALT_HEX64, TOKEN_HEX8, 2048);
                String proof = session.readPasswordDigest();
                if (!RFC5802_VECTOR.equals(proof)) {
                    throw new AssertionError("RFC5802 proof 不符黄金向量: " + proof);
                }
                session.writeAuthOk();
                session.writeReadyForQuery();
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "Gauss_12345", "vb-wal-source", 3_000, 0)) {
                channel.connect();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
    }

    /** 循环 ② 补面：无迭代数尾缀形态回落服务端缺省 10000（sha2.h ITERATION_COUNT）——
     * proof 须命中 10000 迭代参考向量。 */
    @Test
    void sha256ChallengeWithoutIterationsFallsBackToServerDefault() throws Exception {
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                session.readStartup();
                session.writeAuthSha256(SALT_HEX64, TOKEN_HEX8, null);
                String proof = session.readPasswordDigest();
                if (!PROOF_10000_VECTOR.equals(proof)) {
                    throw new AssertionError("无迭代数形态的 proof 须按服务端缺省 10000 迭代派生: " + proof);
                }
                session.writeAuthOk();
                session.writeReadyForQuery();
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "Gauss_12345", "vb-wal-source", 3_000, 0)) {
                channel.connect();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
    }

    /** 循环 ② 补面：迭代数参与派生（1024 迭代产物须不同于 2048 黄金向量）。 */
    @Test
    void sha256ChallengeIterationCountChangesProof() throws Exception {
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                session.readStartup();
                session.writeAuthSha256(SALT_HEX64, TOKEN_HEX8, 1024);
                String proof = session.readPasswordDigest();
                if (RFC5802_VECTOR.equals(proof)) {
                    throw new AssertionError("1024 迭代的 proof 不应与 2048 向量相同——迭代数未参与派生");
                }
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "Gauss_12345", "vb-wal-source", 3_000, 0)) {
                assertThrows(IOException.class, channel::connect, "脚本不回 Ok——以断连收尾即可");
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
    }

    /**
     * 循环 ② 补面（3.0 signature 形态，auth.cpp minor&lt;GAUSS_BASE 分支）：挑战以 64 字符
     * sever_signature 收尾、不携带迭代数——签名合法时客户端按服务端缺省迭代数派生并通过，
     * proof 与 10000 迭代参考向量一致（向量由 opengauss-jdbc RFC5802Algorithm(…,10000) 实跑
     * 生成后内联核对）。坏签名面：客户端校验拒绝（IOException）。
     */
    @Test
    void sha256V30SignatureFormVerifiesServerAndUsesServerDefaultIterations() throws Exception {
        // 10000 迭代（服务端缺省）proof 参考向量——opengauss-jdbc RFC5802Algorithm(…,10000) 实跑产物。
        final String proof10000 = "ad05dacca0f1f1fc459a2bfdc629921c47e4d83e4d71a0091043c44fa750d2f3";
        String goodSignature = OgAuth.serverSignatureHex("Gauss_12345", SALT_HEX64, TOKEN_HEX8, 10_000);
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                session.readStartup();
                session.writeAuthSha256V30(SALT_HEX64, TOKEN_HEX8, goodSignature);
                String proof = session.readPasswordDigest();
                if (!proof10000.equals(proof)) {
                    throw new AssertionError("3.0 signature 形态 proof 须按服务端缺省 10000 迭代派生: " + proof);
                }
                session.writeAuthOk();
                session.writeReadyForQuery();
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "Gauss_12345", "vb-wal-source", 3_000, 0)) {
                channel.connect();
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
        // 坏签名：校验拒绝
        String badSignature = "0".repeat(64);
        try (OgPseudoServer server = OgPseudoServer.start()) {
            server.script(session -> {
                session.readStartup();
                session.writeAuthSha256V30(SALT_HEX64, TOKEN_HEX8, badSignature);
            });
            try (V3ProtocolChannel channel = new V3ProtocolChannel("localhost", server.port(),
                    "gaussdb", "Gauss_12345", "vb-wal-source", 3_000, 0)) {
                IOException e = assertThrows(IOException.class, channel::connect,
                        "sever_signature 不符须拒绝认证");
                assertTrue(e.getMessage().contains("签名校验失败"),
                        "异常须指明签名校验失败: " + e.getMessage());
            }
            server.awaitScript(SCRIPT_TIMEOUT_MS);
        }
    }
}
