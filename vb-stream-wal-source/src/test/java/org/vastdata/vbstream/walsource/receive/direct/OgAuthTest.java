package org.vastdata.vbstream.walsource.receive.direct;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * OgAuth 认证算法黄金向量单测（Task 5 循环 ②）：md5/RFC5802/sha256Md5 期望值由
 * opengauss-jdbc 参考实现（/tmp/og-jdbc.jar 的 org.opengauss.util.MD5Digest，经反射
 * 实跑）生成——真值锚，非闭环；码 11 向量以真 openGauss 5.0.0 服务端 ACCEPTED 的
 * oracle 实锚钉死（参考实现在迭代数/尾缀两处与 5.0.0 服务端不符，见 OgAuth 注）。
 * 向量成分：user=gaussdb / password=Gauss_12345 / 盐 {01,02,03,04} / RFC5802 盐
 * 64 字符 hex + token 8 字符 hex，迭代数 2048 与 1024（迭代数敏感性）。
 */
class OgAuthTest {

    private static final String SALT_HEX64 =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final String TOKEN_HEX8 = "deadbeef";

    /** md5（码 5）：PG 标准三段式，参考实现 encode 实跑产物。 */
    @Test
    void md5PasswordMatchesReferenceVector() {
        byte[] digest = OgAuth.md5Password("gaussdb".getBytes(StandardCharsets.UTF_8),
                "Gauss_12345".getBytes(StandardCharsets.UTF_8), new byte[]{1, 2, 3, 4});
        assertEquals("md584a71656325dc15bf06723e874a46f78",
                new String(digest, StandardCharsets.UTF_8), "md5 黄金向量");
    }

    /** RFC5802 sha256 挑战（码 10 子码 0）：proof = hex(HMAC(storedKey,token) XOR clientKey)，
     * 参考实现 RFC5802Algorithm 实跑产物（2048 迭代）。 */
    @Test
    void rfc5802ProofMatchesReferenceVector() {
        byte[] proof = OgAuth.rfc5802Proof("Gauss_12345", SALT_HEX64, TOKEN_HEX8, 2048);
        assertEquals("acdbf9f191301567f9029afe0a75419f2432a2cf696387ffbe94e9a206ef990c",
                new String(proof, StandardCharsets.UTF_8), "RFC5802 黄金向量（2048 迭代）");
    }

    /** 迭代数敏感性：1024 迭代产物 ≠ 2048 向量，且等于参考实现 1024 实跑产物。 */
    @Test
    void rfc5802IterationCountIsLoadBearing() {
        byte[] proof = OgAuth.rfc5802Proof("Gauss_12345", SALT_HEX64, TOKEN_HEX8, 1024);
        assertEquals("8b52e38c16e1e37777bcbf8f3faa628bcd5c303b5cc64af6f1d62439e39bd284",
                new String(proof, StandardCharsets.UTF_8), "RFC5802 黄金向量（1024 迭代）");
    }

    /**
     * 码 11（AUTH_REQ_MD5_SHA256）——<b>真 openGauss 5.0.0 服务端 ACCEPTED 实锚向量</b>
     * （Task 5 oracle 逐形态试探：md5 输入 = token+serverKey+storedKey+11 字符尾缀+md5Salt、
     * 10000 迭代；其余形态含 opengauss-jdbc 的 2048/token 前缀变体均被拒）。
     */
    @Test
    void md5Sha256PasswordMatchesLiveAcceptedVector() {
        byte[] digest = OgAuth.md5Sha256Password("Sha256_78901",
                "2693b56e96f983769af4c7e1d344c9d4b3c9681233072b3900167ba26b4a8e7a",
                new byte[]{(byte) 0xab, (byte) 0xb8, 0x4c, (byte) 0xb4});
        assertEquals("md583799880cc1a882c1c5c2fba4104bc6d",
                new String(digest, StandardCharsets.UTF_8), "码 11 真容器实锚黄金向量");
    }

    /** 码 10 子码 1（服务端存 md5）："sha256" 前 + sha256(md5hex+盐) 后段。 */
    @Test
    void sha256Md5PasswordMatchesReferenceVector() {
        byte[] digest = OgAuth.sha256Md5Password("gaussdb".getBytes(StandardCharsets.UTF_8),
                "Gauss_12345".getBytes(StandardCharsets.UTF_8), new byte[]{1, 2, 3, 4});
        assertEquals("sha2569fe63aefa7257204fd07c8fd4f1cc4d91f040c9564bc68c141ec7bea0aae6903",
                new String(digest, StandardCharsets.UTF_8), "SHA256_MD5 黄金向量");
    }

    /** 输出面宽度自检：md5 系 35 字节、RFC5802 proof 64 字节（ASCII hex）——'p' 载荷契约。 */
    @Test
    void outputWidthsMatchWireContract() {
        assertEquals(35, OgAuth.md5Password("u".getBytes(), "p".getBytes(), new byte[4]).length);
        assertEquals(64, OgAuth.rfc5802Proof("p", SALT_HEX64, TOKEN_HEX8, 2048).length);
        assertEquals(35, OgAuth.md5Sha256Password("p", SALT_HEX64, new byte[4]).length);
        assertEquals(70, OgAuth.sha256Md5Password("u".getBytes(), "p".getBytes(), new byte[4]).length);
    }
}
