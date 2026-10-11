package org.vastdata.vbstream.walsource.receive.direct;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * openGauss 认证算法面（纯函数）：md5 挑战（AuthenticationMD5Password 码 5）与 sha256
 * 家族挑战（码 10/11）的客户端应答摘要——逐字节移植自 opengauss-jdbc
 * {@code org.opengauss.util.MD5Digest}（/tmp/og-jdbc.jar 反编译核对，分发版
 * 2023-04-07 build）。全部输出的黄金向量由参考实现实跑生成，钉死在
 * {@code OgAuthTest}——移植偏差（含 {@code "Sever Key"} 拼写——上游 typo 是线格式
 * 一部分，字面必须保留）向量即红。
 *
 * <p>算法族谱（opengauss-jdbc ConnectionFactoryImpl.doAuthentication 的分发面）：</p>
 * <ul>
 *   <li>码 5：PG 标准三段式 {@code md5 + hex(md5(hex(md5(password+user)) + salt))}。</li>
 *   <li>码 10 子码 0/2（服务端存 sha256）：RFC5802 形 proof——
 *       {@code K = PBKDF2(password, saltHex, iters, 256bit)}、
 *       {@code serverKey = HmacSHA256(K, "Sever Key")}、
 *       {@code clientKey = HmacSHA256(K, "Client Key")}、
 *       {@code storedKey = SHA256(clientKey)}、
 *       {@code proof = hex(HmacSHA256(storedKey, token) XOR clientKey)}。</li>
 *   <li>码 10 子码 1（服务端存 md5）：
 *       {@code "sha256" + hex(sha256(hex(md5(password+user)) + salt))}。</li>
 *   <li>码 11（AUTH_REQ_MD5_SHA256）：
 *       {@code "md5" + hex(md5(token + hex(serverKey) + hex(storedKey) + salt))}。</li>
 * </ul>
 *
 * <p>边界与异常语义：算法原语缺失（JDK 必带 MD5/SHA-256/HmacSHA256/PBKDF2WithHmacSHA1，
 * 理论不可达）抛 IllegalStateException；输入由调用方保证宽度（盐 4B/64 字符 hex 等），
 * 宽度错仅产出错误摘要不抛——服务端拒绝认证兜底。纯函数无状态，线程安全。</p>
 */
public final class OgAuth {

    /** RFC5802 派生里 server key 的 HMAC 标签——上游 typo "Sever Key" 是线格式，禁止改拼。 */
    private static final byte[] SERVER_KEY_LABEL = "Sever Key".getBytes(StandardCharsets.UTF_8);

    /** RFC5802 派生里 client key 的 HMAC 标签。 */
    private static final byte[] CLIENT_KEY_LABEL = "Client Key".getBytes(StandardCharsets.UTF_8);

    /**
     * openGauss 服务端缺省 PBKDF2 迭代数（sha2.h:53 {@code ITERATION_COUNT = 10000}）——
     * 3.0 形态挑战不携带迭代数字段时的派生基准（与 3.51 挑战显式携带值并存；opengauss-jdbc
     * 硬编码的 2048 是 V1 旧值 {@code ITERATION_COUNT_V1}，对 5.0.0 服务端创建的验证器派生
     * 不出正确键——真容器实测锚，见 Task 5 报告）。
     */
    public static final int SERVER_DEFAULT_ITERATIONS = 10_000;

    /**
     * 码 11 应答 md5 输入尾缀——openGauss 存储验证器的 11 字符 ITERATION_STRING 字段值
     * （sha2.h:80 {@code ITERATION_STRING_LEN = 11}），服务端校验把它并入 md5 输入
     * （crypt.cpp uaMD5+isSHA256 分支取 {@code shadow_pass + SHA256_LENGTH(6)} 直至串尾，
     * 即 盐+serverKey+storedKey+本尾缀）。该值在 5.0.0 服务端缺省迭代创建的全部验证器上
     * 恒定（多角色实测一致；opengauss-jdbc 的 MD5_SHA256encode 不含本尾缀且用 2048 迭代
     * ——对真服务端恒被拒，勿以其为准）。
     */
    static final String ITERATION_STRING_TAIL = "ecdfecefade";

    /** 私有构造器：纯函数工具类不可实例。 */
    private OgAuth() {
    }

    /**
     * md5 挑战应答（码 5）：{@code "md5" + hex(md5(hex(md5(password+user)) + salt))}。
     *
     * @param user     用户名字节（UTF-8）
     * @param password 密码字节（UTF-8）
     * @param salt     4 字节服务端盐
     * @return 35 字节应答（"md5" + 32 个小写 hex 字符）
     */
    public static byte[] md5Password(byte[] user, byte[] password, byte[] salt) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(password);
            md5.update(user);
            byte[] inner = md5.digest();
            StringBuilder innerHex = new StringBuilder(32);
            hexInto(inner, innerHex);
            md5.reset();
            md5.update(innerHex.toString().getBytes(StandardCharsets.UTF_8));
            md5.update(salt);
            byte[] outer = md5.digest();
            StringBuilder out = new StringBuilder(35);
            out.append("md5");
            hexInto(outer, out);
            return out.toString().getBytes(StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺 MD5 原语（不可达）", e);
        }
    }

    /**
     * RFC5802 sha256 挑战应答（码 10 子码 0）：见类注派生链——proof 为 64 个小写 hex
     * 字符的 ASCII 字节。
     *
     * @param password    明文密码
     * @param saltHex64   PBKDF2 盐（64 个 hex 字符 = 32 字节，服务端挑战的第一 token）
     * @param tokenHex8   签名 token（8 个 hex 字符 = 4 字节，服务端挑战的第二 token）
     * @param iterations  PBKDF2 迭代数（3.51 挑战显式携带；3.0 形态回落服务端缺省 10000）
     * @return 64 字节 proof（ASCII hex）
     */
    public static byte[] rfc5802Proof(String password, String saltHex64, String tokenHex8, int iterations) {
        byte[] k = pbkdf2(password, hexToBytes(saltHex64), iterations, 256);
        byte[] clientKey = hmacSha256(k, CLIENT_KEY_LABEL);
        byte[] storedKey = sha256(clientKey);
        byte[] token = hexToBytes(tokenHex8);
        byte[] clientSignature = hmacSha256(storedKey, token);
        byte[] proof = new byte[clientKey.length];
        for (int i = 0; i < proof.length; i++) {
            proof[i] = (byte) (clientSignature[i] ^ clientKey[i]);
        }
        StringBuilder out = new StringBuilder(64);
        hexInto(proof, out);
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 码 11（AUTH_REQ_MD5_SHA256，hba=md5 + 存储验证器 sha256 的混合形态）应答——
     * <b>真 openGauss 5.0.0 服务端实锚</b>（oracle 逐形态试探 ACCEPTED 验证，Task 5）：
     * {@code "md5" + hex(md5(token64 + serverKey_hex + storedKey_hex + ITERATION_STRING 尾缀 + md5Salt))}，
     * K = PBKDF2(password, token, 10000 迭代)。
     *
     * <p>与 opengauss-jdbc {@code MD5_SHA256encode} 的分叉（其无尾缀、2048 迭代、不
     * 含 token 前缀的形态对真服务端恒被拒——离线向量按本实锚形态重制）：服务端校验面
     * 是 crypt.cpp uaMD5+isSHA256 分支的 {@code pg_md5_encrypt(shadow_pass+6, md5Salt, 4)}，
     * 输入即存储验证器去前缀后的全部剩余（盐+serverKey+storedKey+11 字符尾缀）。</p>
     *
     * @param password   明文密码
     * @param tokenHex64 挑战 token（64 个 hex 字符 = 存储验证器的 PBKDF2 盐）
     * @param salt       4 字节 md5Salt（每连接随机）
     * @return 35 字节应答
     */
    public static byte[] md5Sha256Password(String password, String tokenHex64, byte[] salt) {
        try {
            byte[] k = pbkdf2(password, hexToBytes(tokenHex64), SERVER_DEFAULT_ITERATIONS, 256);
            byte[] serverKey = hmacSha256(k, SERVER_KEY_LABEL);
            byte[] clientKey = hmacSha256(k, CLIENT_KEY_LABEL);
            byte[] storedKey = sha256(clientKey);
            StringBuilder combined = new StringBuilder(64 + 128 + ITERATION_STRING_TAIL.length());
            combined.append(tokenHex64);
            hexInto(serverKey, combined);
            hexInto(storedKey, combined);
            combined.append(ITERATION_STRING_TAIL);
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(combined.toString().getBytes(StandardCharsets.UTF_8));
            md5.update(salt);
            StringBuilder out = new StringBuilder(35);
            out.append("md5");
            hexInto(md5.digest(), out);
            return out.toString().getBytes(StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺 MD5 原语（不可达）", e);
        }
    }

    /**
     * 码 10 子码 1（服务端存 md5）应答：
     * {@code "sha256" + hex(sha256(hex(md5(password+user)) + salt))}。
     *
     * @param user     用户名字节
     * @param password 密码字节
     * @param salt     4 字节服务端盐
     * @return 70 字节应答（"sha256" + 64 个 hex 字符）
     */
    public static byte[] sha256Md5Password(byte[] user, byte[] password, byte[] salt) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(password);
            md5.update(user);
            byte[] inner = md5.digest();
            StringBuilder innerHex = new StringBuilder(32);
            hexInto(inner, innerHex);
            byte[] innerHexBytes = innerHex.toString().getBytes(StandardCharsets.UTF_8);
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update(innerHexBytes);
            sha.update(salt);
            StringBuilder out = new StringBuilder(70);
            out.append("sha256");
            hexInto(sha.digest(), out);
            return out.toString().getBytes(StandardCharsets.UTF_8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺消息摘要原语（不可达）", e);
        }
    }

    /**
     * RFC5802 挑战应答（含服务端签名校验，3.0 形态）：proof 计算同
     * {@link #rfc5802Proof(String, String, String, int)}，另按
     * {@code hex(HmacSHA256(serverKey, token)) == serverSignatureHex} 校验服务端身份
     * （auth.cpp 3.0 挑战随发 sever_signature——C 客户端与 opengauss-jdbc &lt;350 分支均校验）。
     *
     * @param password          明文密码
     * @param saltHex64         PBKDF2 盐
     * @param tokenHex8         签名 token
     * @param iterations        迭代数
     * @param serverSignatureHex 服务端随挑战发来的签名（64 个 hex 字符）
     * @return 64 字节 proof
     * @throws IllegalStateException 签名不符（服务端身份可疑——按认证失败上抛）
     */
    public static byte[] rfc5802ProofVerified(String password, String saltHex64, String tokenHex8,
                                              int iterations, String serverSignatureHex) {
        byte[] k = pbkdf2(password, hexToBytes(saltHex64), iterations, 256);
        byte[] serverKey = hmacSha256(k, SERVER_KEY_LABEL);
        String expected = hexOf(hmacSha256(serverKey, hexToBytes(tokenHex8)));
        if (!expected.equals(serverSignatureHex)) {
            throw new IllegalStateException("RFC5802 服务端签名校验不符（server_signature 不匹配）");
        }
        return rfc5802Proof(password, saltHex64, tokenHex8, iterations);
    }

    /**
     * 服务端签名计算（伪服务器测试面）：{@code hex(HmacSHA256(serverKey(password,salt,iters), token))}。
     *
     * @param password   明文密码
     * @param saltHex64  PBKDF2 盐
     * @param tokenHex8  签名 token
     * @param iterations 迭代数
     * @return 64 个 hex 字符的签名
     */
    static String serverSignatureHex(String password, String saltHex64, String tokenHex8, int iterations) {
        byte[] k = pbkdf2(password, hexToBytes(saltHex64), iterations, 256);
        return hexOf(hmacSha256(hmacSha256(k, SERVER_KEY_LABEL), hexToBytes(tokenHex8)));
    }

    /**
     * PBKDF2（PBKDF2WithHmacSHA1，256 bit 输出）——RFC5802 派生链的 K。
     *
     * @param password   明文密码
     * @param salt       盐字节
     * @param iterations 迭代数
     * @param keyBits    输出密钥位宽（协议恒 256）
     * @return 派生密钥
     */
    private static byte[] pbkdf2(String password, byte[] salt, int iterations, int keyBits) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, keyBits);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
                    .generateSecret(spec)
                    .getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException("PBKDF2 派生失败（参数或原语异常）", e);
        }
    }

    /**
     * HmacSHA256 消息认证码。
     *
     * @param key 密钥字节
     * @param msg 消息字节
     * @return 32 字节 MAC
     */
    private static byte[] hmacSha256(byte[] key, byte[] msg) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(msg);
        } catch (Exception e) {
            throw new IllegalStateException("HmacSHA256 计算失败（原语异常）", e);
        }
    }

    /**
     * SHA-256 摘要。
     *
     * @param input 输入字节
     * @return 32 字节摘要
     */
    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺 SHA-256 原语（不可达）", e);
        }
    }

    /**
     * 字节序列化为小写 hex（追加式——与参考实现 bytesToHex 输出一致）。
     *
     * @param bytes 输入
     * @param into  追加目标
     */
    private static void hexInto(byte[] bytes, StringBuilder into) {
        char[] digits = "0123456789abcdef".toCharArray();
        for (byte b : bytes) {
            into.append(digits[(b >> 4) & 0xF]).append(digits[b & 0xF]);
        }
    }

    /**
     * 字节序列化为小写 hex（返回式）。
     *
     * @param bytes 输入
     * @return hex 字符串
     */
    private static String hexOf(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        hexInto(bytes, sb);
        return sb.toString();
    }

    /**
     * hex 字符串解回字节（长度须为偶数；与参考实现 hexStringToBytes 一致——小写大写均收）。
     *
     * @param hex hex 字符串
     * @return 字节序列
     */
    private static byte[] hexToBytes(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) ((Character.digit(hex.charAt(2 * i), 16) << 4)
                    | Character.digit(hex.charAt(2 * i + 1), 16));
        }
        return out;
    }
}
