package org.vastdata.debezium.connector.postgresql.stream;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * raw 字节窥探辅助(组装器路由与回放器 streamXid 重窥共用)。引擎
 * {@code org.vastdata.vbstream.replication.RawPeeks} 的 1:1 重写(文字参照,非依赖)。
 * 全部纯函数。符号位处理约定见 {@link #intAt} 的 javadoc(引擎 Task 12 实测踩坑:
 * 无符号字节必须先 &amp;0xFF 再拼)。
 */
final class RawPeeks {

    private RawPeeks() {
    }

    /** big-endian 读 4 字节有符号整数(oid 等)。每字节先 &amp;0xFF 再移位拼接——byte 有符号,直接 | 会把符号位扩散到高位。 */
    static int intAt(byte[] raw, int offset) {
        return ((raw[offset] & 0xFF) << 24) | ((raw[offset + 1] & 0xFF) << 16)
                | ((raw[offset + 2] & 0xFF) << 8) | (raw[offset + 3] & 0xFF);
    }

    /** big-endian 读 4 字节无符号整数入 long(流式前缀 xid)。 */
    static long unsignedInt(byte[] raw, int offset) {
        return intAt(raw, offset) & 0xFFFFFFFFL;
    }

    /** big-endian 读 8 字节 long(LogicalMsg 的 lsn 窥探)。 */
    static long longAt(byte[] raw, int offset) {
        return (unsignedInt(raw, offset) << 32) | unsignedInt(raw, offset + 4);
    }

    /**
     * 读 offset 起的 null 结尾 UTF-8 字符串(LogicalMsg prefix,异常/告警/留痕路径)。
     * 结束边界由 {@link #cstringEnd} 承担,此处只做区间解码。
     */
    static String cstringAt(byte[] raw, int offset) {
        int end = cstringEnd(raw, offset);
        return new String(raw, offset, end - offset, StandardCharsets.UTF_8);
    }

    /**
     * 责任:定位 offset 起(含)第一个 NUL(0x00)字节的下标——CString 的结束边界。
     * 边界:raw 无 NUL(协议违约)时一直扫描到数组越界抛 AIOOBE——与既有 cstringAt
     * 行为一致,调用方自负线格式合法(窥探只服务日志,不承担协议校验)。
     */
    static int cstringEnd(byte[] raw, int offset) {
        int end = offset;
        while (raw[end] != 0) {
            end++;
        }
        return end;
    }

    /**
     * 责任:拷贝 {@code raw[offset, offset+length)} 的字节副本(LogicalMsg content 窥取)。
     * 返回副本——调用方持有片段不碰原数组(原数组归 CQ appender 单写者所有)。
     * 边界:区间越界由 Arrays.copyOfRange 抛异常(线格式违约,同上不承担校验)。
     */
    static byte[] bytesAt(byte[] raw, int offset, int length) {
        return Arrays.copyOfRange(raw, offset, offset + length);
    }
}
