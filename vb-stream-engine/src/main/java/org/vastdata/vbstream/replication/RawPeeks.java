package org.vastdata.vbstream.replication;

import java.nio.charset.StandardCharsets;

/**
 * raw 字节窥探辅助（从 1.6 TransactionAssembler 的私有静态方法整编为包私有工具，组装器路由与
 * 回放器 streamXid 重窥共用）。全部纯函数。符号位处理约定见 {@link #intAt} 的 javadoc
 * （Task 12 实测踩坑：无符号字节必须先 &amp;0xFF 再拼）。
 */
final class RawPeeks {

    private RawPeeks() {
    }

    /** big-endian 读 4 字节有符号整数（oid 等）。每字节先 &amp;0xFF 再移位拼接——byte 有符号，直接 | 会把符号位扩散到高位。 */
    static int intAt(byte[] raw, int offset) {
        return ((raw[offset] & 0xFF) << 24) | ((raw[offset + 1] & 0xFF) << 16)
                | ((raw[offset + 2] & 0xFF) << 8) | (raw[offset + 3] & 0xFF);
    }

    /** big-endian 读 4 字节无符号整数入 long（流式前缀 xid）。 */
    static long unsignedInt(byte[] raw, int offset) {
        return intAt(raw, offset) & 0xFFFFFFFFL;
    }

    /** big-endian 读 8 字节 long（LogicalMsg 的 lsn 窥探）。 */
    static long longAt(byte[] raw, int offset) {
        return (unsignedInt(raw, offset) << 32) | unsignedInt(raw, offset + 4);
    }

    /** 读 offset 起的 null 结尾 UTF-8 字符串（LogicalMsg prefix，仅异常/告警路径）。 */
    static String cstringAt(byte[] raw, int offset) {
        int end = offset;
        while (raw[end] != 0) {
            end++;
        }
        return new String(raw, offset, end - offset, StandardCharsets.UTF_8);
    }
}
