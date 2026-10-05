package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 磁盘格式 tuple 解码层（TupleDecoder / DecodeKinds）的失败先行测试——TupleBytes
 * DSL 手造载荷（与被测互为双源转录）→ 三入口（payload / pageTuple / entry）解码断言。
 *
 * <p>断言面来自任务书 Step 1：6 列混合类型（含 UTF-8 π 文本 / float8 / bool /
 * timestamp 微秒往返）；null 位图含 dropped 占位（natts &gt; 活列数，spike 发现 26）；
 * 4B varlena 头 7000 字节直击 {@code u32le>>>2} 读长（防 BE 回归，spike 发现 21）；
 * name 63B + NUL 截断；multi-insert entry 的 2B datalen 前缀偏移 +2（spike 教训）。
 * 另补：11 个内建 oid→kind 映射与未知 oid fail-fast、对齐相对 tuple 起点（payload
 * 前置杂字节后仍解对）、skip 消耗 varlena 不取值、external/compressed varlena v1
 * 拒绝（ISE）、natts 超词典长度 fail-fast。</p>
 */
class TupleDecoderTest {

    private final WalLayout layout = WalLayoutV18.INSTANCE;
    private final TupleDecoder decoder = new TupleDecoder(layout);

    /**
     * 用例 1：6 列混合类型 payload 形态往返——int2/UTF-8 文本（π）/float8 位模式/bool/
     * timestamp 微秒（epoch 2000 起点，期望值经 LocalDateTime.plusNanos 独立构造）/
     * int4 高位值。
     */
    @Test
    void sixColumnPayloadRoundTripsMixedTypes() {
        String[] kinds = {"int2", "text", "float8", "bool", "timestamp", "int4"};
        long micros = 1_234_567_890_123L;
        byte[] payload = TupleBytes.of(kinds)
                .i2(-42)
                .text("π-值-π")
                .f8(Math.PI)
                .bool(true)
                .ts(micros)
                .i32(0x7F123456)
                .payload();
        Object[] vals = decoder.decodePayload(payload, 0, kinds);
        assertEquals(6, vals.length);
        assertEquals((short) -42, vals[0]);
        assertEquals("π-值-π", vals[1]);
        assertEquals(Math.PI, vals[2]);
        assertEquals(Boolean.TRUE, vals[3]);
        // 期望 ISO 串不经 Instant/floorDiv 构造，与实现的 epoch 换算互为双源
        assertEquals(LocalDateTime.of(2000, 1, 1, 0, 0).plusNanos(micros * 1_000L).toString(), vals[4]);
        assertEquals(0x7F123456, vals[5]);
    }

    /**
     * 用例 2：null 位图 + dropped 占位——natts=4 而活列只 2（dropped 恒 null 不占字节、
     * bool 列显式 null），int4/text 两活列跨过占位仍按各自对齐解对（spike 发现 26）。
     */
    @Test
    void nullBitmapWithDroppedPlaceholderKeepsAlignment() {
        String[] kinds = {"int4", "dropped", "text", "bool"};
        byte[] payload = TupleBytes.of(kinds)
                .i32(77)
                .text("abc")
                .nullAt(3)
                .payload();
        Object[] vals = decoder.decodePayload(payload, 0, kinds);
        assertEquals(4, vals.length);
        assertEquals(77, vals[0]);
        assertNull(vals[1]);    // dropped：位图 null + 零字节消耗
        assertEquals("abc", vals[2]);
        assertNull(vals[3]);
    }

    /**
     * 用例 3：4B varlena 头 7000 字节——手工直拼（不经 DSL）：u32le = 7004&lt;&lt;2，
     * 解码读长必须走 {@code u32le>>>2}（BE 读法立刻读出天文数字越界，防回归锚）。
     */
    @Test
    void fourByteVarlenaHeaderReadsLittleEndianLength() {
        byte[] body = "x".repeat(7000).getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[5 + 1 + 4 + body.length];
        payload[0] = 1;    // infomask2 = natts 1
        payload[4] = 24;   // t_hoff = MAXALIGN(23)；payload[5] = 垫字节（tuple offset 23）
        payload[6] = (byte) ((7004 << 2) & 0xFF);          // u32le 低字节在先
        payload[7] = (byte) (((7004 << 2) >>> 8) & 0xFF);
        payload[8] = (byte) (((7004 << 2) >>> 16) & 0xFF);
        payload[9] = (byte) (((7004 << 2) >>> 24) & 0xFF);
        System.arraycopy(body, 0, payload, 10, body.length);
        Object[] vals = decoder.decodePayload(payload, 0, new String[]{"text"});
        assertEquals(7000, ((String) vals[0]).length());
        assertEquals("x".repeat(7000), vals[0]);
    }

    /**
     * 用例 4：name 列 63 字节 + 第 64 字节 NUL——解码按首个 NUL 截断，长度恰 63。
     */
    @Test
    void nameKindTruncatesAtFirstNul() {
        String s63 = "n".repeat(63);
        byte[] payload = TupleBytes.of("name").name(s63).payload();
        Object[] vals = decoder.decodePayload(payload, 0, new String[]{"name"});
        assertEquals(s63, vals[0]);
        assertEquals(63, ((String) vals[0]).length());
    }

    /**
     * 用例 5：multi-insert entry 形态——[datalen u16 前缀][payload]，decodeEntry 相对
     * entryOff 偏移 +2；前置 7B 杂字节同时验证对齐相对 tuple 起点而非缓冲起点。
     */
    @Test
    void entryFormSkipsTwoByteDatalenPrefix() {
        String[] kinds = {"bool", "int4", "text"};
        byte[] inner = TupleBytes.of(kinds).bool(false).i32(9).text("值").payload();
        byte[] entry = new byte[7 + 2 + inner.length];
        entry[7] = (byte) inner.length;          // datalen u16 前缀（低字节）
        entry[8] = (byte) (inner.length >>> 8);
        System.arraycopy(inner, 0, entry, 9, inner.length);
        Object[] vals = decoder.decodeEntry(entry, 7, kinds);
        assertEquals(Boolean.FALSE, vals[0]);
        assertEquals(9, vals[1]);
        assertEquals("值", vals[2]);
    }

    /**
     * 用例 6：页内完整 tuple 形态——23B 头前缀嵌入 8192 假页 lpOff 处（含 null 位图），
     * decodePageTuple 与 payload 形态解出同值。
     */
    @Test
    void pageTupleFormDecodesFromEmbeddedOffset() {
        String[] kinds = {"oid", "bool", "timestamp"};
        long micros = 86_400_000_000L;   // 恰一天：无小数位 ISO 串
        TupleBytes tb = TupleBytes.of(kinds).oid(0xFFFFFFFFL).bool(true).ts(micros);
        byte[] tuple = tb.tuple();
        byte[] page = new byte[8192];
        System.arraycopy(tuple, 0, page, 137, tuple.length);
        Object[] vals = decoder.decodePageTuple(page, 137, kinds);
        assertEquals(0xFFFFFFFFL, vals[0]);
        assertEquals(Boolean.TRUE, vals[1]);
        assertEquals("2000-01-02T00:00", vals[2]);
        // 同一 DSL 的 payload 形态解出同值（两形态共享 dataRegion）
        Object[] viaPayload = decoder.decodePayload(tb.payload(), 0, kinds);
        assertArrayEquals(viaPayload, vals);
    }

    /**
     * 用例 7：11 个内建类型 oid 全映射 + 未知 oid fail-fast（消息含 oid 值）。
     */
    @Test
    void forTypeOidMapsBuiltinsAndFailsUnknown() {
        assertEquals("bool", DecodeKinds.forTypeOid(16));
        assertEquals("char", DecodeKinds.forTypeOid(18));
        assertEquals("name", DecodeKinds.forTypeOid(19));
        assertEquals("int8", DecodeKinds.forTypeOid(20));
        assertEquals("int2", DecodeKinds.forTypeOid(21));
        assertEquals("int4", DecodeKinds.forTypeOid(23));
        assertEquals("text", DecodeKinds.forTypeOid(25));
        assertEquals("oid", DecodeKinds.forTypeOid(26));
        assertEquals("float4", DecodeKinds.forTypeOid(700));
        assertEquals("float8", DecodeKinds.forTypeOid(701));
        assertEquals("timestamp", DecodeKinds.forTypeOid(1114));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> DecodeKinds.forTypeOid(0));
        assertTrue(e.getMessage().contains("unregistered type oid"));
        assertTrue(e.getMessage().contains("0"));
    }

    /**
     * 用例 8：varlena 短外部指针（0x01 + 0x12 VARTAG_ONDISK）与 4B 压缩 tag
     * （b0&amp;0x03==0x02）v1 一律 ISE 拒绝——external/compressed 支持留给 v2。
     */
    @Test
    void externalAndCompressedVarlenaRejected() {
        byte[] ext = TupleBytes.of("text").text("hello").payload();
        ext[6] = 0x01;   // 覆盖 varlena 首字节（tuple offset 24）：短外部指针 tag
        ext[7] = 0x12;   // VARTAG_ONDISK
        IllegalStateException e1 = assertThrows(IllegalStateException.class,
                () -> decoder.decodePayload(ext, 0, new String[]{"text"}));
        assertTrue(e1.getMessage().contains("external/compressed varlena unsupported in v1"));

        byte[] comp = TupleBytes.of("text").text("hello").payload();
        comp[6] = 0x02;  // 4B 压缩 varlena tag（(b0&0x03)==0x02，tuple offset 24）
        comp[7] = 0x00;
        comp[8] = 0x00;
        comp[9] = 0x00;
        IllegalStateException e2 = assertThrows(IllegalStateException.class,
                () -> decoder.decodePayload(comp, 0, new String[]{"text"}));
        assertTrue(e2.getMessage().contains("external/compressed varlena unsupported in v1"));
    }

    /**
     * 用例 9：skip 列走读消耗 varlena 但不取值——其后 text 列解对即证明消耗字节数恰准。
     */
    @Test
    void skipKindConsumesVarlenaWithoutValue() {
        String[] kinds = {"text", "skip", "text"};
        byte[] payload = TupleBytes.of(kinds)
                .text("aaa")
                .skipVarlena("b".repeat(200))   // 4B 头长载荷，跳过面更大
                .text("ccc")
                .payload();
        Object[] vals = decoder.decodePayload(payload, 0, kinds);
        assertEquals("aaa", vals[0]);
        assertNull(vals[1]);
        assertEquals("ccc", vals[2]);
    }

    /**
     * 用例 10：natts 超过词典长度 fail-fast（消息含两个计数）。
     */
    @Test
    void nattsExceedingDictionaryFails() {
        byte[] payload = TupleBytes.of("int4", "text").i32(1).text("x").payload();
        payload[0] = 3;    // 篡改 infomask2：natts=3 > 词典 2
        payload[1] = 0;
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> decoder.decodePayload(payload, 0, new String[]{"int4", "text"}));
        assertTrue(e.getMessage().contains("3"));
        assertTrue(e.getMessage().contains("2"));
    }

    /**
     * 用例 11：char/int8/float4 渲染——char 仅左引号单字符（与 spike 渲染逐字一致）、
     * int8 long、float4 float。
     */
    @Test
    void charInt8Float4Render() {
        String[] kinds = {"char", "int8", "float4"};
        byte[] payload = TupleBytes.of(kinds).ch('z').i64(123456789012345L).f4(1.5f).payload();
        Object[] vals = decoder.decodePayload(payload, 0, kinds);
        assertEquals("'z", vals[0]);
        assertEquals(123456789012345L, vals[1]);
        assertEquals(1.5f, vals[2]);
    }

    /**
     * 用例 12：奇总长 1B varlena 头合法性（审查修正锚）——1B 头总长为奇数时
     * b0&amp;0x03 恰为 0x03/0x07（bit0=1 即 1B 头），不得当 compressed 拒：空串
     * （总长 1，b0=0x03）与 "ab"（总长 3，b0=0x07）均须正常解出。
     */
    @Test
    void oddTotalLengthOneByteVarlenaHeaderIsLegal() {
        String[] kinds = {"text", "text"};
        byte[] payload = TupleBytes.of(kinds).text("").text("ab").payload();
        Object[] vals = decoder.decodePayload(payload, 0, kinds);
        assertEquals("", vals[0]);
        assertEquals("ab", vals[1]);
        // 钉死首字节位形：空串 b0=0x03 @tuple24；"ab" 4 对齐到 tuple28（b0=0x07）
        assertEquals(0x03, payload[6] & 0xFF);
        assertEquals(0x07, payload[10] & 0xFF);
        assertEquals(97, payload[11] & 0xFF);   // 'a' 紧随其头，位形钉死双确认
    }

    /**
     * 用例 13：skip 列奇总长 varlena——跳过面与读值面走同一头分派，奇总长 1B 头
     * 的 skip 列须消耗恰准（其后 text 列解对即证明）。
     */
    @Test
    void skipKindConsumesOddTotalLengthVarlena() {
        String[] kinds = {"skip", "text"};
        byte[] payload = TupleBytes.of(kinds).skipVarlena("xy").text("tail").payload();
        Object[] vals = decoder.decodePayload(payload, 0, kinds);
        assertNull(vals[0]);
        assertEquals("tail", vals[1]);
    }

    /**
     * 用例 14：bytea 奇总长 1B 头（手工直拼，kinds 走 bytea 分支）——读长分派与
     * text 同源，b0=0x07（总长 3，2 载荷字节）解出 byte[2]。
     */
    @Test
    void byteaOddTotalLengthOneByteHeaderDecodes() {
        byte[] payload = new byte[5 + 1 + 3];
        payload[0] = 1;    // infomask2 = natts 1
        payload[4] = 24;   // t_hoff = MAXALIGN(23)；payload[5] = 垫字节
        payload[6] = 0x07; // 1B 头：总长 3（奇，b0&0x03==0x03——合法）
        payload[7] = (byte) 0xAA;
        payload[8] = (byte) 0xBB;
        Object[] vals = decoder.decodePayload(payload, 0, new String[]{"bytea"});
        assertArrayEquals(new byte[]{(byte) 0xAA, (byte) 0xBB}, (byte[]) vals[0]);
    }
}
