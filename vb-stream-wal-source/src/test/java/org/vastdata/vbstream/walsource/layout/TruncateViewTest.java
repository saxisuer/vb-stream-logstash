package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import static org.junit.jupiter.api.Assertions.*;

/** {@link HeapViews.TruncateView} 的失败先行测试——vanilla 几何（REL_17/18_STABLE
 * heapam_xlog.h + spike 黄金字节，@12 裁定）：[dbId u32@0][nrelids u32@4][flags u8@8]
 * [3B 未初始化栈垫@9-11][relid u32×n @12 起]；错配 rmid/opcode 抛 ISE；
 * mainLen ≠ 12+4nrelids 抛 ISE（错位自检）。 */
class TruncateViewTest {

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 手造 main：12B 头（含 @9-11 未初始化栈垫——spike 实测非零垃圾，视图不读）+ relids 数组。 */
    private byte[] buildMain(int flags, long... relids) {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 16385L);                        // dbId（视图不消费，占位真值）
        put32(m, relids.length);                 // nrelids
        m.write(flags);                          // flags u8@8
        m.write(0xe4); m.write(0xdb); m.write(0x61);   // @9-11 栈垫（spike 黄金样本同形——非零）
        for (long rel : relids) put32(m, rel);   // relids @12 起（spike 裁定）
        return m.toByteArray();
    }

    private static void put32(ByteArrayOutputStream m, long v) {
        for (int i = 0; i < 4; i++) m.write((int) (v >>> (8 * i)) & 0xFF);
    }

    @Test
    void parsesSingleTableNoOptions() {
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(buildMain(0, 24600L)).build(), 0x100000L, layout);
        HeapViews.TruncateView v = HeapViews.TruncateView.parse(r, layout);
        assertEquals(1, v.nrelids());
        assertArrayEquals(new long[]{24600L}, v.relids());
        assertEquals(0, v.flags());
    }

    @Test
    void parsesMultiTableWithBothOptionBits() {
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100)
                .main(buildMain(HeapOps.XLH_TRUNCATE_CASCADE | HeapOps.XLH_TRUNCATE_RESTART_SEQS,
                        24600L, 24601L, 24602L)).build(), 0x100000L, layout);
        HeapViews.TruncateView v = HeapViews.TruncateView.parse(r, layout);
        assertEquals(3, v.nrelids());
        assertArrayEquals(new long[]{24600L, 24601L, 24602L}, v.relids());
        assertEquals(3, v.flags());
    }

    @Test
    void rejectsMismatchedRmidOrOpcode() {
        WalRecord notHeap = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP2_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(buildMain(0, 1L)).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(notHeap, layout));
    }

    @Test
    void rejectsRelidsBeyondMainLen() {
        // nrelids=2 但 main 只有 1 个 relid 的空间
        byte[] bad = buildMain(0, 24600L);
        bad[4] = 2;
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(bad).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(r, layout));
    }

    @Test
    void rejectsMainTooShort() {
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(new byte[5]).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(r, layout));
    }

    @Test
    void rejectsTrailingBytesBeyondDeclaredNrelids() {
        // nrelids=1 但 main 携带 2 个 relid 的空间——精确自检（mainLen == 12+4n）拒绝
        byte[] bad = buildMain(0, 24600L, 24601L);
        bad[4] = 1;
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(bad).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(r, layout));
    }
}
