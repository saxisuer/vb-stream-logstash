package org.vastdata.vbstream.walsource.receive.direct;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 65B StandbyReplyMessage（'r' 反馈）逐字节黄金断言（Task 5 循环 ③）：字段次序、宽度、
 * 小端、peer_role/peer_state 枚举实值（3/7——replicainternal.h 枚举序，brief 所记 2/4
 * 与头文件不符、以头文件为准）、C 对齐垫位置，全部以硬编码黄金样本钉死。布局出处：
 * walprotocol.h:201 StandbyReplyMessage + receivelog.cpp:372 sendReplyToSender（C 参考
 * 客户端按 sizeof=64 memcpy，加首字节 'r' 总长 65）。
 */
class DirectWalSourceFeedbackTest {

    /** 黄金输入成分（任意但固定的 LSN 前沿与时刻）。 */
    private static final long RECEIVE = 0x0000000100000002L;
    private static final long WRITE = 3L;
    private static final long FLUSH = 4L;
    private static final long APPLY = 0L;
    private static final long APPLY_READ = 5L;
    private static final long SEND_TIME = 0x1122334455667788L;

    /**
     * 循环 ③：replyRequested=true 档的整帧黄金样本——65 字节逐段断言（按 walprotocol.h
     * 字段偏移：receive@1 write@9 flush@17 apply@25 applyRead@33 peer_role@41
     * peer_state@45 sendTime@49 replyRequested@57 垫@58 replyFlags@61）。
     */
    @Test
    void standbyReplyGoldenBytesRequested() {
        byte[] frame = V3ProtocolChannel.encodeStandbyReply(RECEIVE, WRITE, FLUSH, APPLY,
                APPLY_READ, SEND_TIME, true);
        assertEquals(65, frame.length, "总长恰 65（'r' + sizeof(StandbyReplyMessage)=64）");
        assertArrayEquals(new byte[]{'r'}, slice(frame, 0, 1), "首字节 'r'");
        assertArrayEquals(new byte[]{0x02, 0, 0, 0, 1, 0, 0, 0}, slice(frame, 1, 9),
                "receive u64 小端（0x1_00000002）");
        assertArrayEquals(new byte[]{3, 0, 0, 0, 0, 0, 0, 0}, slice(frame, 9, 17), "write u64 小端");
        assertArrayEquals(new byte[]{4, 0, 0, 0, 0, 0, 0, 0}, slice(frame, 17, 25), "flush u64 小端");
        assertArrayEquals(new byte[8], slice(frame, 25, 33), "apply u64 小端（0）");
        assertArrayEquals(new byte[]{5, 0, 0, 0, 0, 0, 0, 0}, slice(frame, 33, 41), "applyRead u64 小端");
        assertArrayEquals(new byte[]{3, 0, 0, 0}, slice(frame, 41, 45),
                "peer_role i32 小端 = 3（STANDBY_MODE，replicainteral.h ServerMode 枚举序）");
        assertArrayEquals(new byte[]{7, 0, 0, 0}, slice(frame, 45, 49),
                "peer_state i32 小端 = 7（BUILDING_STATE，replicainternal.h DbState 枚举序）");
        assertArrayEquals(new byte[]{(byte) 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11},
                slice(frame, 49, 57), "sendTime i64 小端（µs 自 2000-01-01）");
        assertArrayEquals(new byte[]{1}, slice(frame, 57, 58), "replyRequested u8 = 1（心跳档）");
        assertArrayEquals(new byte[3], slice(frame, 58, 61),
                "replyRequested 与 replyFlags 之间的 C 对齐垫 3 字节零");
        assertArrayEquals(new byte[4], slice(frame, 61, 65), "replyFlags u32 小端 = 0（裸接收端）");
    }

    /**
     * 循环 ③ 补面：replyRequested=false 档（keepalive 即时回应形态，receivelog.cpp:789
     * 传 false）仅第 57 位翻 0，其余字节全同。
     */
    @Test
    void standbyReplyNotRequestedDiffersOnlyInRequestBit() {
        byte[] requested = V3ProtocolChannel.encodeStandbyReply(RECEIVE, WRITE, FLUSH, APPLY,
                APPLY_READ, SEND_TIME, true);
        byte[] plain = V3ProtocolChannel.encodeStandbyReply(RECEIVE, WRITE, FLUSH, APPLY,
                APPLY_READ, SEND_TIME, false);
        assertEquals(65, plain.length);
        assertEquals(0, plain[57], "replyRequested 位须为 0");
        for (int i = 0; i < 65; i++) {
            if (i != 57) {
                assertEquals(requested[i], plain[i], "偏移 " + i + " 须与心跳档一致");
            }
        }
    }

    /** 黄金断言辅助：帧切片。 */
    private static byte[] slice(byte[] frame, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(frame, from, out, 0, out.length);
        return out;
    }
}
