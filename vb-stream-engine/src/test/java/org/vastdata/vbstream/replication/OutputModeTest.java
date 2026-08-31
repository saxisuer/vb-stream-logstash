package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** OutputMode 解析单测：默认 STREAMING、合法值大小写宽容、未知值 fail-fast 附可用值（风格同 rollCycle 解析）。属性用例 finally 恢复原值。 */
class OutputModeTest {

    /** 属性未设置时默认 STREAMING（2.0 流式为默认形态）。 */
    @Test
    void defaultsToStreaming() {
        System.clearProperty("vb.output.mode");
        assertEquals(OutputMode.STREAMING, OutputMode.fromSystemProperties());
    }

    /** 合法值大小写宽容（"block" 命中 BLOCK），finally 恢复属性防跨用例污染。 */
    @Test
    void parsesBlockCaseInsensitively() {
        System.setProperty("vb.output.mode", "block");
        try {
            assertEquals(OutputMode.BLOCK, OutputMode.fromSystemProperties());
        } finally {
            System.clearProperty("vb.output.mode");
        }
    }

    /** 未知值启动期 fail-fast（IAE），异常消息附可用值清单；finally 恢复属性。 */
    @Test
    void unknownValueFailsFast() {
        System.setProperty("vb.output.mode", "NOPE");
        try {
            assertThrows(IllegalArgumentException.class, OutputMode::fromSystemProperties);
        } finally {
            System.clearProperty("vb.output.mode");
        }
    }
}
