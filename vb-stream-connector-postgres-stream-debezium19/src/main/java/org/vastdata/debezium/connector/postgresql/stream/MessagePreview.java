package org.vastdata.debezium.connector.postgresql.stream;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * 逻辑消息('M')内容的日志预览件(MS3.5 spec §3.2):把任意字节载荷渲染成单行可读的
 * 预览串,供两处日志时点共用——非事务消息的 reader 即时 INFO 与事务性消息的回放期
 * INFO(两处均不发射下游,预览只服务日志留痕)。
 *
 * <p>判定与截断规则(参照引擎 ConsoleRenderer 的值渲染惯例:纯 ASCII 可打印走 text,
 * 否则 hex):
 * <ul>
 *   <li><b>text 形态</b>:载荷全部字节落在可打印 ASCII 闭区间 0x20(空格)..0x7E('~')
 *       时按 ASCII 字符输出;长度 ≤64 完整输出,超过则截前 64 字符并附
 *       {@code ...(<原字节数>B)} 原长标记(与引擎文本值渲染同款形态)</li>
 *   <li><b>bytea 十六进制形态</b>:载荷含任一区间外字节(控制字符/高位字节)即整条切到
 *       十六进制(判定按整条,混合内容不逐段切换——日志行保持单形态可读),输出前 32
 *       字节的连续小写 hex(无 {@code 0x} 前缀,与 pgjdbc bytea 字面形态对齐);超过 32
 *       字节截断并附同款原长标记</li>
 * </ul>
 *
 * <p>边界:空载荷返回空串(零字节 vacuously 可打印,走 text 路径);null 载荷抛 NPE
 * (调用方违约——协议 content 字段可为空但消息本身已解析成功)。纯函数无副作用、
 * 不抛业务异常,任意线程可调用(reader 与 consumer 两个日志时点)。
 */
final class MessagePreview {

    /** text 形态的截断阈值(字符):超过截前 64 并附原长标记。 */
    private static final int TEXT_LIMIT = 64;

    /** bytea 形态的截断阈值(字节):超过截前 32 字节的 hex 并附原长标记。 */
    private static final int HEX_LIMIT = 32;

    private MessagePreview() {
        // 静态纯函数集,不可实例化
    }

    /**
     * 责任:渲染逻辑消息 content 的日志预览串。关键步骤:先整条扫描可打印性(全部字节
     * 在 0x20..0x7E 闭区间)——可打印走 text 形态(ASCII 解码,>64 截断附
     * {@code ...(<len>B)}),否则走 bytea 十六进制形态(前 32 字节小写 hex,>32 截断附
     * 同款标记)。边界:空载荷返回 "";null 抛 NPE。
     *
     * @param content 消息载荷字节(pgoutput LogicalMsg 的 content 字段,可为空数组)
     * @return 单行预览串(text ≤64 字符或 hex ≤32 字节,截断时附原长标记)
     */
    static String preview(byte[] content) {
        if (content.length == 0) {
            return "";
        }
        boolean printable = true;
        for (byte b : content) {
            if (b < 0x20 || b > 0x7E) {
                printable = false;
                break;
            }
        }
        if (printable) {
            String text = new String(content, StandardCharsets.US_ASCII);
            return text.length() > TEXT_LIMIT
                    ? text.substring(0, TEXT_LIMIT) + "...(" + content.length + "B)"
                    : text;
        }
        int take = Math.min(content.length, HEX_LIMIT);
        String hex = HexFormat.of().formatHex(content, 0, take);
        return content.length > HEX_LIMIT ? hex + "...(" + content.length + "B)" : hex;
    }
}
