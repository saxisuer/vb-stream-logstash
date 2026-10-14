import java.nio.charset.StandardCharsets;
// 注：Windows 控制台重定向产物是 GBK 字节——ISO-8859-1 单字节透传读入（TXN 行 ASCII 不受影响）
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 对拍器（DualPathParityIT 归一化思路的文件面简化版）：解析两份 CDC log，按 xid 收集
 * TXN 块（TXN-BEGIN..TXN-END），取 xid 交集逐块 diff——A（在线基准）与 B（离线重放）
 * 的起停窗口差由交集归一化吸收。exit 0 = 交集内逐块全等。
 *
 * 解析规则：logback 行 ".... INFO org.vastdata.vbstream.walsource.cdc - <msg>"，msg 为
 * TXN-BEGIN xid=N ... / "  [n] ..." / TXN-END ... 三形态；非 CDC logger 行与非事务行
 * （生命周期控制等）忽略。
 */
public class ParityDiff {
    // logback %logger{24} 缩写形态（o.v.v.walsource.cdc）——按尾段 cdc 匹配
    private static final Pattern CDC_LINE = Pattern.compile(
            "INFO\\s+[\\w.]*\\bcdc\\s+-\\s+(.*)$");
    private static final Pattern BEGIN = Pattern.compile("TXN-BEGIN xid=(\\d+)");

    public static void main(String[] args) throws Exception {
        Map<Long, List<String>> a = parseBlocks(Path.of(args[0]));
        Map<Long, List<String>> b = parseBlocks(Path.of(args[1]));
        Set<Long> inter = new TreeSet<>(a.keySet());
        inter.retainAll(b.keySet());

        System.out.println("A(基准)=" + a.size() + " 块, B(离线)=" + b.size() + " 块, 交集=" + inter.size());
        System.out.println("B 独有（离线重放多出的块——不应存在，at-least-once 重叠属 A 内已有）: "
                + diffSet(b.keySet(), a.keySet()));
        System.out.println("A 独有（在线收过但离线未及——检查点前旧事务，预期形态）: "
                + diffSet(a.keySet(), b.keySet()));

        int unequal = 0;
        for (long xid : inter) {
            List<String> la = a.get(xid);
            List<String> lb = b.get(xid);
            if (!la.equals(lb)) {
                unequal++;
                System.out.println("DIFF xid=" + xid);
                for (int i = 0; i < Math.max(la.size(), lb.size()); i++) {
                    String sa = i < la.size() ? la.get(i) : "<缺失>";
                    String sb = i < lb.size() ? lb.get(i) : "<缺失>";
                    if (!sa.equals(sb)) {
                        System.out.println("  A: " + sa);
                        System.out.println("  B: " + sb);
                    }
                }
            }
        }
        System.out.println(unequal == 0
                ? "PASS: 交集 " + inter.size() + " 块逐行全等"
                : "FAIL: " + unequal + " 块不等");
        System.exit(unequal == 0 ? 0 : 1);
    }

    private static Set<Long> diffSet(Set<Long> x, Set<Long> y) {
        Set<Long> out = new TreeSet<>(x);
        out.removeAll(y);
        return out;
    }

    private static Map<Long, List<String>> parseBlocks(Path file) throws Exception {
        Map<Long, List<String>> blocks = new LinkedHashMap<>();
        List<String> current = null;
        for (String line : Files.readAllLines(file, StandardCharsets.ISO_8859_1)) {
            Matcher m = CDC_LINE.matcher(line);
            if (!m.find()) {
                continue;
            }
            String msg = m.group(1).trim();
            Matcher bm = BEGIN.matcher(msg);
            if (bm.find()) {
                current = new ArrayList<>();
                blocks.put(Long.parseLong(bm.group(1)), current);
            } else if (msg.startsWith("TXN-END") || msg.startsWith("[")) {
                if (current != null && (msg.startsWith("[") || msg.startsWith("TXN-END"))) {
                    current.add(msg);
                }
                if (msg.startsWith("TXN-END")) {
                    current = null;
                }
            }
        }
        return blocks;
    }
}
