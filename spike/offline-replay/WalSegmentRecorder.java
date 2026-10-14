import org.vastdata.vbstream.walsource.receive.DirectWalSource;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * WAL 段记录器（spike 专用）：DirectWalSource 收流 → 按 LSN 把字节写进 16MB 段文件
 * （24-hex 段名，与 pg_xlog 段文件同形态）——VastBase 真库的 pg_receivexlog 替代
 * （OG 5.0.0 容器 libpq 对 VastBase 6.0 内核认证握手不兼容，而模块自研 v3 客户端
 * 在真库已验证）。startLsn=0 时 DirectWalSource 首连取 IDENTIFY xlogpos 段对齐——
 * 覆盖当前段头。跨段 chunk 分段写；seek 空洞补零（walker 零区容忍面消化）。
 *
 * args: <walDir> <host> <port> <user> <pass>；脚本控制生命周期（kill 即停）。
 */
public class WalSegmentRecorder {
    private static final long SEG = 16L * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        Files.createDirectories(dir);
        DirectWalSource src = new DirectWalSource(args[1], Integer.parseInt(args[2]),
                args[3], args[4], null, 10_000L, 0L);
        src.start(lsn -> { }, (endLsn, bytes) -> {
            try {
                write(dir, endLsn - bytes.length, bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        System.out.println("[recorder] running, dir=" + dir);
        Thread.currentThread().join();
    }

    /** 把 [start, start+len) 字节按 16MB 段切分写入对应段文件对应偏移。段名 = tli + log + seg（标准 PG 编码，除数 = 0x100000000/段尺寸）。 */
    private static synchronized void write(Path dir, long start, byte[] data) throws Exception {
        long segsPerXLogId = 0x100000000L / SEG;
        long pos = 0;
        while (pos < data.length) {
            long abs = start + pos;
            long segNo = Math.floorDiv(abs, SEG);
            long inSeg = abs - segNo * SEG;
            int len = (int) Math.min(data.length - pos, SEG - inSeg);
            String name = String.format("%08X%08X%08X", 1L, segNo / segsPerXLogId, segNo % segsPerXLogId);
            try (RandomAccessFile f = new RandomAccessFile(dir.resolve(name).toFile(), "rw")) {
                f.seek(inSeg);
                f.write(data, (int) pos, len);
            }
            pos += len;
        }
    }
}
