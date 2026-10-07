package org.vastdata.vbstream.walsource.receive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.util.function.Consumer;

/**
 * WAL 字节流走读状态机：把 pgjdbc 物理复制流送来的任意切分 chunk 重组为
 * {@link WalRecord} 序列——页/记录导航 + contrecord 缝合 + pageaddr 锚定协议。
 *
 * <p>移植自 spike {@code WalParseSpike.WalWalker}（spike/wal-parse），硬编码尺寸改经
 * {@link WalLayout} 注入、记录走读委派 {@link WalRecordParser} 静态入口（Task 3 的
 * 纯函数工具类，无实例可持）；spike 的 {@code getLastReceiveLSN - len} 裸算锚定废除，
 * 改为<strong>页头 pageaddr 是页内导航的唯一权威</strong>（spike 发现 23b 的正式解）：</p>
 * <ol>
 *   <li>chunk 拼接：{@link #feed(long, byte[])} 以调用方传的 chunkEndLsn 推算 chunk 起点
 *   并做 carry 衔接校验（失配且 carry 非空：WARN + 丢弃 carry + {@code carryDrops++}；
 *   carry 为空的锚点偏移静默改锚——页头锚定随后接管纠偏）；</li>
 *   <li>页头锚定：每个页边界（含跨页缝合遇到的续体页头）校验 {@code pageaddr ==
 *   expectedPageAddr}；首个页头初始化期望并要求页对齐（游标地址空间随即重锚到
 *   pageaddr）；失配 → 在合并缓冲内扫描下一个合法页头（magic 对、pageaddr 页对齐且
 *   与游标推算地址差 ≤ 2 页），命中即丢弃错位字节、{@code resyncs++}、WARN、以该
 *   pageaddr 重锚继续；容差窗口扫完仍无 → ISE fail-fast（含双方 LSN 诊断）。</li>
 * </ol>
 *
 * <p>导航骨架（发现 11）：页边界 → 页头校验（长/短页头按 XLP_LONG_HEADER 分档）→
 * 孤立续体跳过（MAXALIGN(页头+rem_len) 补齐）→ 记录头（页尾余量 &lt;{@value #MIN_RECORD_START}
 * 或 totLen==0 = 零垫整页跳过；<b>余量 4..23B 且 totLen 非零 = 记录头自页尾跨页</b>
 * ——xlog.c 对记录起点只保证 xl_tot_len 4 字节在本页，头余部经续体页头缝合，Task 10
 * 修复：原"余量不足头长即跳页"形态会把此类记录整条静默丢弃）→ 记录缝合（跨页按
 * "当前页剩余"取数——累计消耗与页内偏移严禁混用同一坐标系）→ emit。页尾零垫不
 * 预消费（无法区分"已到页尾"与"字节未到"），待下页字节到达后跳页。</p>
 *
 * <p>线程约束：单写者——feed/consumedLsn 限定接收线程（Task 8 的 readPending 循环）
 * 调用；sink 回调在 feed 调用线程内同步执行。</p>
 *
 * <p>重入契约（与 WalStreamReceiver 双侧约定）：断流重连的续传起点 consumedLsn 是
 * <strong>记录对齐</strong>（上条记录 MAXALIGN 后）而非页对齐——首 chunk 不落在页界时
 * 页内直通路（直接按记录头解析）恰好接受这种重入点，其正确性依赖 consumedLsn 恰在
 * MAXALIGN 记录边界这一语义保证；若断流时 carry 挂着半条记录，重连后服务端自
 * consumedLsn（= carry 首字节）重发，衔接校验走一次 carry 丢弃 + 重锚，数据不丢不重。</p>
 */
public final class WalStreamWalker {

    private static final Logger LOG = LoggerFactory.getLogger(WalStreamWalker.class);

    /** 页头 info 位 XLP_FIRST_IS_CONTRECORD：本页首条记录是前页记录的续体（xlog_internal.h）。 */
    private static final int XLP_FIRST_IS_CONTRECORD = 0x0001;

    /** 页头 info 位 XLP_LONG_HEADER：段首长页头（xlog_internal.h）。 */
    private static final int XLP_LONG_HEADER = 0x0002;

    /** XLogPageHeaderData 定偏移：xlp_info u16@2。 */
    private static final int INFO_OFFSET = 2;

    /** XLogPageHeaderData 定偏移：xlp_pageaddr u64@8。 */
    private static final int PAGEADDR_OFFSET = 8;

    /** XLogPageHeaderData 定偏移：xlp_rem_len u32@16。 */
    private static final int REM_LEN_OFFSET = 16;

    /** 再同步扫描的 pageaddr 容差（页为单位）：±2 页内视为可重锚的受控漂移。 */
    private static final int RESYNC_TOLERANCE_PAGES = 2;

    /**
     * 记录起点的最小页内余量（xlog.c CopyXLogRecordToWAL 的断言面：记录起点所在页
     * 至少容纳 xl_tot_len 的 4 字节——头本体可跨页，Task 10 修复依据）。
     */
    private static final int MIN_RECORD_START = 4;

    /** expectedPageAddr 未初始化标记（0 是合法页地址，不能用零作哨兵）。 */
    private static final long PAGEADDR_UNSET = Long.MIN_VALUE;

    private static final byte[] EMPTY = {};

    private final WalLayout layout;
    private final WalStreamMetrics metrics;
    private final Consumer<WalRecord> sink;

    /** 未消费字节（锚定于 carryStartLsn）——chunk 与 chunk 间的缝合缓冲。 */
    private byte[] carry = EMPTY;

    /** carry[0] 的绝对 LSN；亦即"已消费到"的游标前沿（consumedLsn 的真身）。 */
    private long carryStartLsn;

    /** 已喂字节的绝对末尾（自维护数据锚，0 = 未初始化——首块走调用方游标锚）。 */
    private long dataEndLsn;

    /** 是否已 feed 过（首个 chunk 免做衔接校验）。 */
    private boolean sawAny;

    /** 下一个期望页头的 pageaddr（页连续性锚）；PAGEADDR_UNSET 表示尚未初始化。 */
    private long expectedPageAddr = PAGEADDR_UNSET;

    /**
     * 装配一个走读器。
     *
     * @param layout  版本布局描述符（页/页头/记录头尺寸与页魔数的唯一来源）
     * @param metrics 计数器（records/resyncs/contrecords/orphanSkips + census，调用方持有观测）
     * @param sink    已解析记录的交付回调（feed 调用线程内同步执行）
     */
    public WalStreamWalker(WalLayout layout, WalStreamMetrics metrics, Consumer<WalRecord> sink) {
        this.layout = layout;
        this.metrics = metrics;
        this.sink = sink;
    }

    /**
     * 已消费到的绝对 LSN 游标前沿（= carry[0] 的 LSN；从未 feed 时为 0）。
     *
     * <p>Task 8 以本值作周期 flush 确认与断流重连的续传起点——emit 过的记录前沿，
     * 不含 carry 中等待拼装的半条记录/页。</p>
     *
     * @return 下一个未消费字节的 LSN
     */
    public long consumedLsn() {
        return carryStartLsn;
    }

    /**
     * 锚点重置（连接（重）建立时由接收器调用）：清零自维护数据锚——本连接的首个
     * feed 回落调用方游标锚（{@code chunkEndLsn - len}），其后各 chunk 恢复自锚。
     *
     * <p><strong>为什么首块不能预锚请求位</strong>：物理流服务端自请求位读到首个
     * <strong>记录边界</strong> 才起发——请求位落在记录中段时首块真实起点晚于请求位
     * （预锚会错位，实测以挂起/假再同步暴露）。首块以调用方游标锚（pgjdbc
     * {@code getLastReceiveLSN()} 对 data 消息为 dataStart+len，准确）为准；
     * <strong>为什么后续块要自锚（Task 13 对拍 IT 实证）</strong>：keepalive 的
     * walEnd 会使该游标跳变——keepalive/data 交错时以其推算 chunk 起点把字节锚到
     * 错误地址，页界处触发假再同步（重者静默跳段丢记录）。连接内字节流严格连续，
     * 自维护末位锚即免疫；重连清零后由首块游标锚 + 既有 carry 丢弃语义接管纠偏。</p>
     */
    public void resetAnchor() {
        dataEndLsn = 0;
    }

    /**
     * 喂入一个 readPending chunk（任意切分，不必页对齐）。
     *
     * <p>关键步骤：① chunk 起点取<strong>自维护数据锚</strong>（已喂末位；未初始化时
     * 回落调用方游标 {@code chunkEndLsn - len}——两者差值即 keepalive 交错漂移，
     * DEBUG 记录）并做 carry 衔接校验——失配（重连续传重发覆盖 carry）WARN 后丢弃
     * carry 从本 chunk 锚点重启；② carry 拼接为合并缓冲；③ 解析循环消费到"字节不足
     * 需等待"为止，余量回填 carry，carryStartLsn 重置为停点 LSN（再同步重锚后该值
     * 已切换到 pageaddr 权威空间），数据锚推进到已喂末位。</p>
     *
     * <p>边界与异常语义：data 空数组仅推进数据锚；页头/记录头/记录体跨 chunk 分裂时
     * 留待后续 feed；pageaddr 失配超容差抛 ISE（feed 半途抛出时，此前已交付 sink 的
     * 记录不受影响——at-least-once 语义由上游续传兜底）。线程约束：仅接收线程调用。</p>
     *
     * @param chunkEndLsn 本 chunk 最后一个字节之后的 LSN（调用方游标，如
     *                    pgjdbc {@code getLastReceiveLSN()}；仅未初始化时的首块锚
     *                    与漂移 DEBUG 观测用——权威锚为自维护数据末位）
     * @param data        chunk 字节
     */
    public void feed(long chunkEndLsn, byte[] data) {
        long chunkStart;
        if (dataEndLsn != 0) {
            chunkStart = dataEndLsn;
            if (chunkEndLsn - data.length != chunkStart) {
                LOG.debug("chunk anchor drift {} bytes (caller cursor vs self anchor) — self anchor trusted",
                        chunkEndLsn - data.length - chunkStart);
            }
        } else {
            chunkStart = chunkEndLsn - data.length;
        }
        if (sawAny && chunkStart != carryStartLsn + carry.length) {
            if (carry.length > 0) {
                metrics.carryDrops.increment();
                LOG.warn("chunk anchor drift: carry ends at {} but chunk starts at {} — dropping {} carry bytes",
                        Lsn.format(carryStartLsn + carry.length), Lsn.format(chunkStart), carry.length);
                carry = EMPTY;
            }
            // carry 为空时的锚点偏移无数据损失，不打 WARN——游标直接改锚，
            // 页头锚定在下一页界接管纠偏
        }
        if (carry.length == 0) {
            carryStartLsn = chunkStart;
        }
        byte[] merged = new byte[carry.length + data.length];
        System.arraycopy(carry, 0, merged, 0, carry.length);
        System.arraycopy(data, 0, merged, carry.length, data.length);
        sawAny = true;
        ParseResult result = parse(merged, carryStartLsn);
        // carry 切分以解析后的有效长度为准——非零丢弃再同步左移字节后，
        // merged 尾部 (dropped) 字节是陈旧副本，不得回填 carry
        carry = new byte[result.len() - result.consumed()];
        System.arraycopy(merged, result.consumed(), carry, 0, carry.length);
        carryStartLsn = result.endLsn();
        dataEndLsn = carryStartLsn + carry.length;
    }

    /**
     * 解析循环：消费 buf（锚定于 lsn0）尽可能多的字节，返回消费量与停点 LSN。
     *
     * <p>骨架（发现 11）：页边界分支做页头校验/锚定/孤立续体跳过；页内分支做记录头
     * 读取（零垫判别）与跨页缝合（按"当前页剩余"取数）；每条完整记录经
     * {@link WalRecordParser} 走读后交付 sink 并计数。所有"字节不足"路径以 break
     * 收敛——停点 LSN（cur 始终等于 buf[pos] 的绝对地址，再同步重锚只改其值不改其义）
     * 与消费量一并返回。</p>
     *
     * <p>边界与异常语义：totLen 小于记录头长（损坏）抛 ISE；跨页处无
     * XLP_FIRST_IS_CONTRECORD 标记、pageaddr 锚定丢失均按失配路径处理（再同步或
     * ISE）。buf 为调用方（feed）私有的合并缓冲，再同步的原地丢弃可安全破坏其前缀。</p>
     *
     * @param buf  合并缓冲（从 lsn0 起的字节流）
     * @param lsn0 buf[0] 的绝对 LSN
     * @return 消费字节数、停点 LSN（= 未消费首字节的绝对地址）与有效长度（非零丢弃
     *         再同步收缩后的缓冲界，feed 据此切 carry）
     */
    private ParseResult parse(byte[] buf, long lsn0) {
        int bs = layout.walBlockSize();
        int shortHdr = layout.shortPageHeaderSize();
        int recHdr = layout.recordHeaderSize();
        int pos = 0;
        long cur = lsn0;
        int len = buf.length;
        while (true) {
            int pageOff = (int) (cur & (bs - 1));
            if (pageOff == 0) {
                // 短页头整体到达前无法判分档/读 pageaddr（magic 不错检——见下方 anchored 判据）
                if (len - pos < shortHdr) {
                    break;
                }
                int info = u16(buf, pos + INFO_OFFSET);
                int hdrSize = (info & XLP_LONG_HEADER) != 0 ? layout.longPageHeaderSize() : shortHdr;
                if (len - pos < hdrSize) {
                    break;
                }
                long pageaddr = u64(buf, pos + PAGEADDR_OFFSET);
                boolean firstHeader = expectedPageAddr == PAGEADDR_UNSET;
                boolean anchored = u16(buf, pos) == layout.pageMagic()
                        && (firstHeader
                                ? (pageaddr & (bs - 1)) == 0
                                : pageaddr == expectedPageAddr && pageaddr == cur);
                if (!anchored) {
                    // 页头校验失败（坏 magic / 首头未页对齐 / pageaddr 锚定失配）→ 受控再同步
                    Adoption adopted = resyncAt(buf, pos, len, cur,
                            firstHeader ? cur : expectedPageAddr, pageaddr);
                    if (adopted == null) {
                        break;   // 容差窗口未覆盖，等更多字节后重扫
                    }
                    pos = adopted.pos();
                    len = adopted.len();
                    cur = expectedPageAddr;
                    continue;
                }
                if (firstHeader && pageaddr != cur) {
                    // 首页头即与调用方游标漂移：pageaddr 为唯一权威，重锚地址空间
                    metrics.resyncs.increment();
                    LOG.warn("initial pageaddr re-anchor: cursor {} -> page header {}",
                            Lsn.format(cur), Lsn.format(pageaddr));
                    cur = pageaddr;
                }
                expectedPageAddr = pageaddr + bs;
                if ((info & XLP_FIRST_IS_CONTRECORD) != 0) {
                    // 孤立续体（发现 11）：记录头在窗口/再同步点之前，跳余部后继续
                    metrics.orphanSkips.increment();
                    int remLen = u32(buf, pos + REM_LEN_OFFSET);
                    long skipEnd = (cur + hdrSize + remLen + 7) & ~7L;
                    if (skipEnd - cur > len - pos) {
                        break;   // 余部未到齐
                    }
                    pos += (int) (skipEnd - cur);
                    cur = skipEnd;
                    continue;
                }
                pos += hdrSize;
                cur += hdrSize;
            }
            // 页内：记录头位置——<b>头可跨页缝合</b>（Task 10 修复）：xlog.c
            // CopyXLogRecordToWAL 对记录起点的唯一保证是"本页剩余至少容纳 xl_tot_len
            // 的 4 字节"（Assert(freespace >= sizeof(uint32))）——头本体（24B）可自
            // 页尾最后 4..23 字节起跨页，余部经续体页头后继续（实测形态：页尾恰 16B
            // 起 1127B 记录，续体页 rem_len=1111）。原实现把"页尾不足 24B"一律当
            // 整页零垫跳页，会把这类记录<b>整条静默丢弃</b>（对拍 IT 场景 3 实测即红
            // ——TOAST chunk 记录丢失→值降级）。正确判别：不足 4B 必为页尾（XLogSwitch
            // /段尾垫）；≥4B 读 totLen，零 = 垫、非零 = 记录头（余下字节由记录缝合
            // 循环按页缝合——头跨页与体跨页同构）。
            int pageRemain = bs - (int) (cur & (bs - 1));
            if (pageRemain < MIN_RECORD_START) {
                // 不足 4B：不可能是记录起点（PG 断言），必为页尾零垫
                if (len - pos < pageRemain) {
                    break;
                }
                pos += pageRemain;
                cur += pageRemain;
                continue;
            }
            if (len - pos < MIN_RECORD_START) {
                break;   // totLen 4 字节未到齐（记录头跨 chunk 分裂）
            }
            int totLen = u32(buf, pos);
            if (totLen == 0) {
                // 页内零垫：与"字节未到"不可区分，垫满整页才跳
                if (len - pos < pageRemain) {
                    break;
                }
                pos += pageRemain;
                cur += pageRemain;
                continue;
            }
            if (totLen < recHdr) {
                throw new IllegalStateException("corrupt record header: totLen=" + totLen
                        + " < header size " + recHdr + " at " + Lsn.format(cur));
            }
            // 记录缝合：跨页按"当前页剩余"取数（发现 11 坐标系教训——pageEnd 相对 curL 自身）
            long recLsn = cur;
            byte[] rec = new byte[totLen];
            int recPos = 0;
            int src = pos;
            long curL = cur;
            boolean awaitMore = false;
            boolean resynced = false;
            while (recPos < totLen) {
                int pageEnd = bs - (int) (curL & (bs - 1));
                int take = Math.min(totLen - recPos, pageEnd);
                if (len - src < take) {
                    awaitMore = true;   // 记录体未到齐
                    break;
                }
                System.arraycopy(buf, src, rec, recPos, take);
                recPos += take;
                src += take;
                curL += take;
                if (recPos < totLen) {
                    // 越入续体页：整头到达前等待；页头三项全验（magic/标志/pageaddr 锚定）
                    if (len - src < shortHdr) {
                        awaitMore = true;
                        break;
                    }
                    int cInfo = u16(buf, src + INFO_OFFSET);
                    long cPageaddr = u64(buf, src + PAGEADDR_OFFSET);
                    boolean cLong = (cInfo & XLP_LONG_HEADER) != 0;
                    int cHdr = cLong ? layout.longPageHeaderSize() : shortHdr;
                    if (len - src < cHdr) {
                        awaitMore = true;
                        break;
                    }
                    if (u16(buf, src) != layout.pageMagic()
                            || (cInfo & XLP_FIRST_IS_CONTRECORD) == 0
                            || cPageaddr != curL) {
                        // 续体页头违约（缺 contrecord 标志或 pageaddr 锚定失配）：
                        // 丢弃半条记录（含记录头），从下一个合法页头重锚
                        Adoption adopted = resyncAt(buf, pos, len, recLsn, curL, cPageaddr);
                        if (adopted == null) {
                            awaitMore = true;
                            break;
                        }
                        resynced = true;
                        pos = adopted.pos();
                        len = adopted.len();
                        cur = expectedPageAddr;
                        break;
                    }
                    metrics.contrecords.increment();
                    expectedPageAddr = cPageaddr + bs;   // 缝合越页：期望随之推进（否则后续页界必伪再同步）
                    src += cHdr;
                    curL += cHdr;
                }
            }
            if (awaitMore) {
                break;   // 等更多字节（半条记录留在缓冲）
            }
            if (resynced) {
                continue;   // 已重锚到合法页头，回外层页边界分支
            }
            WalRecord parsed = WalRecordParser.parse(rec, recLsn, layout);
            metrics.countRecord(parsed);
            sink.accept(parsed);
            pos = src;
            cur = curL;
            // 记录起点 MAXALIGN 垫齐（下一条记录/页尾零垫的边界）；垫长至多 7
            int pad = (int) (((cur + 7) & ~7L) - cur);
            if (pad > 0) {
                if (len - pos < pad) {
                    break;
                }
                pos += pad;
                cur += pad;
            }
        }
        return new ParseResult(pos, cur, len);
    }

    /**
     * 失配点的受控再同步：在 buf 内自 dropFrom（含）起逐字节扫描下一个合法页头，
     * 命中即原地丢弃错位字节并重锚。
     *
     * <p>合法判据三项全过：magic 对（提示这是真页头而非记录内数据）、pageaddr 页对齐、
     * pageaddr 与游标推算地址（{@code curAtDropFrom + (p - dropFrom)}，缓冲内漂移的
     * 唯一坐标基准）差 ≤ {@value #RESYNC_TOLERANCE_PAGES} 页。命中：dropFrom..p 原地
     * 抹除（数组左移，可能为 0 字节——失配页头自身即合法锚的情形）、{@code resyncs++}、
     * WARN（含期望/实得/重锚三方 LSN）、expectedPageAddr 置为采纳值，返回重锚位置与
     * <strong>收缩后的有效长度</strong>（左移丢弃后尾部 dropped 字节为陈旧副本，调用
     * 方的 len/carry 切分必须随之收缩，否则陈旧区会被二次解析成幻影记录、游标虚进）。
     * dropFrom 起扫描含自身：±32B 漂移形态下失配页头往往就是正确的下一页（pageaddr
     * 可证），此时零丢弃直接重锚，不损失页内记录。</p>
     *
     * <p>边界与异常语义：未命中且自 dropFrom 起缓冲已覆盖 ≥ 2 页（容差窗口必然扫尽）
     * → ISE fail-fast（消息含期望 pageaddr/失配点游标/实得 pageaddr）；未命中且窗口
     * 未覆盖 → 返回 null（调用方等待更多字节后重扫）。</p>
     *
     * @param buf         合并缓冲（再同步可安全破坏 dropFrom 之前与 dropFrom..p 区间）
     * @param dropFrom    错位字节起点（扫描含该位置；缝合路径为整条记录的记录头位）
     * @param len         缓冲当前有效长度
     * @param curAtDropFrom dropFrom 位置的游标 LSN（扫描推算的坐标基准）
     * @param expectedAddr 失配点的期望页址（诊断用）
     * @param foundAddr   失配页头实得 pageaddr（诊断用）
     * @return 重锚位置与收缩后的有效长度；null 表示需等待更多字节
     * @throws IllegalStateException 容差窗口扫尽仍无合法页头
     */
    private Adoption resyncAt(byte[] buf, int dropFrom, int len, long curAtDropFrom,
                              long expectedAddr, long foundAddr) {
        int bs = layout.walBlockSize();
        int hdr = layout.shortPageHeaderSize();
        long tolerance = (long) RESYNC_TOLERANCE_PAGES * bs;
        for (int p = dropFrom; p <= len - hdr; p++) {
            if (u16(buf, p) != layout.pageMagic()) {
                continue;
            }
            long pageaddr = u64(buf, p + PAGEADDR_OFFSET);
            if ((pageaddr & (bs - 1)) != 0) {
                continue;
            }
            long cursorDerived = curAtDropFrom + (p - dropFrom);
            long diff = pageaddr - cursorDerived;
            if (diff > tolerance || diff < -tolerance) {
                continue;
            }
            int dropped = p - dropFrom;
            if (dropped > 0) {
                System.arraycopy(buf, p, buf, dropFrom, len - p);
                metrics.lossyResyncs.increment();   // 越过错位字节 = 数据丢失面（dropped==0 为服务端空页跳过的良性重锚）
            }
            metrics.resyncs.increment();
            LOG.warn("WAL stream resync at {}: expected page header {}, found {} — dropped {} bytes, re-anchored to {}",
                    Lsn.format(cursorDerived), Lsn.format(expectedAddr), Lsn.format(foundAddr),
                    dropped, Lsn.format(pageaddr));
            expectedPageAddr = pageaddr;
            return new Adoption(dropFrom, len - dropped);
        }
        if (len - dropFrom >= tolerance) {
            throw new IllegalStateException("WAL pageaddr anchoring lost: expected "
                    + Lsn.format(expectedAddr) + " but found " + Lsn.format(foundAddr)
                    + " at cursor " + Lsn.format(curAtDropFrom) + " — no legal page header within "
                    + tolerance + " bytes (tolerance " + RESYNC_TOLERANCE_PAGES + " pages)");
        }
        return null;
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b      源数组（长度须覆盖 offset+2）
     * @param offset 起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b      源数组（长度须覆盖 offset+4）
     * @param offset 起始偏移
     * @return 32 位值
     */
    private static int u32(byte[] b, int offset) {
        return (b[offset] & 0xFF)
                | ((b[offset + 1] & 0xFF) << 8)
                | ((b[offset + 2] & 0xFF) << 16)
                | ((b[offset + 3] & 0xFF) << 24);
    }

    /**
     * 就地读 little-endian u64。
     *
     * @param b      源数组（长度须覆盖 offset+8）
     * @param offset 起始偏移
     * @return 64 位值
     */
    private static long u64(byte[] b, int offset) {
        return (u32(b, offset) & 0xFFFFFFFFL) | ((long) u32(b, offset + 4) << 32);
    }

    /**
     * 解析循环的单次返回：消费字节数 + 停点 LSN + 有效长度。
     *
     * <p>endLsn 与 consumed 分离是再同步的必然——重锚后停点 LSN 切换到 pageaddr 权威
     * 空间，不再等于 {@code lsn0 + consumed}；carry 起点必须取 endLsn 而非裸算。
     * len 是非零丢弃再同步收缩后的缓冲有效长度——carry 回填以它为界（数组物理长度
     * 减去被丢弃的陈旧尾），防止陈旧副本进 carry。</p>
     *
     * @param consumed buf 中已消费的字节数（carry 回填的切分点）
     * @param endLsn  未消费首字节的绝对 LSN（carry 的新锚点）
     * @param len     缓冲有效长度（≥ consumed；feed 据此切 carry）
     */
    private record ParseResult(int consumed, long endLsn, int len) {
    }

    /**
     * 一次成功再同步的重锚结果：合法页头所在位置 + 左移丢弃后收缩的缓冲有效长度。
     *
     * @param pos 重锚后合法页头所在的缓冲位置（= 调用方传入的 dropFrom）
     * @param len 收缩后的有效长度（原长减去丢弃字节数；零丢弃时不变）
     */
    private record Adoption(int pos, int len) {
    }
}
