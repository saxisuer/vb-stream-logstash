package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link XactGrouper} 事务组装状态机的失败先行测试——WalBytes/TupleBytes 手造
 * heap 与 XACT 记录序列（stub catalog 快照），经 RecordingListener 断言事件面：
 * 提交时批量发射的事件序 / 交错两桶不串 / ABORT 零 Begin-End / 子事务回滚扣减
 * （expected=过滤前）/ 2PC 挂起-确认-弃三态 / TOPLEVEL_XID 与 ASSIGNMENT 双路归并 /
 * UPDATE-DELETE 前像 / FPW 镜像路径 / TOAST chunk 采集与终态淘汰 / TableMeta 缓存的
 * DDL 失效。
 *
 * <p>发射语义锚（控制器 2026-10 裁定修正）：桶内累积、<b>COMMIT/COMMIT_PREPARED 时
 * 按序批量发射</b>（Begin → 过滤后行 → End）——即时回调 onRow 与 aborted 过滤不可
 * 同时成立（已发射的行无法撤回）；任务书用例①的"事件序 Begin/3×Row/End"由批量
 * 回放产生同序。expectedChanges=桶记账（aborted 过滤前），emittedChanges=过滤后
 * 实付；BatchBegin 携带终值（发射点=提交时点，全量已知）。</p>
 *
 * <p>XACT 布局锚（REL_18_STABLE 实源钉）：commit/abort 最小 main = 裸 i64
 * xact_time；COMMIT/ABORT_PREPARED 的 info 置 HAS_INFO(0x80)，main 走 xinfo 块链，
 * <b>归属键 = twophase chunk xid</b>（记录头 xid 是执行命令的新事务）；PREPARE main =
 * 72B 两阶段文件头（gidlen u16@54、gid@72 按 gidlen 截取非 NUL 结尾）。</p>
 */
class XactGrouperTest {

    /** 测试基准定位器三件组（与 HeapViewsTest 同位形）。 */
    private static final long SPC = 1663L;
    private static final long DB = 16385L;

    /** 用户表：oid == relfilenode == 24600，public.t_stream（id int4 / name text）。 */
    private static final long USER_REL = 24600L;

    /** 用户表二：oid == relfilenode == 24601，public.t_wide（id int4 / flag bool / name text
     * ——三列形态使截断 UPDATE 的 prefix/suffix/mid 三段坐标皆非平凡，Task 8.5）。 */
    private static final long WIDE_REL = 24601L;

    /** 用户表三：oid == relfilenode == 24602，public.t_bitmap（十列 int4——natts=10 使
     * null 位图 2B、t_hoff=32 与无位图的 24 拉开位图宽差，Task 9 补例）。 */
    private static final long BITMAP_REL = 24602L;

    /** toast 关系：oid == relfilenode == 33657（relkind 't'）。 */
    private static final long TOAST_REL = 33657L;

    /** pg_attribute 的固定 oid（catalog 字典三表之一——缓存失效触发键）。 */
    private static final long PG_ATTRIBUTE_RELNODE = 1249L;

    /** 提交时间戳（自 2000-01-01 起微秒）——取整秒倍数使 Instant 断言免重复实现换算。 */
    private static final long COMMIT_MICROS = 2_000_000_000_000L;

    /** COMMIT_MICROS 对应的 Instant（946684800s = 2000-01-01T00:00:00Z + 2e6 s）。 */
    private static final Instant COMMIT_TS = Instant.ofEpochSecond(946_684_800L + 2_000_000L);

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 任务书 ①：单事务 INSERT×3 → COMMIT——事件序 Begin/3×Row/End、expected==emitted==3、commit 字段齐。 */
    @Test
    void singleTxnThreeInsertsEmitBeginRowsEndInOrder() {
        Fixture fx = fixture();
        fx.insert(100, 0, 1, "alice");
        fx.insert(100, 0, 2, "bob");
        fx.insert(100, 0, 3, "carol");
        WalRecord commit = fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=alice}",
                "ROW INSERT public.t_stream before=null after={id=2, name=bob}",
                "ROW INSERT public.t_stream before=null after={id=3, name=carol}",
                "END xid=100 emitted=3 exp=3"), fx.listener.events);

        ChangeOutputListener.BatchBegin begin = fx.listener.begins.get(0);
        assertEquals(commit.lsn(), begin.commitLsn(), "commitLsn = 提交记录 LSN");
        assertEquals(commit.lsn() + align8(commit.totLen()), begin.endLsn(), "endLsn = LSN+MAXALIGN(totLen)");
        assertEquals(COMMIT_TS, begin.commitTs());
    }

    /** 任务书 ②：交错两桶不串——按提交序各发各的批量，行归属正确。 */
    @Test
    void interleavedTransactionsEmitIndependentBatches() {
        Fixture fx = fixture();
        fx.insert(100, 0, 1, "a1");
        fx.insert(200, 0, 7, "b1");
        fx.insert(100, 0, 2, "a2");
        fx.insert(200, 0, 8, "b2");
        fx.commit(200);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=200 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=7, name=b1}",
                "ROW INSERT public.t_stream before=null after={id=8, name=b2}",
                "END xid=200 emitted=2 exp=2",
                "BEGIN xid=100 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=a1}",
                "ROW INSERT public.t_stream before=null after={id=2, name=a2}",
                "END xid=100 emitted=2 exp=2"), fx.listener.events);
    }

    /** 任务书 ③：ABORT → onAborted 单事件，零 Begin/Row/End。 */
    @Test
    void abortedTransactionEmitsAbortedWithoutBeginOrEnd() {
        Fixture fx = fixture();
        fx.insert(300, 0, 1, "doomed");
        fx.abort(300, 0);

        assertEquals(List.of("ABORTED xid=300"), fx.listener.events);
    }

    /** 任务书 ④：SAVEPOINT 子行扣减——expected=过滤前 3、emitted=2，子 xid 的行被剔除。 */
    @Test
    void savepointSubRowsDeductedExpectedStaysPreFilter() {
        Fixture fx = fixture();
        fx.insert(5, 0, 1, "top-a");
        fx.insert(7, 5, 2, "sub-b");       // 子事务行（TOPLEVEL_XID 标记归并到 5）
        fx.abort(7, 5);                    // ROLLBACK TO SAVEPOINT：子 xid ABORT + toplevel 标记
        fx.insert(5, 0, 3, "top-c");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=top-a}",
                "ROW INSERT public.t_stream before=null after={id=3, name=top-c}",
                "END xid=5 emitted=2 exp=3"), fx.listener.events);
    }

    /** 任务书 ⑤：PREPARE 挂起零发射 → COMMIT_PREPARED 发射（gid 解出、twoPhase、xid=被准备事务）。 */
    @Test
    void preparePendsThenCommitPreparedEmitsWithGid() {
        Fixture fx = fixture();
        fx.insert(700, 0, 1, "p1");
        fx.insert(700, 0, 2, "p2");
        fx.feed(WalBytes.prepareRecord(700, COMMIT_MICROS, "gt-42"));
        assertEquals(List.of(), fx.listener.events, "PREPARE 挂起期零发射");

        fx.feed(WalBytes.xactRecordWithGid(HeapOps.XLOG_XACT_COMMIT_PREPARED, 888, COMMIT_MICROS, 700, "gt-42"));

        assertEquals(List.of(
                "BEGIN xid=700 2p=true gid=gt-42 exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=p1}",
                "ROW INSERT public.t_stream before=null after={id=2, name=p2}",
                "END xid=700 emitted=2 exp=2"), fx.listener.events);
    }

    /** 任务书 ⑥：ABORT_PREPARED 弃挂起桶——onAborted 单事件，零 Begin/End。 */
    @Test
    void abortPreparedDiscardsPendingBucket() {
        Fixture fx = fixture();
        fx.insert(700, 0, 1, "p1");
        fx.feed(WalBytes.prepareRecord(700, COMMIT_MICROS, "gt-42"));
        fx.feed(WalBytes.xactRecordWithGid(HeapOps.XLOG_XACT_ABORT_PREPARED, 889, COMMIT_MICROS, 700, "gt-42"));

        assertEquals(List.of("ABORTED xid=700"), fx.listener.events);
    }

    /**
     * PREPARE 半边 gid 直测（审查修复面）：PREPARE 记录的 gid 经 72B 头 gidlen 解出并
     * <b>剥尾 NUL</b>（真实 gidlen=strlen+1，twophase.c 同面）挂到桶上；确认记录无
     * GID chunk（wal_level&lt;logical 真实形态）时回落桶值——gid 断言来源是 PREPARE 侧
     * 而非 COMMIT_PREPARED 侧（审查 High/Medium-1：此前测试⑤的 gid 断言全来自确认侧，
     * PREPARE 解析从未被验证）。
     */
    @Test
    void prepareRecordGidDirectlyPendsOnBucketWithNulStripped() {
        Fixture fx = fixture();
        fx.insert(700, 0, 1, "p1");
        fx.feed(WalBytes.prepareRecord(700, COMMIT_MICROS, "gt-direct"));
        fx.feed(WalBytes.xactRecordWithGid(HeapOps.XLOG_XACT_COMMIT_PREPARED, 890, COMMIT_MICROS, 700, null));

        assertEquals(List.of(
                "BEGIN xid=700 2p=true gid=gt-direct exp=1",
                "ROW INSERT public.t_stream before=null after={id=1, name=p1}",
                "END xid=700 emitted=1 exp=1"), fx.listener.events);
    }

    /** 任务书 ⑦：TOPLEVEL_XID 标记归并——多个子 xid 的行并进同一顶层桶，单批量发射。 */
    @Test
    void toplevelXidMarkerMergesSubxactRows() {
        Fixture fx = fixture();
        fx.insert(7, 5, 1, "s1");
        fx.insert(8, 5, 2, "s2");
        fx.insert(5, 0, 3, "top");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=s1}",
                "ROW INSERT public.t_stream before=null after={id=2, name=s2}",
                "ROW INSERT public.t_stream before=null after={id=3, name=top}",
                "END xid=5 emitted=3 exp=3"), fx.listener.events);
    }

    /** ASSIGNMENT 兜底归并：无 TOPLEVEL 标记的子 xid 行经 assignment 映射并进顶层桶。 */
    @Test
    void assignmentRecordMergesSubxactRowsFallback() {
        Fixture fx = fixture();
        fx.feed(WalBytes.assignmentRecord(5, 7));
        fx.insert(7, 0, 1, "via-assignment");
        fx.insert(5, 0, 2, "top");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=via-assignment}",
                "ROW INSERT public.t_stream before=null after={id=2, name=top}",
                "END xid=5 emitted=2 exp=2"), fx.listener.events);
    }

    /** UPDATE/DELETE 前像：CONTAINS_OLD 置位时 before 来自 main data 尾；无标志 before=null。 */
    @Test
    void updateAndDeleteCarryBeforeImageFromMainTail() {
        Fixture fx = fixture();
        fx.update(100, 0, 1, "alice", 1, "aliced");
        fx.delete(100, 0, 2, "bob");
        fx.deleteNoOld(100, 0, 3);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=3",
                "ROW UPDATE public.t_stream before={id=1, name=alice} after={id=1, name=aliced}",
                "ROW DELETE public.t_stream before={id=2, name=bob} after=null",
                "ROW DELETE public.t_stream before=null after=null",
                "END xid=100 emitted=3 exp=3"), fx.listener.events);
    }

    /** FPW 镜像路径：块只有 image 无 data 时，行经 PageImages 重建页 + 行指针解出。 */
    @Test
    void insertWithFullPageImageDecodesFromRebuiltPage() {
        Fixture fx = fixture();
        byte[] tuple = TupleBytes.of("int4", "text").i32(9).text("fpi").tuple();
        byte[] page = new byte[8192];
        System.arraycopy(tuple, 0, page, 64, tuple.length);
        int itemId = 64 | (1 << 15) | (tuple.length << 17);   // lp_off | LP_NORMAL<<15 | lp_len<<17
        put32(page, 24, itemId);
        byte[] image = new byte[64 + tuple.length];
        System.arraycopy(page, 0, image, 0, image.length);
        byte[] main = new byte[3];                              // xl_heap_insert 最小 main（offnum=1, flags=0）
        main[0] = 1;
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 100)
                .block(0, SPC, DB, USER_REL, 0).image(image, 0, 0).main(main).build();
        fx.feed(raw);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=1",
                "ROW INSERT public.t_stream before=null after={id=9, name=fpi}",
                "END xid=100 emitted=1 exp=1"), fx.listener.events);
    }

    /** TOAST chunk 采集 + 终态淘汰：multi-insert 喂 chunk → resolveExternal 可拼装（byte[] 契约）；任意终态后归集面全清。 */
    @Test
    void toastChunksCollectedThroughGrouperAndClearedAtTerminal() {
        Fixture fx = fixture();
        fx.toastChunks(900, 4242L, "abcdefghij".getBytes(StandardCharsets.UTF_8), 5);

        byte[] before = fx.toast.resolveExternal(pointer(14, 10, 4242L, TOAST_REL), 0);
        assertEquals("abcdefghij", new String(before, StandardCharsets.UTF_8), "终态前 chunk 已归集可拼装");

        fx.commit(901);                                         // 无桶事务的终态同样触发淘汰
        assertTrue(fx.toast.resolveExternal(pointer(14, 10, 4242L, TOAST_REL), 0)
                        == ToastAssembler.UNCHANGED_TOAST_MARKER,
                "终态后归集面已清 → unchanged 哨兵（Task 10 契约：空面不回查，engine 'u' 同形）");
    }

    /**
     * 逐行清窗 + unchanged 渲染（Task 10 值面裁定，双路对拍 IT 场景 3 的离线锚）：
     * 同事务先 INSERT 引用 external 指针（chunk 已归集 → 拼装原值），行施加后清窗
     * （镜像服务端 clear_toast_afterwards 的 toast hash 重置）；随后 UPDATE 新元组
     * 携<b>同一指针</b>（未变列）→ 归集面空 → {@code <toast-unchanged>}（engine 的
     * pgoutput 'u' 同形），绝不把清窗前的残留 chunk"重组"成 engine 不发的值。
     */
    @Test
    void updateWithUnchangedExternalPointerRendersUnchangedToastAfterWindowReset() {
        Fixture fx = fixture();
        fx.toastChunks(100, 4242L, "abcdefghij".getBytes(StandardCharsets.UTF_8), 5);

        // INSERT：新元组 name 列 = external 指针 → 窗口内 chunk 拼装为原值
        byte[] insertTuple = TupleBytes.of("int4", "text")
                .i32(1).externalTextPointer(14, 10, 0, 4242L, TOAST_REL).payload();
        byte[] main = new byte[3];
        main[0] = 1;
        fx.feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 100)
                .block(0, SPC, DB, USER_REL, 0).data(insertTuple).main(main).build());

        // UPDATE：只改 id，name 列同一指针（未变列形态）→ 清窗后归集面空 → 'u'
        byte[] before = TupleBytes.of("int4", "text").i32(1).text("abcdefghij").payload();
        byte[] after = TupleBytes.of("int4", "text")
                .i32(2).externalTextPointer(14, 10, 0, 4242L, TOAST_REL).payload();
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 0x11223344L);                              // old_xmax u32@0
        put16(m, 5);                                        // old_offnum u16@4
        m.write(0);                                         // old_infobits u8@6
        m.write(0x04 | 0x08);                               // flags u8@7 = XLH_UPDATE_CONTAINS_OLD
        put32(m, 0x55667788L);                              // new_xmax u32@8
        put16(m, 9);                                        // new_offnum u16@12
        m.writeBytes(before);
        fx.feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 100)
                .block(0, SPC, DB, USER_REL, 0).data(after).main(m.toByteArray()).build());
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=abcdefghij}",
                "ROW UPDATE public.t_stream before={id=1, name=abcdefghij}"
                        + " after={id=2, name=<toast-unchanged>}",
                "END xid=100 emitted=2 exp=2"), fx.listener.events,
                "INSERT 窗口内拼装原值、行施加后清窗、UPDATE 未变列同指针渲染 unchanged（engine 'u' 同形）");
    }

    /**
     * 子回滚 ABORT 无 toplevel 标记的归并收割（Task 8 实测发现的真实流形态）：ABORT
     * 记录不带 252 块（双路对拍 IT 钉）——子 xid → 顶层映射须从行记录到达时收割；
     * 无收割通道时该 ABORT 会被误判为顶层回滚（整桶弃）或彻底失联（行不扣减）。
     */
    @Test
    void subAbortWithoutToplevelMarkerFilteredViaHarvestedMapping() {
        Fixture fx = fixture();
        fx.insert(5, 0, 1, "top-a");
        fx.insert(7, 5, 2, "sub-b");       // 行记录带 toplevel=5 → 收割 subToTop[7]=5
        fx.abort(7, 0);                    // ABORT 记录无 toplevel（真实 PG 形态）
        fx.insert(5, 0, 3, "top-c");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=top-a}",
                "ROW INSERT public.t_stream before=null after={id=3, name=top-c}",
                "END xid=5 emitted=2 exp=3"), fx.listener.events,
                "无标记 ABORT 经收割映射归并回顶层桶，子行扣减而非整桶弃");
    }

    /**
     * UPDATE 截断 liveness guard——<b>非 FULL 身份形态</b>（Task 8 裁定 + Task 8.5 范围
     * 收窄）：XLH_UPDATE_TRUNCATION 置位且<b>无</b> CONTAINS_OLD_TUPLE（DEFAULT/KEY 身份，
     * 或块无 data 的畸形形态）的 UPDATE 行<b>行级跳过</b>——记录内没有全列旧元组可拼装
     * （KEY 元组非键列全 null，拼装只会产出错值），跳过是有痕丢弃：不发射、
     * {@code skippedTruncatedRows} 计数递增、流不死（后续正常行照常入桶发射）。
     */
    @Test
    void truncatedUpdateWithoutFullIdentitySkippedWithCounterAndStreamSurvives() {
        Fixture fx = fixture();
        fx.truncatedUpdateKeyIdentity(100, 0, 1, "old-key", 2, "new-truncated");
        fx.insert(100, 0, 7, "after-guard");
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=1",
                "ROW INSERT public.t_stream before=null after={id=7, name=after-guard}",
                "END xid=100 emitted=1 exp=1"), fx.listener.events,
                "截断行不入桶，同事务后续正常行照常发射");
        assertEquals(1, fx.grouper.skippedTruncatedRows(), "guard 观测计数恰一次");
    }

    /**
     * 截断 UPDATE 重建（Task 8.5 主面）——prefix&gt;0 且 suffix&gt;0 双段省略形态：
     * t_wide 三列表 UPDATE 只改中段 bool 列，首列 id（4B 数据区前缀）与尾列 name
     * （varlena 尾段）字节不变——块 data 按 heapam.c {@code log_heap_update} 实源形态
     * 手造（[prefix u16][suffix u16][xl_heap_header][位图+垫][mid 段]），main 尾带
     * FULL 身份整行旧元组。断言：重建后像值全等（非跳过、非短元组假 NULL）、前像
     * 解码自旧元组、guard 计数不递增。
     */
    @Test
    void truncatedUpdateFullIdentityPrefixAndSuffixReconstructsRowValues() {
        Fixture fx = fixture();
        fx.truncatedUpdateFull(100, 0, 1, false, "old-name", 1, true, "old-name");
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=1",
                "ROW UPDATE public.t_wide before={id=1, flag=f, name=old-name}"
                        + " after={id=1, flag=t, name=old-name}",
                "END xid=100 emitted=1 exp=1"), fx.listener.events,
                "截断新元组经旧元组字节拼装重建——首尾列来自旧元组、中段来自记录");
        assertEquals(0, fx.grouper.skippedTruncatedRows(), "FULL 身份不走 guard 跳过");
        assertEquals(1, fx.grouper.reconstructedTruncatedRows(), "重建观测计数恰一次");
    }

    /**
     * 截断 UPDATE 重建——prefix=0、suffix&gt;0 只截后缀形态（任务书双形态之二）：UPDATE
     * 只改首列 id（数据区首字节即变，无公共前缀），尾段 bool+text 与旧元组逐字节相同
     * ——块 data 形态 [suffix u16][xl_heap_header][tail 减后缀段]，重建走"整段取记录 +
     * 旧元组尾段回接"分支。
     */
    @Test
    void truncatedUpdateFullIdentitySuffixOnlyReconstructsRowValues() {
        Fixture fx = fixture();
        fx.truncatedUpdateFull(200, 0, 1, true, "keep-tail", 2, true, "keep-tail");
        fx.commit(200);

        assertEquals(List.of(
                "BEGIN xid=200 2p=false gid=null exp=1",
                "ROW UPDATE public.t_wide before={id=1, flag=t, name=keep-tail}"
                        + " after={id=2, flag=t, name=keep-tail}",
                "END xid=200 emitted=1 exp=1"), fx.listener.events,
                "只截后缀形态：记录整段 + 旧元组尾 suffix 字节回接重建");
        assertEquals(0, fx.grouper.skippedTruncatedRows(), "FULL 身份不走 guard 跳过");
        assertEquals(1, fx.grouper.reconstructedTruncatedRows(), "重建观测计数恰一次");
    }

    /**
     * 截断 UPDATE 重建——<b>新旧位图宽不同（t_hoff 不同）</b>形态（Task 9 补例，
     * Task 8.5 minor 清账）：t_bitmap 十列表 UPDATE 把旧元组的 NULL 列 c2 置为值
     * ——旧元组带 2B null 位图（t_hoff=32）、新元组全列活无位图（t_hoff=24），
     * <b>prefix 按 heapam 语义自各自 t_hoff 起比</b>（实源 L9160-9166：oldp/newp 各
     * 加<b>自己</b>的 t_hoff），本用例 prefix=c1 4B、suffix=c3..c10 32B、mid=新 c2
     * 4B。断言：重建后像十列全等（prefix 取旧元组数据区而非错切进位图垫段）、
     * 前像 c2=null、重建计数恰一次——钉死 {@code TruncatedUpdateSplice} 的
     * "旧 tail 自 (旧t_hoff-23) 起取 prefix"坐标与 v1"用新 tHoff 切旧 tail"的
     * 错误形态分界。
     */
    @Test
    void truncatedUpdateFullDifferentBitmapWidthReconstructsRowValues() {
        Fixture fx = fixture();
        fx.truncatedUpdateFullDifferentTHoff(300, 777);
        fx.commit(300);

        assertEquals(List.of(
                "BEGIN xid=300 2p=false gid=null exp=1",
                "ROW UPDATE public.t_bitmap before={c1=1, c2=null, c3=3, c4=4, c5=5, c6=6,"
                        + " c7=7, c8=8, c9=9, c10=10}"
                        + " after={c1=1, c2=777, c3=3, c4=4, c5=5, c6=6, c7=7, c8=8, c9=9, c10=10}",
                "END xid=300 emitted=1 exp=1"), fx.listener.events,
                "位图宽不同形态：prefix 自旧元组自己的 t_hoff 起取，十列重建全等");
        assertEquals(0, fx.grouper.skippedTruncatedRows(), "FULL 身份不走 guard 跳过");
        assertEquals(1, fx.grouper.reconstructedTruncatedRows(), "重建观测计数恰一次");
    }

    /** TableMeta 缓存失效：pg_attribute 上的 heap 记录到达即全清缓存，DDL 后新列即时可见。 */
    @Test
    void catalogHeapRecordInvalidatesTableMetaCache() {
        Fixture fx = fixture();
        fx.insert(100, 0, 1, "two-cols");
        fx.commit(100);
        assertEquals(1, fx.listener.begins.size());

        fx.snapshot.columnsByOid.put(USER_REL, List.of(
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false),
                new CatalogSnapshot.Column(3, "note", 25, false)));   // 模拟 DDL 重放已施加（ADD COLUMN）
        byte[] main = new byte[3];
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 555)
                .block(0, SPC, DB, PG_ATTRIBUTE_RELNODE, 0).data(new byte[] {1}).main(main).build();
        fx.feed(raw);                                           // 字典表 heap 记录 → 缓存全清

        fx.insert(200, 0, 2, "three-cols");                     // 旧元组两列，note 补 null（ADD COLUMN 前形态）
        fx.commit(200);

        assertEquals("ROW INSERT public.t_stream before=null after={id=2, name=three-cols, note=null}",
                fx.listener.events.stream().filter(e -> e.startsWith("ROW") && e.contains("three-cols"))
                        .findFirst().orElseThrow());
    }

    // ---- 测试基建 ----

    /**
     * 逐用例装配：stub 快照 + 真 ToastAssembler（probe=null）+ 录音 listener + LSN
     * 推进的喂入面。
     */
    private Fixture fixture() {
        StubSnapshot snapshot = new StubSnapshot();
        snapshot.table(USER_REL, "public", "t_stream", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false));
        snapshot.table(WIDE_REL, "public", "t_wide", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "flag", 16, false),
                new CatalogSnapshot.Column(3, "name", 25, false));
        snapshot.table(BITMAP_REL, "public", "t_bitmap", "r",
                new CatalogSnapshot.Column(1, "c1", 23, false),
                new CatalogSnapshot.Column(2, "c2", 23, false),
                new CatalogSnapshot.Column(3, "c3", 23, false),
                new CatalogSnapshot.Column(4, "c4", 23, false),
                new CatalogSnapshot.Column(5, "c5", 23, false),
                new CatalogSnapshot.Column(6, "c6", 23, false),
                new CatalogSnapshot.Column(7, "c7", 23, false),
                new CatalogSnapshot.Column(8, "c8", 23, false),
                new CatalogSnapshot.Column(9, "c9", 23, false),
                new CatalogSnapshot.Column(10, "c10", 23, false));
        snapshot.table(TOAST_REL, "pg_toast", "pg_toast_24600", "t");
        snapshot.table(PG_ATTRIBUTE_RELNODE, "pg_catalog", "pg_attribute", "r");
        RecordingListener listener = new RecordingListener();
        ToastAssembler toast = new ToastAssembler(null);
        XactGrouper grouper = new XactGrouper(snapshot, null, layout, toast, listener);
        return new Fixture(snapshot, toast, grouper, listener);
    }

    /**
     * 单用例装配体——持四组件与 LSN 游标（记录按 MAXALIGN 步进，commitLsn/endLsn
     * 断言与真实 WAL 序一致）。
     */
    private static final class Fixture {
        final StubSnapshot snapshot;
        final ToastAssembler toast;
        final XactGrouper grouper;
        final RecordingListener listener;
        private long lsn = 0x100000L;

        Fixture(StubSnapshot snapshot, ToastAssembler toast, XactGrouper grouper, RecordingListener listener) {
            this.snapshot = snapshot;
            this.toast = toast;
            this.grouper = grouper;
            this.listener = listener;
        }

        /** 喂一条记录字节并推进 LSN。 */
        WalRecord feed(byte[] bytes) {
            WalRecord r = WalRecordParser.parse(bytes, lsn, layout());
            grouper.onRecord(r);
            lsn += align8(r.totLen());
            return r;
        }

        /** 一条用户表 INSERT（toplevel=0 即无标记）。 */
        WalRecord insert(int xid, int toplevel, int id, String name) {
            byte[] payload = TupleBytes.of("int4", "text").i32(id).text(name).payload();
            byte[] main = new byte[3];
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0).data(payload).main(main).build());
        }

        /** 一条 UPDATE（CONTAINS_OLD 前像在 main 尾、新元组在块 data）。 */
        WalRecord update(int xid, int toplevel, int oldId, String oldName, int newId, String newName) {
            byte[] before = TupleBytes.of("int4", "text").i32(oldId).text(oldName).payload();
            byte[] after = TupleBytes.of("int4", "text").i32(newId).text(newName).payload();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0x11223344L);                              // old_xmax u32@0
            put16(m, 5);                                        // old_offnum u16@4
            m.write(0);                                         // old_infobits u8@6
            m.write(0x04 | 0x08);                               // flags u8@7 = XLH_UPDATE_CONTAINS_OLD
            put32(m, 0x55667788L);                              // new_xmax u32@8
            put16(m, 9);                                        // new_offnum u16@12
            m.writeBytes(before);                               // 前像载荷殿后（14B 结构之后）
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0).data(after)
                    .main(m.toByteArray()).build());
        }

        /**
         * 一条带前缀截断标志、<b>非 FULL 身份</b>的 UPDATE（liveness guard 锚，Task 8.5
         * 范围收窄后 guard 只吃该形态）：flags 置 PREFIX_FROM_OLD + CONTAINS_OLD_KEY
         * （无 CONTAINS_OLD_TUPLE——记录内无全列旧元组），新元组只有首列（natts=1，
         * 短于两列词典——无 guard 时会静默产出尾列假 NULL 的错值行）。
         */
        WalRecord truncatedUpdateKeyIdentity(int xid, int toplevel, int oldId, String oldName, int newId, String ignoredName) {
            byte[] before = TupleBytes.of("int4", "text").i32(oldId).text(oldName).payload();
            byte[] after = TupleBytes.of("int4").i32(newId).payload();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0x11223344L);                              // old_xmax u32@0
            put16(m, 5);                                        // old_offnum u16@4
            m.write(0);                                         // old_infobits u8@6
            m.write(HeapOps.XLH_UPDATE_CONTAINS_OLD_KEY | HeapOps.XLH_UPDATE_PREFIX_FROM_OLD);
            put32(m, 0x55667788L);                              // new_xmax u32@8
            put16(m, 9);                                        // new_offnum u16@12
            m.writeBytes(before);
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0).data(after)
                    .main(m.toByteArray()).build());
        }

        /**
         * 一条字节忠实的<b>截断 + FULL 身份</b> UPDATE（Task 8.5 重建面锚）：由完整
         * 旧/新元组字节按 heapam.c {@code log_heap_update} 的省略规则（数据区公共
         * 前缀/后缀）推导 prefix/suffix，再按实源注册形态组装——块 data =
         * {@code [prefix u16?][suffix u16?][xl_heap_header 5B][位图+垫 (t_hoff-23)][mid 段]}
         * （prefix=0 时为 tail 减后缀的连续段），main = 14B xl_heap_update（flags 置
         * CONTAINS_OLD_TUPLE + 截断位）+ 整行旧元组载荷殿后。
         */
        WalRecord truncatedUpdateFull(int xid, int toplevel, int oldId, boolean oldFlag, String oldName,
                int newId, boolean newFlag, String newName) {
            byte[] oldPayload = TupleBytes.of("int4", "bool", "text")
                    .i32(oldId).bool(oldFlag).text(oldName).payload();
            byte[] newPayload = TupleBytes.of("int4", "bool", "text")
                    .i32(newId).bool(newFlag).text(newName).payload();
            byte[] oldTail = tailBytes(oldPayload);
            byte[] newTail = tailBytes(newPayload);
            int dataStart = (newPayload[4] & 0xFF) - 23;   // 位图+垫区长（t_hoff-23）
            int prefix = commonPrefix(newTail, oldTail, dataStart);
            int suffix = commonSuffix(newTail, oldTail, dataStart, prefix);
            int flags = HeapOps.XLH_UPDATE_CONTAINS_OLD_TUPLE;
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            if (prefix > 0) {
                flags |= HeapOps.XLH_UPDATE_PREFIX_FROM_OLD;
                put16(data, prefix);
            }
            if (suffix > 0) {
                flags |= HeapOps.XLH_UPDATE_SUFFIX_FROM_OLD;
                put16(data, suffix);
            }
            data.write(newPayload, 0, 5);                  // xl_heap_header 5B
            if (prefix > 0) {
                data.write(newTail, 0, dataStart);         // 位图 + 垫
                data.write(newTail, dataStart + prefix,
                        newTail.length - dataStart - prefix - suffix);   // mid 段
            } else {
                data.write(newTail, 0, newTail.length - suffix);         // tail 减后缀整段
            }
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0x11223344L);                         // old_xmax u32@0
            put16(m, 5);                                   // old_offnum u16@4
            m.write(0);                                    // old_infobits u8@6
            m.write(flags);                                // flags u8@7
            put32(m, 0x55667788L);                         // new_xmax u32@8
            put16(m, 9);                                   // new_offnum u16@12
            m.writeBytes(oldPayload);                      // FULL 旧元组殿后
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, xid)
                    .toplevel(toplevel).block(0, SPC, DB, WIDE_REL, 0).data(data.toByteArray())
                    .main(m.toByteArray()).build());
        }

        /**
         * 一条<b>新旧位图宽不同（t_hoff 不同）</b>的截断 + FULL 身份 UPDATE（Task 9
         * 补例，Task 8.5 minor 清账）：t_bitmap 十列 int4 表，旧元组 c2 为 NULL
         * （null 位图 2B → t_hoff=32）、新元组把 c2 置为 {@code newC2} 且全列活
         * （无位图 → t_hoff=24）——prefix/suffix 按实源语义自<b>各自</b> t_hoff 起的
         * <b>数据区</b>比（heapam.c L9160-9166，见 {@link #commonPrefixAt} 的前提
         * 注记），块 data 双截断头 + 新元组头 + 新位图垫段（本形态 1B 垫）+ mid 段
         * （新 c2），main 尾 FULL 整行旧元组。
         */
        WalRecord truncatedUpdateFullDifferentTHoff(int xid, int newC2) {
            String[] kinds = new String[10];
            Arrays.fill(kinds, "int4");
            TupleBytes oldB = TupleBytes.of(kinds).nullAt(1);
            oldB.i32(1);
            for (int v = 3; v <= 10; v++) {
                oldB.i32(v);
            }
            TupleBytes newB = TupleBytes.of(kinds);
            newB.i32(1).i32(newC2);
            for (int v = 3; v <= 10; v++) {
                newB.i32(v);
            }
            byte[] oldPayload = oldB.payload();
            byte[] newPayload = newB.payload();
            byte[] oldTail = tailBytes(oldPayload);
            byte[] newTail = tailBytes(newPayload);
            int newDataStart = (newPayload[4] & 0xFF) - 23;   // 1：无位图，1B 垫
            int oldDataStart = (oldPayload[4] & 0xFF) - 23;   // 9：位图 2B + 垫 5B
            int prefix = commonPrefixAt(newTail, newDataStart, oldTail, oldDataStart);
            int suffix = commonSuffixAt(newTail, newDataStart, oldTail, oldDataStart, prefix);
            int flags = HeapOps.XLH_UPDATE_CONTAINS_OLD_TUPLE
                    | HeapOps.XLH_UPDATE_PREFIX_FROM_OLD | HeapOps.XLH_UPDATE_SUFFIX_FROM_OLD;
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            put16(data, prefix);
            put16(data, suffix);
            data.write(newPayload, 0, 5);                     // xl_heap_header 5B
            data.write(newTail, 0, newDataStart);             // 新元组自己的位图+垫
            data.write(newTail, newDataStart + prefix,
                    newTail.length - newDataStart - prefix - suffix);   // mid 段（新 c2）
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0x11223344L);                            // old_xmax u32@0
            put16(m, 5);                                      // old_offnum u16@4
            m.write(0);                                       // old_infobits u8@6
            m.write(flags);                                   // flags u8@7
            put32(m, 0x55667788L);                            // new_xmax u32@8
            put16(m, 9);                                      // new_offnum u16@12
            m.writeBytes(oldPayload);                         // FULL 旧元组殿后
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, xid)
                    .toplevel(0).block(0, SPC, DB, BITMAP_REL, 0).data(data.toByteArray())
                    .main(m.toByteArray()).build());
        }

        /** 一条 DELETE（CONTAINS_OLD 前像在 main 尾）。 */
        WalRecord delete(int xid, int toplevel, int id, String name) {
            byte[] before = TupleBytes.of("int4", "text").i32(id).text(name).payload();
            return deleteRaw(xid, toplevel, before, 0x02 | 0x04);
        }

        /** 一条无前像 DELETE（默认 replica identity 形态——flags 无 CONTAINS_OLD）。 */
        WalRecord deleteNoOld(int xid, int toplevel, int id) {
            return deleteRaw(xid, toplevel, null, 0);
        }

        /** DELETE 记录装配（8B 结构 + 可选前像）。 */
        private WalRecord deleteRaw(int xid, int toplevel, byte[] before, int flags) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0xAABBCCDDL);                              // xmax u32@0
            put16(m, 4);                                        // offnum u16@4
            m.write(0);                                         // infobits u8@6
            m.write(flags);                                     // flags u8@7
            if (before != null) {
                m.writeBytes(before);                           // 前像载荷殿后（8B 结构之后）
            }
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0)
                    .main(m.toByteArray()).build());
        }

        /** 一条 COMMIT（toplevel=0 即无标记）。 */
        WalRecord commit(int xid) {
            return feed(WalBytes.xactRecord(HeapOps.XLOG_XACT_COMMIT, xid, COMMIT_MICROS, 0));
        }

        /** 一条 ABORT（toplevel 非 0 即子事务回滚形态）。 */
        WalRecord abort(int xid, int toplevel) {
            return feed(WalBytes.xactRecord(HeapOps.XLOG_XACT_ABORT, xid, COMMIT_MICROS, toplevel));
        }

        /** toast 关系上的 chunk multi-insert（entry = [datalen u16][三列 payload]）。 */
        void toastChunks(int xid, long valueId, byte[] value, int chunkSize) {
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            List<byte[]> payloads = new ArrayList<>();
            for (int off = 0, seq = 0; off < value.length; off += chunkSize, seq++) {
                int len = Math.min(chunkSize, value.length - off);
                byte[] chunk = new byte[len];
                System.arraycopy(value, off, chunk, 0, len);
                payloads.add(TupleBytes.of("oid", "int4", "bytea")
                        .oid(valueId).i32(seq).bytes(chunk).payload());
            }
            m.write(0);                                         // flags u8@0
            m.write(0);                                         // C padding@1
            put16(m, payloads.size());                          // ntuples u16@2
            for (int i = 0; i < payloads.size(); i++) {
                put16(m, i + 1);                                // offsets u16[]@4
            }
            for (byte[] p : payloads) {
                put16(data, p.length);                          // entry 的 datalen 前缀
                data.writeBytes(p);
            }
            feed(WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, xid)
                    .block(0, SPC, DB, TOAST_REL, 0).data(data.toByteArray())
                    .main(m.toByteArray()).build());
        }

        private WalLayout layout() {
            return WalLayoutV18.INSTANCE;
        }
    }

    /**
     * 事件录音 listener——扁平字符串面（断言事件序/内容）+ BatchBegin 强类型面
     * （commitLsn/endLsn/commitTs 断言）。
     */
    private static final class RecordingListener implements ChangeOutputListener {
        final List<String> events = new ArrayList<>();
        final List<BatchBegin> begins = new ArrayList<>();

        @Override
        public void onBegin(BatchBegin begin) {
            begins.add(begin);
            events.add("BEGIN xid=" + begin.xid() + " 2p=" + begin.twoPhase() + " gid=" + begin.gid()
                    + " exp=" + begin.expectedChanges());
        }

        @Override
        public void onRow(RowChange row) {
            events.add("ROW " + row.dml() + " " + row.table().schema() + "." + row.table().table()
                    + " before=" + row.before() + " after=" + row.after());
        }

        @Override
        public void onEnd(BatchEnd end) {
            events.add("END xid=" + end.xid() + " emitted=" + end.emittedChanges()
                    + " exp=" + end.expectedChanges());
        }

        @Override
        public void onAborted(BatchAborted aborted) {
            events.add("ABORTED xid=" + aborted.xid());
        }
    }

    /**
     * 最小 catalog 快照 stub——四查询 + 归一入口按三张 map 应答（oid 即键，relfilenode
     * 缺省身份映射，可经 {@code relnodeToOid} 显式分叉）；列字典可变（DDL 失效用例
     * 中途改写）。
     */
    private static final class StubSnapshot implements CatalogSnapshot {
        final Map<Long, String> relkindByOid = new HashMap<>();
        final Map<Long, String> schemaByOid = new HashMap<>();
        final Map<Long, String> nameByOid = new HashMap<>();
        final Map<Long, List<Column>> columnsByOid = new HashMap<>();

        /** 登记一张表（relfilenode 取身份映射）。 */
        void table(long oid, String schema, String name, String relkind, Column... cols) {
            relkindByOid.put(oid, relkind);
            schemaByOid.put(oid, schema);
            nameByOid.put(oid, name);
            columnsByOid.put(oid, List.of(cols));
        }

        @Override
        public long lsn() {
            return 0;
        }

        @Override
        public List<Column> columnsOf(long relOid) {
            return columnsByOid.getOrDefault(relOid, List.of());
        }

        @Override
        public OptionalLong relfilenodeOf(long relOid) {
            return relkindByOid.containsKey(relOid) ? OptionalLong.of(relOid) : OptionalLong.empty();
        }

        @Override
        public OptionalLong toastOf(long relOid) {
            return relkindByOid.containsKey(relOid) ? OptionalLong.of(0) : OptionalLong.empty();
        }

        @Override
        public Optional<String> schemaOf(long relOid) {
            return Optional.ofNullable(schemaByOid.get(relOid));
        }

        @Override
        public Optional<String> relkindOf(long relOid) {
            return Optional.ofNullable(relkindByOid.get(relOid));
        }

        @Override
        public Optional<String> nameOf(long relOid) {
            return Optional.ofNullable(nameByOid.get(relOid));
        }

        @Override
        public OptionalLong relOidOf(long relNodeOrOid) {
            if (relkindByOid.containsKey(relNodeOrOid)) {
                return OptionalLong.of(relNodeOrOid);           // oid 直认
            }
            return OptionalLong.empty();                        // 无 relfilenode 分叉登记
        }
    }

    /** MAXALIGN 到 8。 */
    private static long align8(int len) {
        return ((long) len + 7) / 8 * 8;
    }

    /** 载荷（[xl_heap_header 5B][tail]）剥 5B 头得 tail（自 tuple offset 23 起字节）。 */
    private static byte[] tailBytes(byte[] payload) {
        return Arrays.copyOfRange(payload, 5, payload.length);
    }

    /**
     * 两 tail 自 dataStart 起（数据区，t_hoff 之后）的公共前缀字节数——heapam.c
     * {@code log_heap_update} 的 prefix 计算同规则（位图+垫区不参与比较）。
     *
     * <p><b>前提注记（Task 9 补）</b>：本形态假设<b>新旧元组位图宽相同</b>（t_hoff
     * 相等，单一 dataStart 对两侧同用）。实源（heapam.c L9160-9166）是各自加
     * <b>自己的</b> t_hoff 后逐字节比——位图宽不同（旧有 null 位图/新无，或 natts
     * 段不同）时须用 {@link #commonPrefixAt} 的双起点形态，否则算出的"前缀"会把
     * 一侧的位图/垫段错切进数据区。</p>
     */
    private static int commonPrefix(byte[] a, byte[] b, int dataStart) {
        return commonPrefixAt(a, dataStart, b, dataStart);
    }

    /**
     * 两 tail 自<b>各自</b>数据区起点的公共前缀字节数——heapam.c 的 prefix 计算实源
     * 语义（oldp = 旧元组 + 旧 t_hoff、newp = 新元组 + 新 t_hoff，位图宽可不同）。
     */
    private static int commonPrefixAt(byte[] a, int aStart, byte[] b, int bStart) {
        int n = 0;
        int max = Math.min(a.length - aStart, b.length - bStart);
        while (n < max && a[aStart + n] == b[bStart + n]) {
            n++;
        }
        return n;
    }

    /**
     * 两 tail 的公共后缀字节数（自尾端反向比较，越过 prefix 段即停）——heapam.c 的
     * suffix 计算同规则；尾端即数据区末端（tuple 末端），坐标系与实源一致。
     *
     * <p><b>前提注记</b>：与 {@link #commonPrefix} 同——单 dataStart 假设位图宽相同；
     * 位图宽不同时用 {@link #commonSuffixAt}。</p>
     */
    private static int commonSuffix(byte[] a, byte[] b, int dataStart, int prefix) {
        return commonSuffixAt(a, dataStart, b, dataStart, prefix);
    }

    /**
     * 两 tail 自<b>各自</b>数据区起点的公共后缀字节数（自尾端反向比较，尾端即数据区
     * 末端——起点差异不影响尾端坐标；上限按两侧各自的数据区余量取小，越过 prefix
     * 段即停）。
     */
    private static int commonSuffixAt(byte[] a, int aStart, byte[] b, int bStart, int prefix) {
        int n = 0;
        int max = Math.min(a.length - aStart, b.length - bStart) - prefix;
        while (n < max && a[a.length - n - 1] == b[b.length - n - 1]) {
            n++;
        }
        return n;
    }

    /** 18B external 指针（ToastAssemblerTest 同布局：01 12 | rawsize | extinfo | valueid | toastrelid）。 */
    private static byte[] pointer(long rawsize, long extsize, long valueid, long toastrelid) {
        byte[] p = new byte[18];
        p[0] = 0x01;
        p[1] = 0x12;
        put32(p, 2, rawsize);
        put32(p, 6, extsize);
        put32(p, 10, valueid);
        put32(p, 14, toastrelid);
        return p;
    }

    /** 就地写 little-endian u32。 */
    private static void put32(byte[] target, int offset, long v) {
        target[offset] = (byte) (v & 0xFF);
        target[offset + 1] = (byte) ((v >>> 8) & 0xFF);
        target[offset + 2] = (byte) ((v >>> 16) & 0xFF);
        target[offset + 3] = (byte) ((v >>> 24) & 0xFF);
    }

    /** 向流写 little-endian u16。 */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }

    /** 向流写 little-endian u32。 */
    private static void put32(ByteArrayOutputStream out, long v) {
        out.write((int) (v & 0xFF));
        out.write((int) (v >>> 8) & 0xFF);
        out.write((int) (v >>> 16) & 0xFF);
        out.write((int) (v >>> 24) & 0xFF);
    }
}
