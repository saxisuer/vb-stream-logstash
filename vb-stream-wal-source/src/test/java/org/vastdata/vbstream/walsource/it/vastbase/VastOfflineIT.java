package org.vastdata.vbstream.walsource.it.vastbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.it.OgParityBaseline;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.state.StateStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * VastBase G100 离线回放档真库 IT（手动档 {@code -Dvb.it.vastbase=true}，offline-replay
 * Task 5）：在线直解路（{@link WalSource#start()} direct 通道）vs 离线重放路
 * （{@link WalSource#startOffline()} 本地检查点 + 段快照目录）四用例——对拍全等 /
 * 离线→在线双向回切 / 离线截停再离线续传（at-least-once）/ endLsn 上界截停。
 *
 * <p><strong>"库死亡"的测试承载（spike 第二轮形态）</strong>：真库不可杀——离线前提由
 * "停在线门面（close 落最终检查点）+ {@link WalSegmentRecorder} 独立收集的段目录"构造：
 * recorder 先于在线门面起（覆盖引导一致点 B 所在段头），门面停后续写的数据仅段目录在
 * 生长（离线可见面），等 {@code recordedFrontierLsn ≥ 目标位点} 后停 recorder 转离线
 * （竞速截尾防线）。对拍形态：两路 {@link OgParityBaseline.WalCapture}（List 版
 * {@code ChangeOutputListener}——BatchBegin.xid → RowChange 渲染文本 → BatchEnd 攒块）
 * 经 {@link OgParityBaseline#assertParity} 按 xid 交集逐行全等（spike {@code ParityDiff}
 * 思路进 JUnit，不落文件）；交集非空由"检查点快照（用例 1）落在阶段中位"构造——快照后
 * 的事务在线收过、离线窗口也全量覆盖，双路必然同解码。</p>
 *
 * <p><strong>页对齐窗口的确定性防线（WAL 布局纪律）</strong>：离线流起点 = 检查点 lsn
 * 页对齐下取整——检查点前一页内的记录会被重投递（at-least-once）。若白名单内事务跨在
 * 该窗口边界，重投递只得尾部行 → 部分重建块与完整块 diff（假红）。防线 = 每个检查点
 * （快照/close 落盘点）与最近的白名单内事务之间垫一段白名单外填充事务
 * （{@link #PAD_ROWS} 行 ≈36KB &gt; 页窗 8KB 的三倍余量）——窗口必然整段落在外表段内，
 * 白名单事务要么完整在窗内（全量重解码，双路同块）、要么完整在窗外（不重发），无部分
 * 重建面。外表（{@code *_ign}）不在 {@code vb.wal.tables} 白名单——重投递行被过滤零
 * 发射。</p>
 *
 * <p><strong>真库卫生</strong>：表 {@code t_offline_it1..4}（+ 外表 {@code _ign} 后缀）、
 * 槽 {@code vb_offline_it1..4}、2PC gid {@code vb_off_it1_g} 一律独立命名 + AfterEach
 * 兜底清理（finally 正常路径亦然）；walDir/state 目录 = {@code target/vastbase-it/}
 * 独立子目录（{@link VastTestEnv#newStateDir} 用例级 wipe）。不触碰既有业务对象。</p>
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastOfflineIT {

    private static final Logger LOG = LoggerFactory.getLogger(VastOfflineIT.class);

    /** 轮询等待节拍（毫秒）。 */
    private static final long POLL_MS = 200;

    /** 常规等待上限（毫秒）——真库跨网档（沿 {@code VastDmlParityIT} 60s）。 */
    private static final long AWAIT_MS = 60_000;

    /** 检查点周期（毫秒）——用例 1 需运行中检查点尽快落盘（拷快照面前置）。 */
    private static final String STATE_INTERVAL_MS = "2000";

    /**
     * 白名单外填充行数：单事务 500 行 × ~72B/行（元组 + 记录头）≈ 36KB——三倍于页对齐
     * 窗（8KB）的隔离带，保证检查点前页的落点远离白名单内事务（见类 javadoc 纪律段）。
     */
    private static final int PAD_ROWS = 500;

    /** 检查点快照拷贝的竞速重试上限（周期检查点 rename 与读窗的毫秒级竞速——.part→rename 原子，终态完整）。 */
    private static final int COPY_ATTEMPTS = 3;

    /** 本用例启动的门面/记录器——AfterEach 兜底关闭（幂等）。 */
    private final List<AutoCloseable> closables = new ArrayList<>();

    /** 本用例登记的物理槽——AfterEach 兜底删除。 */
    private final List<String> slots = new ArrayList<>();

    /** 本用例动用的 2PC gid——AfterEach 兜底 ROLLBACK PREPARED（失败路径释放挂起）。 */
    private final List<String> gids = new ArrayList<>();

    // ---- 用例 1：在线 vs 离线对拍全等 ----

    /**
     * 用例 1 {@code offlineReplayMatchesOnlineBaseline}：recorder 起收集 → 门面 start()
     * （direct）在线跑两批 DDL/DML → 阶段中位拷检查点快照（pad 隔离带 + 外表 DDL 触发
     * 周期检查点——快照 C ∈ (填充段末, 阶段 B 首笔)，离线窗口起点确定性越过阶段 A）→
     * 阶段 B（TOAST 大值 / 事务内 DDL as-of / 2PC / 回滚+控制行——检查点后，在线收过
     * 且离线窗口全量覆盖 → 双路交集的构造面）→ 停门面 → 阶段 C 仅离线可见事务 →
     * 同快照 startOffline() 读尽 → 两路按 xid 交集逐行全等。
     *
     * <p>专项断言：离线 2PC 块 TWO_PHASE+gid（重放重建桶终态钉）；仅离线可见的阶段 C
     * 块逐行字面（extra 列经阶段 B 的 ADD COLUMN 演进后渲 NULL——字典经检查点持久化后
     * 流内演进的离线面）；两路块数恰等（窗口隔离带的确定性推论——阶段 A 在线独有 /
     * 阶段 C 离线独有，交集恰阶段 B 四块）。</p>
     */
    @Test
    void offlineReplayMatchesOnlineBaseline() throws Exception {
        String table = "t_offline_it1";
        String ign = table + "_ign";
        String slot = registerSlot("vb_offline_it1");
        Path walDir = VastTestEnv.newStateDir("off1-wal");
        Path stateDir = VastTestEnv.newStateDir("off1-state");
        Path snapDir = VastTestEnv.newStateDir("off1-snap");
        resetTables(table, ign);
        WalSegmentRecorder recorder = WalSegmentRecorder.start(walDir);
        closables.add(recorder);

        OgParityBaseline.WalCapture online = new OgParityBaseline.WalCapture();
        WalSource wal = VastTestEnv.startOnlineWithRetry(onlineConfig(slot, table, stateDir), online);
        closables.add(wal);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            // 阶段 A：检查点快照之前的在线独有事务（离线窗口按隔离带原理不可达——在线独有面）
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (1, 'seed-a')");
            exec(c, "INSERT INTO " + table + " VALUES (2, 'seed-b')");
            exec(c, "COMMIT");
            await(() -> wal.dmlEmittedBuckets() >= 1, "离线对拍: 阶段 A 在线路未发射（buckets="
                    + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
            // 隔离带 + 检查点触发器：外表填充（零发射）垫开页窗，外表 ADD COLUMN（pg_attribute
            // heap 行事件——REPLAYED 计数面，events=1；INPLACE 只计 INPLACE_UPDATES 不进门）
            // 在填充段之后立刻触发周期检查点——落盘 lsn 必越过填充段末
            padIgnored(c, ign);
            long padEnd = VastTestEnv.writeLsn(c);
            exec(c, "ALTER TABLE " + ign + " ADD COLUMN ckpt_trigger int");
            await(() -> wal.lastCheckpointLsn() >= padEnd, "离线对拍: 运行中检查点未越过填充段末 "
                    + Lsn.format(padEnd) + "（当前 " + Lsn.format(wal.lastCheckpointLsn()) + "）");
        }
        // 检查点快照（离线路的 state 目录与入口凭证）：拷贝时点先于阶段 B 首笔——快照 lsn
        // 确定性落在 (填充段末, 阶段 B 首笔) 开区间
        copyStateSnapshot(stateDir, snapDir);

        String gid = "vb_off_it1_g";
        gids.add(gid);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            // 阶段 B（交集构造面——四块双路同解码）
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (10,"
                    + " (SELECT string_agg(md5(i::text),'') FROM generate_series(1,220) i))");
            exec(c, "UPDATE " + table + " SET id = 110 WHERE id = 10");
            exec(c, "COMMIT");
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (20, 'before-ddl')");
            exec(c, "ALTER TABLE " + table + " ADD COLUMN extra text");
            exec(c, "INSERT INTO " + table + " VALUES (21, 'after-ddl', 'new-col')");
            exec(c, "UPDATE " + table + " SET name = 'before-ddl-upd', extra = 'filled' WHERE id = 20");
            exec(c, "COMMIT");
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (30, 'twophase-row')");
            exec(c, "PREPARE TRANSACTION '" + gid + "'");
            exec(c, "COMMIT PREPARED '" + gid + "'");
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (40, 'doomed')");
            exec(c, "ROLLBACK");
            exec(c, "INSERT INTO " + table + " VALUES (41, 'control-row')");
            await(() -> wal.dmlEmittedBuckets() >= 5, "离线对拍: 阶段 A+B 在线路未发射 5 桶（buckets="
                    + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
        }
        wal.close();

        // 阶段 C（离线独有面）：门面已停——"库死亡"模拟，仅段目录在生长
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (50, 'offline-only')");
            exec(c, "UPDATE " + table + " SET name = 'offline-only-upd' WHERE id = 50");
            exec(c, "COMMIT");
            long eof = VastTestEnv.writeLsn(c);
            await(() -> recorder.recordedFrontierLsn() >= eof, "离线对拍: 段目录未覆盖终位点 "
                    + Lsn.format(eof) + "（已录 " + Lsn.format(recorder.recordedFrontierLsn())
                    + ", terminal=" + recorder.terminalFailure() + "）");
        }
        recorder.close();

        OgParityBaseline.WalCapture offline = new OgParityBaseline.WalCapture();
        WalSource replay = new WalSource(offlineConfig(snapDir, walDir, table, 0L), offline);
        closables.add(replay);
        replay.startOffline();
        await(replay::eofReached, "离线对拍: 离线路未读尽（terminal=" + replay.receiverTerminalFailure() + "）");
        assertTrue(replay.receiverTerminalFailure().isEmpty(), "离线对拍: 离线回放不应终态失败");
        replay.close();

        assertEquals(5, online.blocks().size(), "在线路应恰 5 块（阶段 A 一块 + 阶段 B 四块）:\n" + online.blocks());
        assertEquals(5, offline.blocks().size(), "离线路应恰 5 块（阶段 B 四块 + 阶段 C 一块）:\n" + offline.blocks());
        OgParityBaseline.assertParity(online.blocks(), offline.blocks(), "在线 vs 离线", 0);

        ChangeOutputListener.BatchBegin twoPhase = offline.begins().stream()
                .filter(b -> gid.equals(b.gid())).findFirst()
                .orElseThrow(() -> new AssertionError("离线路应有 gid=" + gid + " 的块（重放重建桶）: "
                        + offline.begins()));
        assertTrue(twoPhase.twoPhase(), "离线 2PC 块应为 TWO_PHASE（重放重建桶终态钉）");
        blockWithRows(offline.blocks(), List.of(
                "INSERT public." + table + " BEFORE=- AFTER=[id=50, name=offline-only, extra=NULL]",
                "UPDATE public." + table + " BEFORE=[id=50, name=offline-only, extra=NULL]"
                        + " AFTER=[id=50, name=offline-only-upd, extra=NULL]"));
        LOG.info("离线对拍完成: 在线 5 块 / 离线 5 块，交集 = 阶段 B 四块逐行全等（含 TOAST/DDL-in-txn/2PC）");
    }

    // ---- 用例 2：离线→在线双向回切 ----

    /**
     * 用例 2 {@code offlineToOnlineResumeInteroperates}：离线跑至 EOF（检查点推进至段
     * 目录末）→ 同 state 目录再 {@code start()}（direct）→ 写新数据完整可见——检查点
     * 双向互通（离线写、在线续）的衔接面验收（spike 实证衔接面的 IT 化）。
     *
     * <p>步骤：在线段一（一笔 + 隔离带 → close 落 P1）→ 两笔离线独有事务 → 段目录收满
     * 停 recorder → 离线读尽（两块逐行字面钉 + EOF）→ close 落 P2 → 同 state 目录在
     * 线回切（resumedFromState 钉续传前提）→ 新数据桶到位逐行字面钉。</p>
     */
    @Test
    void offlineToOnlineResumeInteroperates() throws Exception {
        String table = "t_offline_it2";
        String ign = table + "_ign";
        String slot = registerSlot("vb_offline_it2");
        Path walDir = VastTestEnv.newStateDir("off2-wal");
        Path stateDir = VastTestEnv.newStateDir("off2-state");
        resetTables(table, ign);
        WalSegmentRecorder recorder = WalSegmentRecorder.start(walDir);
        closables.add(recorder);

        OgParityBaseline.WalCapture seg1 = new OgParityBaseline.WalCapture();
        WalSource wal = VastTestEnv.startOnlineWithRetry(onlineConfig(slot, table, stateDir), seg1);
        closables.add(wal);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "INSERT INTO " + table + " VALUES (1, 'online-seg1')");
            await(() -> wal.dmlEmittedBuckets() >= 1, "双向回切: 段一未发射（buckets="
                    + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
            padIgnored(c, ign);
            long padEnd = VastTestEnv.writeLsn(c);
            await(() -> wal.consumedLsn() >= padEnd, "双向回切: 段一未消费到填充段末（consumed="
                    + Lsn.format(wal.consumedLsn()) + " < " + Lsn.format(padEnd) + "）");
        }
        wal.close();

        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (2, 'offline-a')");
            exec(c, "INSERT INTO " + table + " VALUES (3, 'offline-b')");
            exec(c, "COMMIT");
            exec(c, "UPDATE " + table + " SET name = 'offline-b-upd' WHERE id = 3");
            long eof = VastTestEnv.writeLsn(c);
            await(() -> recorder.recordedFrontierLsn() >= eof, "双向回切: 段目录未覆盖终位点 "
                    + Lsn.format(eof) + "（已录 " + Lsn.format(recorder.recordedFrontierLsn()) + "）");
        }
        recorder.close();

        OgParityBaseline.WalCapture offline = new OgParityBaseline.WalCapture();
        WalSource replay = new WalSource(offlineConfig(stateDir, walDir, table, 0L), offline);
        closables.add(replay);
        replay.startOffline();
        await(replay::eofReached, "双向回切: 离线未读尽（terminal=" + replay.receiverTerminalFailure() + "）");
        assertTrue(replay.receiverTerminalFailure().isEmpty(), "双向回切: 离线回放不应终态失败");
        replay.close();

        assertEquals(2, offline.blocks().size(), "离线段应恰两块（隔离带外的两笔事务）:\n" + offline.blocks());
        blockWithRows(offline.blocks(), List.of(
                "INSERT public." + table + " BEFORE=- AFTER=[id=2, name=offline-a]",
                "INSERT public." + table + " BEFORE=- AFTER=[id=3, name=offline-b]"));
        blockWithRows(offline.blocks(), List.of(
                "UPDATE public." + table + " BEFORE=[id=3, name=offline-b]"
                        + " AFTER=[id=3, name=offline-b-upd]"));

        OgParityBaseline.WalCapture seg2 = new OgParityBaseline.WalCapture();
        WalSource wal2 = VastTestEnv.startOnlineWithRetry(onlineConfig(slot, table, stateDir), seg2);
        closables.add(wal2);
        assertTrue(wal2.resumedFromState(), "在线回切应自离线检查点续传（双向互通前提）");
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "INSERT INTO " + table + " VALUES (4, 'after-resume')");
            await(() -> wal2.dmlEmittedBuckets() >= 1, "双向回切: 回切段未发射（buckets="
                    + wal2.dmlEmittedBuckets() + ", terminal=" + wal2.receiverTerminalFailure() + "）");
        }
        wal2.close();
        blockWithRows(seg2.blocks(), List.of(
                "INSERT public." + table + " BEFORE=- AFTER=[id=4, name=after-resume]"));
        LOG.info("离线→在线回切完成: 段一 1 块 / 离线 2 块 / 回切 {} 块（重发面容忍）", seg2.blocks().size());
    }

    // ---- 用例 3：离线截停再离线续传 ----

    /**
     * 用例 3 {@code offlineCrashAndRelayoutRepeatsConsistently}：离线中途截停（endLsn
     * 中位上界——"库死亡后首轮只回放到中位"的确定性形态）→ 二次 startOffline 自一轮
     * 检查点续传读尽 → 尾部事务补发、两轮并集无分叉（at-least-once——同 xid 重发块
     * 逐字段全等）。
     *
     * <p>一轮截停位与首笔事务之间垫隔离带（页窗防部分重建，见类 javadoc 纪律段）；
     * 二轮窗口起点 = 一轮检查点页对齐下取整——同样落在外表段内，首轮事务不重发，
     * 尾部事务完整重放。断言面：一轮含首笔块/不含尾部块 + 检查点 ∈ (0, endLsn]；二轮
 * 续传前提（resumedFromState）+ 尾部块逐行字面 + 检查点前沿不回退；并集零分叉。</p>
     */
    @Test
    void offlineCrashAndRelayoutRepeatsConsistently() throws Exception {
        String table = "t_offline_it3";
        String ign = table + "_ign";
        String slot = registerSlot("vb_offline_it3");
        Path walDir = VastTestEnv.newStateDir("off3-wal");
        Path stateDir = VastTestEnv.newStateDir("off3-state");
        resetTables(table, ign);
        WalSegmentRecorder recorder = WalSegmentRecorder.start(walDir);
        closables.add(recorder);

        OgParityBaseline.WalCapture online = new OgParityBaseline.WalCapture();
        WalSource wal = VastTestEnv.startOnlineWithRetry(onlineConfig(slot, table, stateDir), online);
        closables.add(wal);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "INSERT INTO " + table + " VALUES (1, 'crash-seg1')");
            await(() -> wal.dmlEmittedBuckets() >= 1, "离线截停: 在线段未发射（buckets="
                    + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
            padIgnored(c, ign);
            long padEnd = VastTestEnv.writeLsn(c);
            await(() -> wal.consumedLsn() >= padEnd, "离线截停: 在线段未消费到填充段末（consumed="
                    + Lsn.format(wal.consumedLsn()) + " < " + Lsn.format(padEnd) + "）");
        }
        wal.close();

        long midLsn;
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "BEGIN");
            exec(c, "INSERT INTO " + table + " VALUES (2, 'crash-a1')");
            exec(c, "INSERT INTO " + table + " VALUES (3, 'crash-a2')");
            exec(c, "UPDATE " + table + " SET name = 'crash-a2-upd' WHERE id = 3");
            exec(c, "COMMIT");
            padIgnored(c, ign);
            midLsn = VastTestEnv.writeLsn(c);
            exec(c, "INSERT INTO " + table + " VALUES (4, 'crash-b')");
            long eof = VastTestEnv.writeLsn(c);
            await(() -> recorder.recordedFrontierLsn() >= eof, "离线截停: 段目录未覆盖终位点 "
                    + Lsn.format(eof) + "（已录 " + Lsn.format(recorder.recordedFrontierLsn()) + "）");
        }
        recorder.close();

        OgParityBaseline.WalCapture run1 = new OgParityBaseline.WalCapture();
        WalSource first = new WalSource(offlineConfig(stateDir, walDir, table, midLsn), run1);
        closables.add(first);
        first.startOffline();
        await(first::eofReached, "离线截停: 一轮未到上界 EOF（terminal=" + first.receiverTerminalFailure() + "）");
        assertTrue(first.receiverTerminalFailure().isEmpty(), "离线截停: 一轮回放不应终态失败");
        first.close();
        blockWithRows(run1.blocks(), List.of(
                "INSERT public." + table + " BEFORE=- AFTER=[id=2, name=crash-a1]",
                "INSERT public." + table + " BEFORE=- AFTER=[id=3, name=crash-a2]",
                "UPDATE public." + table + " BEFORE=[id=3, name=crash-a2]"
                        + " AFTER=[id=3, name=crash-a2-upd]"));
        assertNoBlockContaining(run1.blocks(), "crash-b");
        assertTrue(first.lastCheckpointLsn() > 0 && first.lastCheckpointLsn() <= midLsn,
                "离线截停: 一轮检查点应落在 (0, 上界] 内: " + Lsn.format(first.lastCheckpointLsn())
                        + " > " + Lsn.format(midLsn));

        OgParityBaseline.WalCapture run2 = new OgParityBaseline.WalCapture();
        WalSource second = new WalSource(offlineConfig(stateDir, walDir, table, 0L), run2);
        closables.add(second);
        second.startOffline();
        assertTrue(second.resumedFromState(), "二轮离线应自一轮检查点续传");
        await(second::eofReached, "离线截停: 二轮未读尽（terminal=" + second.receiverTerminalFailure() + "）");
        second.close();
        blockWithRows(run2.blocks(), List.of(
                "INSERT public." + table + " BEFORE=- AFTER=[id=4, name=crash-b]"));

        List<OgBlock> union = new ArrayList<>(run1.blocks());
        union.addAll(run2.blocks());
        assertNoDivergentDuplicates(union);
        assertTrue(second.lastCheckpointLsn() >= first.lastCheckpointLsn(), "离线截停: 二轮检查点前沿不应回退");
        LOG.info("离线截停再离线完成: 一轮 {} 块（截停 {}）/ 二轮 {} 块 / 并集 {} 块零分叉",
                run1.blocks().size(), Lsn.format(midLsn), run2.blocks().size(), union.size());
    }

    // ---- 用例 4：endLsn 上界截停 ----

    /**
     * 用例 4 {@code offlineEndLsnBoundsReplay}：取在线前沿的中间位点（两笔上界前/后
     * 事务之间的 write 位点——记录边界）作 endLsn → startOffline → eofReached 且
     * lastCheckpointLsn ≤ endLsn；块面断言：上界前事务完整重放（逐行字面）、上界后
     * 事务零出现（交付上界的截停语义钉）。
     */
    @Test
    void offlineEndLsnBoundsReplay() throws Exception {
        String table = "t_offline_it4";
        String ign = table + "_ign";
        String slot = registerSlot("vb_offline_it4");
        Path walDir = VastTestEnv.newStateDir("off4-wal");
        Path stateDir = VastTestEnv.newStateDir("off4-state");
        resetTables(table, ign);
        WalSegmentRecorder recorder = WalSegmentRecorder.start(walDir);
        closables.add(recorder);

        OgParityBaseline.WalCapture online = new OgParityBaseline.WalCapture();
        WalSource wal = VastTestEnv.startOnlineWithRetry(onlineConfig(slot, table, stateDir), online);
        closables.add(wal);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "INSERT INTO " + table + " VALUES (1, 'bounds-seg1')");
            await(() -> wal.dmlEmittedBuckets() >= 1, "上界截停: 在线段未发射（buckets="
                    + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
            padIgnored(c, ign);
            long padEnd = VastTestEnv.writeLsn(c);
            await(() -> wal.consumedLsn() >= padEnd, "上界截停: 在线段未消费到填充段末（consumed="
                    + Lsn.format(wal.consumedLsn()) + " < " + Lsn.format(padEnd) + "）");
        }
        wal.close();

        long midLsn;
        try (Connection c = VastTestEnv.newSqlConnection()) {
            c.setAutoCommit(true);
            exec(c, "INSERT INTO " + table + " VALUES (2, 'bounds-ta')");
            midLsn = VastTestEnv.writeLsn(c);
            exec(c, "INSERT INTO " + table + " VALUES (3, 'bounds-tb')");
            long eof = VastTestEnv.writeLsn(c);
            await(() -> recorder.recordedFrontierLsn() >= eof, "上界截停: 段目录未覆盖终位点 "
                    + Lsn.format(eof) + "（已录 " + Lsn.format(recorder.recordedFrontierLsn()) + "）");
        }
        recorder.close();

        OgParityBaseline.WalCapture offline = new OgParityBaseline.WalCapture();
        WalSource replay = new WalSource(offlineConfig(stateDir, walDir, table, midLsn), offline);
        closables.add(replay);
        replay.startOffline();
        await(replay::eofReached, "上界截停: 上界到达未置 EOF");
        assertTrue(replay.receiverTerminalFailure().isEmpty(), "上界截停: 离线回放不应终态失败");
        assertTrue(replay.consumedLsn() <= midLsn, "上界截停: 交付前沿越过上界: "
                + Lsn.format(replay.consumedLsn()) + " > " + Lsn.format(midLsn));
        blockWithRows(offline.blocks(), List.of(
                "INSERT public." + table + " BEFORE=- AFTER=[id=2, name=bounds-ta]"));
        assertNoBlockContaining(offline.blocks(), "bounds-tb");
        replay.close();
        assertTrue(replay.lastCheckpointLsn() > 0 && replay.lastCheckpointLsn() <= midLsn,
                "上界截停: 最终检查点应落在 (0, 上界]: " + Lsn.format(replay.lastCheckpointLsn())
                        + " > " + Lsn.format(midLsn));
        LOG.info("endLsn 上界截停完成: 上界 {} 恰含上界前事务块、上界后事务零出现",
                Lsn.format(midLsn));
    }

    // ---- 测试基建 ----

    /**
     * 场景前置：清场重建被测白名单主表（REPLICA IDENTITY FULL——整行前像对拍面）与
     * 白名单外隔离带外表。DDL 先于建槽/引导与 recorder/门面启动（spike 坑 2 的时序
     * 契约——引导快照须含全部场景表）。
     *
     * @param table   白名单主表名（public schema）
     * @param ignored 隔离带外表名（不在 vb.wal.tables 白名单）
     * @throws SQLException DDL 失败
     */
    private static void resetTables(String table, String ignored) throws SQLException {
        VastTestEnv.sql(
                "DROP TABLE IF EXISTS " + table,
                "DROP TABLE IF EXISTS " + ignored,
                "CREATE TABLE " + table + " (id int NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE " + table + " REPLICA IDENTITY FULL",
                "CREATE TABLE " + ignored + " (v int, pad text)");
    }

    /**
     * 在线门面配置：{@link VastTestEnv#params()} 六基础键 + OG 档 direct 通道 +
     * opengauss-jdbc 前缀（判别真库 → VastBase 档 → OG 装配分支）+ 槽/白名单钉场景表 +
     * state 三键（interval 2s 加速运行中检查点——用例 1 的快照面前置；events 1 任一
     * catalog 行事件即触发）。
     *
     * @param slot     物理槽名（vb_offline_it 前缀——真库卫生）
     * @param table    白名单主表名
     * @param stateDir 检查点目录
     * @return WalSource 构造入参配置
     */
    private static Properties onlineConfig(String slot, String table, Path stateDir) {
        Properties cfg = VastTestEnv.params();
        cfg.setProperty(WalSource.KEY_SLOT, slot);
        cfg.setProperty(WalSource.KEY_TABLES, "public." + table);
        cfg.setProperty(WalSource.KEY_STATE_DIR, stateDir.toString());
        cfg.setProperty(WalSource.KEY_STATE_INTERVAL_MS, STATE_INTERVAL_MS);
        cfg.setProperty(WalSource.KEY_STATE_EVENTS, "1");
        return cfg;
    }

    /**
     * 离线门面配置（最小面）：state.dir（入口凭证——缺失/损坏启动期 ISE）+ offline.dir
     * （段快照目录）+ 白名单钉场景表；endLsn &gt; 0 时设可选交付上界（十六进制 LSN 文本）
     * ——host/槽等在线键在离线路不被消费（构造期取默认无害）。
     *
     * @param stateDir 检查点目录（续写——与在线双向互通的载体）
     * @param walDir   段快照目录（recorder 落段）
     * @param table    白名单主表名
     * @param endLsn   交付上界（0 = 无上界，清单读尽即 EOF）
     * @return WalSource 构造入参配置
     */
    private static Properties offlineConfig(Path stateDir, Path walDir, String table, long endLsn) {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_STATE_DIR, stateDir.toString());
        cfg.setProperty(WalSource.KEY_OFFLINE_DIR, walDir.toString());
        cfg.setProperty(WalSource.KEY_TABLES, "public." + table);
        if (endLsn > 0) {
            cfg.setProperty(WalSource.KEY_OFFLINE_END_LSN, Lsn.format(endLsn));
        }
        return cfg;
    }

    /**
     * 登记物理槽（前置清残留 + AfterEach 兜底删除面记账）。
     *
     * @param slot 槽名
     * @return 同名槽（链式用）
     */
    private String registerSlot(String slot) {
        slots.add(slot);
        VastTestEnv.dropPhysicalSlotQuietly(slot);
        return slot;
    }

    /**
     * 白名单外隔离带填充：单事务 {@link #PAD_ROWS} 行（~36KB WAL）——把后续检查点落盘
     * 位与更早的白名单内事务垫开超过页对齐窗（见类 javadoc 纪律段）；外表不在白名单，
     * 行零发射。
     *
     * @param c        会话连接（自动提交——单语句单事务）
     * @param ignored  外表名
     * @throws SQLException 执行失败
     */
    private static void padIgnored(Connection c, String ignored) throws SQLException {
        exec(c, "INSERT INTO " + ignored + " SELECT g, repeat('p', 24) FROM generate_series(1, "
                + PAD_ROWS + ") g");
    }

    /**
     * 拷贝检查点快照（用例 1 的离线路入口凭证）：读 state 目录正名文件字节 → 写快照目录
     * （.part→rename 原子换名保证正名终态完整，读窗竞速由重试吸收——写侧失败被同步器
     * WARN 吞掉不损流，读侧重试即得完整副本）。
     *
     * @param fromDir 在线会话的 state 目录
     * @param toDir   快照目标目录（已清空建好）
     */
    private static void copyStateSnapshot(Path fromDir, Path toDir) {
        Path src = fromDir.resolve(StateStore.FILE_NAME);
        Path dst = toDir.resolve(StateStore.FILE_NAME);
        for (int attempt = 1; attempt <= COPY_ATTEMPTS; attempt++) {
            try {
                Files.write(dst, Files.readAllBytes(src));
                LOG.info("检查点快照拷贝完成: {} -> {}（{}B）", src, dst, Files.size(dst));
                return;
            } catch (IOException e) {
                LOG.debug("检查点快照拷贝竞速重试 {}/{}: {}", attempt, COPY_ATTEMPTS, e.getMessage());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("检查点快照拷贝被中断", ie);
                }
            }
        }
        throw new IllegalStateException("检查点快照拷贝失败（" + COPY_ATTEMPTS + " 次竞速重试后）: " + src);
    }

    /**
     * 断言块列表含行序列恰等于预期的块（逐行字面断言的定位器——未命中即 fail 附期望
     * 与实得全量）。
     *
     * @param blocks       块列表
     * @param expectedRows 期望行文本序列
     */
    private static void blockWithRows(List<OgBlock> blocks, List<String> expectedRows) {
        blocks.stream().filter(b -> b.rows().equals(expectedRows)).findFirst()
                .orElseThrow(() -> new AssertionError("未找到行序列恰等于预期的块:\n期望 " + expectedRows
                        + "\n实得:\n" + blocks));
    }

    /**
     * 断言全部块的行文本不含标记子串（上界/截停后数据零出现的负向钉）。
     *
     * @param blocks 块列表
     * @param marker 禁止出现的标记（场景独有行值字面）
     */
    private static void assertNoBlockContaining(List<OgBlock> blocks, String marker) {
        assertTrue(blocks.stream().flatMap(b -> b.rows().stream()).noneMatch(r -> r.contains(marker)),
                "不应出现含 '" + marker + "' 的行:\n" + blocks);
    }

    /**
     * 断言同 xid 重复块（at-least-once 重发面）逐字段全等——跨轮次并集的一致性钉
     * （{@code OgParityBaseline.byXid} 容忍面的本地形态，交集无关场景用）。
     *
     * @param blocks 并集块列表（出现序）
     */
    private static void assertNoDivergentDuplicates(List<OgBlock> blocks) {
        Map<Long, OgBlock> byXid = new LinkedHashMap<>();
        for (OgBlock b : blocks) {
            OgBlock seen = byXid.putIfAbsent(b.xid(), b);
            if (seen != null) {
                assertEquals(seen, b, "同 xid 重发块须逐字段全等（at-least-once 一致性）: xid=" + b.xid());
            }
        }
    }

    /**
     * 轮询等待条件成立（{@link #POLL_MS} 节拍、{@link #AWAIT_MS} 上限），超时 fail。
     *
     * @param cond 条件（幂等读）
     * @param what 等待对象描述（超时消息上下文）
     * @throws InterruptedException 睡眠被中断
     */
    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MS;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + what);
            }
            Thread.sleep(POLL_MS);
        }
    }

    /**
     * 在连接上顺序执行语句（各自独立执行——事务边界由调用方的显式 BEGIN/COMMIT 或
     * 自动提交控制）。
     *
     * @param conn 连接
     * @param sqls 语句
     * @throws SQLException 任一语句失败
     */
    private static void exec(Connection conn, String... sqls) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : sqls) {
                st.execute(sql);
            }
        }
    }

    /**
     * 兜底清理：停全部门面/记录器（幂等）→ 释放挂起 2PC → 删全部槽 → DROP 全部场景表
     * （正常路径测试内已完成，这里是异常路径保险；次序即依赖次序——持槽者先退场）。
     */
    @AfterEach
    void cleanupOfflineIt() {
        for (AutoCloseable c : closables) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.warn("兜底关闭失败（忽略）: {}", e.getMessage());
            }
        }
        for (String gid : gids) {
            try (Connection c = VastTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("ROLLBACK PREPARED '" + gid + "'");
            } catch (SQLException e) {
                LOG.debug("2PC gid {} 无挂起（正常路径已消费）: {}", gid, e.getMessage());
            }
        }
        for (String slot : slots) {
            VastTestEnv.dropPhysicalSlotQuietly(slot);
        }
        try {
            VastTestEnv.sql(
                    "DROP TABLE IF EXISTS t_offline_it1", "DROP TABLE IF EXISTS t_offline_it1_ign",
                    "DROP TABLE IF EXISTS t_offline_it2", "DROP TABLE IF EXISTS t_offline_it2_ign",
                    "DROP TABLE IF EXISTS t_offline_it3", "DROP TABLE IF EXISTS t_offline_it3_ign",
                    "DROP TABLE IF EXISTS t_offline_it4", "DROP TABLE IF EXISTS t_offline_it4_ign");
        } catch (SQLException e) {
            LOG.warn("清理离线 IT 场景表失败（忽略）: {}", e.getMessage());
        }
    }
}
