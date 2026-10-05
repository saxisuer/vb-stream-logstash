package org.vastdata.vbstream.walsource.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.replay.CatalogRow.AttrRow;
import org.vastdata.vbstream.walsource.replay.CatalogRow.ClassRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * StateStore 原子持久化（spec §7）的失败先行测试——VBWS 检查点格式的六断言面：
 * ①roundtrip 全等（attr/class 两行字典 + 双 tail 存储 + tracked 双 ctid + 两
 * relfilenode + interest/stale 全字段，并经 restoreInto 灌回新 stores 逐面复核）；
 * ②正文中翻一位字节 → 全文件 CRC 拒载 empty；③header formatVersion 改写 → 拒载
 * empty；④只剩 {@code .part} 半成品 → 不 load 且 {@code exists()} 为 false；
 * ⑤连写两次检查点 → 旧检查点被原子替换（load 到第二次内容、无 .part 残留）；
 * ⑥class 表最小行宽形态（relname 空串 ×50）→ 计数防线不误拒（审查修复钉）；
 * ⑦pgVersion 参数化（终审 I1）——17 写 17 / 18 写 18，跨版本 load 错配拒载 empty。
 *
 * <p>byte[] tail 值不参与 record equals（数组恒一性），断言经
 * {@link #assertTailsEqual} 手工逐键比较；其余字段用 record/集合 equals。</p>
 */
class StateStoreTest {

    /** 检查点正名（与 StateStore 契约同锚——文件名属格式契约的一部分）。 */
    private static final String FILE_NAME = "wal-source-state.bin";

    /** 测试用检查点 LSN（打包位，任意合法值）。 */
    private static final long LSN = 0x0000000100000028L;

    @TempDir
    Path dir;

    /**
     * 用例 ①：灌满全字段 stores（两行字典各含 dropped 占位行与 toast 行、双 tail、
     * tracked 双 ctid、两 relfilenode、interest/stale 各一）→ checkpoint → load →
     * StoredState 逐面全等；再 restoreInto 新 CatalogStores 复核四 map/tracked/
     * relfilenode/两集合全回填（Task 15 续传路径的灌回契约）。
     */
    @Test
    void roundtripPreservesEntireStateAndRestoresIntoFreshStores() throws IOException {
        StateStore store = new StateStore(dir, 18);
        CatalogStores stores = filledStores();
        store.checkpoint(stores, LSN);
        assertTrue(store.exists(), "checkpoint 后正式检查点应存在");

        Optional<StoredState> loaded = store.load();
        assertTrue(loaded.isPresent(), "合法检查点应可 load");
        StoredState s = loaded.orElseThrow();
        assertEquals(LSN, s.lsn());
        assertEquals(stores.attrRows(), s.attrs(), "attr 行字典应 roundtrip 全等");
        assertEquals(stores.classRows(), s.classes(), "class 行字典应 roundtrip 全等");
        assertTailsEqual(stores.rawAttrTails(), s.rawAttrTails());
        assertTailsEqual(stores.rawClassTails(), s.rawClassTails());
        assertEquals(stores.trackedTableCtid(), s.trackedTableCtid(), "tracked 表 ctid 应全等");
        assertEquals(stores.trackedToastCtid(), s.trackedToastCtid(), "tracked toast ctid 应全等");
        assertEquals(stores.pgAttrRelfilenode(), s.pgAttrRelfilenode());
        assertEquals(stores.pgClassRelfilenode(), s.pgClassRelfilenode());
        assertEquals(stores.interestRelOids(), s.interestRelOids());
        assertEquals(stores.staleOids(), s.staleOids());

        CatalogStores restored = new CatalogStores();
        s.restoreInto(restored);
        assertEquals(stores.attrRows(), restored.attrRows(), "restoreInto 应回填 attr 行字典");
        assertEquals(stores.classRows(), restored.classRows(), "restoreInto 应回填 class 行字典");
        assertTailsEqual(stores.rawAttrTails(), restored.rawAttrTails());
        assertTailsEqual(stores.rawClassTails(), restored.rawClassTails());
        assertEquals(stores.trackedTableCtid(), restored.trackedTableCtid());
        assertEquals(stores.trackedToastCtid(), restored.trackedToastCtid());
        assertEquals(stores.pgAttrRelfilenode(), restored.pgAttrRelfilenode());
        assertEquals(stores.pgClassRelfilenode(), restored.pgClassRelfilenode());
        assertEquals(stores.interestRelOids(), restored.interestRelOids());
        assertEquals(stores.staleOids(), restored.staleOids());
    }

    /**
     * 用例 ②：合法检查点写就后翻正文中一位字节（文件中段、CRC 覆盖域内）→ load
     * 必须 empty（全文件 CRC32 拒载）且 ERROR 留痕由人工核对——拒载后回落重引导由
     * caller 决定，本断言只钉死"损坏即不载"。
     */
    @Test
    void singleFlippedByteRejectsLoad() throws IOException {
        StateStore store = new StateStore(dir, 18);
        store.checkpoint(filledStores(), LSN);
        Path file = dir.resolve(FILE_NAME);
        byte[] bytes = Files.readAllBytes(file);
        int flipAt = bytes.length / 2;
        bytes[flipAt] ^= 0x01;
        Files.write(file, bytes);
        assertTrue(store.load().isEmpty(), "正文翻一位字节后应拒载 empty");
    }

    /**
     * 用例 ③：合法检查点写就后改写 header 的 formatVersion 字段（偏移 4，u16）→
     * load 必须 empty（版本拒载——即便 CRC 同时不符也不影响 empty 语义，但本断言
     * 独立钉死"版本不符不回退兼容解读"）。
     */
    @Test
    void formatVersionMismatchRejectsLoad() throws IOException {
        StateStore store = new StateStore(dir, 18);
        store.checkpoint(filledStores(), LSN);
        Path file = dir.resolve(FILE_NAME);
        byte[] bytes = Files.readAllBytes(file);
        bytes[4] = (byte) 0xFF;
        bytes[5] = (byte) 0xFF;
        Files.write(file, bytes);
        assertTrue(store.load().isEmpty(), "formatVersion 不符应拒载 empty");
    }

    /**
     * 用例 ④：目录里只留 {@code .part} 半成品（拷贝合法内容也不行——半成品无原子
     * 替换完成语义）→ load 必须 empty 且 {@code exists()} 为 false。
     */
    @Test
    void leftoverPartFileIsNeverLoaded() throws IOException {
        StateStore store = new StateStore(dir, 18);
        store.checkpoint(filledStores(), LSN);
        Path file = dir.resolve(FILE_NAME);
        Files.move(file, dir.resolve(FILE_NAME + ".part"));
        assertTrue(store.load().isEmpty(), ".part 残留不应被 load");
        assertFalse(store.exists(), ".part 残留不算已存在检查点");
    }

    /**
     * 用例 ⑤：连写两次检查点（第二次内容不同——lsn 前进 + 行字典变化）→ load 必须
     * 得到第二次内容（旧检查点被原子替换而非追加/并存），且目录内无 {@code .part}
     * 残留（换名完成即清理暂存）。
     */
    @Test
    void repeatedCheckpointAtomicallyReplacesPreviousState() throws IOException {
        StateStore store = new StateStore(dir, 18);
        CatalogStores first = filledStores();
        store.checkpoint(first, LSN);

        long nextLsn = LSN + 0x30;
        CatalogStores second = filledStores();
        second.attrRows().remove(2L);
        second.trackedTableCtid(11L);
        store.checkpoint(second, nextLsn);

        Optional<StoredState> loaded = store.load();
        assertTrue(loaded.isPresent());
        StoredState s = loaded.orElseThrow();
        assertEquals(nextLsn, s.lsn(), "应载到第二次检查点的 lsn");
        assertEquals(second.attrRows(), s.attrs(), "行字典应为第二次内容");
        assertEquals(11L, s.trackedTableCtid(), "tracked 应为第二次值");
        assertFalse(Files.exists(dir.resolve(FILE_NAME + ".part")), "完成后不应残留 .part");
    }

    /**
     * 用例 ⑥（审查修复钉）：class 表最小行宽形态——relname 全空串的 class 行 50 条
     * （每行恰最小宽 74B：ctid 8 + 8 个落盘 long 64 + UTF 空串前缀 2）且文件其余段
     * 保持最小 → 文件实长 74n+86 &lt; 83n。防线常量若过严（如 83）会把"计数×最小
     * 行宽 &gt; 文件实长"误判损坏而拒载合法检查点——本用例钉死 74 不误拒，roundtrip
     * 全等可 load。
     */
    @Test
    void minimalWidthClassRowsRoundtripWithoutOverStrictGuard() throws IOException {
        StateStore store = new StateStore(dir, 18);
        CatalogStores stores = new CatalogStores();
        for (int i = 0; i < 50; i++) {
            stores.classRows().put(100L + i, new ClassRow(20000 + i, "", 0, 0, 0, 0, 0, 30000 + i, 0));
        }
        store.checkpoint(stores, LSN);

        Optional<StoredState> loaded = store.load();
        assertTrue(loaded.isPresent(), "最小行宽形态的合法检查点不应被计数防线误拒");
        assertEquals(stores.classRows(), loaded.orElseThrow().classes(), "50 条空 relname 行应 roundtrip 全等");
    }

    /**
     * 用例 ⑦（终审 I1）：pgVersion 与构造入参同源——18 存储器写出/读回自洽、17 存储
     * 器同目录同形态亦自洽（17 写 17、18 写 18），而 17 写出的检查点用 18 存储器 load
     * 必须拒载 empty（错配拒载——caller 回落全新引导），反之亦然。修复前 pgVersion
     * 恒写 18：17 侧写出的检查点 header 记 18，版本切换后错配文件无法被校验拒绝。
     */
    @Test
    void pgVersionIsParameterizedAndMismatchedVersionRejectsLoad() throws IOException {
        new StateStore(dir, 18).checkpoint(filledStores(), LSN);
        assertTrue(new StateStore(dir, 18).load().isPresent(), "同版本（18）load 应自洽");
        assertTrue(new StateStore(dir, 17).load().isEmpty(), "18 写出的检查点用 17 存储器 load 应拒载 empty");

        new StateStore(dir, 17).checkpoint(filledStores(), LSN);
        assertTrue(new StateStore(dir, 17).load().isPresent(), "同版本（17）load 应自洽——17 写 17");
        assertTrue(new StateStore(dir, 18).load().isEmpty(), "17 写出的检查点用 18 存储器 load 应拒载 empty");
    }

    /**
     * 构造全字段灌满的 stores（确定性数据）：attr 三行（含 dropped 占位行）、class
     * 两行（用户表 + toast 关系行）、双 tail 各一条、tracked 双 ctid、两
     * relfilenode、interest/stale 各一 oid——覆盖持久化面的全部字段形态。
     *
     * @return 灌满的 stores（ctid 键为任意合法 long，持久化面键值不透明）
     */
    private CatalogStores filledStores() {
        CatalogStores stores = new CatalogStores();
        stores.attrRows().put(1L, new AttrRow(16384, "id", 20, 1, false));
        stores.attrRows().put(2L, new AttrRow(16384, "........pg.dropped.2........", 0, 2, true));
        stores.attrRows().put(3L, new AttrRow(16385, "payload", 25, 1, false));
        stores.classRows().put(10L, new ClassRow(16384, "t_stream", 2200, 16386, 0, 10, 2, 16400, 16401));
        stores.classRows().put(11L, new ClassRow(16401, "pg_toast_16400", 99, 16402, 0, 10, 2, 16405, 0));
        stores.rawAttrTails().put(1L, new byte[] {1, 2, 3, 4});
        stores.rawClassTails().put(10L, new byte[] {9, 8, 7});
        stores.trackedTableCtid(10);
        stores.trackedToastCtid(11);
        stores.pgAttrRelfilenode(6001);
        stores.pgClassRelfilenode(6002);
        stores.interestRelOids().add(16384L);
        stores.staleOids().add(9999L);
        return stores;
    }

    /**
     * tail 存储逐键比较（byte[] 值不参与 Map.equals——数组恒一性，须
     * {@link Arrays#equals} 逐值核对）。
     *
     * @param expected 期望 tail 存储
     * @param actual   实际 tail 存储
     */
    private static void assertTailsEqual(Map<Long, byte[]> expected, Map<Long, byte[]> actual) {
        assertEquals(expected.keySet(), actual.keySet(), "tail 键集应全等");
        for (Map.Entry<Long, byte[]> e : expected.entrySet()) {
            assertArrayEquals(e.getValue(), actual.get(e.getKey()), "tail 值应逐字节全等: ctid=" + e.getKey());
        }
    }
}
