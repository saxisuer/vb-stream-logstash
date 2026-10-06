package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TableFilter}（表过滤 + TableMeta 组装）的失败先行测试——四断言面：
 * ①relkind 过滤（仅 'r' 普通表与 'p' 分区表放行——toast 't'/索引 'i'/视图 'v'/
 * 序列 'S' 一律 miss）；②白名单（空集 = 全放行用户表；非空 = 恰 schema.table
 * 命中才放行）；③系统模式排除（pg_catalog 下的 'r' 表也不进变更流——watched 目录
 * 自身在此面）；④resolve 双入口（relfilenode 反查与 oid 直查解析到同一张表）+
 * TableMeta 组装（columnsOf 有序含 dropped 占位直供）。测试自持一个
 * {@link CatalogSnapshot} 假实现（内存 map），不依赖重放链。
 */
class TableFilterTest {

    /**
     * 内存假快照：class 行（oid → relfilenode/relname/relnamespace/relkind）+
     * nsp 行（nspOid → nspname）+ attr 行（relOid → 列表）——按 {@link CatalogSnapshot}
     * 契约提供全查询面（未知关系 columnsOf 空表 / 标量查询 empty）。
     */
    private static final class FakeSnapshot implements CatalogSnapshot {

        private final Map<Long, Long> relfilenodes = new HashMap<>();
        private final Map<Long, String> relnames = new HashMap<>();
        private final Map<Long, Long> relnamespaces = new HashMap<>();
        private final Map<Long, String> relkinds = new HashMap<>();
        private final Map<Long, String> nspnames = new HashMap<>();
        private final Map<Long, List<Column>> columns = new LinkedHashMap<>();

        /**
         * 登记一张 pg_class 行投影 + 其模式解析所需的 pg_namespace 行。
         *
         * @param oid      关系 oid
         * @param filenode relfilenode（resolve 反查入口）
         * @param name     relname
         * @param nspOid   relnamespace（模式 oid）
         * @param nspname  模式名（pg_namespace 解析）
         * @param relkind  relkind 单字符
         */
        void relation(long oid, long filenode, String name, long nspOid, String nspname, String relkind) {
            relfilenodes.put(oid, filenode);
            relnames.put(oid, name);
            relnamespaces.put(oid, nspOid);
            relkinds.put(oid, relkind);
            nspnames.put(nspOid, nspname);
        }

        /**
         * 登记一列（attnum 升序由调用方保证，columnsOf 排序面照契约执行）。
         *
         * @param oid 关系 oid
         * @param col 列投影
         */
        void column(long oid, Column col) {
            columns.computeIfAbsent(oid, k -> new ArrayList<>()).add(col);
        }

        /** {@inheritDoc}——定值前沿（本测试不消费时序面）。 */
        @Override
        public long lsn() {
            return 0x1000L;
        }

        /** {@inheritDoc}——登记列按 attnum 升序拷贝返回；未登记空表。 */
        @Override
        public List<Column> columnsOf(long relOid) {
            List<Column> cols = new ArrayList<>(columns.getOrDefault(relOid, List.of()));
            cols.sort((a, b) -> Integer.compare(a.attnum(), b.attnum()));
            return cols;
        }

        /** {@inheritDoc}——登记值直供；未登记 empty。 */
        @Override
        public OptionalLong relfilenodeOf(long relOid) {
            Long v = relfilenodes.get(relOid);
            return v == null ? OptionalLong.empty() : OptionalLong.of(v);
        }

        /** {@inheritDoc}——本测试不消费 toast 面，恒 0 值（已登记关系）。 */
        @Override
        public OptionalLong toastOf(long relOid) {
            return relfilenodes.containsKey(relOid) ? OptionalLong.of(0L) : OptionalLong.empty();
        }

        /** {@inheritDoc}——relnamespace 经 nspnames 解析。 */
        @Override
        public Optional<String> schemaOf(long relOid) {
            Long nsp = relnamespaces.get(relOid);
            return nsp == null ? Optional.empty() : Optional.ofNullable(nspnames.get(nsp));
        }

        /** {@inheritDoc}——登记 relkind 直供。 */
        @Override
        public Optional<String> relkindOf(long relOid) {
            return Optional.ofNullable(relkinds.get(relOid));
        }

        /** {@inheritDoc}——登记 relname 直供。 */
        @Override
        public Optional<String> nameOf(long relOid) {
            return Optional.ofNullable(relnames.get(relOid));
        }

        /** {@inheritDoc}——先按 relfilenode 反查、miss 再按 oid 直认。 */
        @Override
        public OptionalLong relOidOf(long relNodeOrOid) {
            for (Map.Entry<Long, Long> e : relfilenodes.entrySet()) {
                if (e.getValue() == relNodeOrOid) {
                    return OptionalLong.of(e.getKey());
                }
            }
            return relfilenodes.containsKey(relNodeOrOid) ? OptionalLong.of(relNodeOrOid) : OptionalLong.empty();
        }
    }

    /**
     * 造一套覆盖测试面的假快照：public 下 t1（'r'，含 dropped 占位列）/t2（'r'）/
     * t_part（'p'），public 下 t_idx（'i'）/t_toast（'t'）/t_view（'v'）/t_seq（'S'），
     * app 下 t1（'r'，同名异模式——白名单 schema 限定面），pg_catalog 下 pg_type
     * （'r'——系统模式排除面）。
     *
     * @return 已登记的假快照
     */
    private static FakeSnapshot snapshot() {
        FakeSnapshot s = new FakeSnapshot();
        s.relation(1001, 20001, "t1", 11, "public", "r");
        s.relation(1002, 20002, "t2", 11, "public", "r");
        s.relation(1003, 20003, "t_part", 11, "public", "p");
        s.relation(1004, 20004, "t_idx", 11, "public", "i");
        s.relation(1005, 20005, "t_toast", 11, "public", "t");
        s.relation(1006, 20006, "t_view", 11, "public", "v");
        s.relation(1007, 20007, "t_seq", 11, "public", "S");
        s.relation(1008, 20008, "t1", 12, "app", "r");
        s.relation(1009, 20009, "pg_type", 13, "pg_catalog", "r");
        s.column(1001, new CatalogSnapshot.Column(1, "id", 23, false));
        s.column(1001, new CatalogSnapshot.Column(2, "........pg.dropped.2........", 25, true));
        s.column(1001, new CatalogSnapshot.Column(3, "v", 25, false));
        return s;
    }

    /**
     * 用例 ①：relkind 过滤——'r' 与 'p'（分区表）命中；'i'/'t'/'v'/'S' 一律
     * miss（toast/索引/视图/序列不进变更流）。
     */
    @Test
    void relkindFilterAdmitsOnlyOrdinaryAndPartitionedTables() {
        TableFilter filter = new TableFilter(snapshot(), Set.of());
        assertTrue(filter.resolve(1001).isPresent(), "普通表 'r' 应放行");
        assertTrue(filter.resolve(1003).isPresent(), "分区表 'p' 应放行");
        assertTrue(filter.resolve(1004).isEmpty(), "索引 'i' 应 miss");
        assertTrue(filter.resolve(1005).isEmpty(), "toast 't' 应 miss");
        assertTrue(filter.resolve(1006).isEmpty(), "视图 'v' 应 miss");
        assertTrue(filter.resolve(1007).isEmpty(), "序列 'S' 应 miss");
    }

    /**
     * 用例 ②：白名单空集 = 用户表全放行（过滤只剩 relkind + 系统模式两道闸）。
     */
    @Test
    void emptyWhitelistAdmitsAllUserTables() {
        TableFilter filter = new TableFilter(snapshot(), Set.of());
        assertTrue(filter.resolve(1001).isPresent());
        assertTrue(filter.resolve(1002).isPresent());
        assertTrue(filter.resolve(1008).isPresent(), "app 模式的用户表在空白名单下同样放行");
    }

    /**
     * 用例 ③：白名单非空 = 恰 schema.table 命中才放行——public.t1 命中而 public.t2
     * miss；同名表 app.t1 miss（白名单匹配带 schema 限定，不裸按表名）。
     */
    @Test
    void whitelistMatchesSchemaQualifiedNamesOnly() {
        TableFilter filter = new TableFilter(snapshot(), Set.of("public.t1"));
        Optional<TableMeta> hit = filter.resolve(1001);
        assertTrue(hit.isPresent(), "白名单命中 public.t1 应放行");
        assertEquals("public", hit.orElseThrow().schema());
        assertEquals("t1", hit.orElseThrow().table());
        assertTrue(filter.resolve(1002).isEmpty(), "白名单外的 public.t2 应 miss");
        assertTrue(filter.resolve(1008).isEmpty(), "同名异模式 app.t1 不命中 public.t1 白名单项");
    }

    /**
     * 用例 ④：系统模式排除——pg_catalog 下的 'r' 表（系统目录，watched 三表自身
     * 所在模式）即便 relkind 放行也不进变更流。
     */
    @Test
    void systemSchemaRelationsAreExcludedEvenOnOrdinaryRelkind() {
        TableFilter filter = new TableFilter(snapshot(), Set.of());
        assertTrue(filter.resolve(1009).isEmpty(), "pg_catalog 下的 'r' 表应被系统模式排除");
    }

    /**
     * 用例 ⑤：miss 面——未知 oid / 未登记 relfilenode 双入口皆 empty。
     */
    @Test
    void unknownRelationMissesBothByOidAndRelfilenode() {
        TableFilter filter = new TableFilter(snapshot(), Set.of());
        assertTrue(filter.resolve(9999).isEmpty(), "未知 oid 应 miss");
        assertTrue(filter.resolve(99999).isEmpty(), "未知 relfilenode 应 miss");
    }

    /**
     * 用例 ⑥：resolve 双入口——同一张表按 relfilenode（heap 块反查形态）与按 oid
     * （元数据直查形态）解析出全等的 TableMeta。
     */
    @Test
    void resolveAcceptsBothRelfilenodeAndOidEntries() {
        TableFilter filter = new TableFilter(snapshot(), Set.of());
        TableMeta byRelfilenode = filter.resolve(20001).orElseThrow();
        TableMeta byOid = filter.resolve(1001).orElseThrow();
        assertEquals(byOid, byRelfilenode, "relfilenode 与 oid 双入口应解析到同一 TableMeta");
        assertEquals(1001L, byOid.relOid());
    }

    /**
     * 用例 ⑦：TableMeta 组装——columns 直供 {@code columnsOf} 的有序含 dropped
     * 占位投影（attnum 1,2,3 序、dropped 位保真），供 Task 5 值面按列对位。
     */
    @Test
    void tableMetaCarriesOrderedColumnsWithDroppedPlaceholder() {
        TableFilter filter = new TableFilter(snapshot(), Set.of());
        TableMeta meta = filter.resolve(1001).orElseThrow();
        List<ColumnMeta> cols = meta.columns();
        assertEquals(3, cols.size());
        assertEquals(new ColumnMeta(1, "id", 23, false), cols.get(0));
        assertEquals(new ColumnMeta(2, "........pg.dropped.2........", 25, true), cols.get(1),
                "dropped 占位列须保留在其 attnum 键位");
        assertEquals(new ColumnMeta(3, "v", 25, false), cols.get(2));
    }
}
