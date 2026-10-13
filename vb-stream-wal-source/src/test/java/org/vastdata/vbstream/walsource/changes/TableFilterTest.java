package org.vastdata.vbstream.walsource.changes;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;

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
 *
 * <p><b>OG 档分区判别面（openGauss M2 T5，用例 ⑧-⑪）</b>：OG 布局（三参构造器）
 * 下 relkind 收窄为仅 'r'（OG 无 'p' 值——分区语义由 pg_class.parttype 承担，
 * og-plan 实测值域 {'n','p'}）、parttype 闸 'n'/'p' 放行 + 未知值 miss + 不可判
 * fail-open；白名单物理表匹配锚（分区 relfilenode 不在 v1 三表字典——fail-safe
 * miss）；V17/V18 档与两参兼容构造器忽略 parttype 闸（零变化锚）。</p>
 *
 * <p><b>OG 档分区身份链面（openGauss M3 T2，用例 ⑫-⑮）</b>：三表解析 miss 后经
 * 第四字典（{@code partitionOf}）反查分区行 → parentid 母表链 → TableMeta 身份 =
 * <b>母表</b>（锚② {@code PARTITION_NAME=mother} 语义重排——基准形态）；白名单
 * <b>双匹配</b>四形态（母表名命中=基准 / 分区名命中=兼容增强 / 双命中 / 都不中）；
 * 链断 fail-safe（WARN 一次 + partitionChainMisses 计数）；vanilla 档无视分区链
 * （零变化锚）。端到端（组装器 + TOAST + 缓存失效）在 OgPartitionIdentityTest。</p>
 */
class TableFilterTest {

    /** 链断 WARN 断言面：TableFilter logger 的捕获器（用例 ⑭ 挂/摘）。 */
    private Logger filterLog;

    private ListAppender<ILoggingEvent> appender;

    /**
     * 挂 ListAppender 捕获 {@link TableFilter} 日志（链断 WARN"恰一次"断言面）。
     */
    @BeforeEach
    void setUp() {
        filterLog = (Logger) LoggerFactory.getLogger(TableFilter.class);
        appender = new ListAppender<>();
        appender.start();
        filterLog.addAppender(appender);
        filterLog.setLevel(Level.DEBUG);
    }

    /**
     * 摘除 appender，防泄漏到后续测试类。
     */
    @AfterEach
    void tearDown() {
        filterLog.detachAppender(appender);
    }

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
        private final Map<Long, String> parttypes = new HashMap<>();
        private final Map<Long, String> nspnames = new HashMap<>();
        private final Map<Long, List<Column>> columns = new LinkedHashMap<>();
        private final Map<Long, CatalogSnapshot.PartitionIdentity> partitions = new HashMap<>();

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
         * 登记 pg_class.parttype 值（OG 档分区判别列）——未登记的关系 parttypeOf 返回
         * empty（"不可判"面：V17/V18 无该列、OG 生产快照未供数时的形态）。
         *
         * @param oid      关系 oid
         * @param parttype parttype 单字符（'n' 普通 / 'p' 分区母表）
         */
        void parttype(long oid, String parttype) {
            parttypes.put(oid, parttype);
        }

        /**
         * 登记一条第四字典分区行（pg_partition 'p' 行的最小投影——M3 T2 分区身份
         * 链的输入面）。
         *
         * @param relfilenode 分区物理段号（heap 记录路由锚）
         * @param relname     分区名
         * @param parentid    母表 pg_class oid
         */
        void partition(long relfilenode, String relname, long parentid) {
            partitions.put(relfilenode, new CatalogSnapshot.PartitionIdentity(relname, parentid));
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

        /** {@inheritDoc}——登记 parttype 直供；未登记 empty（不可判面）。 */
        @Override
        public Optional<String> parttypeOf(long relOid) {
            return Optional.ofNullable(parttypes.get(relOid));
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

        /** {@inheritDoc}——登记分区行直供；未登记 empty（第四字典无行面）。 */
        @Override
        public Optional<PartitionIdentity> partitionOf(long relfilenode) {
            return Optional.ofNullable(partitions.get(relfilenode));
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

    /**
     * 用例 ⑰（vastbase T6）：DML 面列类型重映射——VastBase 档布局下 oradate 列
     * （atttypid 9040，A 档 DDL 关键字 {@code date} 的落点）组装为 timestamp 1114
     * （{@code WalLayout#remapDmlTypeOid} 接缝——磁盘表示同构 8B epoch-2000 微秒，
     * 真库 vastkind 探针实证）；vanilla 档布局对同列恒持原 oid 9040（方言 oid 空间
     * 不越权认领——后续 {@code DecodeKinds} ISE 即矩阵外面）。
     */
    @Test
    void vastbaseLayoutRemapsOracleDateColumnToTimestampForDmlFace() {
        FakeSnapshot s = new FakeSnapshot();
        s.relation(3001, 40001, "t_oradate", 11, "public", "r");
        s.column(3001, new CatalogSnapshot.Column(1, "id", 23, false));
        s.column(3001, new CatalogSnapshot.Column(2, "d", 9040, false));
        TableMeta vbMeta = new TableFilter(s, Set.of(), WalLayoutVVastbase.INSTANCE)
                .resolve(40001).orElseThrow();
        assertEquals(new ColumnMeta(2, "d", 1114, false), vbMeta.columns().get(1),
                "VastBase 档 oradate(9040) 应在 DML 面组装为 timestamp(1114)");
        TableMeta vanillaMeta = new TableFilter(s, Set.of(), WalLayoutV18.INSTANCE)
                .resolve(40001).orElseThrow();
        assertEquals(new ColumnMeta(2, "d", 9040, false), vanillaMeta.columns().get(1),
                "vanilla 档对 9040 无重映射（恒等）");
        TableMeta noLayout = new TableFilter(s, Set.of()).resolve(40001).orElseThrow();
        assertEquals(new ColumnMeta(2, "d", 9040, false), noLayout.columns().get(1),
                "两参构造器（null layout）恒等");
    }

    /**
     * 造一套覆盖 OG 档分区判别面的假快照（openGauss 5.0.0 实测形态锚，og-plan
     * m2pg[PG 档] 2026-10-12）：母表 relkind='r' + parttype='p'（分区母表行在
     * pg_class、不承载行）；普通表 'r'+'n'；vanilla 形态 'p' relkind 行（OG 无此值
     * ——分区语义走 parttype，须 miss）；未转录 parttype 值 'x'（防御 miss）；未登记
     * parttype 的表（不可判 fail-open 放行）。
     *
     * @return 已登记的 OG 档假快照
     */
    private static FakeSnapshot ogSnapshot() {
        FakeSnapshot s = new FakeSnapshot();
        s.relation(2001, 30001, "t_mom", 21, "public", "r");
        s.parttype(2001, "p");
        s.relation(2002, 30002, "t_plain", 21, "public", "r");
        s.parttype(2002, "n");
        s.relation(2003, 30003, "t_vanilla_p", 21, "public", "p");
        s.parttype(2003, "n");
        s.relation(2004, 30004, "t_unknown_parttype", 21, "public", "r");
        s.parttype(2004, "x");
        s.relation(2005, 30005, "t_unjudged", 21, "public", "r");
        s.column(2001, new CatalogSnapshot.Column(1, "id", 23, false));
        s.column(2002, new CatalogSnapshot.Column(1, "id", 23, false));
        return s;
    }

    /**
     * 用例 ⑧（OG 档）：parttype 判别——'n'（普通表）与 'p'（分区母表，按过滤接口
     * parttype∈{'n','p'} 放行——路由实测母表不承载行，放行无行流副作用）放行；
     * vanilla 形态 relkind 'p' miss（OG 无该 relkind 值，分区语义由 parttype 承担）；
     * 未转录 parttype 值 'x' miss（防御面——真实库 pg_class.parttype 值域实测仅
     * {'n','p'}）；parttypeOf 不可判（empty）fail-open 放行（生产快照未供数/续传
     * 半损面，方向与 V17/V18 无该闸的行为面一致）。
     */
    @Test
    void ogDialectParttypeGateAdmitsNormalAndMotherAndSkipsUnknown() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        TableFilter filter = new TableFilter(ogSnapshot(), Set.of(), og);
        assertTrue(filter.resolve(30002).isPresent(), "OG 普通表 'r'+parttype 'n' 应放行");
        assertTrue(filter.resolve(30001).isPresent(),
                "OG 分区母表 'r'+parttype 'p' 应放行（接口语义 parttype∈{'n','p'}；母表不承载行故无行流副作用）");
        assertTrue(filter.resolve(30003).isEmpty(), "OG 无 relkind 'p' 值——vanilla 形态行应 miss（分区语义走 parttype）");
        assertTrue(filter.resolve(30004).isEmpty(), "未转录 parttype 值 'x' 应 miss（防御面）");
        assertTrue(filter.resolve(30005).isPresent(), "parttypeOf 不可判（empty）应 fail-open 放行");
    }

    /**
     * 用例 ⑨（OG 档白名单物理表匹配）：白名单语义仍 schema.table 全名限定；**分区
     * relfilenode 不在 v1 三表字典**（og-plan m2pg 实测：分区仅存在于 pg_partition、
     * 无 pg_class/pg_attribute 行，heap 记录全部路由到分区 relfilenode——range
     * p1/p2 + list pa/pb 四锚）——resolve 对未登记的分区 relfilenode 天然 miss
     * （fail-safe 跳过锚：白名单"分区子表独立匹配"的完整落地需 pg_partition 字典，
     * M3/后续记档），白名单写母表名只命中母表身份（母表不承载行）。
     */
    @Test
    void ogDialectWhitelistMatchesMotherNameButPartitionRelfilenodeMissesDictionary() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        TableFilter filter = new TableFilter(ogSnapshot(), Set.of("public.t_mom"), og);
        assertTrue(filter.resolve(30001).isPresent(), "白名单命中母表名应解析出母表身份");
        assertTrue(filter.resolve(30002).isEmpty(), "白名单外的普通表应 miss");
        assertTrue(filter.resolve(39001).isEmpty(),
                "未登记的分区 relfilenode（不在 pg_class 字典）应 fail-safe miss——OG 分区仅在 pg_partition");
    }

    /**
     * 用例 ⑩（V17/V18 零变化锚）：同一快照在 vanilla 档（V18 布局）完全忽略
     * parttype 闸——'x' parttype 的 'r' 表照常放行；relkind 'p' 分区表放行语义
     * 不受 OG 分支影响。
     */
    @Test
    void vanillaDialectIgnoresParttypeGate() {
        TableFilter filter = new TableFilter(ogSnapshot(), Set.of(), WalLayoutV18.INSTANCE);
        assertTrue(filter.resolve(30004).isPresent(), "V17/V18 档无 parttype 判别面——'x' parttype 不影响放行");
        assertTrue(filter.resolve(30003).isPresent(), "V17/V18 档 relkind 'p' 分区表放行语义零变化");
    }

    /**
     * 用例 ⑪（v2 兼容构造器零变化锚）：两参构造器（layout 缺席 = vanilla 语义）
     * 行为与改造前逐字节一致——relkind 'r'/'p' 放行、不消费 parttype。
     */
    @Test
    void twoArgConstructorKeepsVanillaSemantics() {
        TableFilter filter = new TableFilter(ogSnapshot(), Set.of());
        assertTrue(filter.resolve(30003).isPresent(), "两参构造器（无 layout）保持 relkind 'p' 放行");
        assertTrue(filter.resolve(30004).isPresent(), "两参构造器不消费 parttype——'x' 值照常放行");
    }

    /**
     * 造一套覆盖 OG 档分区身份链面的假快照（M3 T2）：母表 t_mom（'r'+parttype 'p'，
     * 两列——母表列序断言面）+ 两分区 p1/p2（第四字典行，段号 39001/39002——不在
     * pg_class relfilenode 面）。分区名/母表名与既有 ⑧-⑪ 快照共用 t_mom/t_plain。
     *
     * @return 已登记分区链的 OG 档假快照
     */
    private static FakeSnapshot ogPartitionSnapshot() {
        FakeSnapshot s = ogSnapshot();
        s.column(2001, new CatalogSnapshot.Column(2, "v", 25, false));   // 母表第二列
        s.partition(39001, "p1", 2001);
        s.partition(39002, "p2", 2001);
        return s;
    }

    /**
     * 用例 ⑫（M3 T2 身份链）：分区 relfilenode（heap 记录路由锚——不在三表字典）经
     * 第四字典反查 + parentid 母表链解析出 <b>母表身份</b> 的 TableMeta——schema=public、
     * table=t_mom（<b>非分区名 p1</b>）、relOid=母表 oid、columns=<b>母表列序</b>
     * （锚② {@code PARTITION_NAME=mother} 语义重排：基准路 R/I 帧报母表身份，wal 路
     * 渲分区名即对拍必 diff）。
     */
    @Test
    void ogPartitionRelfilenodeResolvesMotherIdentityWithMotherColumns() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        TableFilter filter = new TableFilter(ogPartitionSnapshot(), Set.of(), og);
        TableMeta meta = filter.resolve(39001).orElseThrow();
        assertEquals("public", meta.schema(), "分区行身份 = 母表 schema（锚② 归一到母表）");
        assertEquals("t_mom", meta.table(), "分区行身份 = 母表表名（非分区名 p1——锚② PARTITION_NAME=mother）");
        assertEquals(2001L, meta.relOid(), "TableMeta 携母表 pg_class oid（身份键）");
        assertEquals(2, meta.columns().size(), "列集 = 母表列序（列挂母表 oid——M2 T5 事实）");
        assertEquals(new ColumnMeta(1, "id", 23, false), meta.columns().get(0));
        assertEquals(new ColumnMeta(2, "v", 25, false), meta.columns().get(1));
    }

    /**
     * 用例 ⑬（白名单双匹配四形态）：白名单匹配对象 = 解析后的<b>母表身份</b>——
     * 母表名条目是<b>基准形态</b>（写 public.t_mom 覆盖其全部分区 p1/p2）；分区名
     * 条目是兼容增强（写 public.p1 只命中 p1 路由的行，<b>不</b>覆盖 p2）；双命中
     * 与母表名单独命中同果；都不中 miss。分区名条目按<b>运行期双查</b>实现（先按
     * 母表全名查、未中再按分区全名查——as-of 随第四字典走，ADD/DROP PARTITION 后
     * 不过期）。
     */
    @Test
    void ogWhitelistDualMatchCoversMotherBaselineAndPartitionAlias() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        FakeSnapshot snapshot = ogPartitionSnapshot();

        TableFilter byMother = new TableFilter(snapshot, Set.of("public.t_mom"), og);
        assertTrue(byMother.resolve(39001).isPresent(), "母表名条目=基准形态：覆盖分区 p1 路由的行");
        assertTrue(byMother.resolve(39002).isPresent(), "母表名条目覆盖全部分区（p2 同放行）");

        TableFilter byPartition = new TableFilter(snapshot, Set.of("public.p1"), og);
        assertTrue(byPartition.resolve(39001).isPresent(), "分区名条目=兼容增强：命中 p1 路由的行");
        assertTrue(byPartition.resolve(39002).isEmpty(), "分区名条目不外溢——p2 路由的行不命中 public.p1");

        TableFilter byBoth = new TableFilter(snapshot, Set.of("public.t_mom", "public.p1"), og);
        assertTrue(byBoth.resolve(39001).isPresent(), "双命中形态与母表名单独命中同果");

        TableFilter byNeither = new TableFilter(snapshot, Set.of("public.t_plain"), og);
        assertTrue(byNeither.resolve(39001).isEmpty(), "母表名/分区名都不中 → miss");
    }

    /**
     * 用例 ⑭（链断 fail-safe）：第四字典有分区行但 parentid 悬空（母表不在 pg_class
     * 字典——字典半损/窗口期 DDL 形态）——resolve 返回 empty（跳过该行、不抛）且
     * 留痕：<b>整实例恰一次 WARN</b>（含 relfilenode/分区名/parentid 上下文）+
     * {@code partitionChainMisses} 计数逐次递增（沿 nsp redirect miss 先例）；不同
     * relfilenode 的第二次链断不重复 WARN（全局节流）。正常 miss（第四字典无行、
     * 白名单不中）零 WARN。
     */
    @Test
    void ogPartitionChainBrokenFailsSafeWithWarnOnceAndCounter() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        FakeSnapshot snapshot = ogSnapshot();
        snapshot.partition(39005, "p_orphan", 2077);   // parentid 2077 不在 pg_class 字典
        TableFilter filter = new TableFilter(snapshot, Set.of(), og);

        assertTrue(filter.resolve(39005).isEmpty(), "链断分区行 fail-safe 跳过（empty 不抛）");
        assertEquals(1, filter.partitionChainMisses(), "链断计数递增");
        assertTrue(filter.resolve(39005).isEmpty());
        assertEquals(2, filter.partitionChainMisses(), "同 relfilenode 再查计数照增（调用方缓存不在此面）");

        snapshot.partition(39006, "p_orphan2", 2078);  // 第二个链断 relfilenode
        assertTrue(filter.resolve(39006).isEmpty());
        assertEquals(3, filter.partitionChainMisses());

        List<ILoggingEvent> warns = appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .toList();
        assertEquals(1, warns.size(), "链断 WARN 全局恰一次（节流），实得: " + warns);
        String msg = warns.get(0).getFormattedMessage();
        assertTrue(msg.contains("39005") && msg.contains("p_orphan") && msg.contains("2077"),
                "WARN 须含 relfilenode/分区名/parentid 上下文，实得: " + msg);

        assertTrue(filter.resolve(99999).isEmpty(), "第四字典无行的正常 miss 零痕迹");
        assertEquals(3, filter.partitionChainMisses(), "正常 miss 不进链断计数");
        assertTrue(appender.list.stream().noneMatch(e -> e.getLevel() == Level.WARN
                        && e.getFormattedMessage().contains("99999")),
                "正常 miss 不打 WARN");
    }

    /**
     * 用例 ⑮（V17/V18 零变化锚）：同一快照（第四字典行在场）在 vanilla 档（V18
     * 布局）完全无视分区链——分区 relfilenode 仍 fail-safe miss，分区身份链严格
     * OG 档位门控。
     */
    @Test
    void vanillaDialectIgnoresPartitionChainEntirely() {
        TableFilter filter = new TableFilter(ogPartitionSnapshot(), Set.of(), WalLayoutV18.INSTANCE);
        assertTrue(filter.resolve(39001).isEmpty(),
                "V17/V18 档不消费第四字典——分区 relfilenode 仍 miss（零变化锚）");
    }

    /**
     * 用例 ⑯（M3 T5 修复：EXCHANGE 段号换绑的 oid 碰撞面，OgPartitionIT 实测红转绿）：
     * {@code EXCHANGE PARTITION} 后分区 'p' 行换绑到普通表旧段号——OG 建表
     * relfilenode==oid，故该段号数值 == 普通表 oid，而普通表 pg_class 行的 relfilenode
     * 已同步改走原分区段号：三表的 relfilenode 反查 miss 但 <b>oid 直认兜底</b>会误命中
     * 普通表（IT 实测形态：换绑后母表路由行被渲成普通表名）。修复语义：第四字典 'p' 行
     * 是分区段号的<b>权威属主</b>——OG 档 resolve 先过第四字典（partitionOf 命中即走
     * 母表身份链，链断也不回退三表面——段号属主已定，oid 碰撞的兜底答案必错）；普通表
     * 换绑后的新段号（原分区段号）不在第四字典，经三表 relfilenode 反查照常解析。
     */
    @Test
    void ogExchangeSwappedSegmentResolvesPartitionOwnerOverOidFallback() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        FakeSnapshot s = new FakeSnapshot();
        s.relation(2001, 2001, "t_mom", 2200, "public", "r");
        s.parttype(2001, "p");
        s.column(2001, new CatalogSnapshot.Column(1, "id", 23, false));
        s.column(2001, new CatalogSnapshot.Column(2, "v", 25, false));
        // EXCHANGE 后的 t_swap：oid 2078 不变，relfilenode 已改走原分区段号 39005
        s.relation(2078, 39005, "t_swap", 2200, "public", "r");
        s.parttype(2078, "n");
        // 分区 q1 换绑到 t_swap 旧段号 2078（数值 == t_swap oid——碰撞面）
        s.partition(2078, "q1", 2001);
        TableFilter filter = new TableFilter(s, Set.of("public.t_mom", "public.t_swap"), og);

        TableMeta swapped = filter.resolve(2078).orElseThrow();
        assertEquals("t_mom", swapped.table(),
                "换绑后分区段号 2078 的行应归母表（第四字典权威属主优先于三表 oid 直认兜底）");
        TableMeta plain = filter.resolve(39005).orElseThrow();
        assertEquals("t_swap", plain.table(),
                "普通表换绑后的新段号（原分区段号，不在第四字典）经 relfilenode 反查照常解析");
    }
}
