package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.replay.CatalogRow;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VastBase G100（OG 6.0.0 内核）第四布局档：词典几何/读区偏移/护栏槽位——
 * 全部锚定 2026-10-13 真库实测（spec 附录 A）。
 */
class WalLayoutVVastbaseTest {

    @Test
    void dictionariesMatchProbedColumnSets() {
        WalLayout v = WalLayoutVVastbase.INSTANCE;
        assertEquals(43, v.pgClassKinds().length);      // oidhdr + 42
        assertEquals(29, v.pgAttributeKinds().length);  // 无 oidhdr
        assertEquals(33, CatalogRow.PartitionRow.kinds(v).length);  // oidhdr + 32
        assertEquals(8, CatalogRow.NspRow.kinds(v).length);         // oidhdr + 7
    }

    @Test
    void pgClassDictionarySlotsAnchorProbedColumnOrder() {
        String[] k = WalLayoutVVastbase.INSTANCE.pgClassKinds();
        assertEquals("name128", k[1]);    // relname
        assertEquals("oid", k[7]);        // relfilenode(attnum 7——含 oidhdr 槽=attnum)
        assertEquals("oid", k[12]);       // reltoastrelid(attnum 12)
        assertEquals("char", k[21]);      // relkind(attnum 21)
        assertEquals("bool", k[25]);      // relhassecids(attnum 25,VastBase 插入)
        assertEquals("char", k[33]);      // parttype(attnum 33,OG 32)
        assertEquals("int8", k[41]);      // relminmxid(attnum 41)
        assertEquals("oid", k[42]);       // relrewrite(attnum 42)
    }

    @Test
    void pgAttributeDictionarySlotsAnchorProbedColumnOrder() {
        String[] k = WalLayoutVVastbase.INSTANCE.pgAttributeKinds();
        assertEquals("name128", k[1]);   // attname
        assertEquals("int2", k[5]);      // attnum(槽 5 == attrAttnumIndex,分叉点前)
        assertEquals("bool", k[14]);     // attisdropped(槽 14 == attrDroppedIndex)
        assertEquals("name128", k[19]);  // attalias(VastBase 插入)
        assertEquals("bool", k[20]);     // attisquoted(VastBase 插入)
        assertEquals("name128", k[28]);  // attdroppedname(尾列)
    }

    /** 自检锚(沿"两版 toast 偏移差恰 4B"先例):VastBase 与 OG 的 pg_class 读区偏移差恰 64B。 */
    @Test
    void readOffsetsDifferFromOpenGaussByExactly64() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        assertEquals(64, vb.pgClassRelfilenodeDataOffset() - og.pgClassRelfilenodeDataOffset());
        assertEquals(64, vb.pgClassReltoastrelidDataOffset() - og.pgClassReltoastrelidDataOffset());
        assertEquals(64, vb.pgClassRelkindDataOffset() - og.pgClassRelkindDataOffset());
        assertEquals(128, vb.nameDatumWidth());
    }

    /**
     * T6 实测锚（2026-10-13 真库 vastdump 探针——UPDATE info=0x40 main 前缀
     * {@code [old u16][new u16][flags u8][CSN u64][xid u64]}、DELETE info=0x10 同构）：
     * heap UPDATE/DELETE 的 main 前缀较 OG 各恰 +8（xid u64 殿后无条件在场——OG 仅
     * INIT_PAGE 变体有 +8 前缀）；INPLACE 与 OG 同构（T5 catalog 对拍实证）。以 13
     * 偏移读旧行会落在 xid 尾中段——natts 解出 xid 低位垃圾（首轮 IT 实测复现面）。
     */
    @Test
    void heapRecordPrefixesCarryTrailingXidEightBytesBeyondOpenGauss() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        assertEquals(21, vb.sizeOfHeapUpdate());
        assertEquals(8, vb.sizeOfHeapUpdate() - og.sizeOfHeapUpdate(), "UPDATE: OG 13 + xid u64 尾恰 +8");
        assertEquals(19, vb.sizeOfHeapDelete());
        assertEquals(8, vb.sizeOfHeapDelete() - og.sizeOfHeapDelete(), "DELETE: OG 11 + xid u64 尾恰 +8");
        assertEquals(og.sizeOfHeapInplace(), vb.sizeOfHeapInplace(), "INPLACE 与 OG 同构");
    }

    /**
     * T6 实测锚（真库 pg_type 实查 + vastkind 探针）：A 档 DDL 关键字 {@code date} 落
     * 私有类型 {@code oradate}=9040（8B epoch-2000 微秒——timestamp kind 解码逐值正确
     * 实证）——DML 面重映射 1114；矩阵内 oid 与他 oid 恒等；OG/vanilla 档无重映射
     * （方言 oid 空间不越权认领）。
     */
    @Test
    void oracleDateOidRemapsToTimestampForDmlFaceOnlyOnVastbase() {
        assertEquals(1114L, WalLayoutVVastbase.INSTANCE.remapDmlTypeOid(9040));
        assertEquals(9041L, WalLayoutVVastbase.INSTANCE.remapDmlTypeOid(9041), "他 oid 恒等");
        assertEquals(1114L, WalLayoutVVastbase.INSTANCE.remapDmlTypeOid(1114), "矩阵内 oid 恒等");
        assertEquals(9040L, WalLayoutVOpenGauss.INSTANCE.remapDmlTypeOid(9040), "OG 档无重映射");
        assertEquals(9040L, WalLayoutV18.INSTANCE.remapDmlTypeOid(9040), "vanilla 档无重映射");
    }

    /** 分叉点后护栏槽位 +1;分叉点前不变;majorVersion=6;supports 覆盖 90204。 */
    @Test
    void guardSlotIndexesAndVersionIdentity() {
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        assertEquals(32, vb.classParttypeIndex());     // OG 31 + 1
        assertEquals(35, vb.classReloptionsIndex());   // OG 34 + 1
        assertEquals(38, vb.classRelbucketIndex());    // OG 37 + 1
        assertEquals(13, vb.classReldeltarelidIndex()); // 分叉点前不变
        assertEquals(15, vb.classRelcudescrelidIndex());
        assertEquals(6, vb.majorVersion());
        assertTrue(vb.supports(90204));
        assertTrue(vb.openGaussDialect());             // 继承——startOpenGauss 装配面
        assertEquals(5, vb.attrAttnumIndex());         // 继承(分叉点前)
        assertEquals(14, vb.attrDroppedIndex());
    }

    /**
     * 系统列诊断下界 -9（M4 修复）：VastBase 6.0.0 内核 pg_attribute 系统列集
     * [-9,-1]（xc_node_id=-9、rowid=-8——真库 A/B 双档实查同源），OG 5.0.0 布局
     * 维持 -8——流内 CREATE TABLE 的 xc_node_id(-9) 行触发假阳性"疑似词典错位"
     * WARN 的根因修复锚。
     */
    @Test
    void vastbaseAttrMinSystemAttnumIsMinusNine() {
        assertEquals(-9, WalLayoutVVastbase.INSTANCE.attrMinSystemAttnum());
        assertEquals(-8, WalLayoutVOpenGauss.INSTANCE.attrMinSystemAttnum());
        assertEquals(-6, WalLayoutV18.INSTANCE.attrMinSystemAttnum());
    }

    /**
     * 读区长度自检锚（nsp/partition 面，沿"偏移差恰 64B"先例）：VastBase 的
     * nsp/partition 读区相对 OG 各差恰 64B（name 加宽净效果——nsp 80→144、
     * partition 120→184）；且三行模型的 encodeReadRegion 实长必须与各自声明的
     * readRegionLen 相等（class 面 = relkind 偏移 + 1）——此前 OG nsp 编码实长
     * 76 与声明 80 不符（[76,80) 段缺 4B，prefix/backfill 触达即越界），以此
     * 不变量离线钉死。
     */
    @Test
    void readRegionLengthsDifferFromOpenGaussByExactly64AndEncodingsMatchDeclaredLength() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        assertEquals(64, CatalogRow.NspRow.readRegionLen(vb) - CatalogRow.NspRow.readRegionLen(og));
        assertEquals(144, CatalogRow.NspRow.readRegionLen(vb));
        assertEquals(64, CatalogRow.PartitionRow.readRegionLen(vb) - CatalogRow.PartitionRow.READ_REGION_LEN);
        assertEquals(184, CatalogRow.PartitionRow.readRegionLen(vb));
        CatalogRow.ClassRow cls = new CatalogRow.ClassRow(16389L, "vb_t", 2200L, 16390L, 0L, 10L, 0L, 16426L, 16427L, "r");
        CatalogRow.NspRow nsp = new CatalogRow.NspRow(2200L, "public");
        CatalogRow.PartitionRow part = new CatalogRow.PartitionRow(9016L, "p1", "p", 16389L, 14318L, 16777L);
        // 编码实长 == 声明读区长（两档同时钉——OG 修复 nsp 76→80 的回归锚）
        assertEquals(CatalogRow.NspRow.readRegionLen(og), nsp.encodeReadRegion(og).length);
        assertEquals(CatalogRow.NspRow.readRegionLen(vb), nsp.encodeReadRegion(vb).length);
        assertEquals(CatalogRow.PartitionRow.READ_REGION_LEN, part.encodeReadRegion(og).length);
        assertEquals(CatalogRow.PartitionRow.readRegionLen(vb), part.encodeReadRegion(vb).length);
        assertEquals(og.pgClassRelkindDataOffset() + 1, cls.encodeReadRegion(og).length);
        assertEquals(vb.pgClassRelkindDataOffset() + 1, cls.encodeReadRegion(vb).length);
        assertEquals(208, cls.encodeReadRegion(vb).length);   // OG 144 + 64
    }

    /**
     * VastBase 三读区的编码几何逐段锚：name 区 128B（NUL 尾垫）、关键 oid 落位
     * 148/180（+64）、relkind@207、parttype@128——与 layout 偏移方法互证（防
     * 词典/偏移两副面孔单侧漂移）。
     */
    @Test
    void vastbaseReadRegionEncodingsAnchorGeometry() {
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        CatalogRow.ClassRow cls = new CatalogRow.ClassRow(16389L, "vb_t", 2200L, 16390L, 0L, 10L, 0L, 16426L, 16427L, "r");
        byte[] region = cls.encodeReadRegion(vb);
        assertEquals(16426L, u32le(region, vb.pgClassRelfilenodeDataOffset()), "relfilenode@148");
        assertEquals(16427L, u32le(region, vb.pgClassReltoastrelidDataOffset()), "reltoastrelid@180");
        assertEquals('r', region[vb.pgClassRelkindDataOffset()], "relkind@207");
        assertEquals('v', region[0]);
        assertEquals(0, region[4], "name NUL 尾垫");
        assertEquals(0, region[127], "name 定宽区末字节（128B）");
        assertEquals(2200L, u32le(region, 128), "relnamespace 紧随 name 定宽区");

        CatalogRow.NspRow nsp = new CatalogRow.NspRow(2200L, "public");
        byte[] n = nsp.encodeReadRegion(vb);
        assertEquals('p', n[0]);
        assertEquals(0, n[6]);
        assertEquals(0, n[127], "nspname 定宽区末字节（128B）");
        for (int i = 128; i < 144; i++) {
            assertEquals(0, n[i], "[128,144) nspowner/垫/nsptimeline 不投影 0 占位 @" + i);
        }

        CatalogRow.PartitionRow part = new CatalogRow.PartitionRow(9016L, "p1", "p", 16389L, 14318L, 16777L);
        byte[] p = part.encodeReadRegion(vb);
        assertEquals('p', p[0]);
        assertEquals(0, p[2]);
        assertEquals(0, p[127], "relname 定宽区末字节（128B）");
        assertEquals('p', p[128], "parttype@128");
        assertEquals(16389L, u32le(p, 132), "parentid@132");
        assertEquals(14318L, u32le(p, 148), "relfilenode@148");
        assertEquals(16777L, u32le(p, 180), "reltoastrelid@180");
    }

    /**
     * 全列 42 att 元组经本档词典（oidhdr 前导 + 42 数据列）解码的几何自洽锚：
     * 手造元组按 spec 附录 A 列宽/对齐铺字节，解码后值行长度 43（natts + oidhdr）
     * 且 attnum n 落值行槽 n——分叉点后的新列（relhassecids@25/parttype@33/
     * relminmxid@41/relrewrite@42）逐槽断言，词典任何宽度/次序誊写错误都会使
     * 锚点槽位解错值。
     */
    @Test
    void vastbaseClassTupleDecodesThroughOidHeaderDictionary() {
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        byte[] payload = vastbaseClassTuple();
        Object[] vals = new TupleDecoder(vb).decodePayload(payload, 0, vb.pgClassKinds());
        assertEquals(43, vals.length);
        assertEquals(16389L, vals[0], "元组头 oid 注入值行槽 0");
        assertEquals(16426L, vals[7], "relfilenode（attnum 7）");
        assertEquals(16427L, vals[12], "reltoastrelid（attnum 12）");
        assertEquals("'r", vals[21], "relkind（attnum 21，char 解码左引号形态）");
        assertEquals((short) 3, vals[22], "relnatts（attnum 22，int2）");
        assertEquals(Boolean.TRUE, vals[25], "relhassecids（attnum 25，VastBase 插入列锚）");
        assertEquals("'n", vals[33], "parttype（attnum 33，OG 32）");
        assertEquals("'d", vals[37], "relreplident（attnum 37）");
        assertEquals(800L, vals[41], "relminmxid（attnum 41，int8→Long）");
        assertEquals(0L, vals[42], "relrewrite（attnum 42，oid→Long，VastBase 尾列锚）");
        CatalogRow.ClassRow row = CatalogRow.ClassRow.fromDecoded(vals, vb);
        assertEquals(new CatalogRow.ClassRow(16389L, "vb_t", 2200L, 16390L, 0L, 10L, 0L, 16426L, 16427L, "r"), row);
    }

    /**
     * 手造 VastBase pg_class 全列元组的 WAL 载荷形态：[xl_heap_header 5B]
     * [t_hoff-4 位 oid][42 列数据区]。数据区偏移逐列累计（spec 附录 A 列宽 +
     * 对齐）：name128@0、oid×5@[128,148)、relfilenode@148、reltablespace@152、
     * 垫 4、relpages/reltuples float8@160/168、relallvisible@176、
     * reltoastrelid@180、oid×5@[184,204)、三单字节列@204-206、relkind@207、
     * relnatts/relchecks int2@208/210、bool×6@212-217、relcmprs int1@218、
     * bool×2@219-220、parttype@221、垫 2、relfrozenxid@224、空 varlena×2
     * （0x03）@228/229、relreplident@230、垫 1、relfrozenxid64@232、relbucket
     * @240、空 varlena@244、垫 3、relminmxid@248、relrewrite@256。
     * t_hoff=32 使 oid 落载荷 [10,14)（元组头 9.2 式末成员位）。
     */
    private static byte[] vastbaseClassTuple() {
        ByteArrayOutputStream out = new ByteArrayOutputStream(280);
        put16(out, 42);      // infomask2 = natts 42
        put16(out, 0);       // infomask（无 HASNULL）
        out.write(32);       // t_hoff（oid@t_hoff-4）
        out.write(new byte[5], 0, 5);   // [5,10)：位图垫区（无 null 不读）
        put32(out, 16389);   // 元组头 oid
        byte[] name = "vb_t".getBytes(StandardCharsets.UTF_8);
        out.write(name, 0, name.length);
        out.write(new byte[128 - name.length], 0, 128 - name.length);   // name128 定宽 + NUL 尾垫
        put32(out, 2200);    // relnamespace@128
        put32(out, 16390);   // reltype@132
        put32(out, 0);       // reloftype@136
        put32(out, 10);      // relowner@140
        put32(out, 0);       // relam@144
        put32(out, 16426);   // relfilenode@148
        put32(out, 0);       // reltablespace@152
        out.write(new byte[4], 0, 4);   // [156,160) float8 'd' 对齐垫
        put64(out, 0L);      // relpages@160
        put64(out, 0L);      // reltuples@168
        put32(out, 0);       // relallvisible@176
        put32(out, 16427);   // reltoastrelid@180
        put32(out, 0);       // reltoastidxid@184
        put32(out, 0);       // reldeltarelid@188
        put32(out, 0);       // reldeltaidx@192
        put32(out, 0);       // relcudescrelid@196
        put32(out, 0);       // relcudescidx@200
        out.write(1);        // relhasindex@204
        out.write(0);        // relisshared@205
        out.write('p');      // relpersistence@206
        out.write('r');      // relkind@207
        put16(out, 3);       // relnatts@208
        put16(out, 0);       // relchecks@210
        out.write(0);        // relhasoids@212
        out.write(1);        // relhassecids@213（VastBase 插入）
        out.write(0);        // relhaspkey@214
        out.write(0);        // relhasrules@215
        out.write(0);        // relhastriggers@216
        out.write(0);        // relhassubclass@217
        out.write(0);        // relcmprs int1@218
        out.write(0);        // relhasclusterkey@219
        out.write(0);        // relrowmovement@220
        out.write('n');      // parttype@221
        out.write(new byte[2], 0, 2);   // [222,224) int4 对齐垫
        put32(out, 1234);    // relfrozenxid@224
        out.write(3);        // relacl@228：1B 头空 varlena（0x03——0x01 是 external tag）
        out.write(3);        // reloptions@229：1B 头空 varlena
        out.write('d');      // relreplident@230
        out.write(new byte[1], 0, 1);   // [231,232) int8 对齐垫
        put64(out, 700L);    // relfrozenxid64@232
        put32(out, 0);       // relbucket@240
        out.write(3);        // relbucketkey@244：1B 头空 varlena
        out.write(new byte[3], 0, 3);   // [245,248) int8 对齐垫
        put64(out, 800L);    // relminmxid@248
        put32(out, 0);       // relrewrite@256（VastBase 尾列）
        return out.toByteArray();
    }

    /**
     * little-endian u32 读取（读区编码断言辅助）。
     *
     * @param b 缓冲
     * @param off 偏移
     * @return 无符号 32 位值
     */
    private static long u32le(byte[] b, int off) {
        return (b[off] & 0xFFL) | ((b[off + 1] & 0xFFL) << 8) | ((b[off + 2] & 0xFFL) << 16) | ((b[off + 3] & 0xFFL) << 24);
    }

    /**
     * little-endian u16 写入（手造元组辅助）。
     *
     * @param out 目标流
     * @param v   16 位值
     */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    /**
     * little-endian u32 写入（手造元组辅助）。
     *
     * @param out 目标流
     * @param v   32 位值
     */
    private static void put32(ByteArrayOutputStream out, int v) {
        put16(out, v & 0xFFFF);
        put16(out, (v >>> 16) & 0xFFFF);
    }

    /**
     * little-endian u64 写入（手造元组辅助）。
     *
     * @param out 目标流
     * @param v   64 位值
     */
    private static void put64(ByteArrayOutputStream out, long v) {
        put32(out, (int) (v & 0xFFFFFFFFL));
        put32(out, (int) (v >>> 32));
    }
}
