package org.vastdata.vbstream.walsource.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.replay.CatalogRow.AttrRow;
import org.vastdata.vbstream.walsource.replay.CatalogRow.ClassRow;
import org.vastdata.vbstream.walsource.replay.CatalogRow.NspRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * CatalogStores 检查点的原子持久化器（spec §7）——VBWS 自描述二进制格式 + 双 CRC
 * 校验 + {@code .part} 同目录暂存 → fsync → 原子换名（与 reader 模块
 * {@code .part→rename} publish 范式同一语义：目录里无 {@code .part} 后缀的
 * {@value #FILE_NAME} 即完整检查点）。
 *
 * <p><strong>文件布局（formatVersion=2，大端，DataOutputStream 原生序；v2 增
 * pg_namespace 面——nsp 表/nsp tail 表/pgNspRelnode 标量，class 行增 relkind）</strong>：
 * <pre>
 * header 24B: magic "VBWS" 4B + formatVersion u16 + pgVersion u16（大版本，与构造
 *             入参 layout.majorVersion() 同源——17 写 17、18 写 18，load 校验同源）
 *             + lsn u64 + headerLen u32(=24) + headerCrc32 u32（前 20 字节）
 * body:       attr 表（rowCount u32 + 行[ctid u64 + attrelid u64 + atttypid u64
 *               + attnum u32 + attisdropped u8 + attname u16 前缀 UTF]×n）
 *           → class 表（rowCount u32 + 行[ctid u64 + 8 个 long 字段 + relname UTF
 *               + relkind UTF]×n）
 *           → nsp 表（rowCount u32 + 行[ctid u64 + nspOid u64 + nspname UTF]×n）
 *           → attr tail 表（count u32 + 条目[ctid u64 + len u32 + bytes]×n）
 *           → class tail 表（同上）→ nsp tail 表（同上）
 *           → tracked 双 ctid u64×2 + pgAttr/pgClass/pgNsp relfilenode u64×3
 *           → staleOids（count u32 + oid u64×n）+ interestRelOids（同上）
 * footer 12B: lsn u64（header 复述双验）+ fileCrc32 u32（[0, len-4) 全文件）
 * </pre></p>
 *
 * <p><strong>拒载语义</strong>：任一校验不符（magic / formatVersion / pgVersion /
 * headerLen / header CRC / 全文件 CRC / lsn 双验 / 尾部余量）或解析期
 * EOF——{@link #load()} 一律 {@code Optional.empty()} + ERROR 日志，回落重引导
 * 由 caller 决定；<strong>v1（formatVersion=1）检查点一律拒载</strong>（v2 扩链
 * 裁定：v1 缺 nsp 表段语义不完整，不迁移——拒载回落全新引导是安全侧）；{@code .part}
 * 残留永不 load（{@link #exists()} 亦只认正名文件）。
 * 持久化面 = 完整 CatalogStores 可重建态除 metrics 外全部字段（裁定见
 * {@link StoredState}）。<strong>契约（Med-3）：一个目录同一时刻仅一个活实例写
 * 检查点</strong>——两实例共用目录时后停机者以陈旧状态覆盖新检查点（文件锁后续）。
 * 线程约束：checkpoint 与 load 意图上在停流/装配期单线程
 * 调用（spec §7 取单线程天然一致点）；文件面自身由换名原子性兜底并发读写。</p>
 */
public final class StateStore {

    private static final Logger LOG = LoggerFactory.getLogger(StateStore.class);

    /** 检查点正名（目录里无 {@code .part} 后缀的此文件即完整检查点——文件名属契约）。 */
    public static final String FILE_NAME = "wal-source-state.bin";

    /** 半成品暂存后缀（fsync 完成后原子换名去后缀）。 */
    private static final String PART_SUFFIX = ".part";

    /**
     * 格式版本（header u16——不符拒载，不回退兼容解读）。v2（Task 4 扩链）：增
     * pg_namespace 面（nsp 表/nsp tail/pgNspRelnode）+ class 行 relkind 字段；
     * v1 文件（版本 1）拒载回落全新引导（裁定不迁移）。
     */
    private static final int FORMAT_VERSION = 2;

    /** header 定长 24B（4+2+2+8+4+4——版本演进的前向兼容跳读锚）。 */
    private static final int HEADER_LEN = 24;

    /** footer 定长 12B（lsn 复述 8 + 全文件 CRC 4）。 */
    private static final int FOOTER_LEN = 12;

    /** 格式魔数（文件头 4 字节）。 */
    private static final byte[] MAGIC = {'V', 'B', 'W', 'S'};

    private final Path target;
    private final Path part;

    /** 本存储器绑定的大版本（写 header 与 load 校验同源——17 写 17、18 写 18）。 */
    private final int pgVersion;

    /**
     * 建立指定目录上的检查点存储器（不触碰文件系统——目录与文件按
     * checkpoint/load 时点惰性创建/读取）。
     *
     * <p>pgVersion 与同步器的 layout 同源传入（终审 I1：原固定 18 常量使 PG 17 写出
     * 的检查点 header 也记 18——17/18 共用目录或版本切换时错配文件无法被 load 校验
     * 拒绝）。错配拒载（load 报 pgVersion 不符 → empty）由 caller 回落全新引导。</p>
     *
     * @param dir       检查点所在目录（不存在则 checkpoint 期建目录）
     * @param pgVersion 大版本号（layout.majorVersion()——写与 load 校验同源）
     */
    public StateStore(Path dir, int pgVersion) {
        this.target = dir.resolve(FILE_NAME);
        this.part = dir.resolve(FILE_NAME + PART_SUFFIX);
        this.pgVersion = pgVersion;
    }

    /**
     * 把 stores 全量序列化为一次检查点（持久化面见 {@link StoredState}）。
     *
     * <p>关键步骤：内存序列化（header CRC 与全文件 CRC 双计）→ 写
     * {@code .part}（truncate 覆写）→ {@code FileChannel.force(true)} 落盘 →
     * 同目录 {@code Files.move(ATOMIC_MOVE, REPLACE_EXISTING)} 原子换名（旧检查点
     * 整体替换，无中间态可见；ATOMIC_MOVE 不支持时回落非原子 move + WARN）。边界
     * 与异常语义：IOException 上抛（调用方决定 fail-fast——检查点失败不损既有
     * 正名文件，最坏残留 {@code .part} 半成品，下次 checkpoint 覆写）；目录不存
     * 在则先建。线程约束：意图上单写者（停流/周期检查点线程）。</p>
     *
     * @param stores 待持久化的重放状态容器
     * @param lsn    检查点 LSN（已施加前沿）
     * @throws IOException 序列化/落盘/换名失败
     */
    public void checkpoint(CatalogStores stores, long lsn) throws IOException {
        byte[] payload = serialize(stores, lsn);
        Files.createDirectories(target.getParent());
        try (FileChannel ch = FileChannel.open(part,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(payload);
            while (buf.hasRemaining()) {
                ch.write(buf);    // FileChannel.write 允许部分写——循环至耗尽
            }
            ch.force(true);
        }
        try {
            Files.move(part, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            LOG.warn("文件系统不支持 ATOMIC_MOVE，检查点换名回落非原子 move: {}", target, e);
            Files.move(part, target, StandardCopyOption.REPLACE_EXISTING);
        }
        LOG.info("StateStore 检查点落盘: {} lsn={} attr={} class={} nsp={} stale={}（原子换名完成）",
                target, lsn, stores.attrRows().size(), stores.classRows().size(),
                stores.nspRows().size(), stores.staleOids().size());
    }

    /**
     * 读正名检查点并校验解码（拒载语义见类 javadoc）。
     *
     * <p>关键步骤：正名不存在 → empty（静默——首启无检查点是常态）；存在则整读 +
     * 双 CRC/版本/lsn 双验 + 逐段解码。边界与异常语义：任一校验或解析失败（含
     * 截断 EOF）→ ERROR 日志 + empty（不部分载入、不上抛）；{@code .part} 残留
     * 不在本方法读取面内。线程约束：无共享可变状态，任意线程可调。</p>
     *
     * @return 解出的完整可重建态；无正名文件或校验不符为 empty
     */
    public Optional<StoredState> load() {
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(target);
        } catch (IOException e) {
            LOG.error("StateStore 检查点读取失败，拒载回落: {}", target, e);
            return Optional.empty();
        }
        try {
            return Optional.of(decode(bytes));
        } catch (IOException | RuntimeException e) {
            LOG.error("StateStore 检查点校验/解码不符（magic/版本/CRC/lsn 双验之一），拒载回落: {} 原因: {}",
                    target, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * 正名检查点是否已存在（半成品 {@code .part} 不算——换名完成才算完整检查点）。
     *
     * @return 存在为 true
     */
    public boolean exists() {
        return Files.exists(target);
    }

    /**
     * 全量序列化（内存缓冲一次成型，再整体落盘）：header（前 20 字节 CRC 后补进
     * header 尾）→ body 四表 + 标量/两集 → footer lsn 复述——以上全部经
     * {@link CheckedOutputStream} 逐字节计全文件 CRC，末尾追加 CRC 自身（CRC 不
     * 覆盖自己）。
     *
     * @param stores 待序列化容器
     * @param lsn    检查点 LSN
     * @return 完整文件字节
     * @throws IOException 序列化失败（内存流上实际不发生）
     */
    private byte[] serialize(CatalogStores stores, long lsn) throws IOException {
        ByteArrayOutputStream prefixBuf = new ByteArrayOutputStream(HEADER_LEN - 4);
        DataOutputStream prefix = new DataOutputStream(prefixBuf);
        prefix.write(MAGIC);
        prefix.writeShort(FORMAT_VERSION);
        prefix.writeShort(pgVersion);
        prefix.writeLong(lsn);
        prefix.writeInt(HEADER_LEN);
        prefix.flush();
        byte[] headerPrefix = prefixBuf.toByteArray();
        CRC32 headerCrc = new CRC32();
        headerCrc.update(headerPrefix);

        ByteArrayOutputStream fileBuf = new ByteArrayOutputStream(16 * 1024);
        CRC32 fileCrc = new CRC32();
        DataOutputStream out = new DataOutputStream(new CheckedOutputStream(fileBuf, fileCrc));
        out.write(headerPrefix);
        out.writeInt((int) headerCrc.getValue());
        writeBody(out, stores);
        out.writeLong(lsn);    // footer lsn 复述（与 header 双验）
        out.flush();

        byte[] payload = fileBuf.toByteArray();
        ByteArrayOutputStream res = new ByteArrayOutputStream(payload.length + 4);
        res.write(payload);
        DataOutputStream tail = new DataOutputStream(res);
        tail.writeInt((int) fileCrc.getValue());
        tail.flush();
        return res.toByteArray();
    }

    /**
     * 写 body 段（六表 + 标量 + 两 oid 集，布局见类 javadoc）。
     *
     * @param out    目标流（已处全文件 CRC 计数面）
     * @param stores 数据源
     * @throws IOException 写失败
     */
    private static void writeBody(DataOutputStream out, CatalogStores stores) throws IOException {
        out.writeInt(stores.attrRows().size());
        for (Map.Entry<Long, AttrRow> e : stores.attrRows().entrySet()) {
            AttrRow r = e.getValue();
            out.writeLong(e.getKey());
            out.writeLong(r.attrelid());
            out.writeLong(r.atttypid());
            out.writeInt(r.attnum());
            out.writeBoolean(r.attisdropped());
            out.writeUTF(r.attname());
        }
        out.writeInt(stores.classRows().size());
        for (Map.Entry<Long, ClassRow> e : stores.classRows().entrySet()) {
            ClassRow r = e.getValue();
            out.writeLong(e.getKey());
            out.writeLong(r.relOid());
            out.writeLong(r.relnamespace());
            out.writeLong(r.reltype());
            out.writeLong(r.reloftype());
            out.writeLong(r.relowner());
            out.writeLong(r.relam());
            out.writeLong(r.relfilenode());
            out.writeLong(r.reltoastrelid());
            out.writeUTF(r.relname());
            out.writeUTF(r.relkind() == null ? "" : r.relkind());
        }
        out.writeInt(stores.nspRows().size());
        for (Map.Entry<Long, NspRow> e : stores.nspRows().entrySet()) {
            NspRow r = e.getValue();
            out.writeLong(e.getKey());
            out.writeLong(r.nspOid());
            out.writeUTF(r.nspname());
        }
        writeTails(out, stores.rawAttrTails());
        writeTails(out, stores.rawClassTails());
        writeTails(out, stores.rawNspTails());
        out.writeLong(stores.trackedTableCtid());
        out.writeLong(stores.trackedToastCtid());
        out.writeLong(stores.pgAttrRelfilenode());
        out.writeLong(stores.pgClassRelfilenode());
        out.writeLong(stores.pgNspRelnode());
        writeOids(out, stores.staleOids());
        writeOids(out, stores.interestRelOids());
    }

    /**
     * 写一张 raw tail 表（count u32 + 条目 ctid u64 + len u32 + 字节）。
     *
     * @param out   目标流
     * @param tails tail 存储
     * @throws IOException 写失败
     */
    private static void writeTails(DataOutputStream out, Map<Long, byte[]> tails) throws IOException {
        out.writeInt(tails.size());
        for (Map.Entry<Long, byte[]> e : tails.entrySet()) {
            out.writeLong(e.getKey());
            out.writeInt(e.getValue().length);
            out.write(e.getValue());
        }
    }

    /**
     * 写一张 oid 集（count u32 + oid u64×n）。
     *
     * @param out 目标流
     * @param oids oid 集
     * @throws IOException 写失败
     */
    private static void writeOids(DataOutputStream out, Set<Long> oids) throws IOException {
        out.writeInt(oids.size());
        for (long oid : oids) {
            out.writeLong(oid);
        }
    }

    /**
     * 校验并解码检查点字节。校验序：<strong>先验后解</strong>——header 五项
     * （magic/formatVersion/pgVersion/headerLen/header CRC）与全文件 CRC 先行
     * （CRC 达标才进 body 解析，垃圾 rowCount 不会驱动解析循环），再顺序解码六表
     * 与标量，最后 lsn 复述双验 + 尾部余量恰为 0（多余尾字节视为损坏拒载）。
     *
     * @param bytes 整文件字节
     * @return 解出的完整可重建态
     * @throws IOException 任一校验不符或解析期截断（具体原因进异常消息）
     */
    private StoredState decode(byte[] bytes) throws IOException {
        if (bytes.length < HEADER_LEN + FOOTER_LEN) {
            throw new EOFException("文件过短: " + bytes.length + "B");
        }
        CRC32 fileCrc = new CRC32();
        fileCrc.update(bytes, 0, bytes.length - 4);
        int storedFileCrc = readIntAt(bytes, bytes.length - 4);
        if ((int) fileCrc.getValue() != storedFileCrc) {
            throw new IOException("全文件 CRC32 不符: 算得 " + Integer.toHexString((int) fileCrc.getValue())
                    + " 存 " + Integer.toHexString(storedFileCrc));
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        byte[] magic = new byte[MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IOException("magic 不符: " + new String(magic));
        }
        int formatVersion = in.readUnsignedShort();
        if (formatVersion != FORMAT_VERSION) {
            throw new IOException("formatVersion 不符: " + formatVersion + " 期望 " + FORMAT_VERSION);
        }
        int storedPgVersion = in.readUnsignedShort();
        if (storedPgVersion != pgVersion) {
            throw new IOException("pgVersion 不符: " + storedPgVersion + " 期望 " + pgVersion
                    + "（layout 与检查点错配——拒载回落全新引导）");
        }
        long lsn = in.readLong();
        int headerLen = in.readInt();
        if (headerLen != HEADER_LEN) {
            throw new IOException("headerLen 不符: " + headerLen + " 期望 " + HEADER_LEN);
        }
        int storedHeaderCrc = in.readInt();
        CRC32 headerCrc = new CRC32();
        headerCrc.update(bytes, 0, HEADER_LEN - 4);
        if ((int) headerCrc.getValue() != storedHeaderCrc) {
            throw new IOException("header CRC32 不符");
        }

        Map<Long, AttrRow> attrs = readAttrTable(in, bytes.length);
        Map<Long, ClassRow> classes = readClassTable(in, bytes.length);
        Map<Long, NspRow> nspRows = readNspTable(in, bytes.length);
        Map<Long, byte[]> rawAttrTails = readTails(in, bytes.length);
        Map<Long, byte[]> rawClassTails = readTails(in, bytes.length);
        Map<Long, byte[]> rawNspTails = readTails(in, bytes.length);
        long trackedTableCtid = in.readLong();
        long trackedToastCtid = in.readLong();
        long pgAttrRelfilenode = in.readLong();
        long pgClassRelfilenode = in.readLong();
        long pgNspRelnode = in.readLong();
        Set<Long> staleOids = readOids(in, bytes.length);
        Set<Long> interestRelOids = readOids(in, bytes.length);

        long footerLsn = in.readLong();
        if (footerLsn != lsn) {
            throw new IOException("footer lsn 复述不符: header " + lsn + " footer " + footerLsn);
        }
        if (in.available() != 4) {
            throw new IOException("尾部余量不符: " + in.available() + "B（应恰余全文件 CRC 4B）");
        }
        return new StoredState(lsn, attrs, classes, nspRows, rawAttrTails, rawClassTails, rawNspTails,
                trackedTableCtid, trackedToastCtid, pgAttrRelfilenode, pgClassRelfilenode, pgNspRelnode,
                interestRelOids, staleOids);
    }

    /**
     * 读 attr 行表（垃圾计数防线：rowCount 上限钉在文件实长——每行至少 31B，计数
     * 超过剩余实长即判损坏，截断由 readFully 的 EOF 兜底）。
     *
     * @param in       输入流（已处 header 之后）
     * @param fileLen  文件实长（计数防线锚）
     * @return ctid 键控行字典（保持落盘序）
     * @throws IOException 解析失败/截断
     */
    private static Map<Long, AttrRow> readAttrTable(DataInputStream in, int fileLen) throws IOException {
        int rowCount = in.readInt();
        if (rowCount < 0 || (long) rowCount * 31 > fileLen) {
            throw new IOException("attr 表计数损坏: " + rowCount);
        }
        Map<Long, AttrRow> attrs = new LinkedHashMap<>(Math.max(16, rowCount));
        for (int i = 0; i < rowCount; i++) {
            long ctid = in.readLong();
            long attrelid = in.readLong();
            long atttypid = in.readLong();
            int attnum = in.readInt();
            boolean attisdropped = in.readBoolean();
            String attname = in.readUTF();    // 写序最后（u16 前缀 UTF）
            attrs.put(ctid, new AttrRow(attrelid, attname, atttypid, attnum, attisdropped));
        }
        return attrs;
    }

    /**
     * 读 class 行表（计数防线同 {@link #readAttrTable}——v2 起每行至少 76B：ctid
     * u64=8 + 8 个落盘 long 字段 64 + relname/relkind 两个 UTF 空串前缀各 u16=2；
     * 防线过严会误拒最小行宽形态的合法检查点，见 StateStoreTest 用例 ⑥）。
     *
     * @param in      输入流
     * @param fileLen 文件实长
     * @return ctid 键控行字典（保持落盘序）
     * @throws IOException 解析失败/截断
     */
    private static Map<Long, ClassRow> readClassTable(DataInputStream in, int fileLen) throws IOException {
        int rowCount = in.readInt();
        if (rowCount < 0 || (long) rowCount * 76 > fileLen) {
            throw new IOException("class 表计数损坏: " + rowCount);
        }
        Map<Long, ClassRow> classes = new LinkedHashMap<>(Math.max(16, rowCount));
        for (int i = 0; i < rowCount; i++) {
            long ctid = in.readLong();
            long relOid = in.readLong();
            long relnamespace = in.readLong();
            long reltype = in.readLong();
            long reloftype = in.readLong();
            long relowner = in.readLong();
            long relam = in.readLong();
            long relfilenode = in.readLong();
            long reltoastrelid = in.readLong();
            String relname = in.readUTF();    // 写序最后（u16 前缀 UTF）
            String relkind = in.readUTF();    // v2 增（裸单字符；防御空串容忍）
            classes.put(ctid, new ClassRow(relOid, relname, relnamespace, reltype, reloftype,
                    relowner, relam, relfilenode, reltoastrelid, relkind));
        }
        return classes;
    }

    /**
     * 读 nsp 行表（v2 扩链段；计数防线同 {@link #readAttrTable}——每行至少 18B：
     * ctid u64=8 + nspOid u64=8 + nspname UTF 空串前缀 u16=2）。
     *
     * @param in      输入流（已处 class 表之后）
     * @param fileLen 文件实长（计数防线锚）
     * @return ctid 键控行字典（保持落盘序）
     * @throws IOException 解析失败/截断
     */
    private static Map<Long, NspRow> readNspTable(DataInputStream in, int fileLen) throws IOException {
        int rowCount = in.readInt();
        if (rowCount < 0 || (long) rowCount * 18 > fileLen) {
            throw new IOException("nsp 表计数损坏: " + rowCount);
        }
        Map<Long, NspRow> nspRows = new LinkedHashMap<>(Math.max(16, rowCount));
        for (int i = 0; i < rowCount; i++) {
            long ctid = in.readLong();
            long nspOid = in.readLong();
            String nspname = in.readUTF();
            nspRows.put(ctid, new NspRow(nspOid, nspname));
        }
        return nspRows;
    }

    /**
     * 读一张 raw tail 表（count u32 + 条目 ctid u64 + len u32 + 字节；len 超
     * 文件实长即判损坏）。
     *
     * @param in      输入流
     * @param fileLen 文件实长（len 防线锚）
     * @return tail 存储（保持落盘序）
     * @throws IOException 解析失败/截断
     */
    private static Map<Long, byte[]> readTails(DataInputStream in, int fileLen) throws IOException {
        int count = in.readInt();
        if (count < 0 || (long) count * 12 > fileLen) {
            throw new IOException("tail 表计数损坏: " + count);
        }
        Map<Long, byte[]> tails = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            long ctid = in.readLong();
            int len = in.readInt();
            if (len < 0 || len > fileLen) {
                throw new IOException("tail 条目长度损坏: ctid=" + ctid + " len=" + len);
            }
            byte[] data = new byte[len];
            in.readFully(data);
            tails.put(ctid, data);
        }
        return tails;
    }

    /**
     * 读一张 oid 集（count u32 + oid u64×n；计数防线同 tail 表）。
     *
     * @param in      输入流
     * @param fileLen 文件实长
     * @return oid 集（保持落盘序）
     * @throws IOException 解析失败/截断
     */
    private static Set<Long> readOids(DataInputStream in, int fileLen) throws IOException {
        int count = in.readInt();
        if (count < 0 || (long) count * 8 > fileLen) {
            throw new IOException("oid 集计数损坏: " + count);
        }
        Set<Long> oids = new LinkedHashSet<>();
        for (long i = 0; i < count; i++) {
            oids.add(in.readLong());
        }
        return oids;
    }

    /**
     * 从字节数组指定位点读大端 u32（尾段全文件 CRC 的直读——流序读至该处前已耗尽
     * 顺序面）。
     *
     * @param bytes 字节数组
     * @param off   起点
     * @return u32 值（int 视图）
     */
    private static int readIntAt(byte[] bytes, int off) {
        return ((bytes[off] & 0xFF) << 24) | ((bytes[off + 1] & 0xFF) << 16)
                | ((bytes[off + 2] & 0xFF) << 8) | (bytes[off + 3] & 0xFF);
    }
}
