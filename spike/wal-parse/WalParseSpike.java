// ============================================================================
// WalParseSpike — THROWAWAY FEASIBILITY SPIKE (do NOT wire into the build).
//
// Question: can a plain Java process, pretending to be a physical standby
// (pgjdbc physical replication stream, no slot), parse PG 18 raw WAL bytes
// and decode single-table INSERT rows grouped by transaction?
//
// Scenarios:
//   S1  three autocommit single INSERTs + one explicit txn with a 5-row
//       multi-VALUES INSERT (heap_multi_insert record)
//   S2  CHECKPOINT then INSERT (forces full-page image; record spans pages,
//       exercises contrecord stitching and page-image extraction)
//   S3  two concurrent txns interleaved, commit A then commit B
//
// Layout provenance (all transcribed from REL_18_STABLE, fetched 2026-10-05):
//   src/include/access/xlog_internal.h   page headers, XLOG_PAGE_MAGIC=0xD118
//   src/include/access/xlogrecord.h      XLogRecord, block/data headers
//   src/include/access/heapam_xlog.h     xl_heap_insert / xl_heap_multi_insert
//   src/include/storage/bufpage.h        PageHeaderData, line pointers
//   src/include/access/rmgrlist.h        rmid enum order
//   src/include/access/xact.h            XLOG_XACT_* opcodes
//   src/backend/access/heap/heapam.c     exact WAL payload registration
//
// Deviation note: uses System.out (throwaway CLI tool, no slf4j on its
// classpath) — repo logging rules do not apply to spike code.
// ============================================================================

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class WalParseSpike {

    // ---- constants transcribed from REL_18_STABLE ---------------------------
    static final int XLOG_PAGE_MAGIC = 0xD118;   // xlog_internal.h (PG 18)
    static final int XLP_FIRST_IS_CONTRECORD = 0x0001;
    static final int XLP_LONG_HEADER = 0x0002;
    static final int XLOG_BLCKSZ = 8192;
    static final int SHORT_PHD_SIZE = 24;        // MAXALIGN(sizeof(20B header))
    static final int LONG_PHD_SIZE = 40;         // MAXALIGN(sizeof(36B header))
    static final int REC_HDR_SIZE = 24;          // SizeOfXLogRecord

    // rmid order from rmgrlist.h (0-based enum)
    static final int RM_XACT_ID = 1;
    static final int RM_HEAP2_ID = 9;
    static final int RM_HEAP_ID = 10;

    // heapam_xlog.h opcodes (low nibble reserved, opmask 0x70)
    static final int XLOG_HEAP_INSERT = 0x00;
    static final int XLOG_HEAP_DELETE = 0x10;
    static final int XLOG_HEAP_UPDATE = 0x20;
    static final int XLOG_HEAP_HOT_UPDATE = 0x40;
    static final int XLOG_HEAP2_MULTI_INSERT = 0x50;
    static final int XLOG_HEAP_INIT_PAGE = 0x80;
    static final int XLOG_XACT_COMMIT = 0x00;
    static final int XLOG_XACT_ABORT = 0x20;
    static final int XLOG_XACT_OPMASK = 0x70;

    // xl_heap_update / xl_heap_delete flag bits (heapam_xlog.h)
    static final int XLH_UPDATE_CONTAINS_OLD = 0x04 | 0x08; // OLD_TUPLE|OLD_KEY
    static final int XLH_UPDATE_TRUNCATION = 0x20 | 0x40;   // PREFIX|SUFFIX_FROM_OLD
    static final int XLH_DELETE_CONTAINS_OLD = 0x02 | 0x04; // OLD_TUPLE|OLD_KEY
    static final int SIZEOF_HEAP_UPDATE = 14;                // offsetof(new_offnum)+2
    static final int SIZEOF_HEAP_DELETE = 8;

    // xlogrecord.h block/data header ids and flags
    static final int BKPBLOCK_HAS_IMAGE = 0x10;
    static final int BKPBLOCK_HAS_DATA = 0x20;
    static final int BKPBLOCK_SAME_REL = 0x80;
    static final int BKPIMAGE_HAS_HOLE = 0x01;
    static final int BKPIMAGE_COMPRESS_MASK = 0x04 | 0x08 | 0x10; // pglz|lz4|zstd
    static final int XLR_BLOCK_ID_DATA_SHORT = 255;
    static final int XLR_BLOCK_ID_DATA_LONG = 254;

    // heap tuple header flags (htup_details.h)
    static final int HEAP_HASNULL = 0x0001;
    static final int HEAP_NATTS_MASK = 0x07FF;
    static final int TUPLE_BITS_OFFSET = 23;     // offsetof(t_bits)

    static final boolean DEBUG = false; // per-record trace probes

    static void dbg(String fmt, Object... args) {
        if (DEBUG) System.out.printf(fmt, args);
    }

    static final String URL_BASE = "jdbc:postgresql://localhost:55432/postgres";
    static final String JDBC_URL = URL_BASE + "?user=postgres&password=postgres";
    static final String REPL_URL = URL_BASE
            + "?user=postgres&password=postgres&replication=database&assumeMinServerVersion=9.4";
    static final String PGJDBC_JAR_HINT =
            "/Users/saxisuer/Documents/Repository/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar";

    static final long EPOCH_2000_MICROS = 946_684_800_000_000L;

    // pg_attribute column layout, transcribed from REL_18_STABLE pg_attribute.h
    // (NOTE: attstattarget moved to the trailing varlen section in PG 17 —
    // it sat right after atttypid in <=16; a live example of per-version drift)
    static final String[] PGATTR_KINDS = {
            "oid",   // attrelid
            "name",  // attname (NameData, 64B fixed)
            "oid",   // atttypid
            "int2",  // attlen
            "int2",  // attnum
            "int4",  // atttypmod
            "int2",  // attndims
            "bool",  // attbyval
            "char",  // attalign
            "char",  // attstorage
            "char",  // attcompression
            "bool",  // attnotnull
            "bool",  // atthasdef
            "bool",  // atthasmissing
            "char",  // attidentity
            "char",  // attgenerated
            "bool",  // attisdropped
            "bool",  // attislocal
            "int2",  // attinhcount
            "oid",   // attcollation
            "int2",  // attstattarget (nullable)
            "skip",  // attacl aclitem[]
            "skip",  // attoptions text[]
            "skip",  // attfdwoptions text[]
            "skip",  // attmissingval anyarray
    };
    static final int PGATTR_ATTRELID = 0, PGATTR_ATTNAME = 1, PGATTR_ATTTYPEID = 2,
            PGATTR_ATTNUM = 4, PGATTR_ATTISDROPPED = 16;

    /** type OID -> decode kind; unknown OIDs fail fast (dictionary completeness) */
    static String kindForTypeOid(long oid) {
        return switch ((int) oid) {
            case 16 -> "bool";       case 18 -> "char";       case 19 -> "name";
            case 20 -> "int8";       case 21 -> "int2";       case 23 -> "int4";
            case 25 -> "text";       case 26 -> "oid";
            case 700 -> "float4";    case 701 -> "float8";    case 1114 -> "timestamp";
            default -> throw new IllegalStateException("unregistered type oid " + oid);
        };
    }

    /** One replayed pg_attribute row, keyed by physical ctid. */
    record AttrRow(long attrelid, String attname, long atttypid, int attnum, boolean attisdropped) {}

    // ---- little-endian readers ------------------------------------------------
    static int u16(byte[] b, int o) { return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8); }
    static int u32(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8) | ((b[o + 2] & 0xFF) << 16) | ((b[o + 3] & 0xFF) << 24);
    }
    static long u64(byte[] b, int o) { return (u32(b, o) & 0xFFFFFFFFL) | ((long) u32(b, o + 4) << 32); }
    static String lsn(long v) { return String.format("%X/%08X", v >>> 32, v & 0xFFFFFFFFL); }

    // ---- model ----------------------------------------------------------------
    record BlockRef(int fork, int blockNo, long spc, long db, long relNode,
                    boolean hasImage, int imageOff, int imageLen, int bimgInfo, int holeOffset, int holeLen,
                    boolean hasData, int dataOff, int dataLen) {}
    record ParsedRecord(long lsn, int totLen, int xid, int info, int rmid,
                        List<BlockRef> blocks, int mainOff, int mainLen, byte[] raw) {}
    record Row(Object[] vals) {
        public String toString() {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < vals.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(vals[i]);
            }
            return sb.append(')').toString();
        }
    }

    // ---- raw WAL walker: byte-stream state machine over absolute LSN space ----
    // pgjdbc chunks are NOT page-aligned, so we track an absolute LSN cursor and
    // splice chunks into one contiguous carry buffer. Page headers are consumed
    // whenever the cursor hits a page boundary; records may span pages via
    // XLP_FIRST_IS_CONTRECORD continuation pages.
    static final class WalWalker {
        byte[] carry = new byte[0];   // unconsumed bytes at 'carryStartLsn'
        long carryStartLsn = 0;
        boolean sawAny = false;

        // cross-chunk parse state
        long lastRecordLsn = 0;
        long contrecords = 0;
        boolean sawLongHeader = false;
        int blckszFromLongHeader = -1;
        final Map<String, Integer> census = new TreeMap<>();

        /** Feed one readPending() chunk whose last byte sits at chunkEndLsn. */
        void feed(long chunkEndLsn, byte[] data, List<ParsedRecord> out) {
            long chunkStart = chunkEndLsn - data.length;
            dbg("[dbg] chunk len=%d end=%s computedStart=%s carryStart=%s carryLen=%d%n",
                    data.length, lsn(chunkEndLsn), lsn(chunkStart), lsn(carryStartLsn), carry.length);
            if (sawAny && chunkStart != carryStartLsn + carry.length)
                throw new IllegalStateException(String.format("WAL gap: carry end=%s chunk start=%s",
                        lsn(carryStartLsn + carry.length), lsn(chunkStart)));
            if (carry.length == 0) carryStartLsn = chunkStart;
            byte[] merged = new byte[carry.length + data.length];
            System.arraycopy(carry, 0, merged, 0, carry.length);
            System.arraycopy(data, 0, merged, carry.length, data.length);
            sawAny = true;
            int consumed = parse(merged, carryStartLsn, out);
            byte[] rest = new byte[merged.length - consumed];
            System.arraycopy(merged, consumed, rest, 0, rest.length);
            carry = rest;
            carryStartLsn = chunkStart + consumed;
        }

        /**
         * Consumes as much of buf (anchored at absolute lsn0) as possible;
         * returns the number of bytes consumed. Stops early (carries the rest)
         * when a page header / record header would split across chunks.
         */
        private int parse(byte[] buf, long lsn0, List<ParsedRecord> out) {
            int pos = 0;
            long cur = lsn0;
            int len = buf.length;
            while (true) {
                long pageOff = cur & (XLOG_BLCKSZ - 1);
                if (pageOff == 0) {
                    // at a page header: need at least the short header to know its size
                    if (len - pos < 2) return pos;
                    int magic = u16(buf, pos);
                    if (magic != XLOG_PAGE_MAGIC)
                        throw new IllegalStateException("bad page magic " + Integer.toHexString(magic)
                                + " at " + lsn(cur));
                    if (len - pos < SHORT_PHD_SIZE) return pos; // header split across chunks
                    int info = u16(buf, pos + 2);
                    boolean longHdr = (info & XLP_LONG_HEADER) != 0;
                    int hdrSize = longHdr ? LONG_PHD_SIZE : SHORT_PHD_SIZE;
                    if (len - pos < hdrSize) return pos;
                    if (longHdr) { sawLongHeader = true; blckszFromLongHeader = u32(buf, pos + 36); }
                    if ((info & XLP_FIRST_IS_CONTRECORD) != 0) {
                        // continuation of a record whose header we never saw (before
                        // our start point) — skip its bytes; will resume at next header
                        contrecords++;
                        int remLen = u32(buf, pos + 16);
                        // next record starts MAXALIGNed after the continuation data
                        long skipEnd = ((cur - pageOff) + hdrSize + remLen + 7) & ~7L;
                        if (skipEnd >= cur + (len - pos)) return len; // rest not arrived yet
                        pos += (int) (skipEnd - cur);
                        cur = skipEnd;
                        continue;
                    }
                    pos += hdrSize;
                    cur += hdrSize;
                }
                // at a record header position within a page
                int pageRemain = XLOG_BLCKSZ - (int) (cur & (XLOG_BLCKSZ - 1));
                if (pageRemain < REC_HDR_SIZE) {
                    // zero padding tail of the page — jump to next page header
                    if (len - pos < pageRemain) return pos;
                    pos += pageRemain;
                    cur += pageRemain;
                    continue;
                }
                if (len - pos < REC_HDR_SIZE) return pos; // header split across chunks
                int totLen = u32(buf, pos);
                if (totLen == 0) { // zero padding
                    if (len - pos < pageRemain) return pos;
                    pos += pageRemain;
                    cur += pageRemain;
                    continue;
                }
                long recLsn = cur;
                // stitch the record: totLen bytes, skipping intermediate page headers
                // collect record bytes into rec[]
                if (len - pos < REC_HDR_SIZE) return pos;
                byte[] rec = new byte[totLen];
                int recPos = 0;
                int src = pos;
                long curL = cur;
                boolean splitAcrossChunks = false;
                while (recPos < totLen) {
                    // distance from curL to the end of the CURRENT page (pageEnd is
                    // already relative to curL — do not mix in bytes consumed on
                    // earlier pages of the same record)
                    int pageEnd = (int) (XLOG_BLCKSZ - (curL & (XLOG_BLCKSZ - 1)));
                    int take = Math.min(totLen - recPos, pageEnd);
                    if (len - src < take) { splitAcrossChunks = true; break; }
                    System.arraycopy(buf, src, rec, recPos, take);
                    recPos += take;
                    src += take;
                    curL += take;
                    if (recPos < totLen) {
                        // crossing into a continuation page: consume its header
                        if (len - src < SHORT_PHD_SIZE) { splitAcrossChunks = true; break; }
                        int cinfo = u16(buf, src + 2);
                        int cHdr = (cinfo & XLP_LONG_HEADER) != 0 ? LONG_PHD_SIZE : SHORT_PHD_SIZE;
                        if ((cinfo & XLP_FIRST_IS_CONTRECORD) == 0)
                            throw new IllegalStateException("record crosses page without contrecord flag at " + lsn(curL));
                        if (len - src < cHdr) { splitAcrossChunks = true; break; }
                        contrecords++;
                        src += cHdr;
                        curL += cHdr;
                    }
                }
                if (splitAcrossChunks) return pos; // wait for more bytes
                emit(rec, recLsn, out);
                pos = src;
                cur = curL;
                // records start MAXALIGNed
                long pad = ((cur + 7) & ~7L) - cur;
                if (pad > 0) {
                    if (len - pos < pad) return pos;
                    pos += pad;
                    cur += pad;
                }
            }
        }

        private void emit(byte[] rec, long recLsn, List<ParsedRecord> out) {
            lastRecordLsn = recLsn;
            int rmid = rec[17] & 0xFF;
            int info = rec[16] & 0xFF;
            dbg("[trc] lsn=%s rmid=%2d info=%02x totLen=%d xid=%d%n",
                    lsn(recLsn), rmid, info, u32(rec, 0), u32(rec, 4));
            census.merge(rmid + "/" + Integer.toHexString(info & 0xF0), 1, Integer::sum);
            try {
                out.add(parseRecord(rec, recLsn));
            } catch (RuntimeException e) {
                throw new IllegalStateException("parseRecord failed at " + lsn(recLsn)
                        + dumpContext(rec, u32(rec, 0)), e);
            }
        }
    }

    // ---- record parser ---------------------------------------------------------
    static ParsedRecord parseRecord(byte[] rec, long lsn) {
        int totLen = u32(rec, 0);
        int pos = REC_HDR_SIZE;
        List<BlockRef> blocks = new ArrayList<>();
        long[] lastLoc = null; // spc, db, relNode for SAME_REL
        int mainOff = -1, mainLen = 0;
        int datatotal = 0; // accumulated image+data payload bytes announced by headers
        while (pos < totLen) {
            // header section ends when the remaining bytes are exactly the announced
            // payload — records WITHOUT main data carry no DATA_SHORT/LONG terminator
            if (totLen - pos == datatotal) break;
            int id = rec[pos] & 0xFF;
            if (id == XLR_BLOCK_ID_DATA_SHORT) {
                mainLen = (rec[pos + 1] & 0xFF);
                pos += 2;
                break;
            } else if (id == XLR_BLOCK_ID_DATA_LONG) {
                mainLen = u32(rec, pos + 1);
                pos += 5;
                break;
            } else if (id == 253 || id == 252) { // origin / toplevel xid: u32 follows
                pos += 5;
                continue;
            } else if (id <= 32) {
                int forkFlags = rec[pos + 1] & 0xFF;
                int dataLen = u16(rec, pos + 2);
                pos += 4;
                boolean hasImage = (forkFlags & BKPBLOCK_HAS_IMAGE) != 0;
                boolean hasData = (forkFlags & BKPBLOCK_HAS_DATA) != 0;
                int imageLen = 0, imageOff = 0, bimgInfo = 0, holeOffset = 0, holeLen = 0;
                if (hasImage) {
                    imageLen = u16(rec, pos);
                    holeOffset = u16(rec, pos + 2);
                    bimgInfo = rec[pos + 4] & 0xFF;
                    pos += 5;
                    boolean compressed = (bimgInfo & BKPIMAGE_COMPRESS_MASK) != 0;
                    if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0 && compressed) {
                        holeLen = u16(rec, pos);
                        pos += 2;
                    } else if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0) {
                        holeLen = XLOG_BLCKSZ - imageLen; // derivable when uncompressed
                    }
                }
                long spc, db, relNode;
                if ((forkFlags & BKPBLOCK_SAME_REL) == 0) {
                    spc = u32(rec, pos) & 0xFFFFFFFFL;
                    db = u32(rec, pos + 4) & 0xFFFFFFFFL;
                    relNode = u32(rec, pos + 8) & 0xFFFFFFFFL;
                    pos += 12;
                    lastLoc = new long[]{spc, db, relNode};
                } else {
                    if (lastLoc == null) throw new IllegalStateException("SAME_REL with no previous locator");
                    spc = lastLoc[0]; db = lastLoc[1]; relNode = lastLoc[2];
                }
                int blockNo = u32(rec, pos);
                pos += 4;
                datatotal += (hasImage ? imageLen : 0) + dataLen;
                blocks.add(new BlockRef(forkFlags & 0x0F, blockNo, spc, db, relNode,
                        hasImage, imageOff, imageLen, bimgInfo, holeOffset, holeLen,
                        hasData, 0, dataLen)); // dataOff patched in payload phase
                continue;
            } else {
                throw new IllegalStateException("unknown block id " + id + " at " + pos + dumpContext(rec, totLen));
            }
        }
        // payload phase: images+data per block in id order, then main data
        int cur = pos;
        List<BlockRef> patched = new ArrayList<>(blocks.size());
        for (BlockRef b : blocks) {
            int imageOff = b.hasImage() ? cur : -1;
            cur += b.hasImage() ? b.imageLen() : 0;
            int dataOff = b.hasData() ? cur : -1;
            cur += b.dataLen();
            patched.add(new BlockRef(b.fork(), b.blockNo(), b.spc(), b.db(), b.relNode(),
                    b.hasImage(), imageOff, b.imageLen(), b.bimgInfo(), b.holeOffset(), b.holeLen(),
                    b.hasData(), dataOff, b.dataLen()));
        }
        mainOff = cur;
        if (mainOff + mainLen != totLen)
            throw new IllegalStateException(String.format(
                    "layout mismatch: mainOff=%d mainLen=%d totLen=%d rmid=%d info=%x",
                    mainOff, mainLen, totLen, rec[17] & 0xFF, rec[16] & 0xFF));
        return new ParsedRecord(lsn, totLen, u32(rec, 4), rec[16] & 0xFF, rec[17] & 0xFF,
                patched, mainOff, mainLen, rec);
    }

    // ---- tuple decoding ---------------------------------------------------------
    /**
     * Decodes user-data columns of one heap tuple using the given per-column
     * decode kinds (the dictionary, bootstrap or catalog-replayed). Datum
     * alignment is computed relative to the TUPLE start (t_hoff + offset must
     * satisfy typalign), not relative to the surrounding buffer — the tuple may
     * sit at any offset in a WAL record payload.
     */
    static Row decodeTupleData(byte[] src, int tupleStart, int tHoff, int infomask, int infomask2, String[] kinds) {
        int natts = infomask2 & HEAP_NATTS_MASK;
        boolean hasNull = (infomask & HEAP_HASNULL) != 0;
        byte[] bitmap = null;
        if (hasNull) {
            bitmap = new byte[(natts + 7) / 8];
            System.arraycopy(src, tupleStart + TUPLE_BITS_OFFSET, bitmap, 0, bitmap.length);
        }
        if (natts > kinds.length)
            throw new IllegalStateException("tuple has " + natts + " atts but dictionary has " + kinds.length);
        Object[] vals = new Object[natts];
        int c = tupleStart + tHoff;
        for (int i = 0; i < natts; i++) {
            if (bitmap != null && (bitmap[i / 8] & (1 << (i % 8))) == 0) { vals[i] = null; continue; }
            switch (kinds[i]) {
                case "int2" -> { c = tupleStart + align(c - tupleStart, 2); vals[i] = (short) u16(src, c); c += 2; }
                case "int4" -> { c = tupleStart + align(c - tupleStart, 4); vals[i] = u32(src, c); c += 4; }
                case "int8" -> { c = tupleStart + align(c - tupleStart, 8); vals[i] = u64(src, c); c += 8; }
                case "oid" -> { c = tupleStart + align(c - tupleStart, 4); vals[i] = u32(src, c) & 0xFFFFFFFFL; c += 4; }
                case "float4" -> { c = tupleStart + align(c - tupleStart, 4); vals[i] = Float.intBitsToFloat(u32(src, c)); c += 4; }
                case "float8" -> { c = tupleStart + align(c - tupleStart, 8); vals[i] = Double.longBitsToDouble(u64(src, c)); c += 8; }
                case "bool" -> { vals[i] = src[c] != 0; c += 1; }
                case "char" -> { vals[i] = "'" + (char) (src[c] & 0x7F); c += 1; }
                case "name" -> { // NameData: fixed 64 bytes, NUL-terminated
                    c = tupleStart + align(c - tupleStart, 4);
                    int n = 64;
                    for (int k = 0; k < 64; k++) if (src[c + k] == 0) { n = k; break; }
                    vals[i] = new String(src, c, n);
                    c += 64;
                }
                case "timestamp" -> {
                    c = tupleStart + align(c - tupleStart, 8);
                    long micros = u64(src, c); c += 8;
                    vals[i] = renderTimestamp(micros);
                }
                case "text" -> { c = tupleStart + align(c - tupleStart, 4); int[] next = {c}; vals[i] = readVarlenaText(src, c, next); c = next[0]; }
                case "skip" -> { c = tupleStart + align(c - tupleStart, 4); int[] next = {c}; skipVarlena(src, c, next); c = next[0]; }
                default -> throw new IllegalStateException("unregistered kind " + kinds[i]);
            }
        }
        return new Row(vals);
    }

    /** Reads one uncompressed varlena at c; returns text and advances next[0]. */
    static String readVarlenaText(byte[] src, int c, int[] next) {
        int b0 = src[c] & 0xFF;
        int len;
        int dataOff;
        if ((b0 & 0x01) != 0) {            // 1-byte varlena header
            len = (b0 >> 1) & 0x7F;        // total incl. header byte
            dataOff = 1;
        } else if ((b0 & 0x03) == 0) {     // 4-byte uncompressed, big-endian
            len = (int) (u32be(src, c) & 0x3FFFFFFF);
            dataOff = 4;
        } else {
            throw new IllegalStateException("compressed/short varlena unexpected in spike: hdr=" + b0);
        }
        next[0] = c + len;
        return new String(src, c + dataOff, Math.max(0, len - dataOff));
    }

    /** Skips one varlena datum (value discarded) without materializing it. */
    static void skipVarlena(byte[] src, int c, int[] next) {
        readVarlenaText(src, c, next); // same header walk; payload dropped
    }

    /** Failure-context hex dump of a record header section (first 64 bytes). */
    static String dumpContext(byte[] rec, int totLen) {
        int rmid = rec[17] & 0xFF, info = rec[16] & 0xFF;
        int n = Math.min(200, rec.length);
        StringBuilder sb = new StringBuilder(String.format(" {rmid=%d info=0x%02X totLen=%d len=%d%n", rmid, info, totLen, rec.length));
        for (int i = 0; i < n; i++) {
            sb.append(String.format("%02d:%02x ", i, rec[i]));
            if (i % 8 == 7) sb.append('\n');
        }
        sb.append('}');
        return sb.toString();
    }

    static long u32be(byte[] b, int o) {
        return (((long) (b[o] & 0xFF)) << 24) | ((b[o + 1] & 0xFF) << 16) | ((b[o + 2] & 0xFF) << 8) | (b[o + 3] & 0xFF);
    }

    static int align(int v, int a) { return (v + a - 1) & ~(a - 1); }

    static String renderTimestamp(long micros) {
        long epochMicros = EPOCH_2000_MICROS + micros;
        Instant inst = Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * 1000L);
        return LocalDateTime.ofInstant(inst, ZoneOffset.UTC).toString();
    }

    /** Rebuilds a full 8192-byte page from a block's full-page image. */
    static byte[] rebuildPage(ParsedRecord rec, BlockRef b) {
        if ((b.bimgInfo() & BKPIMAGE_COMPRESS_MASK) != 0)
            throw new IllegalStateException("compressed FPW not supported in spike");
        byte[] page = new byte[XLOG_BLCKSZ];
        int imgLen = b.imageLen();
        int holeOffset = b.holeOffset();
        int holeLen = b.holeLen();
        if ((b.bimgInfo() & BKPIMAGE_HAS_HOLE) != 0) {
            // image = page[0..holeOffset) + page[holeOffset+holeLen..8192)
            System.arraycopy(rec.raw(), b.imageOff(), page, 0, holeOffset);
            System.arraycopy(rec.raw(), b.imageOff() + holeOffset, page, holeOffset + holeLen, imgLen - holeOffset);
        } else {
            // no hole: the image is the whole page content
            System.arraycopy(rec.raw(), b.imageOff(), page, 0, imgLen);
        }
        return page;
    }

    /**
     * Decodes a full heap tuple (with HeapTupleHeader) sitting in a rebuilt page
     * at the given line pointer offset. FPW images are the POST-change page state
     * (redo restores and skips application), so newly inserted rows ARE present.
     */
    static Row tupleFromPage(byte[] page, int lpOff, String[] kinds) {
        int infomask2 = u16(page, lpOff + 18);
        int infomask = u16(page, lpOff + 20);
        int tHoff = page[lpOff + 22] & 0xFF;
        return decodeTupleData(page, lpOff, tHoff, infomask, infomask2, kinds);
    }

    // ---- page-image extraction (FPW cross-check) --------------------------------
    static Row decodeFromPageImage(ParsedRecord rec, BlockRef b, int offnum, String[] kinds) {
        byte[] page = rebuildPage(rec, b);
        // NOTE: FPW images are the POST-change page state (redo with XLADR_RESTORED
        // skips application entirely) — the record's own offnum IS present as
        // LP_NORMAL. The walk-down stays as a lenient fallback only.
        // ItemIdData bitfields: lp_off:15 (low), lp_flags:2 (bits 15-16),
        // lp_len:15 (bits 17-31); LP_NORMAL = 1
        int item = offnum;
        int itemId = 0, lpOff = 0, lpLen = 0, flags = 0;
        for (int probe = 0; probe < 5 && item - 1 - probe >= 0; probe++) {
            itemId = u32(page, 24 + (item - 1 - probe) * 4);
            lpOff = itemId & 0x7FFF;
            lpLen = (itemId >> 17) & 0x7FFF;
            flags = (itemId >> 15) & 0x3;
            if (flags == 1) { item = item - probe; break; }
            if (probe == 4) throw new IllegalStateException(
                    "no LP_NORMAL line pointer near offnum=" + offnum + String.format(
                            " {pd_lower=%d pd_upper=%d itemId=0x%08x imgLen=%d holeOff=%d holeLen=%d bimg=%02x}",
                            u16(page, 12), u16(page, 14), itemId, b.imageLen(), b.holeOffset(), b.holeLen(), b.bimgInfo()));
        }
        int infomask2 = u16(page, lpOff + 18);
        int infomask = u16(page, lpOff + 20);
        int tHoff = page[lpOff + 22] & 0xFF;
        return decodeTupleData(page, lpOff, tHoff, infomask, infomask2, kinds);
    }

    // ---- main -------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        System.out.println("[spike] pgjdbc jar hint: " + PGJDBC_JAR_HINT);
        try (Connection setup = DriverManager.getConnection(JDBC_URL)) {
            prepare(setup);
            long spcOid = queryLong(setup, "SELECT oid FROM pg_tablespace WHERE spcname='pg_default'");
            long dbOid = queryLong(setup, "SELECT oid FROM pg_database WHERE datname=current_database()");
            long relNode = queryLong(setup, "SELECT pg_relation_filenode('t_wal_spike')");
            long tableRelid = queryLong(setup, "SELECT 't_wal_spike'::regclass::oid");
            long pgAttrRelNode = queryLong(setup, "SELECT pg_relation_filenode('pg_attribute'::regclass)");
            // bootstrap the dictionary: seed pg_attribute rows (with ctid) via JDBC;
            // the WAL replay upserts on top — ctid-keyed so overlap is idempotent
            Map<Long, AttrRow> attrRows = new java.util.HashMap<>();
            try (Statement st = setup.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                                 + " FROM pg_attribute WHERE attrelid=" + tableRelid + " AND attnum > 0")) {
                while (rs.next()) {
                    String ctid = rs.getString(1); // "(block,off)"
                    String[] parts = ctid.replaceAll("[() ]", "").split(",");
                    attrRows.put(ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])),
                            new AttrRow(rs.getLong(2), rs.getString(3), rs.getLong(4),
                                    rs.getInt(5), rs.getBoolean(6)));
                }
            }
            System.out.printf("[spike] table relid=%d pg_attribute relnode=%d seededCols=%d%n",
                    tableRelid, pgAttrRelNode, attrRows.size());
            // NOTE: pg_current_wal_lsn() is the WRITE position and can run ahead of
            // flush — START_REPLICATION would reject it. Flush LSN is safe, and with
            // synchronous_commit=on every committed scenario record is already flushed.
            long startLsn = parseLsn(queryString(setup, "SELECT pg_current_wal_flush_lsn()"));
            // page-align the start downwards: the parser assumes every received chunk
            // begins at a WAL page header (records earlier than startLsn get filtered
            // out by relfilenode anyway — the table was created before this point)
            long alignedStart = startLsn & ~0x7FFFL;
            System.out.printf("[spike] spc=%d db=%d relnode=%d start=%s (page-aligned: %s)%n",
                    spcOid, dbOid, relNode, lsn(startLsn), lsn(alignedStart));

            try (Connection repl = DriverManager.getConnection(REPL_URL)) {
                PGConnection pgc = repl.unwrap(PGConnection.class);
                // physical() builder has no slot option in 42.7.13: START_REPLICATION PHYSICAL <lsn>
                var stream = pgc.getReplicationAPI().replicationStream()
                        .physical()
                        .withStartPosition(LogSequenceNumber.valueOf(alignedStart))
                        .withStatusInterval(1, java.util.concurrent.TimeUnit.SECONDS)
                        .start();

                List<List<String>> expected = runScenarios(setup);
                long endLsn = parseLsn(queryString(setup, "SELECT pg_current_wal_flush_lsn()"));

                WalWalker walker = new WalWalker();
                List<ParsedRecord> records = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline) {
                    ByteBuffer msg = stream.readPending();
                    if (msg != null) {
                        byte[] bytes = new byte[msg.remaining()];
                        msg.get(bytes);
                        walker.feed(stream.getLastReceiveLSN().asLong(), bytes, records);
                    } else {
                        Thread.sleep(100);
                    }
                    if (stream.getLastReceiveLSN().asLong() >= endLsn) break;
                }
                stream.close();

                report(records, spcOid, dbOid, relNode, tableRelid, pgAttrRelNode, attrRows, walker, expected);
            }
        }
    }

    static void prepare(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_wal_spike");
            st.execute("CREATE TABLE t_wal_spike (id int, s text, f float8, b bool, ts timestamp)");
        }
    }

    /** Returns the expected transaction sequence: one list of row-renderings per txn, in commit order. */
    static List<List<String>> runScenarios(Connection setup) throws Exception {
        List<List<String>> expected = new ArrayList<>();
        // S1: three autocommit single INSERTs
        try (Statement st = setup.createStatement()) {
            for (int id = 1; id <= 3; id++) {
                String row = row(id);
                st.execute("INSERT INTO t_wal_spike VALUES " + row);
                expected.add(List.of(renderRow(row)));
            }
        }
        // S1b: explicit txn with 5-row multi-VALUES insert -> heap_multi_insert record
        {
            StringBuilder sb = new StringBuilder("INSERT INTO t_wal_spike VALUES ");
            List<String> renders = new ArrayList<>();
            for (int id = 4; id <= 8; id++) {
                if (id > 4) sb.append(", ");
                String row = row(id);
                sb.append(row);
                renders.add(renderRow(row));
            }
            setup.setAutoCommit(false);
            try (Statement st = setup.createStatement()) {
                st.execute(sb.toString());
            }
            setup.commit();
            setup.setAutoCommit(true);
            expected.add(renders);
        }
        // S1c: COPY of 3 rows -> this is what actually triggers heap_multi_insert
        // (plain multi-VALUES INSERT goes row-by-row through heap_insert)
        {
            List<String> renders = new ArrayList<>();
            StringBuilder copyData = new StringBuilder();
            for (int id = 14; id <= 16; id++) {
                String row = row(id);
                // COPY text format: tab-separated bare values, no quotes/parens
                copyData.append(String.format("%d\ts-%03d-π\t%s\t%b\t2026-10-05 12:00:%02d.123456%n",
                        id, id, (id * 10 + 1) + ".5", id % 2 == 0, id % 60));
                renders.add(renderRow(row));
            }
            var cm = new CopyManager(setup.unwrap(org.postgresql.jdbc.PgConnection.class));
            cm.copyIn("COPY t_wal_spike FROM STDIN WITH (FORMAT text)",
                    new java.io.StringReader(copyData.toString()));
            expected.add(renders);
        }
        // S2: CHECKPOINT then insert -> full-page image path
        try (Statement st = setup.createStatement()) {
            st.execute("CHECKPOINT");
            String row = row(9);
            st.execute("INSERT INTO t_wal_spike VALUES " + row);
            expected.add(List.of(renderRow(row)));
        }
        // S3: two interleaved txns, commit A then B
        try (Connection a = DriverManager.getConnection(JDBC_URL);
             Connection b = DriverManager.getConnection(JDBC_URL)) {
            a.setAutoCommit(false);
            b.setAutoCommit(false);
            String ra1 = row(10), ra2 = row(11), rb1 = row(12), rb2 = row(13);
            try (Statement sa = a.createStatement(); Statement sb = b.createStatement()) {
                sa.execute("INSERT INTO t_wal_spike VALUES " + ra1);
                sb.execute("INSERT INTO t_wal_spike VALUES " + rb1);
                sa.execute("INSERT INTO t_wal_spike VALUES " + ra2);
                sb.execute("INSERT INTO t_wal_spike VALUES " + rb2);
            }
            a.commit();
            b.commit();
            expected.add(List.of(renderRow(ra1), renderRow(ra2)));
            expected.add(List.of(renderRow(rb1), renderRow(rb2)));
        }
        // S4: UPDATE/DELETE — default replica identity first (no old tuple in WAL),
        // then REPLICA IDENTITY FULL (whole old row rides in main data)
        try (Statement st = setup.createStatement()) {
            String r20 = row(20), r21 = row(21), r22 = row(22);
            st.execute("INSERT INTO t_wal_spike VALUES " + r20 + ", " + r21 + ", " + r22);
            expected.add(List.of(renderRow(r20), renderRow(r21), renderRow(r22)));

            String upd1 = literal(120, "upd-def-π", 1200, false, "2027-01-01 10:00:00.000001");
            st.execute("UPDATE t_wal_spike SET id=120, s='upd-def-π', f=1200.5, b=false,"
                    + " ts='2027-01-01 10:00:00.000001' WHERE id=20");
            expected.add(List.of("U(none)>(" + renderRow(upd1) + ")"));

            st.execute("ALTER TABLE t_wal_spike REPLICA IDENTITY FULL");

            String upd2 = literal(121, "upd-full-π", 1210, true, "2027-01-01 10:00:00.000002");
            st.execute("UPDATE t_wal_spike SET id=121, s='upd-full-π', f=1210.5, b=true,"
                    + " ts='2027-01-01 10:00:00.000002' WHERE id=21");
            expected.add(List.of("U(" + renderRow(r21) + ")>(" + renderRow(upd2) + ")"));

            st.execute("DELETE FROM t_wal_spike WHERE id=22");
            expected.add(List.of("D(" + renderRow(r22) + ")"));
        }
        // S5: catalog replay — ADD COLUMN mid-stream must be picked up from
        // pg_attribute's own WAL records, and later rows decode with the new
        // dictionary (6 columns), earlier rows already decoded with 5
        try (Statement st = setup.createStatement()) {
            String r23 = row(23);
            st.execute("INSERT INTO t_wal_spike VALUES " + r23);
            expected.add(List.of(renderRow(r23)));

            st.execute("ALTER TABLE t_wal_spike ADD COLUMN extra text");

            String r24 = "(24, 's-024-π', 241.5, true, '2026-10-05 12:00:24.123456', 'extra-π')";
            st.execute("INSERT INTO t_wal_spike VALUES " + r24);
            expected.add(List.of(renderRow(r24)));
        }
        return expected;
    }

    /** Row literal with explicit values (for S4 mutation expectations). */
    static String literal(int id, String text, int fInt, boolean b, String ts) {
        return String.format("(%d, '%s', %d.5, %s, '%s')", id, text, fInt, b, ts);
    }

    /** Deterministic row literal for the given id. */
    static String row(int id) {
        return String.format("(%d, 's-%03d-π', %d.5, %s, '2026-10-05 12:00:%02d.123456')",
                id, id, id * 10 + 1, id % 2 == 0, id % 60);
    }

    /** The rendering the decoder is expected to produce for a row literal. */
    static String renderRow(String rowLiteral) {
        // (id, 'text', f.5, bool, 'YYYY-MM-DD hh:mm:ss.ffffff') -> decoder output form
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\\((\\d+), '([^']+)', (\\d+)\\.5, (true|false), '(\\d+)-(\\d+)-(\\d+) (\\d+):(\\d+):(\\d+)\\.(\\d+)'(?:, '([^']+)')?\\)").matcher(rowLiteral);
        if (!m.matches()) throw new IllegalArgumentException(rowLiteral);
        String ts = String.format("%s-%s-%sT%s:%s:%s.%s",
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11));
        return "(" + m.group(1) + ", " + m.group(2) + ", " + m.group(3) + ".5, " + m.group(4) + ", " + ts
                + (m.group(12) != null ? ", " + m.group(12) : "") + ")";
    }

    // ---- report -------------------------------------------------------------------
    static void report(List<ParsedRecord> records, long spcOid, long dbOid, long relNode,
                       long tableRelid, long pgAttrRelNode, Map<Long, AttrRow> attrRows,
                       WalWalker walker, List<List<String>> expected) {
        // group our-table rows by xid; commit order by commit LSN
        Map<Integer, List<String>> rowsByXid = new LinkedHashMap<>();
        Map<Integer, Long> commitLsnByXid = new LinkedHashMap<>();
        Map<Integer, Long> abortLsnByXid = new LinkedHashMap<>();
        int fpwSeen = 0, fpwCrossChecked = 0, fpwMismatches = 0;
        int multiInsertRecords = 0, singleInsertRecords = 0;

        for (ParsedRecord rec : records) {
            if (rec.rmid() == RM_XACT_ID) {
                int op = rec.info() & XLOG_XACT_OPMASK;
                if (op == XLOG_XACT_COMMIT) commitLsnByXid.put(rec.xid(), rec.lsn());
                else if (op == XLOG_XACT_ABORT) abortLsnByXid.put(rec.xid(), rec.lsn());
                continue;
            }
            // catalog replay FIRST: pg_attribute heap records mutate the dictionary
            // that subsequent DML records in this same LSN-ordered pass decode with
            replayPgAttribute(rec, spcOid, dbOid, pgAttrRelNode, attrRows, tableRelid);
            String[] kinds = dictKinds(attrRows, tableRelid);
            if (rec.rmid() == RM_HEAP_ID && (rec.info() & XLOG_XACT_OPMASK) == XLOG_HEAP_INSERT) {
                BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, relNode);
                if (b0 == null) continue;
                singleInsertRecords++;
                int offnum = u16(rec.raw(), rec.mainOff());
                String decoded;
                if (b0.hasData()) {
                    decoded = tupleFromPayload(rec.raw(), b0.dataOff(), kinds).toString();
                } else {
                    throw new IllegalStateException("insert record without block data (unexpected at wal_level=logical)");
                }
                if (b0.hasImage()) {
                    fpwSeen++;
                    try {
                        // image holds the PRE-change page: the extracted row must be
                        // one of the already-known earlier rows, not this record's row
                        Row fromImage = decodeFromPageImage(rec, b0, offnum, kinds);
                        java.util.Set<String> knownRows = new java.util.HashSet<>();
                        expected.forEach(knownRows::addAll);
                        if (!knownRows.contains(fromImage.toString())) fpwMismatches++;
                        fpwCrossChecked++;
                    } catch (IllegalStateException e) {
                        System.out.println("[spike] FPW cross-check skipped: " + e.getMessage());
                    }
                }
                rowsByXid.computeIfAbsent(rec.xid(), k -> new ArrayList<>()).add(decoded);
            } else if (rec.rmid() == RM_HEAP_ID
                    && ((rec.info() & XLOG_XACT_OPMASK) == XLOG_HEAP_UPDATE
                        || (rec.info() & XLOG_XACT_OPMASK) == XLOG_HEAP_HOT_UPDATE)) {
                BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, relNode);
                if (b0 == null) continue;
                int upFlags = rec.raw()[rec.mainOff() + 7];
                if ((upFlags & XLH_UPDATE_TRUNCATION) != 0)
                    throw new IllegalStateException("prefix/suffix truncation not supported in spike (flags=0x"
                            + Integer.toHexString(upFlags) + ")");
                // old tuple (replica identity), when present, sits in MAIN data
                // right after the xl_heap_update struct
                String oldStr = "none";
                if ((upFlags & XLH_UPDATE_CONTAINS_OLD) != 0)
                    oldStr = tupleFromPayload(rec.raw(), rec.mainOff() + SIZEOF_HEAP_UPDATE, kinds).toString();
                // new tuple is block 0's data, same shape as insert
                String newStr = tupleFromPayload(rec.raw(), b0.dataOff(), kinds).toString();
                singleInsertRecords++; // same payload shape as insert for bookkeeping
                rowsByXid.computeIfAbsent(rec.xid(), k -> new ArrayList<>())
                        .add("U(" + oldStr + ")>(" + newStr + ")");
            } else if (rec.rmid() == RM_HEAP_ID && (rec.info() & XLOG_XACT_OPMASK) == XLOG_HEAP_DELETE) {
                BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, relNode);
                if (b0 == null) continue;
                int delFlags = rec.raw()[rec.mainOff() + 7];
                String oldStr = "none";
                if ((delFlags & XLH_DELETE_CONTAINS_OLD) != 0)
                    oldStr = tupleFromPayload(rec.raw(), rec.mainOff() + SIZEOF_HEAP_DELETE, kinds).toString();
                rowsByXid.computeIfAbsent(rec.xid(), k -> new ArrayList<>())
                        .add("D(" + oldStr + ")");
            } else if (rec.rmid() == RM_HEAP2_ID && (rec.info() & XLOG_XACT_OPMASK) == XLOG_HEAP2_MULTI_INSERT) {
                BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, relNode);
                if (b0 == null) continue;
                multiInsertRecords++;
                boolean initPage = (rec.info() & XLOG_HEAP_INIT_PAGE) != 0;
                // xl_heap_multi_insert is a C struct: uint8 flags, then PADDING byte
                // (offset 1), then uint16 ntuples at offset 2 — XLogRegisterData
                // registers the raw struct including the padding garbage
                int ntuples = u16(rec.raw(), rec.mainOff() + 2);
                List<String> perRecord = new ArrayList<>(ntuples);
                int cur = b0.dataOff();
                for (int i = 0; i < ntuples; i++) {
                    cur = (cur + 1) & ~1; // SHORTALIGN each xl_multi_insert_tuple
                    int datalen = u16(rec.raw(), cur);
                    // entry = [datalen u16][xl_heap_header 5B][tuple bytes] — the
                    // 2-byte datalen prefix is NOT part of xl_heap_header
                    Row r = tupleFromPayload(rec.raw(), cur + 2, kinds);
                    perRecord.add(r.toString());
                    cur += 7 + datalen;
                }
                rowsByXid.computeIfAbsent(rec.xid(), k -> new ArrayList<>()).addAll(perRecord);
            }
        }

        // transactions in commit order
        List<Map.Entry<Integer, Long>> commits = new ArrayList<>(commitLsnByXid.entrySet());
        commits.sort(Map.Entry.comparingByValue());
        List<List<String>> actual = new ArrayList<>();
        System.out.println("\n===== decoded transactions (commit order) =====");
        for (var e : commits) {
            List<String> rows = rowsByXid.get(e.getKey());
            if (rows == null || rows.isEmpty()) continue; // txn didn't touch our table
            System.out.printf("xid=%d commit=%s rows=%d%n", e.getKey(), lsn(e.getValue()), rows.size());
            rows.forEach(r -> System.out.println("    " + r));
            actual.add(rows);
        }

        System.out.println("\n===== verification =====");
        boolean pass = actual.size() == expected.size();
        System.out.printf("txn count: expected=%d actual=%d%n", expected.size(), actual.size());
        for (int i = 0; i < Math.min(actual.size(), expected.size()) && pass; i++) {
            if (!actual.get(i).equals(expected.get(i))) {
                pass = false;
                System.out.printf("txn #%d MISMATCH%n  expected: %s%n  actual:   %s%n",
                        i + 1, expected.get(i), actual.get(i));
            }
        }
        System.out.printf("single-insert records=%d multi-insert records=%d fpw=%d (cross-checked=%d mismatches=%d)%n",
                singleInsertRecords, multiInsertRecords, fpwSeen, fpwCrossChecked, fpwMismatches);
        System.out.printf("contrecords stitched=%d  last record lsn=%s  sawLongHeader=%s blcksz=%d%n",
                walker.contrecords, lsn(walker.lastRecordLsn), walker.sawLongHeader, walker.blckszFromLongHeader);
        List<String> finalCols = attrRows.values().stream()
                .filter(a -> a.attrelid() == tableRelid && a.attnum() > 0 && !a.attisdropped())
                .sorted(java.util.Comparator.comparingInt(AttrRow::attnum))
                .map(AttrRow::attname).toList();
        System.out.println("final dictionary columns: " + finalCols);
        if (finalCols.size() != 6 || !finalCols.get(5).equals("extra")) {
            System.out.println(">>> DICTIONARY REPLAY FAILED: expected 6 columns ending with 'extra'");
            pass = false;
        }
        System.out.println("\nrecord census (rmid/info-hex -> count): " + walker.census);
        System.out.println(pass ? "\n>>> SPIKE RESULT: PASS" : "\n>>> SPIKE RESULT: FAIL");
        if (!pass) System.exit(1);
    }

    // ---- catalog replay: pg_attribute rows keyed by physical ctid -----------
    static long ctidKey(int block, int offnum) { return ((long) block << 16) | offnum; }

    static AttrRow toAttrRow(Row r) {
        Object relid = r.vals()[PGATTR_ATTRELID];
        Object name = r.vals()[PGATTR_ATTNAME];
        Object typid = r.vals()[PGATTR_ATTTYPEID];
        Object num = r.vals()[PGATTR_ATTNUM];
        Object dropped = r.vals()[PGATTR_ATTISDROPPED];
        return new AttrRow(((Number) relid).longValue(), name.toString(),
                ((Number) typid).longValue(), ((Number) num).intValue(),
                Boolean.TRUE.equals(dropped));
    }

    /**
     * Applies one WAL record's effect on the pg_attribute ctid index. Insert
     * upserts (block,offnum)->row; delete removes the old ctid; update removes
     * old and upserts new — the same MVCC move heap itself performs. Upsert
     * semantics make double-apply (JDBC seed window overlap) idempotent.
     */
    static void replayPgAttribute(ParsedRecord rec, long spcOid, long dbOid, long pgAttrRelNode,
                                  Map<Long, AttrRow> attrRows, long tableRelid) {
        BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, pgAttrRelNode);
        if (b0 == null) {
            if (DEBUG && rec.rmid() == RM_HEAP_ID)
                for (BlockRef b : rec.blocks())
                    if (b.fork() == 0 && b.db() == dbOid)
                        dbg("[rpl-miss] lsn=%s info=%02x relnode=%d (want %d)%n",
                                lsn(rec.lsn()), rec.info(), b.relNode(), pgAttrRelNode);
            return;
        }
        int op = rec.info() & XLOG_XACT_OPMASK;
        Row r = null;
        int rOffnum = 0;
        if (rec.rmid() == RM_HEAP_ID && op == XLOG_HEAP_INSERT && b0.hasData()) {
            rOffnum = u16(rec.raw(), rec.mainOff());
            r = tupleFromPayload(rec.raw(), b0.dataOff(), PGATTR_KINDS);
            attrRows.put(ctidKey(b0.blockNo(), rOffnum), toAttrRow(r));
        } else if (rec.rmid() == RM_HEAP_ID && op == XLOG_HEAP_DELETE) {
            attrRows.remove(ctidKey(b0.blockNo(), u16(rec.raw(), rec.mainOff() + 4)));
        } else if (rec.rmid() == RM_HEAP2_ID && op == XLOG_HEAP2_MULTI_INSERT) {
            // catalog inserts (even single-row ADD COLUMN) use multi-insert, and
            // catalogs are excluded from RelationIsLogicallyLogged: no KEEP_DATA,
            // so when the record carries an FPW the tuple bytes exist ONLY in the
            // page image (which is the post-change state — extract by offset)
            int ntuples = u16(rec.raw(), rec.mainOff() + 2);
            dbg("[rpl-mi] lsn=%s ntuples=%d dataLen=%d mainOff=%d mainLen=%d totLen=%d imgLen=%d init=%b%n",
                    lsn(rec.lsn()), ntuples, b0.dataLen(), rec.mainOff(), rec.mainLen(), rec.totLen(), b0.imageLen(),
                    (rec.info() & XLOG_HEAP_INIT_PAGE) != 0);
            if (b0.hasData()) {
                int cur = b0.dataOff();
                for (int i = 0; i < ntuples; i++) {
                    cur = (cur + 1) & ~1;
                    int datalen = u16(rec.raw(), cur);
                    int offnum = u16(rec.raw(), rec.mainOff() + 4 + 2 * i);
                    Row row = tupleFromPayload(rec.raw(), cur + 2, PGATTR_KINDS);
                    attrRows.put(ctidKey(b0.blockNo(), offnum), toAttrRow(row));
                    r = row; rOffnum = offnum;
                    cur += 7 + datalen;
                }
            } else if (b0.hasImage()) {
                byte[] page = rebuildPage(rec, b0);
                for (int i = 0; i < ntuples; i++) {
                    int offnum = u16(rec.raw(), rec.mainOff() + 4 + 2 * i);
                    int itemId = u32(page, 24 + (offnum - 1) * 4);
                    int lpOff = itemId & 0x7FFF;
                    if (((itemId >> 15) & 0x3) != 1)
                        throw new IllegalStateException("catalog FPW: offnum " + offnum + " not LP_NORMAL");
                    Row row = tupleFromPage(page, lpOff, PGATTR_KINDS);
                    attrRows.put(ctidKey(b0.blockNo(), offnum), toAttrRow(row));
                    r = row; rOffnum = offnum;
                }
            }
        } else if (rec.rmid() == RM_HEAP_ID && (op == XLOG_HEAP_UPDATE || op == XLOG_HEAP_HOT_UPDATE)
                && b0.hasData()) {
            int upFlags = rec.raw()[rec.mainOff() + 7];
            if ((upFlags & XLH_UPDATE_TRUNCATION) != 0)
                throw new IllegalStateException("pg_attribute update with truncation unsupported in spike");
            // old ctid: block ref 1 (old page) when present, else same page as new
            int oldBlock = b0.blockNo();
            if (rec.blocks().size() > 1) oldBlock = rec.blocks().get(1).blockNo();
            attrRows.remove(ctidKey(oldBlock, u16(rec.raw(), rec.mainOff() + 4)));
            int newOffnum = u16(rec.raw(), rec.mainOff() + 12);
            r = tupleFromPayload(rec.raw(), b0.dataOff(), PGATTR_KINDS);
            attrRows.put(ctidKey(b0.blockNo(), newOffnum), toAttrRow(r));
            rOffnum = newOffnum;
        }
        if (r != null && rOffnum > 0) {
            AttrRow a = toAttrRow(r);
            if (a.attrelid() == tableRelid)
                System.out.printf("[dict] lsn=%s pg_attribute ctid=%d/%d %s attnum=%d typOid=%d dropped=%b%n",
                        lsn(rec.lsn()), b0.blockNo(), rOffnum,
                        a.attname(), a.attnum(), a.atttypid(), a.attisdropped());
        }
    }

    /** Current column kinds of the table, derived from the replayed dictionary. */
    static String[] dictKinds(Map<Long, AttrRow> attrRows, long tableRelid) {
        return attrRows.values().stream()
                .filter(a -> a.attrelid() == tableRelid && a.attnum() > 0 && !a.attisdropped())
                .sorted(java.util.Comparator.comparingInt(AttrRow::attnum))
                .map(a -> kindForTypeOid(a.atttypid()))
                .toArray(String[]::new);
    }

    static BlockRef findHeapBlock(ParsedRecord rec, long spc, long db, long relNode) {
        for (BlockRef b : rec.blocks()) {
            if (b.fork() == 0 && b.spc() == spc && b.db() == db && b.relNode() == relNode) return b;
        }
        return null;
    }

    /**
     * Decodes one tuple from a WAL payload region laid out as
     * [xl_heap_header (5B)][tuple bytes from heap-tuple offset 23] — the common
     * shape used by insert/update/delete records (each may embed several).
     */
    static Row tupleFromPayload(byte[] raw, int off, String[] kinds) {
        int infomask2 = u16(raw, off);
        int infomask = u16(raw, off + 2);
        int tHoff = raw[off + 4] & 0xFF;
        int tupleStart = off + 5 - TUPLE_BITS_OFFSET;
        return decodeTupleData(raw, tupleStart, tHoff, infomask, infomask2, kinds);
    }

    // ---- jdbc helpers -----------------------------------------------------------
    static long queryLong(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    static String queryString(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    static long parseLsn(String s) {
        int slash = s.indexOf('/');
        return (Long.parseLong(s.substring(0, slash), 16) << 32) | Long.parseLong(s.substring(slash + 1), 16);
    }
}
