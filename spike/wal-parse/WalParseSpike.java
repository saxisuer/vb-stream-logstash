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
    static final int XLOG_HEAP_INPLACE = 0x70;
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

    // pg_class column kinds, transcribed from the live PG 18.6 server
    // (SELECT attnum, atttypid FROM pg_attribute WHERE attrelid='pg_class')
    static final String[] PGCLASS_KINDS = {
            "oid",   // oid (catalogs store oid as a regular first column)
            "name",  // relname
            "oid",   // relnamespace
            "oid",   // reltype
            "oid",   // reloftype
            "oid",   // relowner
            "oid",   // relam
            "oid",   // relfilenode          <-- index 7
            "oid",   // reltablespace
            "int4",  // relpages
            "float4",// reltuples
            "int4",  // relallvisible
            "int4",  // relallfrozen
            "oid",   // reltoastrelid        <-- index 13
            "bool",  // relhasindex
            "bool",  // relisshared
            "char",  // relpersistence
            "char",  // relkind
            "int2",  // relnatts
            "int2",  // relchecks
            "bool",  // relhasrules
            "bool",  // relhastriggers
            "bool",  // relhassubclass
            "bool",  // relrowsecurity
            "bool",  // relforcerowsecurity
            "bool",  // relispopulated
            "char",  // relreplident
            "bool",  // relispartition
            "oid",   // relrewrite
            "int4",  // relfrozenxid (xid)
            "int4",  // relminmxid (multi-xid)
            "skip",  // relacl aclitem[]
            "skip",  // reloptions text[]
            "skip",  // relpartbound pg_node_tree
    };
    static final int PGCLASS_OID = 0, PGCLASS_RELNAME = 1, PGCLASS_RELEFILENODE = 7,
            PGCLASS_RELTOASTRELID = 13;
    static final String[] TOAST_KINDS = {"oid", "int4", "bytea"}; // chunk_id, chunk_seq, chunk_data

    /** One replayed pg_class row, keyed by physical ctid (cols 3-7 kept for
     *  value-based reconstruction of truncated updates: cols 1-7 are all
     *  fixed-width and sum to 92 bytes of the data region). */
    record ClassRow(long relOid, String relname, long relnamespace, long reltype, long reloftype,
                    long relowner, long relam, long relfilenode, long reltoastrelid) {
        /** Encodes data-region bytes of cols 1..7 (oid, name, 5x oid) — 92 bytes. */
        byte[] encodeFirstSevenCols() {
            java.io.ByteArrayOutputStream o = new java.io.ByteArrayOutputStream();
            writeU32(o, relOid);
            byte[] n = relname.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            o.write(n, 0, Math.min(63, n.length));
            for (int i = n.length; i < 64; i++) o.write(0);
            writeU32(o, relnamespace);
            writeU32(o, reltype);
            writeU32(o, reloftype);
            writeU32(o, relowner);
            writeU32(o, relam);
            return o.toByteArray();
        }
    }

    static void writeU32(java.io.ByteArrayOutputStream o, long v) {
        o.write((int) (v & 0xFF)); o.write((int) ((v >> 8) & 0xFF));
        o.write((int) ((v >> 16) & 0xFF)); o.write((int) ((v >> 24) & 0xFF));
    }

    // single-threaded throwaway spike: shared decode/replay context as statics
    static long spcOid, dbOid, tableRelid, pgAttrRelnode, pgClassRelnode;
    static long trackedTableCtid, trackedToastCtid;
    static long toastRelid;
    static final Map<Long, AttrRow> ATTR_ROWS = new java.util.HashMap<>();
    static final Map<Long, ClassRow> CLASS_ROWS = new java.util.HashMap<>();
    /** TOAST chunk store: valueid -> (chunk_seq -> data bytes). */
    static final Map<Long, java.util.TreeMap<Integer, byte[]>> TOAST_CHUNKS = new java.util.HashMap<>();
    /** raw tuple tails (bytes from heap-tuple offset 23) per ctid, for the two
     *  replayed catalogs — needed to reconstruct prefix/suffix-truncated updates */
    static final Map<Long, byte[]> RAW_PGATTR = new java.util.HashMap<>();
    static final Map<Long, byte[]> RAW_PGCLASS = new java.util.HashMap<>();
    /** aux JDBC connection for on-demand raw-tail fetch (pg_read_binary_file). */
    static Connection TAIL_CONN;

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
            if (sawAny && chunkStart != carryStartLsn + carry.length) {
                // chunk anchoring (via getLastReceiveLSN - len) drifted — empirically
                // seen as ±32-byte skips under keepalive/data interleaving. Resync by
                // dropping the carry and restarting from this chunk's own anchor.
                // A production walker needs a stronger anchoring protocol (pageaddr
                // validation / contrecord chains); recorded as a spike finding.
                System.out.printf("[walker] RESYNC: carry end=%s chunk start=%s — dropping carry%n",
                        lsn(carryStartLsn + carry.length), lsn(chunkStart));
                carry = new byte[0];
            }
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
                case "bytea" -> { c = tupleStart + align(c - tupleStart, 4); int[] next = {c}; vals[i] = readVarlenaBytes(src, c, next); c = next[0]; }
                case "skip" -> { c = tupleStart + align(c - tupleStart, 4); int[] next = {c}; skipVarlena(src, c, next); c = next[0]; }
                default -> throw new IllegalStateException("unregistered kind " + kinds[i]);
            }
        }
        return new Row(vals);
    }

    /** Reads one uncompressed varlena at c; returns text and advances next[0]. */
    static String readVarlenaText(byte[] src, int c, int[] next) {
        // 4-byte varlena headers are stored NATIVE little-endian as (len << 2 | tag)
        // — empirically confirmed against live TOAST chunks (SET_VARSIZE_4B writes
        // va_header = len << 2; the "big-endian" folklore is wrong for this build)
        int b0 = src[c] & 0xFF;
        int len;
        int dataOff;
        if (b0 == 0x01 && src[c + 1] == 0x12) { // external on-disk TOAST pointer:
            next[0] = c + 18;                   // [tag 0x01][VARTAG_ONDISK 0x12][16B payload]
            return resolveExternal(src, c);
        }
        if ((b0 & 0x01) != 0) {            // 1-byte varlena header
            len = (b0 >> 1) & 0x7F;        // total incl. header byte
            dataOff = 1;
        } else if ((b0 & 0x03) == 0) {     // 4-byte uncompressed
            len = u32(src, c) >>> 2;       // total incl. 4-byte header
            dataOff = 4;
        } else {
            throw new IllegalStateException("compressed/short varlena unexpected in spike: hdr=" + b0);
        }
        next[0] = c + len;
        return new String(src, c + dataOff, Math.max(0, len - dataOff));
    }

    /** Reads one varlena datum as raw bytes (chunk payloads etc.). */
    static byte[] readVarlenaBytes(byte[] src, int c, int[] next) {
        int b0 = src[c] & 0xFF;
        int len, dataOff;
        if ((b0 & 0x01) != 0) { len = (b0 >> 1) & 0x7F; dataOff = 1; }
        else if ((b0 & 0x03) == 0) { len = u32(src, c) >>> 2; dataOff = 4; }
        else throw new IllegalStateException("non-plain varlena for bytea: hdr=" + b0);
        next[0] = c + len;
        byte[] out = new byte[Math.max(0, len - dataOff)];
        System.arraycopy(src, c + dataOff, out, 0, out.length);
        return out;
    }

    /**
     * Resolves a 16-byte external TOAST pointer (varatt_external, varatt.h):
     * rawsize i32 / extinfo u32 (extsize bits 0-29 + method bits 30-31) /
     * valueid u32 / toastrelid u32 — by reassembling collected chunks.
     */
    static String resolveExternal(byte[] src, int c) {
        // short-varlena external format (empirically pinned): [0x01][0x12 VARTAG_ONDISK]
        // [rawsize u32][extinfo u32][valueid u32][toastrelid u32] — 18 bytes total
        int rawsize = u32(src, c + 2);                      // incl. 4-byte varlena header
        int extinfo = u32(src, c + 6);
        int extsize = extinfo & 0x3FFFFFFF;
        long valueid = u32(src, c + 10) & 0xFFFFFFFFL;
        long toastRelOfPointer = u32(src, c + 14) & 0xFFFFFFFFL;
        dbg("[ext] rawsize=%d extsize=%d valueid=%d toastrel=%d%n",
                rawsize, extsize, valueid, toastRelOfPointer);
        if (extsize < rawsize - 4)
            throw new IllegalStateException("compressed external value (pglz/lz4) not supported in spike: valueid="
                    + valueid + " extsize=" + extsize + " rawsize=" + rawsize);
        java.util.TreeMap<Integer, byte[]> chunks = TOAST_CHUNKS.get(valueid);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        if (chunks != null) for (byte[] b : chunks.values()) out.writeBytes(b);
        byte[] assembled = out.toByteArray();
        if (assembled.length != extsize)
            throw new IllegalStateException("TOAST reassembly mismatch: valueid=" + valueid
                    + " assembled=" + assembled.length + " expected=" + extsize
                    + " (toastrel of pointer " + toastRelOfPointer + ", chunks " + (chunks == null ? -1 : chunks.size()) + ")");
        return new String(assembled, java.nio.charset.StandardCharsets.UTF_8);
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
            // static decode/replay context (single-threaded spike)
            TAIL_CONN = DriverManager.getConnection(JDBC_URL);
            spcOid = queryLong(setup, "SELECT oid FROM pg_tablespace WHERE spcname='pg_default'");
            dbOid = queryLong(setup, "SELECT oid FROM pg_database WHERE datname=current_database()");
            tableRelid = queryLong(setup, "SELECT 't_wal_spike'::regclass::oid");
            pgAttrRelnode = queryLong(setup, "SELECT pg_relation_filenode('pg_attribute'::regclass)");
            pgClassRelnode = queryLong(setup, "SELECT pg_relation_filenode('pg_class'::regclass)");
            toastRelid = queryLong(setup, "SELECT reltoastrelid FROM pg_class WHERE oid=" + tableRelid);
            long relNode = queryLong(setup, "SELECT pg_relation_filenode('t_wal_spike')");
            // seed pg_attribute rows (with ctid) for the table AND its toast relation
            try (Statement st = setup.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                                 + " FROM pg_attribute WHERE attrelid IN (" + tableRelid + "," + toastRelid
                                 + ") AND attnum > 0")) {
                while (rs.next()) {
                    String[] parts = rs.getString(1).replaceAll("[() ]", "").split(",");
                    ATTR_ROWS.put(ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])),
                            new AttrRow(rs.getLong(2), rs.getString(3), rs.getLong(4),
                                    rs.getInt(5), rs.getBoolean(6)));
                }
            }
            // seed pg_class rows (with ctid) for the table and its toast relation —
            // relfilenode transitions (TRUNCATE / rewrite) replay on top of these
            try (Statement st = setup.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT ctid::text, oid, relname, relnamespace, reltype, reloftype,"
                                 + " relowner, relam, relfilenode, reltoastrelid FROM pg_class"
                                 + " WHERE oid IN (" + tableRelid + "," + toastRelid + ")")) {
                while (rs.next()) {
                    String[] parts = rs.getString(1).replaceAll("[() ]", "").split(",");
                    long key = ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                    ClassRow cr = new ClassRow(rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getLong(5),
                            rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getLong(10));
                    CLASS_ROWS.put(key, cr);
                    if (cr.relOid() == tableRelid) trackedTableCtid = key;
                    else trackedToastCtid = key;
                }
            }
            System.out.printf("[spike] relid=%d filenode=%d toastRelid=%d pgAttrRelnode=%d pgClassRelnode=%d cols=%d trackedTable=%d trackedToast=%d%n",
                    tableRelid, relNode, toastRelid, pgAttrRelnode, pgClassRelnode, ATTR_ROWS.size(),
                    trackedTableCtid, trackedToastCtid);
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

                report(records, walker, expected);
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
        // S6: relfilenode lifecycle — TRUNCATE assigns a NEW relfilenode (both for
        // the table and its toast relation); decoding must follow via the replayed
        // pg_class rows, or every later record would fail the relnode filter
        try (Statement st = setup.createStatement()) {
            st.execute("TRUNCATE t_wal_spike");
            String r25 = "(25, 's-025-π', 251.5, false, '2026-10-05 12:00:25.123456', 'post-truncate')";
            st.execute("INSERT INTO t_wal_spike VALUES " + r25);
            expected.add(List.of(renderRow(r25)));
        }
        // S7: TOAST reassembly — a wide INCOMPRESSIBLE value (hex chain) is stored
        // as uncompressed external chunks; the decoder must reassemble by pointer
        setup.setAutoCommit(false);
        try (PreparedStatement ps = setup.prepareStatement(
                "INSERT INTO t_wal_spike VALUES (26, ?, 261.5, true, '2026-10-05 12:00:26.123456', 'wide')")) {
            StringBuilder wide = new StringBuilder("w");
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] d = "seed-26".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            while (wide.length() < 7000) {
                d = md.digest(d);
                for (byte b : d) wide.append(String.format("%02x", b));
            }
            wide.setLength(7000);
            ps.setString(1, wide.toString());
            ps.executeUpdate();
            expected.add(List.of("(26, " + wide + ", 261.5, true, 2026-10-05T12:00:26.123456, wide)"));
        }
        setup.commit();
        setup.setAutoCommit(true);
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
    static void report(List<ParsedRecord> records, WalWalker walker, List<List<String>> expected) {
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
            // catalog replay FIRST: dictionaries (pg_attribute/pg_class) and the
            // table's CURRENT relfilenode all move with the WAL stream, so that
            // later DML in this same LSN-ordered pass decodes with as-of state
            replayCatalogs(rec);
            ClassRow table = CLASS_ROWS.get(trackedTableCtid);
            long relNode = table != null ? table.relfilenode() : -1;
            String[] kinds = dictKinds(ATTR_ROWS, tableRelid);
            if (rec.rmid() == RM_HEAP_ID && (rec.info() & XLOG_XACT_OPMASK) == XLOG_HEAP_INSERT) {
                BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, relNode);
                if (b0 == null) continue;
                singleInsertRecords++;
                int offnum = u16(rec.raw(), rec.mainOff());
                if (b0.dataLen() > 55)
                    dbg("[wide] lsn=%s totLen=%d dataLen=%d head=%s%n",
                            lsn(rec.lsn()), rec.totLen(), b0.dataLen(),
                            java.util.HexFormat.of().formatHex(java.util.Arrays.copyOfRange(
                                    rec.raw(), b0.dataOff(), Math.min(b0.dataOff() + 40, rec.raw().length))));
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
        List<String> finalCols = ATTR_ROWS.values().stream()
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

    /** Raw tail (bytes from heap-tuple offset 23) of a data-path single tuple:
     *  block data = [xl_heap_header 5B][tail] with total length dataLen. */
    static byte[] tailOf(byte[] raw, int off, int dataLen) {
        return java.util.Arrays.copyOfRange(raw, off + 5, off + dataLen);
    }

    /** Raw tail of one multi-insert entry: [datalen u16][xl_heap_header 5B][tail]. */
    static byte[] tailOfMulti(byte[] raw, int cur, int datalen) {
        return java.util.Arrays.copyOfRange(raw, cur + 7, cur + 7 + datalen);
    }

    record Reconstructed(byte[] tail, Row row) {}

    /**
     * Value-based reconstruction of a pg_class truncated update: prefix bytes (if
     * within cols 1-7, 92 bytes) are re-ENCODED from the known old row, the middle
     * comes from the record, and the suffix is zero-filled — valid because the
     * suffix columns are fixed-width (values unread) or NULL varlenas (skipped via
     * the bitmap). Returns null when prefix extends past col 7 (unsupported).
     */
    static Reconstructed reconstructClassTruncated(ParsedRecord rec, BlockRef b0, ClassRow oldRow) {
        int upFlags = rec.raw()[rec.mainOff() + 7];
        int cur = b0.dataOff();
        int prefix = 0, suffix = 0;
        if ((upFlags & 0x20) != 0) { prefix = u16(rec.raw(), cur); cur += 2; }
        if ((upFlags & 0x40) != 0) { suffix = u16(rec.raw(), cur); cur += 2; }
        int im2 = u16(rec.raw(), cur);
        int im = u16(rec.raw(), cur + 2);
        int tHoff = rec.raw()[cur + 4] & 0xFF;
        cur += 5;
        if (prefix > 88) return null; // would need old values of cols 8+ (relpages etc.)
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        if (prefix == 0) {
            int chunkLen = b0.dataLen() - (cur - b0.dataOff());
            out.write(rec.raw(), cur, chunkLen);          // includes bitmap area
        } else {
            int bitmapLen = tHoff - TUPLE_BITS_OFFSET;
            out.write(rec.raw(), cur, bitmapLen);          // bitmap [+ padding]
            cur += bitmapLen;
            byte[] enc = oldRow.encodeFirstSevenCols();
            out.write(enc, 0, prefix);
            int midLen = b0.dataLen() - (cur - b0.dataOff());
            out.write(rec.raw(), cur, midLen);
        }
        out.write(new byte[suffix], 0, suffix);            // zero-filled unread tail
        byte[] tail = out.toByteArray();
        byte[] full = new byte[TUPLE_BITS_OFFSET + tail.length];
        System.arraycopy(tail, 0, full, TUPLE_BITS_OFFSET, tail.length);
        return new Reconstructed(tail, decodeTupleData(full, 0, tHoff, im, im2, PGCLASS_KINDS));
    }

    /**
     * Reconstructs the new tuple of a prefix/suffix-truncated update by splicing
     * the logged middle with the stored old tail (heapam.c chunk layout:
     * [prefix u16?][suffix u16?][xl_heap_header 5B] then, prefix==0 ? full tail
     * minus suffix : [bitmap+pad (t_hoff-23 B)][data region from t_hoff+prefix]),
     * then decoding with the header fields carried by the record.
     */
    static Reconstructed reconstructTruncated(ParsedRecord rec, BlockRef b0, byte[] oldTail, String[] kinds) {
        int upFlags = rec.raw()[rec.mainOff() + 7];
        int cur = b0.dataOff();
        int prefix = 0, suffix = 0;
        if ((upFlags & 0x20) != 0) { prefix = u16(rec.raw(), cur); cur += 2; }   // PREFIX_FROM_OLD
        if ((upFlags & 0x40) != 0) { suffix = u16(rec.raw(), cur); cur += 2; }   // SUFFIX_FROM_OLD
        int im2 = u16(rec.raw(), cur);
        int im = u16(rec.raw(), cur + 2);
        int tHoff = rec.raw()[cur + 4] & 0xFF;
        cur += 5;
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        if (prefix == 0) {
            // logged chunk = tail minus suffix; append the unchanged old tail end
            int chunkLen = b0.dataLen() - (cur - b0.dataOff());
            out.write(rec.raw(), cur, chunkLen);
            out.write(oldTail, oldTail.length - suffix, suffix);
        } else {
            int bitmapLen = tHoff - TUPLE_BITS_OFFSET;
            out.write(rec.raw(), cur, bitmapLen);
            cur += bitmapLen;
            int oldDataStart = tHoff - TUPLE_BITS_OFFSET;
            out.write(oldTail, oldDataStart, prefix);
            int midLen = b0.dataLen() - (cur - b0.dataOff());
            out.write(rec.raw(), cur, midLen);
            out.write(oldTail, oldTail.length - suffix, suffix);
        }
        byte[] tail = out.toByteArray();
        byte[] full = new byte[TUPLE_BITS_OFFSET + tail.length];
        System.arraycopy(tail, 0, full, TUPLE_BITS_OFFSET, tail.length);
        return new Reconstructed(tail, decodeTupleData(full, 0, tHoff, im, im2, kinds));
    }

    /**
     * On-demand raw tail for rows that predate the stream window: read the heap
     * page via pg_read_binary_file and extract the tuple at the ctid. NOTE: this
     * reads the CURRENT on-disk state — valid for prefix/suffix bytes because
     * those regions are unchanged by the update being reconstructed (and our
     * tracked fields sit in the logged middle). Throwaway-spike shortcut.
     */
    /**
     * Tail of a TRACKED pg_class row, read at its CURRENT ctid (queried by oid) —
     * the right fallback for truncated updates that themselves moved the row, so
     * the record's old ctid is already dead on the final-state page.
     */
    static byte[] fetchTrackedClassTail(long classRelOid) {
        try (Statement st = TAIL_CONN.createStatement();
             ResultSet rs = st.executeQuery("SELECT ctid::text FROM pg_class WHERE oid=" + classRelOid)) {
            if (!rs.next()) return null;
            String[] parts = rs.getString(1).replaceAll("[() ]", "").split(",");
            return fetchTailOnDemand(pgClassRelnode,
                    ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])));
        } catch (SQLException e) {
            return null;
        }
    }

    static byte[] fetchTailOnDemand(long relfilenode, long ctid) {
        int block = (int) (ctid >>> 16);
        int offnum = (int) (ctid & 0xFFFF);
        try (Statement st = TAIL_CONN.createStatement();
             ResultSet rs = st.executeQuery(String.format(
                     "SELECT pg_read_binary_file('base/%d/%d', %d, 8192)",
                     dbOid, relfilenode, block * 8192L))) {
            if (!rs.next()) return null;
            byte[] page = rs.getBytes(1);
            int itemId = u32(page, 24 + (offnum - 1) * 4);
            if (((itemId >> 15) & 0x3) != 1) return null; // moved/dead at final state
            int lpOff = itemId & 0x7FFF;
            int lpLen = (itemId >> 17) & 0x7FFF;
            return java.util.Arrays.copyOfRange(page, lpOff + TUPLE_BITS_OFFSET, lpOff + lpLen);
        } catch (SQLException e) {
            return null;
        }
    }

    /** A decoded heap-level event on a watched relation (catalog or TOAST). */
    record HeapEvent(int op, long oldCtid, long newCtid, Row row, byte[] rawTail) {
        static final int INS = 0, DEL = 1, UPD = 2;
    }

    /**
     * Extracts heap-level row events of one WAL record for a watched relfilenode,
     * decoding tuples via the given kinds. Handles the data path AND the FPW-image
     * path (catalogs log no tuple data when an FPW is taken; the image is the
     * post-change page state, so new rows are extracted by offset from it).
     */
    static List<HeapEvent> heapEvents(ParsedRecord rec, long watchedRelnode, String[] kinds, Map<Long, byte[]> rawStore) {
        List<HeapEvent> out = new ArrayList<>();
        if (rec.rmid() != RM_HEAP_ID && rec.rmid() != RM_HEAP2_ID) return out;
        BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, watchedRelnode);
        if (b0 == null) return out;
        int op = rec.info() & XLOG_XACT_OPMASK;
        if (rec.rmid() == RM_HEAP_ID && op == XLOG_HEAP_INSERT) {
            int offnum = u16(rec.raw(), rec.mainOff());
            if (b0.hasData()) {
                dbg("[ins] lsn=%s relnode=%d dataLen=%d totLen=%d im2=%d im=%04x hoff=%d head=%s%n",
                        lsn(rec.lsn()), watchedRelnode, b0.dataLen(), rec.totLen(),
                        u16(rec.raw(), b0.dataOff()), u16(rec.raw(), b0.dataOff() + 2),
                        rec.raw()[b0.dataOff() + 4] & 0xFF,
                        java.util.HexFormat.of().formatHex(java.util.Arrays.copyOfRange(
                                rec.raw(), b0.dataOff(), Math.min(b0.dataOff() + 32, rec.raw().length))));
                out.add(new HeapEvent(HeapEvent.INS, 0, ctidKey(b0.blockNo(), offnum),
                        tupleFromPayload(rec.raw(), b0.dataOff(), kinds),
                        tailOf(rec.raw(), b0.dataOff(), b0.dataLen())));
            } else if (b0.hasImage()) {
                byte[] page = rebuildPage(rec, b0);
                int lpOff = u32(page, 24 + (offnum - 1) * 4) & 0x7FFF;
                out.add(new HeapEvent(HeapEvent.INS, 0, ctidKey(b0.blockNo(), offnum),
                        tupleFromPage(page, lpOff, kinds), null));
            }
        } else if (rec.rmid() == RM_HEAP_ID && op == XLOG_HEAP_DELETE) {
            out.add(new HeapEvent(HeapEvent.DEL, ctidKey(b0.blockNo(), u16(rec.raw(), rec.mainOff() + 4)), 0, null, null));
        } else if (rec.rmid() == RM_HEAP_ID && (op == XLOG_HEAP_UPDATE || op == XLOG_HEAP_HOT_UPDATE)) {
            int upFlags = rec.raw()[rec.mainOff() + 7];
            // OLD heap block is block ref id 1 — but the record may also carry
            // visibility-map blocks (ids 2/3, fork 1): pick by fork, not by index
            int oldBlock = -1;
            for (BlockRef b : rec.blocks())
                if (b.fork() == 0 && b.blockNo() != b0.blockNo()) { oldBlock = b.blockNo(); break; }
            if (oldBlock < 0) oldBlock = b0.blockNo(); // same-page (HOT) update
            long oldCtid = ctidKey(oldBlock, u16(rec.raw(), rec.mainOff() + 4));
            int newOffnum = u16(rec.raw(), rec.mainOff() + 12);
            if (b0.hasData()) {
                byte[] tail;
                Row r;
                dbg("[upd] lsn=%s info=%02x flags=%02x dataLen=%d mainLen=%d totLen=%d im2=%d%n",
                        lsn(rec.lsn()), rec.info(), upFlags, b0.dataLen(), rec.mainLen(), rec.totLen(),
                        u16(rec.raw(), b0.dataOff()));
                if ((upFlags & XLH_UPDATE_TRUNCATION) != 0) {
                    // pg_class: value-based reconstruction; when the record's old ctid is
                    // unknown (prune moved the row outside our view), try the tracked
                    // table/toast rows as candidates and SELF-VERIFY by the decoded OID
                    // column — healing the tracked ctid chain on match
                    Reconstructed rc = null;
                    if (watchedRelnode == pgClassRelnode) {
                        ClassRow oldRow = CLASS_ROWS.get(oldCtid);
                        if (oldRow == null) {
                            for (ClassRow cand : new ClassRow[]{
                                    CLASS_ROWS.get(trackedTableCtid), CLASS_ROWS.get(trackedToastCtid)}) {
                                if (cand == null) continue;
                                Reconstructed t = reconstructClassTruncated(rec, b0, cand);
                                if (t != null
                                        && ((Number) t.row().vals()[PGCLASS_OID]).longValue() == cand.relOid()) {
                                    rc = t;
                                    // heal: this record's new position is the row's live ctid
                                    if (cand == CLASS_ROWS.get(trackedTableCtid))
                                        trackedTableCtid = ctidKey(b0.blockNo(), newOffnum);
                                    else trackedToastCtid = ctidKey(b0.blockNo(), newOffnum);
                                    dbg("[cls] self-heal: oid=%d tracked ctid -> %d/%d%n",
                                            cand.relOid(), b0.blockNo(), newOffnum);
                                    break;
                                }
                            }
                        } else {
                            rc = reconstructClassTruncated(rec, b0, oldRow);
                        }
                    }
                    if (rc == null) {
                        byte[] oldTail = rawStore == null ? null : rawStore.get(oldCtid);
                        if (oldTail == null && rawStore != null)
                            oldTail = fetchTailOnDemand(watchedRelnode, oldCtid);
                        if (oldTail == null) {
                            // untracked catalog row (not ours): its truncated update is noise
                            dbg("[rpl] skipping truncated update of untracked row ctid=%d relnode=%d%n",
                                    oldCtid, watchedRelnode);
                            return out;
                        }
                        rc = reconstructTruncated(rec, b0, oldTail, kinds);
                    }
                    tail = rc.tail();
                    r = rc.row();
                } else {
                    tail = tailOf(rec.raw(), b0.dataOff(), b0.dataLen());
                    r = tupleFromPayload(rec.raw(), b0.dataOff(), kinds);
                }
                out.add(new HeapEvent(HeapEvent.UPD, oldCtid, ctidKey(b0.blockNo(), newOffnum), r, tail));
            } else if (b0.hasImage()) {
                byte[] page = rebuildPage(rec, b0);
                int lpOff = u32(page, 24 + (newOffnum - 1) * 4) & 0x7FFF;
                out.add(new HeapEvent(HeapEvent.UPD, oldCtid, ctidKey(b0.blockNo(), newOffnum),
                        tupleFromPage(page, lpOff, kinds), null));
            }
        } else if (rec.rmid() == RM_HEAP2_ID && op == XLOG_HEAP2_MULTI_INSERT) {
            int ntuples = u16(rec.raw(), rec.mainOff() + 2);
            boolean init = (rec.info() & XLOG_HEAP_INIT_PAGE) != 0;
            if (b0.hasData()) {
                int cur = b0.dataOff();
                for (int i = 0; i < ntuples; i++) {
                    cur = (cur + 1) & ~1;
                    int datalen = u16(rec.raw(), cur);
                    int offnum = init ? i + 1 : u16(rec.raw(), rec.mainOff() + 4 + 2 * i);
                    out.add(new HeapEvent(HeapEvent.INS, 0, ctidKey(b0.blockNo(), offnum),
                            tupleFromPayload(rec.raw(), cur + 2, kinds),
                            tailOfMulti(rec.raw(), cur, datalen)));
                    cur += 7 + datalen;
                }
            } else if (b0.hasImage()) {
                byte[] page = rebuildPage(rec, b0);
                for (int i = 0; i < ntuples; i++) {
                    int offnum = init ? i + 1 : u16(rec.raw(), rec.mainOff() + 4 + 2 * i);
                    int lpOff = u32(page, 24 + (offnum - 1) * 4) & 0x7FFF;
                    out.add(new HeapEvent(HeapEvent.INS, 0, ctidKey(b0.blockNo(), offnum),
                            tupleFromPage(page, lpOff, kinds), null));
                }
            }
        }
        return out;
    }

    static ClassRow toClassRow(Row r) {
        return new ClassRow(((Number) r.vals()[PGCLASS_OID]).longValue(),
                r.vals()[PGCLASS_RELNAME].toString(),
                ((Number) r.vals()[2]).longValue(), ((Number) r.vals()[3]).longValue(),
                ((Number) r.vals()[4]).longValue(), ((Number) r.vals()[5]).longValue(),
                ((Number) r.vals()[6]).longValue(),
                ((Number) r.vals()[PGCLASS_RELEFILENODE]).longValue(),
                ((Number) r.vals()[PGCLASS_RELTOASTRELID]).longValue());
    }

    /**
     * Replay hook for one record: applies pg_attribute/pg_class row events to the
     * ctid-keyed dictionaries (following pg_class ctid moves for our table and its
     * toast relation), and harvests TOAST chunk rows into the reassembly store.
     */
    /**
     * Replays heap2 PRUNE records (on-access / vacuum scan / vacuum cleanup) on
     * the watched catalogs: pruning physically RELOCATES live HOT-chain roots
     * into freed holes (redirected[]) and kills line pointers (nowdead/nowunused)
     * — without this, ctid tracking loses rows whenever autovacuum compacts a
     * catalog page between our seed and the next update (empirically hit in S6).
     */
    static void replayPrune(ParsedRecord rec) {
        if (rec.rmid() != RM_HEAP2_ID) return;
        int op = rec.info() & XLOG_XACT_OPMASK;
        if (op != 0x10 && op != 0x20 && op != 0x30) return; // PRUNE_ON_ACCESS / VACUUM_SCAN / VACUUM_CLEANUP
        for (long relnode : new long[]{pgAttrRelnode, pgClassRelnode}) {
            BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, relnode);
            if (b0 == null) continue;
            int flags = rec.raw()[rec.mainOff() + 1] & 0xFF;
            dbg("[prn-dbg] lsn=%s relnode=%d info=%02x flags=%02x mainLen=%d dataLen=%d totLen=%d main=%s data=%s%n",
                    lsn(rec.lsn()), relnode, rec.info(), flags, rec.mainLen(), b0.dataLen(), rec.totLen(),
                    java.util.HexFormat.of().formatHex(java.util.Arrays.copyOfRange(rec.raw(),
                            rec.mainOff(), Math.min(rec.mainOff() + 8, rec.raw().length))),
                    java.util.HexFormat.of().formatHex(java.util.Arrays.copyOfRange(rec.raw(),
                            b0.dataOff(), Math.min(b0.dataOff() + 24, rec.raw().length))));
            int cur = b0.dataOff();
            int end = b0.dataOff() + b0.dataLen();
            try {
                if ((flags & 0x10) != 0) {      // freeze plans: nplans u16 + 2B pad + plans*12B
                    cur += 4 + u16(rec.raw(), cur) * 12;
                }
                if ((flags & 0x20) != 0) {      // redirected: pairs (old,new) relocate
                    int n = u16(rec.raw(), cur);
                    cur += 2;
                    for (int i = 0; i < n; i++) {
                        int from = u16(rec.raw(), cur), to = u16(rec.raw(), cur + 2);
                        cur += 4;
                        remapCtid(relnode, b0.blockNo(), from, to);
                    }
                }
                if ((flags & 0x40) != 0) {      // nowdead
                    int n = u16(rec.raw(), cur);
                    cur += 2;
                    for (int i = 0; i < n; i++) dropCtid(relnode, b0.blockNo(), u16(rec.raw(), cur + 2 * i));
                    cur += 2 * n;
                }
                if ((flags & 0x80) != 0) {      // nowunused
                    int n = u16(rec.raw(), cur);
                    cur += 2;
                    for (int i = 0; i < n; i++) dropCtid(relnode, b0.blockNo(), u16(rec.raw(), cur + 2 * i));
                    cur += 2 * n;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                dbg("[prn] parse overflow at lsn=%s relnode=%d flags=%02x — skipping remainder%n",
                        lsn(rec.lsn()), relnode, flags);
            }
            if (cur > end)
                dbg("[prn] layout mismatch: walked %d past dataLen end %d (flags=%02x)%n", cur, end, flags);
        }
    }

    static void remapCtid(long relnode, int block, int from, int to) {
        long oldKey = ctidKey(block, from), newKey = ctidKey(block, to);
        boolean isClass = relnode == pgClassRelnode;
        Map<Long, ?> rows = isClass ? CLASS_ROWS : ATTR_ROWS;
        if (rows.remove(oldKey) != null) {
            if (isClass) {
                ClassRow v = null; // value must be re-put: use raw maps to carry values
            }
        }
        // move decoded rows and raw tails
        if (isClass) {
            ClassRow cr = remapClass(oldKey, newKey);
            if (cr != null) System.out.printf("[prn] lsn-block %d: pg_class redirect %d->%d oid=%d%n",
                    block, from, to, cr.relOid());
        } else {
            AttrRow ar = (AttrRow) ((Map) ATTR_ROWS).remove(oldKey);
            if (ar != null) ATTR_ROWS.put(newKey, ar);
            byte[] t = RAW_PGATTR.remove(oldKey);
            if (t != null) RAW_PGATTR.put(newKey, t);
        }
    }

    static ClassRow remapClass(long oldKey, long newKey) {
        ClassRow cr = CLASS_ROWS.remove(oldKey);
        if (cr != null) CLASS_ROWS.put(newKey, cr);
        byte[] t = RAW_PGCLASS.remove(oldKey);
        if (t != null) RAW_PGCLASS.put(newKey, t);
        if (trackedTableCtid == oldKey) trackedTableCtid = newKey;
        if (trackedToastCtid == oldKey) trackedToastCtid = newKey;
        return cr;
    }

    static void dropCtid(long relnode, int block, int off) {
        long key = ctidKey(block, off);
        if (relnode == pgClassRelnode) {
            if (trackedTableCtid == key || trackedToastCtid == key)
                dbg("[prn] WARNING: tracked ctid %d pruned dead/unused%n", key);
            CLASS_ROWS.remove(key);
            RAW_PGCLASS.remove(key);
        } else {
            ATTR_ROWS.remove(key);
            RAW_PGATTR.remove(key);
        }
    }

    /**
     * Replays XLOG_HEAP_INPLACE on tracked pg_class rows (TRUNCATE/ANALYZE path —
     * PG 18 rewrites the new relfilenode IN PLACE: ctid unchanged, block data is
     * the new data region from t_hoff, header/bitmap untouched). Column offsets
     * in the data region are fixed, so relfilenode@92 / reltoastrelid@120 read
     * straight off the payload without any header knowledge.
     */
    static void replayInplace(ParsedRecord rec) {
        if (rec.rmid() != RM_HEAP_ID || (rec.info() & XLOG_XACT_OPMASK) != XLOG_HEAP_INPLACE) return;
        BlockRef b0 = findHeapBlock(rec, spcOid, dbOid, pgClassRelnode);
        if (b0 == null || !b0.hasData()) return;
        int offnum = u16(rec.raw(), rec.mainOff());
        long key = ctidKey(b0.blockNo(), offnum);
        ClassRow cr = CLASS_ROWS.get(key);
        if (cr == null) return; // row we don't track
        // data-region offsets: cols 1-7 (oid+name+5xoid) = 4+64+20 = 88 bytes,
        // relfilenode@88, then 5 fixed cols, reltoastrelid@112
        long newFileno = u32(rec.raw(), b0.dataOff() + 88) & 0xFFFFFFFFL;
        long newToast = u32(rec.raw(), b0.dataOff() + 112) & 0xFFFFFFFFL;
        CLASS_ROWS.put(key, new ClassRow(cr.relOid(), cr.relname(), cr.relnamespace(), cr.reltype(),
                cr.reloftype(), cr.relowner(), cr.relam(), newFileno, newToast));
        RAW_PGCLASS.remove(key); // tail is stale now; value-based reconstruction covers the future
        dbg("[cls] lsn=%s INPLACE oid=%d filenode %d -> %d toast %d -> %d%n",
                lsn(rec.lsn()), cr.relOid(), cr.relfilenode(), newFileno, cr.reltoastrelid(), newToast);
    }

    static void replayCatalogs(ParsedRecord rec) {
        replayPrune(rec);
        replayInplace(rec);
        for (HeapEvent ev : heapEvents(rec, pgAttrRelnode, PGATTR_KINDS, RAW_PGATTR)) {
            if (ev.op() == HeapEvent.DEL) { ATTR_ROWS.remove(ev.oldCtid()); RAW_PGATTR.remove(ev.oldCtid()); }
            else {
                if (ev.op() == HeapEvent.UPD) { ATTR_ROWS.remove(ev.oldCtid()); RAW_PGATTR.remove(ev.oldCtid()); }
                ATTR_ROWS.put(ev.newCtid(), toAttrRow(ev.row()));
                if (ev.rawTail() != null) RAW_PGATTR.put(ev.newCtid(), ev.rawTail());
            }
        }
        for (HeapEvent ev : heapEvents(rec, pgClassRelnode, PGCLASS_KINDS, RAW_PGCLASS)) {
            if (ev.op() == HeapEvent.DEL) {
                dbg("[cls] lsn=%s DEL ctid=%d (tracked table=%d toast=%d)%n",
                        lsn(rec.lsn()), ev.oldCtid(), trackedTableCtid, trackedToastCtid);
                CLASS_ROWS.remove(ev.oldCtid());
                RAW_PGCLASS.remove(ev.oldCtid());
            } else {
                ClassRow before = ev.op() == HeapEvent.UPD ? CLASS_ROWS.get(ev.oldCtid()) : null;
                ClassRow cr = toClassRow(ev.row());
                if (ev.op() == HeapEvent.UPD) {
                    CLASS_ROWS.remove(ev.oldCtid());
                    RAW_PGCLASS.remove(ev.oldCtid());
                }
                CLASS_ROWS.put(ev.newCtid(), cr);
                if (ev.rawTail() != null) RAW_PGCLASS.put(ev.newCtid(), ev.rawTail());
                if (ev.op() == HeapEvent.UPD) {
                    dbg("[cls] lsn=%s upd oid=%d filenode %d -> %d (toast %d -> %d) ctid %d -> %d tracked=%d%n",
                            lsn(rec.lsn()), cr.relOid(),
                            before != null ? before.relfilenode() : -1, cr.relfilenode(),
                            before != null ? before.reltoastrelid() : -1, cr.reltoastrelid(),
                            ev.oldCtid(), ev.newCtid(), trackedTableCtid);
                }
                if (ev.oldCtid() == trackedTableCtid && ev.op() == HeapEvent.UPD) trackedTableCtid = ev.newCtid();
                if (ev.oldCtid() == trackedToastCtid && ev.op() == HeapEvent.UPD) trackedToastCtid = ev.newCtid();
                // a recreated toast relation arrives as a NEW pg_class row whose oid
                // equals the table's current reltoastrelid
                if (ev.op() == HeapEvent.INS && cr.relOid() != 0) {
                    ClassRow t = CLASS_ROWS.get(trackedTableCtid);
                    if (t != null && cr.relOid() == t.reltoastrelid()) trackedToastCtid = ev.newCtid();
                }
            }
        }
        if (trackedToastCtid != 0) {
            ClassRow toast = CLASS_ROWS.get(trackedToastCtid);
            if (toast != null && toast.relfilenode() != 0) {
                for (HeapEvent ev : heapEvents(rec, toast.relfilenode(), TOAST_KINDS, null)) {
                    if (ev.op() != HeapEvent.INS) continue;
                    Row r = ev.row();
                    long chunkId = ((Number) r.vals()[0]).longValue();
                    int seq = ((Number) r.vals()[1]).intValue();
                    byte[] data = (byte[]) r.vals()[2];
                    TOAST_CHUNKS.computeIfAbsent(chunkId, k -> new java.util.TreeMap<>()).put(seq, data);
                }
            }
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
