package org.vastdata.vbstream.reader.format;

/**
 * VBFG 内部精简类型系统。列类型在 TABLE_DEF 中声明一次，事件里只编码值；
 * decimal 与时间类型由捕获端按 string 模式写入（跨网闸同步的设计决策，见
 * vb-cdc-file-transform 的设计文档）。
 *
 * <p><b>持久化契约</b>：{@link #id()}（ordinal+1）已写进落地文件的 TABLE_DEF——
 * 枚举只能追加，不能重排、改名、删除，否则旧文件无法解码。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 TypeCode（2026-09-11 快照）。
 */
public enum TypeCode {

    BOOL,
    INT,
    FLOAT32,
    FLOAT64,
    STRING,
    BYTES,
    // ---- 以下为时间类型扩展（值一律字符串载荷，见各自落地形态）----
    /** 本地时区墙钟，形态 yyyy-MM-dd HH:mm:ss.SSSSSS */
    TIMESTAMP,
    /** ISO-8601 原文（如 2026-09-10T07:30:00.000000Z） */
    TIMESTAMPTZ,
    /** 形态 yyyy-MM-dd */
    DATE,
    /** 形态 HH:mm:ss.SSSSSS（微秒保真） */
    TIME,
    /** 形态 {@code <微秒数> microseconds}（总时长无损，年月结构不可还原） */
    INTERVAL;

    private static final TypeCode[] VALUES = values();

    /** 持久化类型码（ordinal+1，0 保留给非法值）。 */
    public int id() {
        return ordinal() + 1;
    }

    /** 责任：按持久化码取枚举。边界：域外值抛 IAE（损坏文件信号）。 */
    public static TypeCode of(int id) {
        if (id < 1 || id > VALUES.length) {
            throw new IllegalArgumentException("未知类型码: " + id);
        }
        return VALUES[id - 1];
    }
}
