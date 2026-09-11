package org.vastdata.vbstream.reader.format;

/**
 * DML 操作类型。
 *
 * <p><b>持久化契约</b>：{@link #id()}（ordinal）已写进落地文件的 EVENT 记录——
 * 枚举只能追加，不能重排、改名、删除。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 Op（2026-09-11 快照）。
 */
public enum Op {

    CREATE,
    UPDATE,
    DELETE,
    READ;

    private static final Op[] VALUES = values();

    /** 持久化操作码（ordinal）。 */
    public int id() {
        return ordinal();
    }

    /** 责任：按持久化码取枚举。边界：域外值抛 IAE（损坏文件信号）。 */
    public static Op of(int id) {
        if (id < 0 || id >= VALUES.length) {
            throw new IllegalArgumentException("未知操作码: " + id);
        }
        return VALUES[id];
    }
}
