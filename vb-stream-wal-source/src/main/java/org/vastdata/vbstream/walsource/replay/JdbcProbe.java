package org.vastdata.vbstream.walsource.replay;

/**
 * pg_class 末态探测接缝（Task 12 自愈校验的 JDBC 面，spec §6②）——把"该 oid 的行
 * 现在长什么样、身在何处"收敛为单方法接口，使 {@link SelfHealer} 的校验逻辑可
 * stub 离线测试；生产实现 {@link JdbcProbeImpl} 持普通 SQL 会话做点查（Task 13 IT
 * 接线）。
 *
 * <p>线程约束：约定在重放线程单写者上下文调用（与 {@link CatalogReplay} 同缝）；
 * 实现方自行保证会话的线程封闭或并发安全。</p>
 */
public interface JdbcProbe {

    /**
     * 点查指定关系 oid 在 pg_class 的<strong>末态</strong>行（含当前物理 ctid）——
     * 自愈校验的对照面：截断记录的中段新值是否与该行一致、记录 new 位是否仍是
     * 该行的现居位。
     *
     * <p>查询形如 {@code SELECT ctid::text, relname, relnamespace, reltype, reloftype,
     * relowner, relam, relfilenode, reltoastrelid FROM pg_class WHERE oid=?}
     * （注意拼写 reloftype）。边界与异常语义：行不存在返回 null（关系已删，候选
     * 直接拒绝）；基础设施失败由实现方抛非受检异常（fail-fast，见
     * {@link JdbcProbeImpl}）。</p>
     *
     * @param relOid 关系 oid
     * @return 末态行（ctid 键 + 行模型）；行不存在 null
     */
    ProbedRow currentClassRow(long relOid);

    /**
     * 按物理 ctid 点查 pg_attribute 末态行——attr 面截断自愈的<strong>精确采纳</strong>
     * 探测面（Task 13）：更新会移动行位，故"末态仍居记录 new 位"的行即该记录施加后
     * 的精确状态（更新移位、INPLACE 不移位但重放收敛），整行采纳无需中段值校验。
     *
     * <p>查询形如 {@code SELECT ctid, attrelid, attname, atttypid, attnum, attisdropped
     * FROM pg_attribute WHERE ctid = ?::tid}。边界与异常语义：该位无行返回 null
     * （行已再迁移，记录形态过时——采纳拒绝）；基础设施失败抛非受检异常（fail-fast，
     * 同 {@link #currentClassRow}）。</p>
     *
     * @param ctidText ctid 文本形态（"(block,off)"，与记录 new 位同源渲染）
     * @return 末态行值模型；该位无行 null
     */
    CatalogRow.AttrRow currentAttrRowByCtid(String ctidText);

    /**
     * 按物理 ctid 点查 pg_class 末态行——class 面<strong>精确采纳</strong>探测面
     * （Task 13）：更新必移行位，"末态仍居记录 new 位"的行即该记录施加后的精确状态，
     * 整行采纳无候选值时序问题（对抗性风暴下字典陈旧副本的列 1-7 是历史值，以其为
     * 前缀源重建会写出时代错位的行且可能不被后续事件纠正）。
     *
     * <p>查询形如 {@code SELECT ctid::text, oid, relname, relnamespace, reltype,
     * reloftype, relowner, relam, relfilenode, reltoastrelid FROM pg_class
     * WHERE ctid = ?::tid}（pg_class 无 ctid 索引，走小表顺序扫描——点查代价可忽略）。
     * 边界与异常语义：该位无行返回 null（行已再迁移——采纳拒绝）；基础设施失败抛
     * 非受检异常（fail-fast，同 {@link #currentClassRow}）。</p>
     *
     * @param ctidText ctid 文本形态（"(block,off)"，与记录 new 位同源渲染）
     * @return 末态行（ctid 键 + 行模型）；该位无行 null
     */
    ProbedRow currentClassRowByCtid(String ctidText);

    /**
     * 末态探测产物——物理行位（ctid 键，式同 {@link CatalogReplay#ctidKey}）+
     * 行值模型。record 的数组无关性不涉及（ClassRow 为纯标量 record）。
     *
     * @param ctidKey 末态行物理 ctid 键（{@code (block << 16) | offnum}）
     * @param row     末态行值模型
     */
    record ProbedRow(long ctidKey, CatalogRow.ClassRow row) {

        /**
         * 由 JDBC 的 {@code ctid::text} 形态（"(block,offnum)"）解析出探测产物——
         * 生产实现共用，避免各调用点重复解析。
         *
         * <p>边界与异常语义：非 {@code (b,o)} 形态由 NumberFormatException 裸抛
         * （上游是服务端确定性输出，自洽前提）；线程约束：纯函数。</p>
         *
         * @param ctidText ctid 文本形态（如 "(5,2)"）
         * @param row      末态行值模型
         * @return 探测产物
         */
        public static ProbedRow parse(String ctidText, CatalogRow.ClassRow row) {
            String[] parts = ctidText.replaceAll("[() ]", "").split(",");
            return new ProbedRow(
                    CatalogReplay.ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])), row);
        }
    }
}
