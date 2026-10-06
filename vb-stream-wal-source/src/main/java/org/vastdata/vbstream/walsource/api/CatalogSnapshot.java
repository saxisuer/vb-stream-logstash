package org.vastdata.vbstream.walsource.api;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 伪备库 catalog 的 as-of 快照查询契约（api 包输出面，spec §5）——v2 engine 与冒烟
 * Main 经本接口读 {@code CatalogSynchronizer} 维护的 pg_attribute / pg_class /
 * pg_namespace 字典（v2 起三张 watched 目录），不感知 ctid 重放实现。
 *
 * <p>一致性语义：一次调用内的各查询相对同一 {@link #lsn()} 前沿（v1 = 最近一次
 * 已施加记录的 MAXALIGN 末尾）；跨调用随消费推进单调不减。线程约束：实现按
 * "读线程与重放线程交错"提供尽力一致视图（v1 单线程直通下的天然一致，见
 * {@code CatalogSynchronizer} javadoc）。</p>
 */
public interface CatalogSnapshot {

    /**
     * 本快照的前沿 LSN——字典内容已重放至该位点（下一条未施加记录的起点）；同步器
     * 启动后自流起点（槽 P₀ 与引导 flush LSN 的较大值）起算，首条记录施加前即引导
     * 一致点而非 0。
     *
     * @return 已消费前沿（打包 long；同步器未启动为 0）
     */
    long lsn();

    /**
     * 指定关系的列字典（pg_attribute as-of 视图）。
     *
     * <p>关键语义：按 attnum 升序返回；<strong>含 dropped 占位列</strong>（DROP
     * COLUMN 后 pg_attribute 保留的存储占位行，spike 发现 26——列序与 attnum 键位
     * 不得塌陷）；未知关系返回空表而非 null。</p>
     *
     * @param relOid 关系 oid
     * @return 列列表（attnum 升序，含 dropped 占位）
     */
    List<Column> columnsOf(long relOid);

    /**
     * 指定关系当前的 relfilenode（pg_class as-of 视图，TRUNCATE/重写经 INPLACE/UPD
     * 跟随后的链尾当前值）。
     *
     * @param relOid 关系 oid
     * @return 当前 relfilenode；关系不在字典中（未引导/已删除）为 empty——区别于
     *         "已知但值为 0"（of(0)）
     */
    OptionalLong relfilenodeOf(long relOid);

    /**
     * 指定关系当前的 toast 关系 oid（reltoastrelid as-of 视图）。
     *
     * @param relOid 关系 oid
     * @return toast 关系 oid（无 toast 为 of(0)）；关系不在字典中为 empty
     */
    OptionalLong toastOf(long relOid);

    /**
     * 指定关系的模式名（pg_class.relnamespace 经 pg_namespace 解析为 nspname 的
     * as-of 视图，v2 扩链面——schema 名即 pg_namespace 解析，"public" 等）。
     *
     * @param relOid 关系 oid
     * @return 模式名；关系不在字典中、或其 relnamespace 无对应 nsp 行（字典半损）为 empty
     */
    Optional<String> schemaOf(long relOid);

    /**
     * 指定关系的 relkind（pg_class.relkind 的 as-of 视图，单字符——'r' 普通表 /
     * 'p' 分区表 / 't' toast / 'i' 索引 / 'v' 视图 / 'S' 序列等，v2 表过滤的分派键）。
     *
     * @param relOid 关系 oid
     * @return relkind 单字符（如 "r"）；关系不在字典中为 empty
     */
    Optional<String> relkindOf(long relOid);

    /**
     * 指定关系的表名（pg_class.relname 的 as-of 视图——白名单 {@code schema.table}
     * 匹配与 TableMeta 组装面）。
     *
     * @param relOid 关系 oid
     * @return relname；关系不在字典中为 empty
     */
    Optional<String> nameOf(long relOid);

    /**
     * 把 relfilenode 或关系 oid 归一解析为关系 oid（双入口）：先按 relfilenode
     * 反查（heap 块头形态——块只携带 relfilenode），miss 再按 oid 直认（元数据
     * 形态）。oid 在 pg_class 内唯一、relfilenode 同库引导下唯一（跨库碰撞为
     * 理论残留，v1 块匹配同口径）。
     *
     * @param relNodeOrOid relfilenode 或关系 oid
     * @return 关系 oid；字典无匹配为 empty
     */
    OptionalLong relOidOf(long relNodeOrOid);

    /**
     * 一列的字典投影（pg_attribute 行的最小值面）。
     *
     * @param attnum  列号（1-based 用户列；引导裁定仅取 attnum&gt;0，系统列不进字典）
     * @param name    列名（DROP COLUMN 后为 "........pg.dropped.N........" 占位名）
     * @param typeOid 列类型 oid（atttypid）
     * @param dropped 是否已删列的存储占位（attisdropped——真列时该位为 false）
     */
    record Column(int attnum, String name, long typeOid, boolean dropped) {
    }
}
