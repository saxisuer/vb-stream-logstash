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
     * 指定关系的 parttype（<b>openGauss 独有列</b> pg_class.parttype 的 as-of 视图：
     * 'n' 普通表 / 'p' 分区母表——og-plan m2pg[PG 档] live 实查值域恰 {'n','p'}，
     * 2026-10-12；OG 档分区表的母表 relkind 仍 'r'，分区语义由本列承担）。
     *
     * <p>M2 T5 前置接缝：{@code TableFilter} 的 OG 档判别消费本方法。缺省 empty =
     * "不可判"——V17/V18 无该列（快照实现不覆写），openGauss 档生产快照当前亦未
     * 供数（ClassRow/CatalogStores 无该列投影——M2 T7 存储护栏已对 parttype='p'
     * 放行入字典，但 parttype 值面仍不随字典携带；M3 分区身份链经 pg_partition
     * 第四字典（{@link #partitionOf}）承担，不依赖本列供数）；消费方（TableFilter
     * OG 分支）对 empty 按 fail-open 放行，与 V17/V18 无此闸的行为面一致。</p>
     *
     * @param relOid 关系 oid
     * @return parttype 单字符（'n'/'p'）；不可判（无该列/未供数）为 empty
     */
    default Optional<String> parttypeOf(long relOid) {
        return Optional.empty();
    }

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
     * 分区 relfilenode → 分区身份链投影（<b>openGauss M3 第四字典面</b>，T0 锚②）：
     * openGauss 原生分区的 heap 记录路由到分区自己的 relfilenode（pg_partition
     * 'p' 行，不在 pg_class 三表字典），本方法把该段号反查为分区名 + <b>母表
     * pg_class oid</b>（parentid）——消费方（{@code TableFilter} 的 OG 分区身份链）
     * 再经 parentid 取母表身份/列集，变更归属按<b>母表</b>建模（基准路 pgoutput
     * R/I 帧同形，{@code PARTITION_NAME=mother}）。
     *
     * <p>缺省恒 empty：V17/V18 无 pg_partition、openGauss 档未引导/未回填第四腿时
     * 的 no-op 锚——分区链消费面严格 OG 档门控，vanilla 路径零侵入。</p>
     *
     * @param relfilenode 分区物理段号（heap 记录路由锚）
     * @return 分区身份（分区名 + 母表 oid）；第四字典无该段号的行（母表行 'r' 的
     *         relfilenode 恒 0、普通关系段号天然 miss）为 empty
     */
    default Optional<PartitionIdentity> partitionOf(long relfilenode) {
        return Optional.empty();
    }

    /**
     * 字典形态代际戳（catalog 字典行变更/索引重建时自增的代际计数）——消费方按
     * <b>"值变即全清缓存"</b>语义使用：相邻两次调用间值不等即字典形态可能已变，
     * 由字典形态派生的缓存（如表身份/列词典、toast 关系判定）应整体失效再重建。
     *
     * <p>缺省 0 = 恒不过期：不感知代际的实现（测试替身/轻量实现）返回常量，消费方
     * 的代际失效面天然关闭、行为与无本方法时一致——既有实现零改动兼容。生产实现
     * （{@code CatalogSynchronizer.SnapshotView}）透传 {@code CatalogStores} 的
     * 代际戳真值。</p>
     *
     * @return 字典形态代际戳（单调不减；缺省 0 = 恒不过期）
     */
    default long shapeEpoch() {
        return 0L;
    }

    /**
     * 分区身份链的最小投影（pg_partition 'p' 行的路由面五字段之三元——消费面
     * 恰需分区名与母表 oid；relfilenode 即查询键、reltoastrelid 归 TOAST 挂靠面
     * 不进本投影）。
     *
     * @param relname  分区名（pg_partition.relname，定宽 name）
     * @param parentid 母表 pg_class oid（'p' 行 parentid@68；母表行该列=自身 oid）
     */
    record PartitionIdentity(String relname, long parentid) {
    }

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
