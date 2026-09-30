package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;

import java.sql.Types;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link VersionedRelationRegistry} 的版本语义验收:同 oid 多版本按 seq 时间线记录,
 * asOf 二分取"变更发生时刻"的定义(DDL 中途重发 Relation 后,旧 seq 的行不能按新 schema 渲染);
 * prune 的边界保留(恰好 == minSeq 保留、每 oid 至少留最新一条、低水位时刻生效版自身 seq 可更早)
 * 与 find 最新视图(live 解码点的渲染视图)。引擎 {@code vb-stream-engine} 的
 * {@code VersionedRelationRegistryTest}(84 行)前五用例 1:1 翻译,另加同 seq 幂等补钉;
 * 引擎 {@code RelationRegistryTest}(43 行)的覆盖式缓存语义并入 find 用例(引擎侧父类
 * {@code RelationRegistry} 不随译,取舍见任务报告)。版本载荷是 {@link ResolvedRelation}:
 * wire 与 Table 双形态随同一版本一起入日志,断言以 wire 表名为主、TableId 互证。
 */
class VersionedRelationRegistryTest {

    private final VersionedRelationRegistry reg = new VersionedRelationRegistry();

    /**
     * 构造单列 wire Relation 样本。
     * 步骤:固定 schema=public、replicaIdentity='d'、一列 (name="c", typeId=25 text, typmod=-1,
     * partOfKey=true),仅 oid 与表名随参数变化,用表名区分版本(断言读 {@code wire().table()})。
     * 边界:无——纯样本工厂,不触达被测状态。
     */
    private static PgOutputMessage.Relation rel(int oid, String name) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", name,
                'd', List.of(new RelationColumn("c", 25, -1, true)));
    }

    /**
     * 构造最小假 Debezium Table:TableId(public.name) + 单列 c(text)。
     * 步骤:Table.editor() 起手,tableId 定位,addColumn 补一列(jdbcType=VARCHAR、typeName=text),
     * create() 收口——与 wire 侧 rel 的单列同名同序,模拟 Task 7 JDBC enrich 的最小形态。
     * 边界:catalog 传 null(PG 连接器惯例,TableId 内部归一为空串);不连库。
     */
    private static Table table(String name) {
        return Table.editor()
                .tableId(new TableId(null, "public", name))
                .addColumn(Column.editor().name("c").jdbcType(Types.VARCHAR).type("text").create())
                .create();
    }

    /**
     * 构造 wire + Table 同名配对的版本载荷。
     * 步骤:两工厂以同一表名各造一形态,包成 {@link ResolvedRelation}。
     * 边界:无——纯样本工厂;两形态表名一致是测试约定(真实流里 Task 7 enrich 保证一致)。
     */
    private static ResolvedRelation resolved(int oid, String name) {
        return new ResolvedRelation(rel(oid, name), table(name));
    }

    /**
     * asOf 二分取 ≤ asOfSeq 的最新版本:恰在版本切换前取旧版、恰在切换 seq 上取新版、
     * 远后取新版——floor 语义的三段边界。
     * 关键步骤:v1@10 / v2@50 两版入日志 → require 以 49 / 50 / 999 三查询点断言;
     * 49 查询点顺带断言 Table 侧同版本(Debezium 渲染视图随 asOf 一起回退,不取最新)。
     * 边界:asOfSeq 恰等于切换 seq(50)必须取 v2——floor 是"≤"含端点。
     */
    @Test
    void asOfTakesLatestVersionAtOrBeforeSeq() {
        reg.accept(10, resolved(1, "v1"));
        reg.accept(50, resolved(1, "v2"));
        assertEquals("v1", reg.require(1, 49).wire().table());   // 边界:恰在切换前
        assertEquals("v1", reg.require(1, 49).table().id().table());   // Table 视图同版本
        assertEquals("v2", reg.require(1, 50).wire().table());   // 边界:恰在切换 seq 上
        assertEquals("v2", reg.require(1, 999).wire().table());
    }

    /**
     * 同 seq 幂等补钉(引擎测试未覆盖,javadoc 钉死的语义):同 oid 同 seq 的重复接受跳过——
     * floorIndex 命中且 seq 相等即视为同一条消息重复投递,先到者胜(即使内容不同)。
     * 关键步骤:v1@10 入日志 → 同 seq 再投不同内容 → require(1,10) 仍答 v1 → 之后 v2@20
     * 正常追加(幂等跳过不破坏后续插入)。
     * 边界:同 seq 不同内容也跳过(seq 与消息位置一一对应,真实流里不会出现)。
     */
    @Test
    void sameSeqReacceptIsIdempotent() {
        reg.accept(10, resolved(1, "v1"));
        reg.accept(10, resolved(1, "v1-prime"));   // 同 seq 不同内容:重复投递,先到者胜
        assertEquals("v1", reg.require(1, 10).wire().table());
        assertEquals("v1", reg.find(1).orElseThrow().wire().table());
        reg.accept(20, resolved(1, "v2"));          // 幂等跳过不挡后续版本
        assertEquals("v2", reg.require(1, 20).wire().table());
    }

    /**
     * miss fail-fast:oid 完全无版本、或全部版本晚于 asOfSeq 时抛 ISE("未先行到达"语义,
     * 缓存 miss 即协议流异常)。
     * 关键步骤:仅 oid=1 入一版 → require 未知 oid=2 与 require(1, 9)(全部在未来)各断言 ISE。
     * 边界:两分支都测——"无版本"与"有版本但都太晚"同语义。
     */
    @Test
    void missingOrFutureVersionFailsFast() {
        reg.accept(10, resolved(1, "v1"));
        assertThrows(IllegalStateException.class, () -> reg.require(2, 99));   // 无版本
        assertThrows(IllegalStateException.class, () -> reg.require(1, 9));    // 全部在未来
    }

    /**
     * prune 的两条基本边界:恰好 == minSeq 的版本保留(floor 含端点,prune(50) 后 require(1,50)
     * 仍可答、require(1,49) 抛 ISE 即 v1 已剪);过度剪枝保护——minSeq 超过全部版本 seq 时
     * 每个 oid 至少留最新一条(整列保留),require 永不因此 miss。
     * 关键步骤:v1@10 / v2@50 → pruneBelow(50) 两向断言 → pruneBelow(10_000) 后 require(1,10_001)
     * 仍答 v2。
     * 边界:minSeq 恰在切换点与远超全部版本两形态。
     */
    @Test
    void pruneKeepsAtLeastLatestAndExactBoundary() {
        reg.accept(10, resolved(1, "v1"));
        reg.accept(50, resolved(1, "v2"));
        reg.pruneBelow(50);
        assertEquals("v2", reg.require(1, 60).wire().table());
        assertThrows(IllegalStateException.class, () -> reg.require(1, 49));
        reg.pruneBelow(10_000);                       // 过度剪枝保护
        assertEquals("v2", reg.require(1, 10_001).wire().table());
    }

    /**
     * 剪枝保留"低水位时刻生效"的版本(floor),而非"seq ≥ 低水位"的版本(引擎终审 Fix B
     * 语义钉子,1:1 保留):低水位 30 落在 v1 生效期内——v1 自身 seq(10)虽早于 30,仍是
     * 存活桶旧单元(asOf ∈ [10,49))解析的目标,不得剪。若按"丢弃 seq &lt; 30"实现,
     * require(1,29) 会误抛 ISE,组装器在"R(v1) → 开桶 → DDL 重发 R(v2) → 他桶完结触发剪枝"
     * 的并发 DDL 流形下回放必崩。
     * 关键步骤:低水位 30 / 31(均落 v1 生效期)各剪一次并断言 v1 仍可答,直到低水位越过
     * v2 切换点 50,v1 才可安全剪掉(require(1,49) 抛 ISE、require(1,50) 答 v2)。
     * 边界:生效期内两次剪枝结果一致;越过切换点是唯一的可剪点。
     */
    @Test
    void pruneKeepsVersionInEffectAtWatermarkEvenIfItsSeqIsOlder() {
        reg.accept(10, resolved(1, "v1"));
        reg.accept(50, resolved(1, "v2"));
        reg.pruneBelow(30);                            // 低水位落在 v1 生效期内
        assertEquals("v1", reg.require(1, 29).wire().table());   // v1 仍可答(未误剪)
        assertEquals("v1", reg.require(1, 30).wire().table());
        assertEquals("v2", reg.require(1, 50).wire().table());
        reg.pruneBelow(31);                            // 低水位仍在 v1 生效期内:同上,v1 保留
        assertEquals("v1", reg.require(1, 30).wire().table());
        reg.pruneBelow(50);                            // 越过 v2 切换点:v1 才可安全剪掉
        assertThrows(IllegalStateException.class, () -> reg.require(1, 49));
        assertEquals("v2", reg.require(1, 50).wire().table());
    }

    /**
     * find 最新视图(引擎 {@code RelationRegistryTest.cachesLatestRelationByOid} 的覆盖式缓存
     * 语义并入:同 oid 再下发即定义变化,find 答 最后到达 版本;未知 oid 返回 empty 走宽松视图)。
     * 关键步骤:两个 oid 各入版本、oid=1 追加 v2 → find(1) 答 v2(wire 与 TableId 互证)、
     * find(999) 为 empty。
     * 边界:live 解码点的渲染视图恒取最新,不做 asOf——DDL 前的旧消息在 live 点本就按当下
     * 到达序渲染。
     */
    @Test
    void findReturnsLatestViewAndEmptyForUnknownOid() {
        reg.accept(10, resolved(1, "t_a"));
        reg.accept(20, resolved(2, "t_b"));
        reg.accept(30, resolved(1, "t_a_v2"));   // 同 oid 再下发即定义变化
        assertEquals("t_a_v2", reg.find(1).orElseThrow().wire().table());
        assertEquals("t_a_v2", reg.find(1).orElseThrow().table().id().table());
        assertTrue(reg.find(999).isEmpty());
    }
}
