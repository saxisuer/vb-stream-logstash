package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Table;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;

import java.util.Objects;

/**
 * wire Relation + Debezium {@link Table} 的双形态表定义——引擎版本日志存
 * {@code Relation}(wire)的唯一泛化点(引擎 {@code org.vastdata.vbstream.replication} 的
 * registry 族 1:1 重写中,版本载荷由此 record 取代 wire 单形态,文字参照非依赖)。
 *
 * <p>为什么需要两个形态:wire Relation 是<b>协议列序的真源</b>——pgoutput 元组值的列序、
 * replica identity 键列、类型 oid 都以它为准,解码(元组 → 值)只能按它对位;{@link Table}
 * 是<b>Debezium 渲染视图</b>——TableId / 列名 / JDBC 类型经 Task 7 的 enrich(读 'R' 时在
 * reader 线程做 JDBC 类型解析)填出,Debezium 的 schema 与行值渲染(electron/Emitter 体系)
 * 消费它。两形态随同一版本一起入版本日志、一起进快照,asOf 取版后双视图自洽(DDL 后旧单元
 * 的 wire 与 Table 同时回退到变更时刻)。
 *
 * <p>不可变 record,可跨线程传递;registry / 快照按引用浅拷共享(无拷贝开销)。
 */
public record ResolvedRelation(PgOutputMessage.Relation wire, Table table) {

    /**
     * 紧凑构造器:两组件 fail-fast 拒 null。
     * 步骤:requireNonNull 各查一次。
     * 边界:任一形态缺失属装配错误(上游 Task 7 enrich / 测试工厂保证双形态齐备),
     * 宁可在构造点抛 NPE,不留到回放渲染期炸在 consumer 线程。
     * 线程约束:无状态校验,任意线程构造。
     */
    public ResolvedRelation {
        Objects.requireNonNull(wire, "wire");
        Objects.requireNonNull(table, "table");
    }
}
