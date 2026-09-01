package org.vastdata.debezium.connector.postgresql.stream;

import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;

/**
 * Relation 解析接缝:'R' 消息的 wire Relation → {@link ResolvedRelation}(wire + Debezium
 * Table 双形态)。引擎侧 'R' 路由直接把 wire Relation 记入版本日志;connector 的
 * {@link VersionedRelationRegistry} 存双形态,Table 的构建(类型解析 / JDBC enrich)
 * 是个可注入策略——本接口就是那个注缝点(MS2 设计,相对引擎的已文档化偏差)。
 *
 * <p>两个实现:测试用假实现(直接包 wire + {@code Table.editor()} 造最小 Table,不连库);
 * Task 7 的 {@code RelationTableFactory} 是真实现(wire 解码 + JDBC enrich 建 Table,
 * reader 线程持 main 连接调用)。
 *
 * <p>线程约束:接口本身无状态;真实现内部持 JDBC 连接,按实现方声明调用线程
 * (设计上 'R' 处理发生在 reader 线程)。
 */
@FunctionalInterface
public interface RelationResolver {

    /**
     * 责任:把到达的 wire Relation 解析成版本日志载荷(双形态)。
     * 边界与异常语义:解析失败(JDBC enrich 出错等)按实现方 fail-fast 上抛,原样终止
     * 读取线程;返回值组件由 {@link ResolvedRelation} 的紧凑构造器拒 null。
     *
     * @param seq  该 'R' 消息的序号(≡ CQ index,组装器自 pipe.append 取得——真实现可
     *             用它做诊断/追踪,不参与解析本身)
     * @param wire live 解码出的 wire Relation(协议列序真源)
     * @return 双形态表定义(不可变,随后由 registry 以 seq 记入版本日志)
     */
    ResolvedRelation resolve(long seq, PgOutputMessage.Relation wire);
}
