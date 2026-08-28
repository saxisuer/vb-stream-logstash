package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

/**
 * 消息消费者契约。里程碑 2 的 Chronicle Queue 写入器实现同一接口即可接入。
 * 渲染视图参型自 {@link RelationRegistry} 放宽为 {@link RelationLookup}（1.7 设计 §4.3）：
 * live 解码点传 registry（reader 线程最新视图）、回放解码点传桶内不可变快照——
 * 实现方对 Relation 来源实现无关，跨线程竞争由接口形态消除。
 */
@FunctionalInterface
public interface PgOutputListener {

    /**
     * 责任：单条解码后消息的交付（附渲染用的 Relation 宽松查询视图）。
     * 边界：视图 find miss 由实现方降级处理（本契约不做 fail-fast）；回调线程与实现方约定见各实现 javadoc。
     *
     * @param message 解码后的 pgoutput 消息
     * @param lookup  Relation 宽松查询视图（registry 或不可变快照，见类 javadoc）
     */
    void onMessage(PgOutputMessage message, RelationLookup lookup);
}
