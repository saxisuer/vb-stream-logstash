package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

/** 消息消费者契约。里程碑 2 的 Chronicle Queue 写入器实现同一接口即可接入。 */
@FunctionalInterface
public interface PgOutputListener {

    void onMessage(PgOutputMessage message, RelationRegistry registry);
}
