package org.vastdata.debezium.connector.postgresql.stream;

/**
 * raw 字节消费者契约:{@link ReplicationSession#run(RawMessageListener)} 的交付接缝——
 * 引擎 {@code org.vastdata.vbstream.replication.RawMessageListener} 的 1:1 重写
 * (文字参照,非依赖)。
 *
 * <p>分量语义:{@code raw} 是<b>完整单条</b> pgoutput 消息的字节序列——含首个类型字节
 * 与其后全部字段(流式块内消息含可选的 Int32 xid 前缀),即解码器可直接消费的整体。
 * 每次回调的数组为该条消息<b>独占新建</b>,调用方可无复制地长期持有(Chronicle Queue
 * 落盘、延迟回放皆安全)——本模块的桶记账组装器正是靠此承诺把字节直接 append 进管道。
 * {@code walStart} 是该消息所属复制协议 'w' 帧的行级 WAL 位点(帧头 walStart——产生
 * 本条输出的 WAL 记录位点;会话在取到本帧载荷后<b>取后即读</b> {@code getLastReceiveLSN()}
 * 得到,逻辑流下恒为本帧位点,pgjdbc 42.7.13 反汇编实证),组装器将其嵌入管道记录供
 * consumer 回放期作行级 LSN 交付。
 *
 * <p>线程约束:回调在执行 run() 的复制读取线程(reader)内<b>同步</b>调用——回调耗时
 * 直接拖慢消息循环与 LSN 反馈,实现方不应阻塞。
 */
@FunctionalInterface
public interface RawMessageListener {

    /**
     * 消费一条 raw 消息及其行级 WAL 位点。抛出的异常经 run 循环原样上抛,终止读取线程。
     *
     * @param raw      完整单条消息字节(独占数组,可长期持有)
     * @param walStart 本帧的行级 WAL 位点(帧头 walStart)
     */
    void onRaw(byte[] raw, long walStart);
}
