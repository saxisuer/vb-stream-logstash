/**
 * vb-stream-reader——debezium-embedded 宿主冒烟应用:以 {@code DebeziumEngine.create(Connect.class)}
 * 工厂新建引擎实例加载同仓连接器 {@code org.vastdata.debezium.connector.postgresql.stream.
 * PostgresStreamConnector},{@code ChangeConsumer} 回调逐条拿 {@code SourceRecord} 输出 slf4j 日志。
 *
 * <p>组件四件:{@link org.vastdata.vbstream.reader.ReaderProperties}——{@code -Dvb.*} 系统属性
 * 剥前缀零映射透传为 Debezium 配置(连接器 6 个专属项与 engine 高级项零代码可用)+ reader 自有
 * 默认值注入;{@link org.vastdata.vbstream.reader.LogChangeConsumer}——CDC 记录行渲染与
 * offset 记账(markProcessed/markBatchFinished 手动提交);{@link org.vastdata.vbstream.reader.
 * EngineLifecycle}——引擎装配、run 线程与停机收敛闸门(Main 与 IT 共用);
 * {@link org.vastdata.vbstream.reader.Main}——入口。
 *
 * <p>运行期注记:连接器内 Chronicle Queue 的 mmap 在 Java 17 需 5 项 {@code --add-opens}
 * (与根 pom surefire argLine 同源);CDC 数据走专用 logger 名
 * {@code org.vastdata.vbstream.reader.cdc}(INFO,与系统日志分离可独立调级)。
 */
package org.vastdata.vbstream.reader;
