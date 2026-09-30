package org.vastdata.debezium.connector.postgresql.stream;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * 连接器任务类的<b>最小桩</b>(Task 3):当前唯一职责是让
 * {@link PostgresStreamConnector#taskClass()} 的类引用可编译、ServiceLoader/SPI
 * 清单指向的类可加载——完整装配(连接工厂/schema/dispatcher/流式源协调器)在
 * Task 7 全文替换本文件,四个生命周期方法届时获得真实实现。
 *
 * <p>桩的边界:start 为空操作、poll 恒返回 null(Connect runtime 视为"暂无记录")、
 * stop 为空操作、version 与连接器同源取 {@link Module#version()}(kafka 3.1.0 的
 * Task.version() 是抽象方法,必须实现);不经本桩断言任何引擎行为(任务级测试归
 * Task 7 之后)。
 */
public class PostgresStreamConnectorTask extends SourceTask {

    /**
     * 返回任务版本:kafka 3.1.0 的 Task 接口将 version() 声明为抽象方法,桩与连接器
     * {@link PostgresStreamConnector#version()} 同源取模块版本。
     *
     * @return {@link Module#version()},永不抛错
     */
    @Override
    public String version() {
        return Module.version();
    }

    /**
     * 任务启动:桩实现为空操作——配置解析与服务装配由 Task 7 的全文实现接管。
     *
     * @param props Connect runtime 传入的任务配置(taskConfigs 注入后的副本)
     */
    @Override
    public void start(Map<String, String> props) {
        // 最小桩:无状态,不解析配置
    }

    /**
     * 轮询下一条批记录:桩实现恒返回 null(无记录可发),仅满足 SourceTask 抽象签名
     * 的可编译性;真实流式取数在 Task 7 实现。
     *
     * @return 恒为 null(桩语义:本轮无记录)
     */
    @Override
    public List<SourceRecord> poll() {
        return null;
    }

    /**
     * 任务停止:桩实现为空操作——真实停机次序(session → assembler → pipe)由 Task 7 实现。
     */
    @Override
    public void stop() {
        // 最小桩:无资源可停
    }
}
