package org.vastdata.debezium.connector.postgresql.stream;

import ch.qos.logback.classic.LoggerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 测试域日志绑定防回归:断言 slf4j 实际绑定到 logback,而非 NOP。
 * 背景(2026-09-01 NOP 事故与用户裁决):slf4j-api 若锚 1.7.36,provided 直连声明按
 * Maven 最近优先钉住版本,调解掉 logback-classic 1.5.18 传递的 slf4j 2.x;而 logback
 * 1.3+ 已移除 1.x 的 StaticLoggerBinder 绑定机制,1.7.36 的 LoggerFactory 找不到绑定类
 * 即回落 NOP——测试域全部日志调用静默吞掉。用户裁决将 slf4j-api 升 2.0.x:2.x 与
 * Debezium(1.7 编译面)二进制兼容,且经 ServiceLoader(SLF4JServiceProvider)机制
 * 重新发现 logback 的 LogbackServiceProvider,绑定恢复。本测试守住该裁决不被
 * 依赖调整(版本回锚/剪测试域依赖)无声回退。
 */
class LoggingBindingTest {

    /**
     * 断言 slf4j 绑定工厂是 logback 的 LoggerContext。
     * 实现方式:直接检查 {@link LoggerFactory#getILoggerFactory()} 的运行时类型——
     * 绑定健康时为 {@code ch.qos.logback.classic.LoggerContext}(LogbackServiceProvider
     * 建立的真工厂);绑定回归时为 {@code org.slf4j.helpers.NOPFactory}(NOP 回落),
     * assertInstanceOf 失败并在消息中指向本类 javadoc 的事故背景。无边界分支:该断言
     * 不依赖任何外部状态(ServiceLoader 初始化在首个 getLogger 调用内完成)。
     */
    @Test
    void slf4jBindsToLogbackInTestDomain() {
        assertInstanceOf(LoggerContext.class, LoggerFactory.getILoggerFactory(),
                "slf4j 绑定回归:期望 logback LoggerContext,实得 NOP——见 LoggingBindingTest 类 javadoc 的 NOP 事故与 2.0.x 裁决背景");
    }
}
