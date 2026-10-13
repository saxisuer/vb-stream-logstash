package org.vastdata.vbstream.walsource.api;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WalSource 的 relname 探测面:SQL 常量形状 + attlen 解析(128/64/无行)三分支。
 * 真库探测路径归手动档 IT(VastCatalogSyncIT)——本类只锚 SQL 与解析纯函数。
 */
class VastProbeDiscriminatorTest {

    /**
     * 探测 SQL 形状锚:命中 pg_attribute 的 attlen 列、经 'pg_class'::regclass 定位
     * 自身关系、按 'relname' 列名过滤——三者齐备才把 NAMEDATALEN 探测到正确的列上
     * (实现字面与 {@link WalSource#RELNAME_PROBE_SQL} 一致)。
     */
    @Test
    void probeSqlTargetsPgClassRelnameAttlen() {
        assertTrue(WalSource.RELNAME_PROBE_SQL.contains("attlen")
                && WalSource.RELNAME_PROBE_SQL.contains("'pg_class'::regclass")
                && WalSource.RELNAME_PROBE_SQL.contains("'relname'"));
    }

    /**
     * 解析纯函数三分支:128（VastBase NAMEDATALEN 加宽——判别放行档）/ 64（OG 与
     * vanilla 9.2 同宽——探测可得但非 VastBase）/ 无行（探测不可得）——探测值的
     * 消费语义归 {@code WalLayouts.forServer(int, String, int)}，不可得经
     * queryRelnameAttlen 收敛 -1 后落既有 vanilla 9.2 ISE 面（消息附探测值可诊断）。
     */
    @Test
    void parseRelnameAttlenResolvesProbedWidthOrEmpty() throws Exception {
        assertEquals(OptionalInt.of(128), WalSource.parseRelnameAttlen(singleRow(128)),
                "128 = VastBase G100 的 NAMEDATALEN 加宽(唯一放行档)");
        assertEquals(OptionalInt.of(64), WalSource.parseRelnameAttlen(singleRow(64)),
                "64 = OG/vanilla 9.2 同宽(探测可得但非 VastBase)");
        assertTrue(WalSource.parseRelnameAttlen(singleRow(null)).isEmpty(),
                "无行 = 探测不可得(empty)——收敛 -1 后落既有 vanilla 9.2 ISE 面");
    }

    /**
     * 单行 ResultSet 桩（attlen 传 null 表示空结果集/无行）：next 单真一假推进、
     * getInt 直回脚本值、未脚本化方法一经调用即 AssertionError——防"桩吞掉真实调用"
     * 的静默假绿（沿 {@code OgWalSourceConfigTest.FakeJdbc} 惯例）。
     *
     * @param attlen 脚本列值（null = 空结果集）
     * @return ResultSet 代理
     */
    private static ResultSet singleRow(Integer attlen) {
        boolean[] consumed = new boolean[1];
        return (ResultSet) Proxy.newProxyInstance(VastProbeDiscriminatorTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, (p, method, args) -> switch (method.getName()) {
                    case "next" -> attlen != null && !consumed[0] && (consumed[0] = true);
                    case "getInt" -> attlen;
                    case "close" -> null;
                    default -> throw new AssertionError("ResultSet." + method.getName() + " 未脚本化");
                });
    }
}
