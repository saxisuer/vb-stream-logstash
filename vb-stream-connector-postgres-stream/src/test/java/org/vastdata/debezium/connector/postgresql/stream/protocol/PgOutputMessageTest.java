package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 消息模型值语义契约测试：数组组件（LogicalMsg.content、Truncate.relationOids）
 * 必须值相等（record 默认 equals 对数组退化为引用相等，故需显式 override——
 * Task 4 parser 断言与 Task 5 解码器对照测试的地基），外加 TupleValue 的
 * 'u' TOAST 未变 ≠ NULL 语义区分（CDC 正确性关键）。
 * 前两用例为引擎 PgOutputMessageTest 的 1:1 翻写（断言值不变，仅换包名与 RelationColumn 类名）。
 */
class PgOutputMessageTest {

    /**
     * 验证意图：LogicalMsg 的 byte[] content 组件必须值相等——同组件、不同数组实例的
     * 两个消息 equals 为真且 hashCode 一致；不 override 时 record 默认按数组引用比较，
     * 此断言即失败。
     */
    @Test
    void logicalMsgContentValueEquality() {
        PgOutputMessage.LogicalMsg a = new PgOutputMessage.LogicalMsg(
                OptionalLong.empty(), true, 4L, "p", new byte[]{1, 2});
        PgOutputMessage.LogicalMsg b = new PgOutputMessage.LogicalMsg(
                OptionalLong.empty(), true, 4L, "p", new byte[]{1, 2});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * 验证意图：Truncate 的 int[] relationOids 组件必须值相等——同组件、不同数组实例
     * equals 为真且 hashCode 一致（数组是本 record 唯一需 override 的组件：
     * EnumSet 与 OptionalLong 自身即值语义）。
     */
    @Test
    void truncateRelationOidsValueEquality() {
        PgOutputMessage.Truncate a = new PgOutputMessage.Truncate(
                OptionalLong.of(7L), EnumSet.of(TruncateOption.CASCADE), new int[]{1, 2});
        PgOutputMessage.Truncate b = new PgOutputMessage.Truncate(
                OptionalLong.of(7L), EnumSet.of(TruncateOption.CASCADE), new int[]{1, 2});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * 验证意图：'u' TOAST 未变（UNCHANGED_TOAST）不是 NULL——NULL 是明确的列值空，
     * UNCHANGED_TOAST 是 TOAST 列值未变、服务端不发送值（值不可得），两者混同会把
     * "未知"错当"空"渲染（CDC 正确性关键）；同时锁定两个公共常量各自绑定正确的
     * 值形态（换绑或合并成单一形态即失败）。
     */
    @Test
    void unchangedToastIsNotNull() {
        assertNotEquals(TupleValue.UNCHANGED_TOAST, TupleValue.NULL);
        assertFalse(TupleValue.UNCHANGED_TOAST instanceof TupleValue.Null);
        assertFalse(TupleValue.NULL instanceof TupleValue.UnchangedToast);
    }
}
