package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * TupleValue.Binary 值语义契约测试（引擎 TupleValueTest 的 1:1 翻写）：
 * byte[] 组件的值相等 + hashCode 一致——record 默认 equals 对数组退化
 * 为引用相等，必须显式 override，否则 Task 5 解码器对照断言全数失真。
 */
class TupleValueTest {

    /**
     * 验证意图：Binary 的 byte[] value 组件必须值相等——同字节内容、不同数组实例
     * 的两个 Binary 相等；不同内容不相等（删 override 此断言即失败）。
     */
    @Test
    void binaryValueEquality() {
        // record 默认 equals 对 byte[] 组件退化为引用相等，必须 override 为值相等
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}), new TupleValue.Binary(new byte[]{1, 2}));
        assertNotEquals(new TupleValue.Binary(new byte[]{1, 2}), new TupleValue.Binary(new byte[]{1, 3}));
    }

    /**
     * 验证意图：值相等语义下 hashCode 必须一致（equals/hashCode 契约）——
     * 相同内容两实例 hashCode 相同，否则该 record 进 HashSet/HashMap 后行为错乱。
     */
    @Test
    void binaryHashCodeConsistentWithValueEquality() {
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}).hashCode(),
                new TupleValue.Binary(new byte[]{1, 2}).hashCode());
    }
}
