package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TupleValueTest {

    @Test
    void binaryValueEquality() {
        // record 默认 equals 对 byte[] 组件退化为引用相等，必须 override 为值相等（Task 5 断言依赖）
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}), new TupleValue.Binary(new byte[]{1, 2}));
        assertNotEquals(new TupleValue.Binary(new byte[]{1, 2}), new TupleValue.Binary(new byte[]{1, 3}));
    }

    @Test
    void binaryHashCodeConsistentWithValueEquality() {
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}).hashCode(),
                new TupleValue.Binary(new byte[]{1, 2}).hashCode());
    }
}
