package org.vastdata.vbstream.protocol;

import java.util.Arrays;

/** TupleData 单列值。'u' 是 TOAST 未变列（值不发送），流式大事务高频出现。 */
public sealed interface TupleValue {

    TupleValue NULL = new Null();
    TupleValue UNCHANGED_TOAST = new UnchangedToast();

    record Null() implements TupleValue {}

    record UnchangedToast() implements TupleValue {}

    record Text(String value) implements TupleValue {}

    /** 数组组件需值相等语义（record 默认 equals 对 byte[] 退化为引用相等），故显式 override。 */
    record Binary(byte[] value) implements TupleValue {

        @Override
        public boolean equals(Object o) {
            return o == this || o instanceof Binary other && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }
}
