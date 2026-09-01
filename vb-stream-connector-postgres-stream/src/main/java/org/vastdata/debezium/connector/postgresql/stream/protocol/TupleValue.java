package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.util.Arrays;

/**
 * TupleData 的单列值：sealed interface 四形态，对应元组数据里每列的种类字节
 * （'n'/'u'/'t'/'b'）。引擎同名接口的 1:1 重写。
 *
 * <p><b>'u' TOAST 未变 ≠ NULL（CDC 正确性关键）</b>：NULL 是明确的列值空；
 * UNCHANGED_TOAST 是 TOAST 列值未变、服务端不发送值（<b>值不可得</b>，非空）——
 * 流式大事务高频出现（大字段不变时仅发一个 'u' 字节）。两者混同会把"未知"
 * 错当"空值"渲染，故形态严格分离且公共常量各自独立。
 */
public sealed interface TupleValue {

    /** 空值单例（'n'）——列值为 SQL NULL，语义为"明确为空"。 */
    TupleValue NULL = new Null();

    /** TOAST 未变单例（'u'）——值不可得而非 NULL，与 {@link #NULL} 严格区分。 */
    TupleValue UNCHANGED_TOAST = new UnchangedToast();

    /** 'n'：列值为 SQL NULL（明确为空）。 */
    record Null() implements TupleValue {}

    /** 'u'：TOAST 列未变、服务端不发送值——值不可得，不是 NULL。 */
    record UnchangedToast() implements TupleValue {}

    /**
     * 't'：文本形态列值。
     *
     * @param value UTF-8 解码后的列值字符串
     */
    record Text(String value) implements TupleValue {}

    /**
     * 'b'：二进制形态列值。
     * 数组组件需值相等语义（record 默认 equals 对 byte[] 退化为引用相等），故显式 override。
     *
     * @param value 原始字节（按列类型的二进制格式，解码归类型系统/下游）
     */
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
