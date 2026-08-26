package org.vastdata.vbstream.protocol;

/** TupleData 单列值。'u' 是 TOAST 未变列（值不发送），流式大事务高频出现。 */
public sealed interface TupleValue {

    TupleValue NULL = new Null();
    TupleValue UNCHANGED_TOAST = new UnchangedToast();

    record Null() implements TupleValue {}

    record UnchangedToast() implements TupleValue {}

    record Text(String value) implements TupleValue {}

    record Binary(byte[] value) implements TupleValue {}
}
