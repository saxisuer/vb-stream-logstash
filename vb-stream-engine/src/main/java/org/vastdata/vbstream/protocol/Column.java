package org.vastdata.vbstream.protocol;

/** 关系列。typmod 为 PG 18 起随协议下发的 atttypmod；partOfKey 对应列 flags 的 bit0。 */
public record Column(String name, int typeId, int typeModifier, boolean partOfKey) {}
