package org.vastdata.vbstream.protocol;

/** 关系列。typmod 为随协议下发的 atttypmod（PG 10 起即在 Relation 消息中，REL_17/18_STABLE 源码实证一致）；partOfKey 对应列 flags 的 bit0。 */
public record Column(String name, int typeId, int typeModifier, boolean partOfKey) {}
