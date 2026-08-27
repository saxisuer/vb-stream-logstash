package org.vastdata.vbstream.replication;

/** 行级 DML 种类，对应 pgoutput 的 Insert/Update/Delete 三种消息。 */
public enum DmlKind { INSERT, UPDATE, DELETE }
