package org.vastdata.debezium.connector.postgresql.stream;

/** 行级 DML 种类,对应 pgoutput 的 Insert/Update/Delete 消息(格式见解码设计 spec 附录 A)。引擎 {@code DmlKind} 的 1:1 重写(文字参照,非依赖)。 */
public enum DmlKind {
    /** 新增行:仅 after 元组(before 恒 empty)。 */
    INSERT,
    /** 更新行:after 必有;before 是否存在取决于表 replica identity('K'/'O',可无)。 */
    UPDATE,
    /** 删除行:仅 before 元组(after 恒 empty)。 */
    DELETE
}
