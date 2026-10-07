package org.vastdata.vbstream.walsource.changes;

import java.util.Map;

/**
 * TOAST chunk 的 JDBC 回查接口——窗口内新写值缺口的补全面（Task 10 值面裁定后的
 * 收窄角色）。
 *
 * <p><b>为什么需要回查（角色收窄注记）</b>：归集面<b>部分</b>命中而拼装不完整时——
 * 典型是 FPI 镜像形态的 chunk 记录在采集面被跳过后留下的缺口（服务端解码面不受
 * FPI 形态影响、会发值，wal 侧须回查 toast 表末态补齐才能对齐）——toast 表是普通
 * 可查表，经 SQL 按 (valueid) 取全量 chunk 拼装原值。<b>归集面全空不再回查</b>：
 * 该形态（未变列指针/跨事务/重启窗口前）engine 的 pgoutput 恒发 'u'
 * （LOGICALREP_COLUMN_UNCHANGED，proto.c），wal 侧同形渲染
 * {@code <toast-unchanged>}，回查反而产出 engine 没有的值破坏双路对拍——依据全文
 * 见 {@code ToastAssembler} 类 javadoc。</p>
 *
 * <p><b>键语义（实测钉，2026-10-07 docker PG 18.6）</b>：参数 {@code toastOid} 是
 * external 指针携带的 {@code va_toastrelid}（toast 关系自身 oid）；<b>toast 表名
 * {@code pg_toast_<oid>} 的后缀是主表 oid 而非 toast 关系 oid</b>（实测
 * 主表 33654 的 toast 关系 oid=33657、表名 {@code pg_toast_33654}）——实现须先经
 * {@code pg_class} 按 oid 解析 relname，不能拿 toastOid 直接拼名。</p>
 *
 * <p>返回契约：成功时返回 {@code chunk_seq → chunk_data} 的映射（可为空映射 =
 * 该值无 chunk）；基础设施故障允许以 RuntimeException 上抛——由
 * {@code ToastAssembler} 统一降级为 {@code toast-unavailable} + WARN，不 fail 流。</p>
 */
public interface ToastProbe {

    /**
     * 回查指定 TOAST 值的全量 chunk。
     *
     * @param toastOid external 指针的 va_toastrelid（toast 关系 oid，表名解析见类 javadoc）
     * @param valueid  external 指针的 va_valueid（toast 表 chunk_id 列的查键）
     * @return chunk_seq → chunk_data 字节（空映射 = 无行；不返回 null）
     */
    Map<Long, byte[]> fetchChunks(long toastOid, long valueid);
}
