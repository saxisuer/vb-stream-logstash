/**
 * PostgreSQL WAL 直解源（spec §4）：绕过逻辑解码输出插件，从 WAL 物理流直接解析变更记录。
 * 布局/解析/接收组件由后续任务在此包根之上搭建。
 */
package org.vastdata.vbstream.walsource;
