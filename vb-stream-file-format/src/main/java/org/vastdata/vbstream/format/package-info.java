/**
 * VBFG 落地文件格式（契约层）：精简事件模型 + 二进制流式读写。
 *
 * <p>移植自 vb-cdc-file-transform 仓的 cdc-file-format 模块（2026-09-11 快照，写侧 +
 * 读侧完整移植）——目标是通过该契约层产出的落地文件与该项目 cdc-sink 的消费格式<b>逐字节
 * 互通</b>。选择源码移植而非 Maven 依赖是为保持本仓 {@code mvn test} 单命令独立构建；
 * 代价是格式实现两份，<b>同步契约</b>：字节布局任何不兼容变更须在两仓同步递增
 * {@code ChangeFileWriter.VERSION}；{@code TypeCode}/{@code Op} 枚举只追加、不重排不改名。
 * RoundTrip 测试（Writer→Reader）锚定移植正确性。
 */
package org.vastdata.vbstream.format;
