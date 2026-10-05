# vb-stream-wal-source — WAL 直解源

不创建备库、不占主库逻辑解码 CPU：pgjdbc **物理复制流**接收原始 WAL 字节，纯 Java
解析记录布局，自维护 pg_attribute/pg_class 的 ctid 重放 as-of 字典（"伪装备库"）。
v1 交付 catalog 同步 + WAL 接收两块；用户表 DML 解码与 engine 汇入属 v2。

## 快速开始

前置 PG（仓库自带 Docker 环境，物理复制只需 `wal_level≥replica`，镜像已含
`max_slot_wal_keep_size=2GB` 兜底）：

```bash
cd src/docker && docker compose up -d && cd ../..     # PG 18 @ localhost:55432
mvn -q -pl vb-stream-wal-source compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "vb-stream-wal-source/target/classes:$(cat vb-stream-wal-source/target/cp.txt)" \
     -Dvb.wal.host=localhost -Dvb.wal.port=55432 \
     -Dvb.wal.state.dir=/tmp/wal-source-state \
     org.vastdata.vbstream.walsource.Main
```

（macOS/Linux classpath 分隔符 `:`，Windows 为 `;`。无需 `--add-opens`——本模块零
Chronicle 依赖。运行期每 10s 打一行 `smoke:` 周期统计 INFO——消费前沿 LSN + 计数器 +
census 前 3 形态，state 启用时附 `ckpt=`/`adv=` 检查点与槽推进观测；Ctrl-C 优雅退出
含最终 best-effort 检查点 + 槽推进；启动失败 exit 1。）

配置键全表（`-Dvb.wal.*` 九键，默认值即下表；单一来源 `WalSource`/`StateConfig`）：

| 键 | 默认 | 语义 |
|---|---|---|
| `vb.wal.host` | `localhost` | PG 主机 |
| `vb.wal.port` | `5432` | PG 端口（非数字启动期 fail-fast） |
| `vb.wal.db` | `postgres` | 库名 |
| `vb.wal.user` | `postgres` | 用户（须有 REPLICATION 权限） |
| `vb.wal.pass` | `postgres` | 密码 |
| `vb.wal.slot` | `wal_source` | 物理复制槽名（自动建/复用，`immediately_reserve=true`） |
| `vb.wal.state.dir` | 缺省 = 禁用 | 检查点目录（启用后：续传/周期落盘/槽推进整个生命周期面；一目录一活实例） |
| `vb.wal.state.interval.ms` | `30000` | 周期检查点间隔（毫秒） |
| `vb.wal.state.events` | `1000` | catalog 行事件数阈值（与间隔先到为准） |

## 架构

设计全文见 [docs/superpowers/specs/2026-10-05-wal-source-module-design.md](../docs/superpowers/specs/2026-10-05-wal-source-module-design.md)（先导实验
[docs/wal-direct-decode-spike.md](../docs/wal-direct-decode-spike.md)，9 场景 27 条发现）。
一句话：五个面向接口的组件——版本化布局描述符 `WalLayout`（V17/V18，逐常量标注
REL_STABLE 源码出处，未注册版本启动即拒）、物理流接收 `WalStreamReceiver`/`WalStreamWalker`
（pageaddr 锚定协议 + 断流自动重连）、记录解析 `WalRecordParser`（heap 家族 → 强类型
record，FPW 页镜像重建）、ctid 重放 `CatalogSynchronizer`（JDBC 一致性引导 + 竞速三修正：
引导一致性 / ctid 寻址精确采纳自愈（末态回填）/ pageaddr + 自维护数据锚）、检查点持久化
`StateStore`（单文件 VBWS 格式，CRC 双验 + 原子 rename，损坏回落全新引导）——由 `WalSource`
门面装配为一次 `start()`，单线程直通（接收→解析→重放→周期落盘）。运行依赖仅 pgjdbc +
slf4j-api，组件细节与坑位见模块 `CLAUDE.md`。

## 验收

对抗性对拍 IT：autovacuum 拉满 / 并发 DDL / 随机 CHECKPOINT 干扰下执行 DDL 场景序列，
快照与 JDBC REPEATABLE READ 实查**逐行全等**（ctid / relfilenode / toast 映射 / dropped /
列序）——PG 17/18 双版本矩阵，`mvn test -pl vb-stream-wal-source` 单命令全跑
（128 用例 = 离线 108 + Testcontainers IT 20，后者需本机 Docker）。

## 已知限制（详版见模块 CLAUDE.md）

- **压缩 FPW / external varlena 不支持**——运维前提 `wal_compression=off`（CHECKPOINT
  后首写必带页镜像）；TOAST 重组属 v2
- **末态回填语义**：自愈只承诺 catalog **末态**正确（对拍面即末态全等），丢页/断链窗口
  内的中间代际不可恢复
- 56KB 服务端侧流形态跳变未根因（pageaddr 重锚自愈、对拍未破，`resyncs ≤ 2` 容差 +
  lossyResyncs 观测面）；PG 17 干扰风暴与 DDL 并发交织偶发丢 INSERT 事件（矩阵已次序
  规避，跟进项记档）
- VACUUM FULL 全表重发的 CDC 语义、two_phase 记录消费属 v1 非目标
