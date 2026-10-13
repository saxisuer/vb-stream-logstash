# vb-stream-wal-source VastBase G100 适配设计(第四布局档)

- **日期**:2026-10-13
- **状态**:设计定稿(用户已批准方案 A 与七节设计);**T0-T7 已收官(同日)——执行偏差回填见 §9**
- **前置**:M1/M2/M3 已收官(OG 5.0.0 档 catalog + DML 面 + A 档 + 分区捕获);本 spec 为独立适配线,不占用 OG M 系列编号(M4 仍指 B/'C' 档)
- **实测基础**:2026-10-13 对真实 VastBase G100 V3.0(Build 9,OG 6.0.0 内核,172.16.107.133:55433/test_a)的探测与全装配试跑,锚定数据见附录 A

---

## §0 背景与实测发现

VastBase G100 是海量数据基于 openGauss 内核的发行版。目标实例(实测锚定):

- **version() 报 `PostgreSQL 9.2.4 on x86_64-pc-linux-gnu, compiled by g++ (GCC) 7.3.0, 64-bit`——无任何 openGauss/VastBase branding**(branding 只在 `v$version` 视图:`Vastbase G100 V3.0 (Build 9) Release ... openGauss version:6.0.0`)
- `server_version_num=90204`(OG 系撞号,与 vanilla 9.2 同码)
- 全库 A 档(`pg_database.datcompatibility='A'`,含缺省库);`wal_level=logical`;test 用户 rolreplication=t

### 0.1 现有代码的实测行为(判别层)

`WalLayouts.forServer(90204, "PostgreSQL 9.2.4 ...")` 落"vanilla PG 9.2 非目标版本"分支,启动期 ISE fail-fast,**零槽副作用**(ISE 先于建槽,M2 护栏纪律生效)——行为正确,但 VastBase 被拒之门外。

### 0.2 临时分支全装配试跑(判别放宽后)——兼容面/不兼容面实证

**全通**(OG 5.0.0 档装配直接工作):

- OG 变体建槽(`dummy_standby` 语法)✅
- 四字典种子引导(pg_attribute 13282 行 / pg_class 1263 行 / pg_namespace 33 行 / pg_partition 0 行,段号回填全齐)——**种子 SQL 面零改动实证**(42/29 列超集下显式列名投影全兼容)✅
- direct 裸 v3 直连(认证/IDENTIFY_SYSTEM/START_REPLICATION/CopyBoth 流面)✅
- **WAL 记录层**:132019 条记录零 resync、零 reconnect,census 分布稳定(rmid 18/11/10)——页魔数 0xD074/记录头 32B/rmid 分派/块引用在 OG 6.0 内核上全兼容 ✅
- A 档护栏放行(M3 白名单 {'PG','A'})、DML 面装配 ✅

**折在 catalog 重放层**(唯一不兼容点):

```
ISE: tuple has 42 atts but dictionary has 40
    at TupleDecoder.decodeTupleData(TupleDecoder.java:203)
    ← CatalogReplay.heapEvents ← CREATE TABLE 写入的 pg_class heap INSERT 行
```

**根因(探测钉死,附录 A)**:VastBase 的 catalog 元组几何与 OG 5.0.0 存在三处系统性差异——

1. **NAMEDATALEN 64→128**:`name` 类型全 catalog 加宽(pg_class.relname / pg_attribute.attname / pg_partition.relname / pg_namespace.nspname 的 attlen 全部 128)——所有含 name 列的词典**读区偏移统一 +64B**
2. **pg_class 42 列**(OG 40):槽 24(attnum 25)插入 `relhassecids`(bool,OG 词典无此列),分叉点后槽位整体 +1;尾部 `relbucketkey` 后新增 `relminmxid`(int8)与 `relrewrite`(oid)
3. **pg_attribute 29 列**(OG 24):槽 19(attnum 20)插入 `attalias`(name)+ `attisquoted`(bool);尾部新增 `attidentity`(char)/`attextraopt`(int8)/`attdroppedname`(name)

### 0.3 适配结论

连接/协议/建槽/引导/记录层**全兼容**(M1–M3 成果直接复用);不兼容集中在**词典几何**(四 watched 词典内容 + name kind 宽度 + 读区偏移)与**判别**(无 branding 需第二探测面)。→ 新增第四布局档 `WalLayoutVVastbase`,方案 A(独立类继承 OG 档,覆写差异面)。

---

## §1 范围与非目标

**范围**:VastBase G100 **V3.0(Build 9,OG 6.0.0 内核)** 的 catalog 同步面 + DML 面——与 OG 5.0.0 档同构的完整能力(四字典 / 检查点续传 / direct+file 双通道 / DML 输出 / 分区捕获 / A 档)。

**非目标**:

- VastBase 其他版本线(2.x 系/更早内核)不承诺——判别探测命中但版本面不认时 ISE
- 列存(`ORIENTATION=COLUMN`,compress_kits CU 族)/ 段页式/bucket/UStore——护栏沿用(M1 ASTORE 限定,词典面 relcudescrelid 等检查继续有效)
- B/'C' 档整库——护栏沿用(M4 指引不变)
- VastBase 特有扩展类型(vector 模块等)的值面渲染——矩阵外类型沿现有 `0x` 降级路径

---

## §2 布局档结构(方案 A)

```java
public final class WalLayoutVVastbase extends WalLayoutVOpenGauss {
    public static final WalLayoutVVastbase INSTANCE = new WalLayoutVVastbase();
    // 覆写:pgClassKinds() / pgAttributeKinds() / pgPartitionKinds() / pgNamespaceKinds()
    //       / 三个 pg_class 读区偏移 / majorVersion()
    // 继承:页魔数/页头/记录头/heap 尺寸(13/11/2)/块引用/walker 锚定协议/槽位索引方法
    //       (attrAttnumIndex=5、attrDroppedIndex=14、classRelfilenodeIndex=6 等——
    //        投影槽位全在各自词典分叉点之前,值不变,继承即正确)
}
```

- **词典宿主统一上移**:现 `NspRow#kinds()` 与 `PartitionRow.KINDS` 自持于 CatalogRow 静态常量(单版本无分派需求);VastBase 引入第二版本后,四词典选择权统一上移 layout——`WalLayout` 接口新增 `pgPartitionKinds()` / `pgNamespaceKinds()`(V17/V18 无此两表,返回值不被调用;OG/VastBase 各返自档词典),CatalogRow 静态常量改为包内引用源(或直接搬移),`pgClassKinds()`/`pgAttributeKinds()` 沿现有形态
- **`majorVersion() = 6`**:档号延续(OG=5)。StateStore 检查点 `pgVersion` 字段存档号——v5 检查点在 VastBase 档拒载回落全新引导(安全侧,无需 formatVersion 升版:PartitionRow 序列化格式不变,词典差异不进 state 面)
- **`openGaussDialect()` 沿 true**(继承):TableFilter OG 语义(relkind 收窄 + parttype 闸)、XactGrouper OG 接缝(OgXactView)、MULTI_INSERT OG 分支等全部沿用

## §3 词典几何明细

### 3.1 pg_class 词典(oidhdr + 42 项,总长 43)

与 OG 5.0(oidhdr + 40)的差异:

| 差异 | 位置(词典槽,oidhdr=0) | kind |
|---|---|---|
| relname 加宽 | 1 | `name` → **`name128`** |
| 插入 relhassecids | 24(attnum 25,relhasoids 后) | `bool` |
| relbucketkey 后新增 relminmxid | 40 | `int8` |
| 新增 relrewrite | 41 | `oid` |

分叉点前(槽 0–23)逐项与 OG 同 kind(仅槽 1 宽度变);分叉点后(槽 25–41)按 VastBase 列序重排(relhaspkey..parttype..relbucketkey 各 +1 槽,relcmprs `int1`、reloptions `text` 等同 kind 沿用)。**parttype 槽 31→32 无投影消费者**(M3 分区身份链走 pg_partition 字典,pg_class.parttype 值面不随字典携带——`CatalogSnapshot.parttypeOf` javadoc 锚),词典数组写对即可。

**读区偏移覆写**(统一 +64,name 加宽净效果):

| layout 方法 | OG 5.0 | VastBase | 计算(数据区累计) |
|---|---|---|---|
| `pgClassRelfilenodeDataOffset()` | 84 | **148** | relname@0(128)+oid×5=128+20 |
| `pgClassReltoastrelidDataOffset()` | 116 | **180** | 148+4(tablespace)+8×2(pages/tuples,d 对齐 156/168)+4(allvisible)=180 |
| relkind@数据区(类注释锚;投影走词典槽 20,非偏移方法) | 143 | **207** | 180+oid×5+bool×2+char×1(逐项累计) |

### 3.2 pg_attribute 词典(29 项,无 oidhdr)

| 差异 | 位置(词典槽) | kind |
|---|---|---|
| attname 加宽 | 1 | `name` → **`name128`** |
| 插入 attalias | 19(attnum 20,attcollation 后) | `name128` |
| 插入 attisquoted | 20 | `bool` |
| 尾部新增 attidentity | 26 | `char` |
| 尾部新增 attextraopt | 27 | `int8` |
| 尾部新增 attdroppedname | 28 | `name128` |

槽 0–18 与 OG 逐项同 kind;槽 21–25(attacl/attoptions/attfdwoptions/attinitdefval/attkvtype)与 OG 槽 19–23 同 kind(`skip`×3 + `skip` + `int1`)。**投影槽位(IX_ATTRELID=0/IX_ATTNAME=1/IX_ATTTYPEID=2/attrAttnumIndex=5/attrDroppedIndex=14)全在分叉点(19)之前——AttrRow 与 changes 包零改动**。

### 3.3 pg_partition 词典(oidhdr + 32,总长 33)

**列集与列序零分叉**(32 列逐列同名同序实测),仅槽 1 `relname` → `name128`。读区偏移:relfilenode 84→**148**、reltoastrelid 116→**180**、parttype@数据区 64→**128**(PartitionRow 投影槽位经词典,不变)。

### 3.4 pg_namespace 词典(oidhdr + 7)

**零分叉**,仅槽 1 `nspname` → `name128`(nspowner 等后继偏移经词典走读自然正确)。

### 3.5 TupleDecoder 新 kind:`name128`

128B 定宽、对齐 'c'(1B)、渲染为 String(去尾零,同 `name` 分支语义)——与 `name`(64B)平级的静态分支,**不做宽度参数化**(kind 词汇表保持静态字符串形态,沿 `int1` 先例)。

## §4 判别扩展

### 4.1 探测面

`WalSource.start()` 现流程:建 SQL 会话 → `queryServerVersionNum` → 9 万段查 `version()` 文本 → `WalLayouts.forServer` 分发。扩展:

- 9 万段且 `version()` 文本**不含** "openGauss" 时(现行将落 vanilla 9.2 ISE 的分支),经已建 SQL 会话追加探测:

```sql
SELECT attlen FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relname'
```

- **128 → VastBase 档**;**64/其他/SQLException → 现行 vanilla 9.2 ISE 不变**(消息补 VastBase 探测结果,便于排障)。真 vanilla 9.2 零扰动;其他无 branding 的 OG 系发行版(64B name)仍拒绝
- 探测在建槽**之前**(M2 护栏纪律:ISE 先于槽副作用)
- 探测只跑一次(9 万段无 branding 才触发),17/18 档零触碰

### 4.2 分发签名

`WalLayouts` 保持 `forServer(int, String)` 纯函数语义不变(离线测试锚不破);新增:

```java
public static WalLayout forServer(int pgVersionNum, String versionText, int pgClassRelnameAttlen)
```

三参重载:9 万段 + 含 "openGauss 5.0." → OG 档;9 万段 + 无 branding + attlen=128 → **VastBase 档**;其余与两参版同面。`WalSource.start()` 改调三参版(探测值透传)。

### 4.3 OG 装配分支门控改形态

现 `WalSource.start()`:`if (layout == WalLayoutVOpenGauss.INSTANCE)` **实例相等**——子类化后 VastBase 不命中。改为 `layout instanceof WalLayoutVOpenGauss`(VastBase 档转 `startOpenGauss` 同一装配分支:OG 变体建槽/双通道/bootstrap OG 种子/state 决策全部沿用——0.2 实测已证这些面在 VastBase 上工作)。

## §5 装配面沿用清单(零改动断言,验收验证)

| 面 | 断言 | 依据 |
|---|---|---|
| 种子 SQL(四字典) | 零改动 | 0.2 实测:42/29 列超集下显式列名投影全通 |
| 槽管理(dummy_standby) | 零改动 | 0.2 实测 |
| direct v3 协议/双通道 | 零改动 | 0.2 实测(132019 条记录) |
| 兼容模式护栏 {'PG','A'} | 零改动 | M3 已含 'A';test_a 实测放行 |
| 存储护栏(列存/段页式/UStore) | 零改动 | 检查面(relcudescrelid/reloptions sniff)在词典前段,分叉点之前 |
| changes 包(XactGrouper/TOAST/渲染/TableFilter) | 零改动 | 消费行模型与投影槽位,槽位全在分叉点前 |
| heap 记录族(INS/UPD/DEL/MULTI_INSERT/PRUNE) | 零改动(待验收) | 记录层 132019 条实证;heap 载荷层由对拍钉死 |
| StateStore | 档位隔离(majorVersion 6) | v5 检查点拒载回落全新引导 |

## §6 T0 spike:逻辑流基准路探测(先行)

M2 的双路对拍基准路 = opengauss-jdbc `logical()` in-process 逻辑流 + 独立归一化器。VastBase 上未验证,spike 探测(T0,半天):

1. opengauss-jdbc `logical()` 能否连 VastBase 复制流(认证码:gaussdb 撞码 11 死角在 VastBase 是否同形?——若同,沿 M2 专用角色 + hba sha256 行方案,需在测试库建角色)
2. 逻辑槽/publication 建删语法
3. pgoutput 帧形态:proto_version=1 下 B 帧 xid Int64 / TupleData Int16 ncols 前缀与 OG 是否逐字节同形(决定归一化器复用度)
4. 分区表逻辑流身份(PARTITION_NAME=mother 语义在 VastBase 是否同 OG)

**结论落 `docs/wal-source-vastbase-adaptation-spike.md`**;对拍基座按结论定:同形 → 直接复用 `OgParityBaseline.Normalizer`;异形 → 增量分支。**spike 不通(逻辑流整体不可用)→ 验收降级**:字典 vs JDBC 实查对拍 + DML 冒烟字面验证(用户已批的回退路径)。

## §7 验收矩阵

### 7.1 离线单测(常规 CI,零 Docker)

- `WalLayoutVVastbaseTest`:词典长度(43/29/33/8)/ 逐 kind 序列断言 / 读区偏移(148/180/207/128)/ **自检锚:"VastBase 与 OG 的 pgClassRelfilenodeDataOffset 差恰 64B、pg_partition 同差 64B"**(沿"两版 toast 偏移差恰 4B"先例)/ majorVersion=6
- `WalLayoutsVastbaseDiscriminatorTest`:三参分发(90204+branding / 90204+无 branding+128 / 90204+无 branding+64 / 17/18 零变化)
- `TupleDecoderTest` 增 `name128` 黄金向量(128B 定宽 + 尾零剥离 + null 位图)
- 词典宿主上移的重定位断言(NspRow/PartitionRow kinds 经 layout 分派的 vanilla/OG 零回归锚)

### 7.2 手动档 IT(`-Dvb.it.vastbase=true`,连真库 172.16.107.133:55433/test_a;常规 `mvn test` 跳过,沿 `Pg17DefaultWorkMemTest` 模式)

- `VastCatalogSyncIT`:干扰 DDL 序列下四字典 vs JDBC REPEATABLE READ 实查**逐行全等**(M1 `OgCatalogSyncIT` 断言面同源翻译)
- `VastDmlParityIT`:双路对拍(T0 后按结论装配基准路)——v2 六场景 + 分区专项(母表身份/白名单双匹配/EXCHANGE)+ A 档专形(date 时段)
- `VastLifecycleIT`:启停/检查点续传/损坏回落/槽推进
- 测试槽/表独立前缀 + finally 清理(真库卫生纪律);库端前置(publication/专用角色)由 IT 幂等自建(T0 结论后定)

### 7.3 真库手动冒烟

全装配 Main 对真库跑通:引导 → 收流 → DML 事务块输出(TXN-BEGIN/逐行/TXN-END)→ 分区行母表身份输出 → Ctrl-C 优雅停机。

## §8 风险与已知边界

- **OG 6.0 heap 记录族变体未逐族锚定**:记录层(census/rmid)实证兼容,但 heap 载荷族(MULTI_INSERT isCompressed/PRUNE 老格式/截断 UPDATE 读区)的 6.0 形态需对拍钉死——对拍矩阵覆盖即验收;发现变体走 layout 新接缝(不静默)
- **MultiXactId 变体**(OG 已知限制)沿袭:xmax 为 MultiXactId 的 UPDATE/DELETE 前缀漂移 ISE——非目标声明
- **真库干扰**:手动档 IT 在用户测试库上跑,须避开业务表(orders 等既有表不触碰);槽/表 vb_wal_ 前缀 + 用例级清理
- **name128 的溢出语义**:VastBase 标识符最长 128B(实测 attlen),渲染剥离尾零即可;>128B 形态不存在于 name 定宽存储
- **判别探测的假阳性面**:真 vanilla 9.2(64B name)不受影响;未来其他 NAMEDATALEN≠64 的发行版会被误认 VastBase——可接受(探测值 128 当前唯 VastBase),消息附探测值可诊断

---

## §9 执行偏差回填(T0-T7 收官,2026-10-13)

实施期对设计的四处修正/补充(逐项经审查裁定正当,原章节保留作历史):

1. **§0.2/§5 "记录层(heap 载荷)零改动"修正为"记录头全兼容,heap 载荷 xid 宽度分叉"**
   (T6 真库对拍首轮 9 红逼出的真差异,设计期 132019 条 census 实证只覆盖记录头/块引用面):
   VastBase 的 heap UPDATE/DELETE main 前缀较 OG 各 **+8(xid u64 殿后,无条件在场——OG 仅
   INIT_PAGE 变体带前置 pd_xid_base)**——`sizeOfHeapUpdate()=21`/`sizeOfHeapDelete()=19`
   覆写 + `XactGrouper` 旧行元组偏移补 `ogInitShift`(rec)(兼修 OG 档 INIT_PAGE 变体的
   同型潜在错位)。vastdump 探针锚:UPDATE info=0x40 main `[old u16][new u16][flags u8]
   [CSN u64][xid u64]`、DELETE info=0x10 同构;INSERT 常态 offnum@0 与 OG 等同不受影响、
   INIT_PAGE 变体 +8 前缀原样成立(离线双档锚:`OgXactGrouperSeamTest` 场景⑥⑦)。
2. **第二主代码接缝:`WalLayout#remapDmlTypeOid`(缺省恒等,VastBase 覆写
   9040[oradate]→1114[timestamp])**(T6):A 档 DDL 关键字 `date` 落私有类型 oradate
   (oid 9040,typlen=8)——**非 OG A 档的 1114**(M3 结论在 VastBase 反转);磁盘表示与
   timestamp 同构(8B epoch-2000 微秒),`TableFilter.assembleMeta` 经接缝归一后
   DecodeKinds/渲染零改动;catalog 面 atttypid 保持 9040 原值(catalog 对拍不受扰)。
3. **§2 "词典宿主统一上移(接口新增 pgPartitionKinds/pgNamespaceKinds)"执行为
   CatalogRow 版本感知分派**(T2 执行裁定):沿 M2/M3 已有的 `CatalogRow` kinds 分派先例
   (`NspRow#kinds(WalLayout)`/`PartitionRow.KINDS` 按布局实例判别)而非字面上移接口——
   接口面零新增方法,vanilla/OG 词典内容零变化(离线零回归锚);`nameDatumWidth()` 是
   唯一新增的布局接缝(name128 kind 定宽由其驱动)。
4. **§3.1 补充:护栏槽位三覆写**(T3,spec 词典差异表未列):relhassecids 插入使分叉点后
   槽位整体 +1——`classParttypeIndex()` 31→**32**、`classReloptionsIndex()` 34→**35**、
   `classRelbucketIndex()` 37→**38**(存储引擎护栏三判别列的物理槽位;消费面经继承的
   `classValsSlotDelta()`=1 落回 attnum 值行槽位,与 OG 同式桥接)。

另两项验收期裁定记档(非偏差,spec §8 风险面的实测落点):**uuid_out RAW 风格(32 位大写
hex 无连字符)+ numeric_out 剥小数尾零(Oracle NUMBER 语义)**——同一字节值的两种合法文本
形态,对拍矩阵避开 + javadoc 记档(沿 OG M2 float 先例),方言渲染分支归后续任务;
**逻辑解码器 DDL-in-txn 不丢弃后续行**(与 OG 的静默丢弃裁定反转,与 vanilla 同形)——
场景 4 改双块全量 parity,断言强度升不降。

---

## 附录 A:实测锚定数据(2026-10-13,172.16.107.133:55433/test_a)

**v$version banner**:

```
(Vastbase G100 V3.0 (Build 9) Release) compiled at 2026-03-31 11:59:25 commit 31338 last mr
 product name:Vastbase G100  version:V3.0 (Build 9) Release  patch:1  commit:31338
 openGauss version:6.0.0  host:x86_64-pc-linux-gnu  support module:BASIC, VECTOR
```

**pg_class 42 列**(attnum | attname | atttypid | attlen | attalign | attbyval,关键行):

```
1 relname 19 128 c f | 7 relfilenode 26 4 i t | 9/10 relpages/reltuples 701 8 d t
12 reltoastrelid 26 4 i t | 21 relkind 18 1 c t | 24 relhasoids 16 1 c t
25 relhassecids 16 1 c t(OG 无此列) | 30 relcmprs 5545 1 c t | 33 parttype 18 1 c t
34 relfrozenxid 31 4 i t | 35 relacl 1034 -1 | 36 reloptions 1009 -1
38 relfrozenxid64 28 8 d t | 39 relbucket 26 4 i t | 40 relbucketkey 22 -1 i f
41 relminmxid 28 8 d t(OG 槽 40) | 42 relrewrite 26 4 i t(OG 词典已删此列,VastBase 复归)
```

**pg_attribute 29 列**(attnum | attname | atttypid | attlen,关键行):

```
2 attname 19 128 | 3 atttypid 26 4 | 4 attstattarget 23 4 | 6 attnum 21 2
15 attisdropped 16 1 | 17 attcmprmode 5545 1 | 19 attcollation 26 4
20 attalias 19 128(OG 无) | 21 attisquoted 16 1(OG 无) | 25 attinitdefval 17 -1
26 attkvtype 5545 1 | 27 attidentity 18 1(OG 无) | 28 attextraopt 20 8(OG 无)
29 attdroppedname 19 128(OG 无)
```

**pg_partition 32 列**:列名/列序与 OG 5.0.0 逐列全等(1 relname ... 32 subpartitionno),relname attlen=128。

**pg_namespace 7 列**:nspname(128)/nspowner/nsptimeline(int8)/nspacl/in_redistribution/nspblockchain/nspcollation——与 OG 词典七列同名同序,nspname 128。

**全装配试跑关键日志**(临时判别分支,测后已还原):

```
物理槽 vb_wal_probe 创建成功(openGauss 档 dummy_standby=false)
catalog 引导完成 (layout PG5): pg_attribute 13282 行 / pg_class 1263 行 / pg_namespace 33 行 / pg_partition 0 行
START_REPLICATION 进入 CopyBoth 流面: START_REPLICATION SLOT "vb_wal_probe" 1/F3000000
smoke: lsn=1/F3E22FA0 records=132009 resyncs=0 reconnects=0 censusTop3=[18/0=49573, 11/0=29405, 10/0=28825]
ISE: tuple has 42 atts but dictionary has 40 (TupleDecoder.java:203 ← pg_class heap INSERT)
```

**探测工具**:临时 `VastProbe`/`VastSql`(置于 `target/probe`,不入库);判别临改已 `git checkout` 还原并重编译验证。

## 附录 B:任务分解线索(供 writing-plans)

T0 spike(逻辑流)→ T1 布局档类 + name128 kind + 词典上移(离线单测锚)→ T2 判别三参 + startOpenGauss 门控(离线)→ T3 真库 catalog 对拍 IT → T4 DML 双路对拍 IT(T0 结论)→ T5 生命周期 IT + 文档收口(模块/根 CLAUDE.md、README、调研文档 VastBase 段)。
