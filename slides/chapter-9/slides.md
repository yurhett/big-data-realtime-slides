---
theme: easy-jyy
title: 大数据实时计算与应用 · 第9章 管理 HBase
description: HBase 表与列簇的数据描述、表管理 API（HBaseAdmin）以及集群管理操作
author: 吴斌
date: 2023-06-01
tags:
  - 大数据实时计算与应用
  - HBase
  - 表管理
  - 集群管理
transition: fade-out
mdc: true
drawings:
  persist: false
---

# 第9章 管理 HBase

> 从表结构定义到整集群的管理与运维

<!--
前面几章我们一直在"用" HBase：增删改查、进阶特性。这一章我们切换到"管"的视角——你作为一个管理员或架构师，怎么定义一张表的结构、怎么创建和修改表、怎么查看和管理整个集群的状态。HBase 的管理核心是 HBaseAdmin 这个类。学会了它，你不仅能操作数据，还能掌控 HBase 集群的整体健康状况。这对后面第 14 章做实时应用的运维打底非常重要。

[Sources]
- 吴斌，《大数据实时计算与应用》，清华大学出版社，第 9 章。
-->

---
class: compact
---

## 本章目录

1. **9.1 HBase 数据描述**：表、列簇、属性
2. **9.2 表管理 API**：HBaseAdmin 基础操作与集群管理

<!--
同学们请看本章两大板块。9.1 我们先把 HBase 的"结构"讲清楚——表、列簇、属性怎么定义，它们决定了数据如何存储；9.2 转入管理 API，用 HBaseAdmin 这个类完成建表、删表、改结构，以及集群状态查看、region 管理这些运维操作。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9 章，章节结构。
-->

---
layout: section
class: compact
---

## 9.1 HBase 数据描述

<ol class="outline">
  <li class="current">表（HTableDescriptor）</li>
  <li>列簇（HColumnDescriptor）</li>
  <li>属性</li>
</ol>

<!--
在 HBase 里建表，本质上是定义表结构和列簇结构——这些定义关系到数据如何存储、何时存储。我们从表、列簇、属性三个层面逐个看。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.1 节。
-->

---
class: compact
---

## 先理解：表、列簇、列，到底谁套谁

这三者容易混，用"房子"来比喻就不乱了：

- **表（Table）**：整栋**大楼**
- **列簇（Column Family）**：大楼里的**一层**（或一个大区）。同一层的东西会"就近存放"，便于一起读
- **列（Column）**：这一层里的**一个房间/一件东西**（如"姓名"")
- **行（Row）**：**A 户 / B 户**这样按"行键"划分的一户人家，它的东西可以跨越好几个列簇

**完整地址** = `行键(row) + 列簇:列(family:column)`

**重点**：列簇是**物理存储的分组**，建表时就要定好且**多了不好改**——所以列簇要提前规划，别乱加。

<!--
管理 HBase 前，先把"表、列簇、列"的关系理清，否则改起来会迷糊。我们用一个房子比喻：整张表就是整栋大楼；列簇是大楼里的"一层"，同层的东西会挨着放、方便一起读；列则是这层里的一个房间或一件东西。而"行"是某户人家（按行键标识），这户人可以同时拥有好几层的东西。所以定位一个数据，就是"哪户人家的哪一层、哪件东西"。特别要记住：列簇是**物理层面**的分组，建表就要定好，且通常不好改——所以列簇必须提前想清楚，不能随手加。理解了这点，后面的表管理操作就顺理成章了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.1 节，基础。
-->

---
class: compact
---

## 表：HTableDescriptor

- 数据最终存储在一张或多张表中；用表的目的之一是**控制表中的所有列**以共享某些特性
- 构造函数：`HTableDescriptor(String name)`、`HTableDescriptor(byte[] name)`、`HTableDescriptor(HTableDescriptor desc)`
- 表名通常以 String 或 `byte[]` 表示，会作为**存储路径的一部分**，因此必须符合文件名规范
- 与 RDBMS 不同，HBase 列式存储允许用户存储**大量信息到同一张表**

<!--
**[核心]** 表是 HBase 最大的存储单元，而表名有个很容易被忽略的约束：因为表名会成为存储路径的一部分，所以它必须符合文件命名规范——不能用非法字符。这是 HBase 跟关系型数据库一个明显的不同。另外 HBase 鼓励把大量信息放进少数几张表，而不是像 RDBMS 那样拆成很多张表，这与它面向列、可动态加列的特性相符。建表本质就是构造一个 HTableDescriptor，指定表名（可有可无地带上列簇）。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.1 节。
-->

---
class: compact
---

## 列簇：HColumnDescriptor

- 列簇定义所有列的**共享信息**；可通过客户端**创建任意数量的列**
- 定位某一列需将列簇名与列名合并：`family(列簇名):qualifier(列名)`
  - 列簇名必须是**可见字符**；列名可由**任意二进制字符**组成
- 构造：`HColumnDescriptor(familyName, maxVersions, compression, inMemory, blockCacheEnabled, blocksize, timeToLive, bloomFilter, scope)` 等
- 列簇名只能通过构造函数设置；其余参数可用 setter 设置

<!--
**[核心]** 列簇是 HBase 存储规划的粒度。你有多少列簇，数据就大致按列簇分组存。关键要记住：**完整列名 = 列簇名 + 冒号 + 列名**。比如 col1:q1，col1 是列簇，q1 是列。而且两者的命名规则完全不同——列簇名必须是可见字符（因为它是路径/存储分组的依据），列名则可以是任意二进制（灵活）。列簇一创建，很多属性（版本数、压缩、布隆过滤器、存活时间等）就固定下来了，且列簇名只能通过构造函数指定，不可改。所以**列簇的设计要慎重**，因为它是"物理层面"的划分。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.1 节。
-->

---
class: compact easy-table-sm
---

## 列簇的常用属性

| 方法 | 描述 |
| --- | --- |
| `getMaxVersions()` / `setMaxVersions(int)` | 获取 / 设置列簇保留的最大版本数 |
| `getBlocksize()` / `setBlocksize(int)` | 获取 / 设置列簇存储块大小 |
| `isBlockCacheEnable()` / `setBlockCacheEnable(boolean)` | 是否允许使用缓存块 |
| `getTimeToLive()` / `setTimeToLive(int)` | 获取 / 设置数据生存时间 |
| `isInMemory()` / `setInMemory(boolean)` | 获取 / 设置 in-memory 属性 |
| `getScope()` / `setScope(int)` | 获取 / 设置跨集群同步开关 |

<!--
**[带读]** 列簇上能配的一堆属性，本质都是在定义"这一族数据怎么存、留多久、要不要缓存、要不要同步"。挑重要的说：maxVersions 决定同一单元格保留几个历史版本，设成 1 最省空间；timeToLive 是数据生存期，过期自动删，适合做有生命周期的数据；blockCacheEnable 决定要不要缓存块，读多写少的列簇开缓存能提速；inMemory 把热数据优先放内存；scope 决定能否跨集群复制。这些参数是平衡**存储、性能、生命周期**的关键旋钮，值得为每个列簇仔细权衡。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.1 节。
-->

---
class: compact easy-table-sm
---

## 表的其他属性

| 方法 | 描述 |
| --- | --- |
| `getName()` / `getNameAsString()` / `setName()` | 获取/设置表名 |
| `getMaxFileSize()` / `setMaxFileSize(long)` | 获取/设置表中 region 的大小 |
| `isReadOnly()` / `setReadOnly(boolean)` | 获取/设置只读参数 |
| `getMemStoreFlushSize()` / `setMemStoreFlushSize(long)` | 获取/设置写缓冲区大小 |
| `isDeferredLogFlush()` / `setDeferredLogFlush(boolean)` | 获取/设置延时日志刷写 |

<!--
**[带读]** 除了列簇，表本身也有属性。maxFileSize 很关键——它决定一个 region 多大就该切分，直接关系到表的分布式切分粒度；调小能让数据更均匀分布，但 region 变多也带来更大管理开销。memStoreFlushSize 是内存写缓冲区的大小，决定多快把内存数据刷到磁盘。readOnly 可以把表设为只读，比如做快照或保护数据。deferredLogFlush 控制是否延后写日志。设计表的时侯，这几个参数决定了你的表是"偏写多的时候怎么刷内存"和"偏读多的时候怎么分 region"。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.1 节。
-->

---
layout: section
class: compact
---

## 9.2 表管理 API

<ol class="outline">
  <li class="current">HBaseAdmin 基础操作</li>
  <li>集群管理</li>
</ol>

<!--
结构定义好了，我们看怎么实际操作这些表。管理动作集中在 HBaseAdmin 这个类上——建表、删表、改结构、管理集群，都在这里。我们分基础操作和集群管理两部分看。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact
---

## HBaseAdmin：管理入口

- HBaseAdmin 类实现建表、创建列簇、检查表是否存在、修改表结构等功能
- 实例化前提：`HBaseAdmin(Configuration conf)`
- 考虑到安全与效率，管理类实例使用后应**销毁**；HBaseAdmin 实现了 `Abortable` 接口（`void abort(String why, Throwable e)`）
- 常用方法：`getMaster()`（master 远程对象）、`isMasterRunning()`、`getConnection()`、`getConfiguration()`、`close()`

<!--
**[核心]** HBaseAdmin 是管理动作的统一入口。使用它的前提是实例化它，传入配置。有两个使用规范值得记住：一，管理类实例用完要 close 销毁，因为它维护的资源比较重；二，它实现了 Abortable 接口，abort 方法用于紧急情况下中断。除此之外，它能拿到 master 远程对象、检查 master 是否在跑、获取连接等。这些方法让你能从客户端代码里窥探和干预集群的管理层面。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact
---

## 建表与检查

- **建表**：
  - `void createTable(HTableDescriptor desc)`：创建表（无 region）
  - `void createTable(desc, startKey, endKey, numRegions)`：创建表并分配指定数量的 region
  - `void createTable(desc, splitKeys)` / `createTableAsync(desc, splitKeys)`：按预划分的 key 建表
- **检查**：`boolean tableExists(table)`、`HTableDescriptor[] listTables()`、`getTableDescriptor(table)`

```java
HTableDescriptor desc = new HTableDescriptor(tableName);
desc.addFamily(new HColumnDescriptor(col));
admin.createTable(desc);          // 建表
boolean avail = admin.tableExists(table);   // 检查是否存在
```

<!--
**[看代码]** 建表有几种方式。最简单的 createTable(desc) 建一张空表；更进阶的可以传起始、终止行键和期望的 region 数，让 HBase 一次性把它切分好；也能显式传 splitKeys 来按你要的边界预先划分。为什么要预划分 region？因为如果一开始只有一个 region，数据会先集中写同一个节点，等它慢慢分裂，前期热点严重。所以合理规划起始就很重要。代码里展示的是最标准流程：new 一个 HTableDescriptor 加列簇，createTable，再用 tableExists 确认。检查表是否存在、列出所有表、获取描述符，都是常用的管理查询。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact
---

## 表的状态：启用 / 禁用

- 对表的状态操作：`disableTable(table)`、`enableTable(table)`、`isTableEnabled/Disabled(table)`、`isTableAvailable(table)`
- **关键规则**：一个**启用状态**下的表，用户**无法删除或修改其表结构**
- 因此改结构、删表前，先 `disableTable` 把表禁用

<!--
**[核心]** 表有个"状态"概念：启用（enabled）或禁用（disabled）。这里有个很重要的规则——**启用中的表不能删、也不能改结构**。这很像关系数据库里"正在被使用、不能动"的约束。所以任何涉及删除或改结构的操作，第一步都要先 disableTable 把表禁用，操作完再 enableTable 恢复。这一点写管理代码时特别容易踩坑：直接 deleteTable 一个启用中的表，会抛出异常。记住这个顺序：**禁用 → 操作 → 启用**。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact
---

## 删除与修改表结构

- **删除表**：`void deleteTable(table)`（必须先禁用）
- **修改表结构**：`void modifyTable(table, HTableDescriptor des)`（需先禁用，再用新的描述符修改后启用）
- 修改列簇：`addColumn(table, des)`、`deleteColumn(table, column)`、`modifyColumn(table, des)`

```java
admin.disableTable(tad);            // 先禁用
HTableDescriptor td = admin.getTableDescriptor(tad);
td.addFamily(new HColumnDescriptor(c2));   // 增加列簇
td.setMaxFileSize(1024 * 1024 * 1024L);    // 修改 region 大小
admin.modifyTable(tad, td);         // 应用新结构
admin.enableTable(tad);             // 恢复启用
```

<!--
**[看代码]** 删除和改结构，都要严格遵守"先禁用、后操作、再启用"的顺序。看代码：先 disableTable，拿到当前的表描述符，往里面 addFamily 加一个列簇、setMaxFileSize 改 region 大小，然后 modifyTable 提交新结构，最后 enableTable 恢复。这一整套流程，正是把前面讲过的"列簇、属性"概念落地成真正的结构变更。注意如果你直接在启用状态试图 deleteTable，会报错——所以一定要养成"先禁用再动手"的习惯。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact easy-table-sm
---

## 9.2.2 集群管理

HBaseAdmin 还能对集群进行管理，包含查看集群状态、执行表级任务、管理 region 服务器：

| 方法 | 描述 |
| --- | --- |
| `checkHBaseAvailable(conf)` | 验证客户端能否与 HBase 集群通信 |
| `getClusterStatus()` | 查询集群状态信息 |
| `flush(tableName)` | 将 region 数据刷写到磁盘 |
| `compact(tableName)` / `majorCompact(tableName)` | 合并文件 / 后台队列合并 |
| `split(tableName[, splitPoint])` | 拆分 region 或整表 |

<!--
**[带读]** 集群管理的工具按用途分组最好记。这一页是"查看/数据整理"类：checkHBaseAvailable 测连通性、getClusterStatus 拿全集群状况；flush 把内存数据强制刷盘、compact 触发文件合并（major 更彻底）、split 手动切分 region——常在"表太大、数据卡内存、要调分布"时手动干预。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact easy-table-sm
---

## 集群管理：region 与停机

| 方法 | 描述 |
| --- | --- |
| `closeRegion(regionName, hostAndPort)` | 关闭特定 region |
| `assign(region, force)` / `unassign(region, force)` | region 上线 / 下线 |
| `move(region, destRegion)` | 把 region 移到目标服务器 |
| `balanceSwitch(boolean)` / `balancer()` | 开关 / 执行负载均衡 |
| `shutdown()` / `stopMaster()` / `stopRegionServer(host)` | 关闭集群 / master / region 服务器 |

<!--
**[带读]** 这一页是"region 调整与停机"类。assign/unassign 控制一个 region 上线还是下线，move 把它挪到别的服务器，balancer 做负载均衡——这些常用于某台机器负载过高时。结尾的 shutdown、stopMaster、stopRegionServer 是关机操作，shutdown 最彻底。分清"查看 / 调整负载 / 停机"三类，是管理 HBase 的基本功。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact
---

## 查看集群状态示例

```java
ClusterStatus status = admin.getClusterStatus();     // 获取集群状态
System.out.println("HBase Version: " + status.getHBaseVersion());
System.out.println("No. Live Servers: " + status.getServersSize());
System.out.println("No. Dead Servers: " + status.getDeadServers());
System.out.println("No. Regions: " + status.getRegionsCount());
System.out.println("Avg Load: " + status.getAverageLoad());

for (ServerName server : status.getServers()) {       // 迭代每台服务器
    ServerLoad load = status.load(server);
    System.out.println("Host: " + server.getHostname()
        + "; Regions: " + load.getNumberOfRegions());
    for (Map.Entry<byte[], RegionLoad> e : load.getRegionsLoad().entrySet()) {
        RegionLoad rl = e.getValue();
        System.out.println("Region: " + Bytes.toStringBinary(rl.getName())
            + "; Requests: " + rl.getRequestsCount());
    }
}
```

- 依次获取：集群整体（版本、存活/死亡服务器数、region 数、负载）→ 每台服务器 → 服务器内每个 region 的三层信息

<!--
**[看代码]** 这是最有用的运维代码之一：三层循环打印集群状态。第一层是集群整体——版本号、存活服务器数、死了几台、总 region 数、平均负载，一眼看出集群健不健康。第二层遍历每台服务器——主机名、它管几个 region。第三层深入每个 region——名字、读写请求数。有了这三层信息，你就能发现：某台服务器 region 太多是不是负载不均？某个 region 请求量异常是不是热点？这就是运维 HBase 时定位问题的核心手段，远比盯着 Web 页面直观。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9.2 节。
-->

---
class: compact
---

## 想一想（自检）

**问题 1**：想删除一张表，通常的操作顺序是什么？为什么？

> 提示：注意"启用状态"的限制。

**问题 2**：一次发现某台 Region Server 负载特别高、其他机器很闲，你会用 HBaseAdmin 的哪些方法来调节？

> 提示：想想"移动 region"和"负载均衡"。

<!--
这两题把"表管理"和"集群管理"都考到了。第一题：删除表的正规顺序是**先禁用（disableTable）→ 再删除（deleteTable）**。因为在启用状态下，HBase 不允许删表或改结构，必须先禁用让表"停下来"，才能安全操作。第二题：可以用 **move** 把这台机器上的某些 region 移到其他机器，或直接用 **balancer()** 触发负载均衡算法，让它自动重新分配 region，达到各机器均衡。能答出这些，说明你已经具备管理 HBase 表和集群的基本动手能力了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9 章，自检。
-->

---
class: compact
---

## 本章小结

1. **数据描述**：表（HTableDescriptor）控制最大结构；列簇（HColumnDescriptor）是物理存储划分，名必可见；属性决定版本数、TTL、缓存、region 大小等
2. **表管理**：HBaseAdmin 负责建表、检查、修改；**启用状态不能删/改结构**，必须先禁用
3. **集群管理**：getClusterStatus 三层查看、flush/compact/split/move/balancer 调整负载、shutdown/stopMaster/stopRegionServer 停机

<!--
**[过渡]** 我们收一下第九章。结构上，你要分清"表、列簇、属性"三层，尤其牢记列簇是物理划分、名字只能建时定、且必须可见；操作上，死记一个铁律——**启用状态不能删改结构，先禁用后操作再启用**；运维上，getClusterStatus 三层信息是诊断健康与热点的抓手。至此，HBase 这部分的管理就完整了。下一章，我们要回到实时计算的主战场，认识 Storm 这个计算引擎。

[Sources]
- 吴斌，《大数据实时计算与应用》第 9 章，本章小结。
-->
