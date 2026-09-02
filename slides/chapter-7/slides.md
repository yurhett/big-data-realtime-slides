---
theme: easy-jyy
title: 大数据实时计算与应用 · 第7章 HBase 基础操作
description: HBase 的 CRUD 操作、批处理、行锁、扫描以及其他客户端 API（HTable 与 Bytes）
author: 吴斌
date: 2023-06-01
tags:
  - 大数据实时计算与应用
  - HBase
  - CRUD
transition: fade-out
mdc: true
drawings:
  persist: false
---

# 第7章 HBase 基础操作

> 动手对 HBase 表做增、查、改、删

<!--
上一章我们认识了 HBase 的架构，这一章就真正上手操作数据。一句话总结本章：围绕 HBase 最主要的客户端接口——HTable 类，学会用代码对表做增、查、改、删（即 CRUD），以及批处理、行锁、扫描这些进阶用法。很多同学可能觉得写点 Java 代码很枯燥，但你要把 HBase 用起来，这些就是绕不开的基本功。我们不讲高深理论，就脚踏实地看 HBase 到底提供了哪些方法、每个方法解决什么问题。

[Sources]
- 吴斌，《大数据实时计算与应用》，清华大学出版社，第 7 章。
-->

---
class: compact
---

## 本章目录

1. **7.1 CRUD 操作**：Put（增/改）、Get（查）、Delete（删）
2. **7.2 批处理操作**：batch 批量提交
3. **7.3 行锁**：保证单行操作的原子性
4. **7.4 扫描**：Scan 整表/区间扫描
5. **7.5 其他操作**：HTable 与 Bytes 工具

<!--
同学们请看本章主线。CRUD 是四个基础动作，我们会逐个看 Put、Get、Delete 这三个最重要的；之后讲批处理，因为一次一条太慢；再讲行锁，因为它关乎并发安全；最后是扫描，用于批量读取，以及一些实用的工具方法。这条线走完，你就掌握了操作 HBase 表的完整工具箱。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7 章，章节结构。
-->

---
layout: section
class: compact
---

## 7.1 CRUD 操作

<ol class="outline">
  <li class="current">Put 操作（增 / 改）</li>
  <li>Get 操作（查）</li>
  <li>Delete 操作（删）</li>
</ol>

<!--
CRUD 是数据库最基础的四字诀：增、查、改、删。HBase 把它映射成 Put、Get、Delete 这三个类。我们按 Put 开始，它同时承担"增"和"改"两个动作——因为 HBase 里写入覆盖就是更新。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
class: compact
---

## 先理解：HBase 如何"定位"一个格子

在 HBase 里，你**无法像 Excel 那样用"某行某列"来引用**，因为它没有固定的列。它定位一个数据单元靠三把"坐标钥匙"：

1. **行键（row）**：一行数据是谁 → 比如"用户 1001"
2. **列簇（family）**：数据从属于哪一大类 → 比如"个人信息" / "购买记录"
3. **列（column）**：这个大类下具体哪一个字段 → 比如"姓名" / "手机号"

**完整写法类似**：`row=1001, 个人信息:手机号 = 138xxxx`（列簇和列之间用冒号 `:` 连接）

可以理解成"**按人找、再按大类找、最后按小项找**"，三步锁定一个格子。

<!--
这一章要写很多 put、get、delete，但核心其实是"你怎么定位一个格子"。HBase 不像表格有固定列，它定位数据靠三把钥匙：行键（这一行是谁）、列簇（属于哪一大类）、列（大类下具体哪一项）。写出来就像"用户1001 的个人信息：手机号"。你把它想象成"先按人找到这行，再按大类翻到那个区域，最后找具体那一项"——三步就锁定一个格子。把这三把钥匙刻在脑子里，后面的 put/get/delete 都是在"给这三把钥匙填值"。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节，基础。
-->

---
class: compact
---

## 基础：KeyValue 与 Put

- **KeyValue 对象**：HBase 底层存储的重要类，代表数据在底层存储时的状态
- 一个 KeyValue 代表表中的一个数据单元，包含：**行值（row）、列簇（family）、列（column）、时间戳（timestamp）、值（value）**
- 以这些信息确定表中的**唯一一个数据单元**
- 插入一条数据 = KeyValue 序列化后传给集群，集群据此操作

**Put 类核心构造与方法**：`Put(byte[] row)` 设定行键；`add(family, qualifier, value)` 添加列簇/列/值；`getRow()`、`setWriteToWAL(boolean)`（控制是否写预写日志）等。

<!--
**[核心]** 先理解 HBase 存储的最小单元——KeyValue。它不叫"单元格"，但本质就是一个由 row、family、column、timestamp、value 五元组唯一确定的存储单元。你插入一条数据，其实就是构造一个 KeyValue，序列化后发到集群，集群照着它的五元组去定位和保存。这里特别提醒一个 HBase 特性：没有真正意义上的"修改"，同一单元格再写入新值，靠时间戳区分新旧版本，读取时取最新。所以 Put 既是增也是改。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
class: compact
---

## Put 的三种插入方式

- **`void put(Put p)`**：向表添加一行，发送一次 **RPC** 请求
- **`boolean checkAndPut(row, family, qualifier, value, put)`**：**原子操作**，若检查条件失败则整个改动失效；多客户端同时改同一数据时效率高
- **`void put(List<Put>)` + `flushCommits()`**：**批量插入**，用列表装载多行数据一次性提交

**写缓冲区**：每次 Put 都是一次 RPC，数据量大时开销大。HBase 提供写缓冲区——Put 先被缓冲区收集，溢出或主动 `flushCommits()` 时才一次性 RPC 发送。

<!--
**[核心]** Put 有三种姿势，对应不同的需求。单条 put 最直观；checkAndPut 则适合"条件满足才写入"的原子场景——多个客户端抢着改同一数据时特别有用，因为要么都改、要么都不改，不会出现中间态；批量 put 则把多行攒进一个列表一起提交。这里的关键概念是"写缓冲区"：因为每条 put 都触发 RPC 太贵，HBase 让你先攒着，攒够了或手动 flushCommits() 时才真正发一批过去。学会用缓冲区，是写 HBase 性能的第一课。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
class: compact
---

## Put 示例（关键步骤）

```java
Configuration conf = HBaseConfiguration.create();
conf.set("hbase.zookeeper.quorum", "main1");   // 创建配置
HTable table = new HTable(conf, "test");       // 实例化客户端
table.setAutoFlush(false);                     // 启用写缓冲区
Put put = new Put(Bytes.toBytes("row1"));      // 指定行键创建 Put
put.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
table.put(put);                                // 单行写入（先入缓冲区）
table.flushCommits();                          // 强制刷写，真正 RPC 发送
```

- 核心要点：`setAutoFlush(false)` 启用写缓冲区；`flushCommits()` 主动刷写
- `checkAndPut(...)` 返回是否成功——检查列存在则失败、不存在则成功

<!--
**[看代码]** 这页把前面概念串成一个可跑的 put。注意几个关键步骤：先建配置、指定 Zookeeper；再 new 一个 HTable 指向 test 表；`setAutoFlush(false)` 这行很关键，它把写缓冲区打开，让后面的 put 先攒着不发；然后构造 Put 指定 row1，add 把 col1:q1 的值 v1 塞进去；`table.put(put)` 实际是进缓冲区；直到 `flushCommits()` 才真正发到服务器。至于 checkAndPut，它返回一个布尔值——若指定列已存在则返回 false 不写，若不存在则写入返回 true。这样一个简单的条件写入就完成了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
class: compact
---

## Get 操作：查询

- 每次 Get 取**一行**（用 row 指定），但不限制取多少列/单元格
- 每次 RPC 只发送一个 Get 对象的数据
- 查询结果每一行封装为一个 **Result 对象**（内部封装 KeyValue 数组）

**核心方法**：`Get(byte[] row)` 设行键；`addFamily()` / `addColumn()` 指定返回的列簇/列；`setTimeStamp()` / `setTimeRange()` 指定时间；`setMaxVersion()` 指定返回的版本数。

- 多行获取：`Result[] get(List<Get>)`
- **`getRowOrBefore(row, family)`**：若存在行则返回该行的列簇结果；不存在则返回排序表中该行键的**前一条**结果；找不到则返回 null

<!--
**[核心]** Get 用来查，它每次按行键取一行。你可以通过 addFamily 或 addColumn 限定只拿某些列，减少数据传输；也能用时间戳、版本数精确控制取哪个版本。多行就构造一个 Get 列表，一次 get(List) 返回 Result 数组。这里有个很实用的方法 getRowOrBefore——按行键查，如果这行没有，就返回排好序表里的**前一条**。这在做"找最近的一条边界记录"时非常好用。记住，查出来的每一行都是一个 Result，里面才是真正的 KeyValue 数据。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
class: compact
---

## Delete 操作：删除

- Delete 与 Put 功能相逆、**结构相似**，也含一个 KeyValue 数组
- **关键**：一次 Delete 并不立刻删除数据，只给对应 KeyValue 打上**删除标记**；等到下一次 region 合并、分裂时才真正移除
- 单个 `delete(Delete d)` / 列表 `delete(List<Delete>)` / 原子 `checkAndDelete(...)`
- Delete 可指定：删除某列簇、某列、某列的某个版本

<!--
**[核心]** Delete 这里有个非常重要的点：它不像传统数据库那样立即删掉。HBase 是"带删除标记"的——你先在数据上打个标记，真正的物理删除要等到 region 合并或分裂时才执行。这意味着，删除后的旧数据其实还在磁盘上，只是对你的读取不可见。这也解释了为什么 HBase 能恢复已被删除的数据（在一定时间内）。删除的粒度很灵活：整列簇、整列、单列某个版本都可以。还有个 checkAndDelete，条件满足才删，返回 true/false。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
class: compact
---

## 7.1 小结

- **Put**：既是增也是改；`checkAndPut` 做条件原子写；写缓冲区提升性能
- **Get**：单行查询；`addFamily/addColumn` 裁剪列；`getRowOrBefore` 找前一条
- **Delete**：打标记而非真删；`checkAndDelete` 条件删除
- CRUD 三者都围绕 **KeyValue**（row/family/column/timestamp/value）展开

<!--
**[过渡]** 我们把 CRUD 收一下。增用 Put、查用 Get、删用 Delete，但它们不是孤立的三件套——背后都围绕同一个 KeyValue 五元组展开，都在用时间戳管理版本，都支持"条件式"操作（checkAndPut / checkAndDelete）。理解了这个统一底层，三个操作就会很自然地融会贯通。但 CRUD 一次只碰一行或几行，性能上还有个更大的诉求——批处理。我们进入 7.2。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.1 节。
-->

---
layout: section
class: compact
---

## 7.2 批处理操作

<ol class="outline">
  <li class="current">batch 批量提交</li>
  <li>返回结果与注意事项</li>
</ol>

<!--
前面那些基于列表的操作（get(List)、delete(List)）底层其实都是基于批次方法 batch 实现的。这一节我们把 batch 本身讲清楚。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.2 节。
-->

---
class: compact
---

## batch 方法

```kotlin
Void batch(List<Row> actions, Object[] results)   // 可访问部分结果
Object[] batch(List<Row> actions)                 // 不可访问部分结果
```

- **Row** 是 Put、Get、Delete 的父类，因此 batch 可混合作不同类型操作
- **注意**：同一行的 Put 和 Delete **不能**放在同一个 batch 中（处理顺序不同会得到不同结果）
- 使用 batch() 时 Put 实例**不会**进入客户端写缓冲区
- batch 请求是**同步**的，直接发送到服务器端，无延迟或中断

<!--
**[核心]** batch 是批量操作的底层引擎，它的妙处在于：因为 Put、Get、Delete 都继承自 Row，所以你能在一次 batch 里混着放不同类型的操作，比如几个 put、几个 get、几个 delete。但有两个坑要记住：第一，**同一行的 Put 和 Delete 不能混在一个 batch**，因为它们处理的先后顺序不同会导致结果不一样；第二，用 batch 时 Put 不走写缓冲区，请求是同步直接发的。这就和前面"先攒后发"的缓冲区策略不一样了，用的时候心里要有数。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.2 节。
-->

---
class: compact
---

## batch 的返回结果

| 结果 | 描述 |
| --- | --- |
| null | 连接远程服务器失败 |
| EmptyResult | Put 或 Delete 操作成功 |
| Result | Get 操作成功；若未查询到行/列则返回空的 Result |
| Throwable | 服务器端产生异常 |

<!--
**[带读]** 这张表是判断 batch 每个操作成败的标准。逐行看：null 表示连接失败，比如目标机器连不上，这是个危险的信号；EmptyResult 表示写操作（put/delete）成功，因为它不返回数据；Result 表示读操作（get）成功了，即使没查到东西也返回一个空的 Result；Throwable 则是服务器端出了异常。所以遍历 batch 的结果数组时，你就能逐个判断每个操作到底是成功了、失败了、还是压根没连上。这是 batch 相比"整体提交"的一大优势——它能把每次操作的结果分开告诉你。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.2 节。
-->

---
class: compact
---

## batch 示例（关键步骤）

```java
List<Row> batch = new ArrayList<Row>();
Put put = new Put(row2); put.add(c2, q1, Bytes.toBytes("v6"));
batch.add(put);                     // res 0: 写操作
Get get = new Get(row1); get.addColumn(c1, q2);
batch.add(get);                     // res 1: 读操作
Delete delete = new Delete(row3); delete.addColumn(c1, q1);
batch.add(delete);                  // res 2: 删除
Get get1 = new Get(row1); get1.addFamily(Bytes.toBytes("NoExist"));
batch.add(get1);                    // res 3: 读不存在的列簇

Object[] res = new Object[batch.size()];
table.batch(batch, res);            // 批量执行，结果存入 res 数组
for (int i = 0; i < res.length; i++)
    System.out.println("res " + i + ": " + res[i]);
```

- 结果数组逐项对应加入顺序；`res 3` 读不存在的列簇会抛出 `NoSuchColumnFamilyException`

<!--
**[看代码]** 我们用实例看 batch 怎么用。构造一个 List<Row>——因为 Row 是所有操作的父类，所以 Put、Get、Delete 都能往里加。看这段，依次放了写、读、删、再读一个不存在的列簇。然后 `table.batch(batch, res)` 把它们一次性发过去，结果放到 res 数组里，数组下标严格对应你加入的顺序。遍历打印时，你能看到每个操作的结果类型：写和删是 keyvalues=NONE，读有数据，而读不存在的列簇直接抛 NoSuchColumnFamilyException——提醒你 column family 一定要写对。这就是 batch 的完整面貌：混装、同步、逐项返回结果。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.2 节。
-->

---
layout: section
class: compact
---

## 7.3 行锁

<ol class="outline">
  <li class="current">为什么需要行锁</li>
  <li>加锁与解锁 API</li>
</ol>

<!--
分布式环境下，多个客户端可能同时改同一行数据。HBase 如何保证这行操作"原子性"？答案就是行锁。但行锁是把双刃剑，用不好会死锁。这一节我们看它怎么用、又要注意什么。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.3 节。
-->

---
class: compact
---

## 行锁：保证原子性

- 服务器在**串行方式**执行中，每个操作必须保证原子性，HBase 利用**行锁**保证
- **使用需谨慎**：两个客户端很可能同时被对方锁住，而只有对方能解开锁，形成**死锁**
- 行锁**必须针对整行**，指定行键，一旦获得锁定权即防止其他并发修改

**API**：

```txt
RowLock lockRow(byte[] row)    // 以行键为参数生成 RowLock 实例
void unlockRow(RowLock r)      // 解锁
```

<!--
**[核心]** 行锁解决的是并发问题。当两个客户端同时想改同一行，如果没有锁，就会互相覆盖。HBase 提供 lockRow 给整行加锁，加锁后其他人就被挡在外面，保证这一系列对同一行的操作是原子的。但务必小心：行锁极易产生死锁——比如客户端 A 锁了行 X 想再锁行 Y，客户端 B 锁了行 Y 想再锁行 X，两个人就这么永远等下去了。所以使用行锁要尽量短、一次锁一行、顺序一致，这是分布式并发编程的常识。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.3 节。
-->

---
class: compact
---

## 行锁的阻塞与超时

- 一个客户端想修改另一客户端加锁的数据时，必须**等待锁释放**或**锁超时**
- 默认锁超时是一分钟；可在 `hbase-site.xml` 修改：

```xml
<property>
    <name>hbase.regionserver.lease.period</name>
    <value>120000</value>
</property>
```

- 持锁时配合 `Put(row, lock)` 构造函数，把锁对象传给写操作

<!--
**[核心]** 锁不是永远等下去，它有两道闸：要么等对方释放，要么等超时。默认超时一分钟，不够可以调。注意它叫 hbase.regionserver.lease.period，本质是 region server 的租约期。实际使用中，你拿到 RowLock 后，要把这个锁对象传进 Put 的构造函数，也就是 `new Put(row, lock)`，这样这次写入才算是"持锁写入"。所以行锁的三板斧就是：lockRow 加锁、new Put(row, lock) 带锁写、unlockRow 解锁，用完一定要解锁，否则别人会一直等。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.3 节。
-->

---
layout: section
class: compact
---

## 7.4 扫描

<ol class="outline">
  <li class="current">Scan 的两种创建方式</li>
  <li>限制条件与遍历</li>
</ol>

<!--
Put、Get、Delete 一次都只能处理单行，可业务上经常需要快速扫过一张表或一个区间。Scan 就是为批量读取而生。我们看它怎么创建、怎么加过滤条件、怎么遍历。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.4 节。
-->

---
class: compact
---

## Scan：两种创建方式

| 隐式 | 显式 |
| --- | --- |
| `ResultScanner getScanner(byte[] family)` | `Scan(byte[] startRow, Filter filter)` |
| `ResultScanner getScanner(byte[] family, byte[] qualifier)` | `Scan(byte[] startRow)` |
| | `Scan(byte[] startRow, byte[] stopRow)` |

- **隐式**：调用一次列表扫描方法，ResultScanner 会在扫描请求发送前隐式创建一个 Scan 对象
- **显式**：通过 `startRow` 指定扫描起始行键；扫描区间**包含起始行、不含终止行**；若参数未精确匹配，会匹配相等或大于起始行的行键

<!--
**[核心]** 创建 Scan 有两种路子。隐式最简单：直接调 getScanner(family) 或 getScanner(family, qualifier)，它会替你建好 Scan 对象并返回一个扫描器。显式则完全由你掌控，用 Scan(startRow) 等构造，能指定起始行、终止行，还能配过滤器。这里有个细节要记牢：扫描区间是"**左闭右开**"——包含起始行，但不包含终止行。而且如果给你的 startRow 精确不存在，它会自动匹配比它大一点的那一行。这个边界行为直接影响你的扫描范围。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.4 节。
-->

---
class: compact
---

## 加入限制条件

- `addFamily(family)` / `addColumn(family, qualifier)`：只读指定列簇/列
- `setTimeStamp(ts)` / `setTimeRange(min, max)`：按时间筛选
- `setStartRow(row)` / `setStopRow(row)`：划定行区间
- `setMaxVersions(n)`：返回版本数（不设默认一个版本）
- `setFilter(filter)`：设置过滤器

**遍历**：扫描将每行封装为一个 Result 放入迭代器；`next()` 返回下一个可用的行；用 `close()` 告知服务器释放扫描资源。

<!--
**[核心]** Scan 的另一半魅力在于可以加各种限制条件，把"全表扫"变成"精确扫"。你可以只挑某些列簇、某些列；按时间戳或时间范围过滤；划定起止行；控制要返回几个版本；还能挂一个过滤器做更复杂的筛选。这些都是为了减少数据传输、加快扫描速度。遍历也很人性化：每行是一个 Result，用 next() 或 for 循环逐个取，取完记得 close() 告诉服务器释放资源——这一步容易被忘，但不做会占着服务器资源不放。总的来说，Scan 就是一个"可加约束的迭代式批量读取"。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.4 节。
-->

---
class: compact
---

## Scan 示例（关键步骤）

```java
Scan scan1 = new Scan();                              // 空 Scan：全表扫描
ResultScanner scanner1 = table.getScanner(scan1);
for (Result res : scanner1) System.out.println(res);
scanner1.close();                                     // 释放远程资源

Scan scan3 = new Scan();
scan3.addColumn(c1, q1).addColumn(c2, q1)
     .setStartRow(row1).setStopRow(row3);             // 限定列 + 行区间
ResultScanner scanner3 = table.getScanner(scan3);
```

- `new Scan()` 全表；`addColumn(...)` 限定列；`setStartRow/setStopRow` 限定范围
- 用 builder 风格把多个限制条件链式追加到 Scan 实例

<!--
**[看代码]** 我们用两个例子感受 Scan 的灵活度。第一个是"空 Scan"——new 一个空的 Scan，getScanner 后就得到了一个扫描器，for 循环遍历就能把整张表读出来，但是注意最后一定要 scanner.close() 释放远程资源。第二个例子是"精确 Scan"：用 addColumn 限定只要 col1:q1 和 col2:q1 两列，再用 setStartRow(row1)、setStopRow(row3) 把范围锁在 row1 到 row3 之间。它采用的正是 builder 链式写法，一个方法接一个方法地追加条件。全表扫和定向扫，等你掌握了这两个，Scan 就算入门了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.4 节。
-->

---
layout: section
class: compact
---

## 7.5 其他操作

<ol class="outline">
  <li class="current">HTable 方法</li>
  <li>Bytes 工具</li>
</ol>

<!--
最后我们补两块实用的工具：HTable 实例自带的一些方法和 Bytes 这个字节工具类。它们不复杂，但经常用到。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.5 节。
-->

---
class: compact
---

## HTable 常用方法

- `void close()`：使用完调用一次，**会刷写所有客户端缓冲的写操作**
- `byte[] getTableName()`：获取表名
- `isTableEnabled(table)`：检查表在 Zookeeper 中是否被标识为启用
- `getStartKeys()` / `getEndKeys()`：获取表中所有 region 的起始/终止行键
- `getRegionLocation()` / `getRegionsInfo()`：获取某行或全表 region 的位置信息
- `clearRegionCache()`：清空缓存的 region 位置信息

<!--
**[带读]** HTable 实例自带的方法，有几条值得记。close() 一定要记得调——它不只是关连接，还会把缓冲区里残留的写操作刷出去，忘了调用可能丢数据。getStartKeys 和 getEndKeys 能拿到所有 region 的边界行键，这在分析一张表怎么被切分时很有用。getRegionLocation 告诉你某一行落在哪个 region，是理解 HBase 数据分布的好帮手。最后 clearRegionCache 用来清掉本地缓存的 region 位置，避免读到过期的分布信息。这些方法虽然不起眼，但排查问题时经常要用。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.5 节。
-->

---
class: compact
---

## Bytes 工具类

- 便捷地在 Java 原生数据类型与 `byte[]` 之间转换；**所有操作都无需创建新实例**
- `toStringBinary()`：把不可打印信息转成人工可读的十六进制
- `compareTo()` / `equals()`：比较两个 `byte[]`
- `add()`：把两个字节数组拼接成一个新数组
- `head()` / `tail()`：取数组头部 / 尾部
- `binarySearch()`：在字节数组中二分查找目标值
- `incrementBytes()`：把一个 long 转成字节数组，与 long 相加后返回字节数组

<!--
**[带读]** 最后简单看 Bytes。HBase 里一切数据本质都是字节数组，而 Java 的字符串、long、int 都要转换成 byte[] 才能写进 HBase，反过来读出来也要转回。Bytes 就是干这个的工具类，而且都是静态方法，用起来很方便。几个高频的：toStringBinary 把二进制转成可读形式方便看；add 拼接字节；binarySearch 做二分查找。你留意前面所有例子里的 Bytes.toBytes(...) 就是这个类的功劳。掌握它，你读写 HBase 时的类型转换就不再麻烦。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7.5 节。
-->

---
class: compact
---

## 想一想（自检）

**问题 1**：为什么 HBase 的 "写缓冲区" 能提升性能？它和 `flushCommits()` 是什么关系？

> 提示：想想"每次发一条"和"攒一批再发"的差别。

**问题 2**：用 `Delete` 删除一个单元格后，为什么旧数据并没有立刻消失？

> 提示：回忆删除标记和 region 合并、分裂。

<!--
这两题考实际操作背后的原理。第一题：每条 put 都是一次 RPC，数据量大时太慢；写缓冲区让你把若干 put **先攒在内存里**，等攒够或手动 `flushCommits()` 时**一次性发送**，大大减少网络请求次数——所以快。第二题：HBase 删除是**打"删除标记"**而不是真正擦掉，真正的物理删除要等到下一次 region 合并、分裂时才执行。这样做的原因是分布式环境下"物理立删"代价太高，用标记更高效安全。答对这两题，说明你不仅会调 API，还理解它背后的权衡。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7 章，自检。
-->

---
class: compact
---

## 本章小结

1. **CRUD**：Put（增/改，支持 checkAndPut 原子写与写缓冲区）、Get（单行查询）、Delete（打标记而非真删）
2. **batch**：Put/Get/Delete 混装批量提交，同步执行、逐项返回结果；同行的 Put 与 Delete 不能混在一个 batch
3. **行锁**：`lockRow`／`unlockRow` 保证单行原子性，需防死锁、注意超时
4. **Scan**：全表或区间扫描，可加列、时间、行区间、版本、过滤器等约束，用完 `close()`
5. **HTable / Bytes**：管理表信息与字节/原生类型转换的实用工具

<!--
**[过渡]** 我们把第七章收尾。CRUD 是我们操作 HBase 的四个基本动作，batch 解决了"怎么一次干更多活"，行锁解决了"并发下别出错"，Scan 解决了"怎么高效批量读"，再加上 HTable 和 Bytes 两个工具，你就拥有了完整的数据操作能力。但这些都是最基础的读写。下一章我们要上难度了——看 HBase 的那些高阶特性：过滤器、计数器、协处理器，它们让 HBase 不只是"存"，更能"算"。

[Sources]
- 吴斌，《大数据实时计算与应用》第 7 章，本章小结。
-->
