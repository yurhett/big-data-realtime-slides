---
theme: easy-jyy
title: 大数据实时计算与应用 · 第8章 HBase 高阶特性
description: HBase 的过滤器（比较/专用/附加）、计数器（单/多）、协处理器（观察者/终端）
author: 吴斌
date: 2023-06-01
tags:
  - 大数据实时计算与应用
  - HBase
  - 过滤器
  - 协处理器
transition: fade-out
mdc: true
drawings:
  persist: false
---

# 第8章 HBase 高阶特性

> 让 HBase 不只是“存”，更能“算”和“筛”

<!--
上一章我们学会了 HBase 的增删改查，但那是基础功夫。这一章我们上难度，看 HBase 的三个高阶特性：过滤器、计数器和协处理器。为什么需要它们？想想看，如果数据有上亿行，你每次都把数据拉回客户端再过滤，网络早就垮了。过滤器的价值就是把"筛"放到服务器端去做；计数器让你在海量点击流里实时统计而不重不漏；协处理器更是把一部分计算直接搬到了数据存放端。这三招学会，你才算真正用好了 HBase。

[Sources]
- 吴斌，《大数据实时计算与应用》，清华大学出版社，第 8 章。
-->

---
class: compact
---

## 本章目录

1. **8.1 过滤器**：比较过滤器、专用过滤器、附加过滤器、FilterList
2. **8.2 计数器**：单计数器与多计数器
3. **8.3 协处理器**：观察者模式与终端模式

<!--
同学们请看本章三大块。8.1 过滤器是重中之重，我们会看到 HBase 怎么把过滤逻辑下沉到服务器端，从而大幅减少数据传输；8.2 计数器解决"实时统计不重不漏"的问题；8.3 协处理器最进阶，它允许你把代码搬进 region 服务器。这三者本质都是在"离数据更近的地方干活"。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8 章，章节结构。
-->

---
layout: section
class: compact
---

## 8.1 过滤器

<ol class="outline">
  <li class="current">过滤器的组成与比较运算符</li>
  <li>比较过滤器</li>
  <li>专用过滤器与附加过滤器</li>
  <li>FilterList 组合过滤</li>
</ol>

<!--
过滤器是一套为完成较高级需求提供的 API。它可以理解为 HBase 版的 where 子句——但相比 SQL，它被设计成能在服务器端、甚至在 region 内部执行，把符合条件的数据再返回客户端。这一节我们层层深入。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact
---

## 先理解：在哪过滤？——服务器端 vs 客户端

**场景**：你有 1 亿行数据，只想留下"年龄大于 18"的行。

**方式一：把数据全搬回来自己筛（客户端过滤）**：

- 1 亿行**全部通过网络**搬到你的电脑，你再筛 → **又慢又占带宽**，简直是灾难

**方式二：让服务器筛完再给你（服务器端过滤）**：

- 你把"过滤条件"发给服务器，它**在存数据的地方筛**，只把符合条件的传给你 → **快得多**

**HBase 的过滤器就是方式二**：它把过滤逻辑"送到数据那端"去执行，只返回你真正需要的少数数据。

<!--
这一节讲"过滤器"，但你要先懂它为什么存在。假设你有 1 亿行数据、只想找"年龄大于 18"的。最笨的办法是：把 1 亿行全部搬到你的电脑上，再一点点筛——网络瞬间就爆了。聪明的办法是：把过滤条件告诉服务器，让它在**数据存放的地方**直接筛，只把符合条件的少数传给你。HBase 的过滤器走的正是这条路——过滤发生在**服务器端**而不是客户端。记住这个区别，你就明白过滤器最大的价值不是"筛选本身"，而是"大大减少数据传输"。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节，基础。
-->

---
class: compact
---

## 过滤器是什么

- **作用**：对从数据库获取的数据进行过滤，只返回符合条件的 → 减少 region 服务器向客户端传输的数据量，提高效率
- **三要素**：过滤器本身 + **比较器** + **比较运算符**（扩展类过滤器还带有其他参数）
- 与 SQL 的 `where` 很相似，但不用常规比较符，而是有一套**专门定义的比较运算符**
- 通过配置 **Get / Scan 对象**使用；请求发送后，过滤器对象被序列化传送到 region 服务器，在**服务器端**起过滤作用

<!--
**[核心]** 记住过滤器的两大要点。第一，它能**减少数据传输**——因为过滤在服务器端就完成了，只有符合条件的数据才返回客户端，网络压力和客户端计算压力都小了。第二，它由"过滤器 + 比较器 + 比较运算符"组成。比较运算类似 SQL 的 where，但 HBase 用了一套自己定义的比较运算符，比普通比较符更适用于字节比较。最关键的是，过滤器是跟着 Get/Scan 一起序列化发到 region 服务器执行的，这就是它高效的根本原因。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact easy-table-sm
---

## 比较运算符（CompareOp 枚举）

| 操作 | 描述 |
| --- | --- |
| LESS | 小于预设定值 |
| LESS_OR_EQUAL | 小于或等于预设定值 |
| EQUAL | 等于预设定值 |
| NOT_EQUAL | 不等于预设定值 |
| GREATER_OR_EQUAL | 大于或等于预设定值 |
| GREATER | 大于预设定值 |
| NO_OP | 排除一切值 |

- 全部封装在 **CompareOp** 枚举类中，通过它引用运算符
- 常见比较器：`BinaryComparator`、`BinaryPrefixComparator`、`NullComparator`、`BitComparator`、`RegexStringComparator`、`SubstringComparator`

<!--
**[带读]** 这张表是过滤器的"运算符集"，它代替了常规的比较符号。七种运算覆盖了你日常需要的所有判断：小于、小于等于、等于、不等于、大于等于、大于，还有个特殊的 NO_OP 表示"什么都不要"。它们被统一封装在 CompareOp 枚举里。选好运算符后，还得配一个比较器——也就是"按什么规则比"。常见的有二进制精确比较、前缀比较、空值比较、正则、子串比较等等。**运算符决定"怎么比"，比较器决定"比什么规则"**，两者搭配才构成一次完整的过滤判断。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact
---

## 比较过滤器（CompareFilter）

专门用于比较的过滤器，通过比较运算符 + 比较类实现需求：`CompareFilter(CompareOp, WriteByte)`。

| 过滤器 | 描述 |
| --- | --- |
| **RowFilter** | 基于行键过滤 |
| **FamilyFilter** | 基于列簇过滤 |
| **QualifierFilter** | 基于列名（qualifier）过滤 |
| **ValueFilter** | 基于数值过滤 |
| **DependentColumnFilter** | 指定一个参考列，用它控制其他列的过滤 |

```java
Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                              new BinaryComparator(row2));
scan.setFilter(filter);
```

<!--
**[带读]** 比较过滤器是专门做"比较"的一族。它们按过滤对象分了五类：按行键、按列簇、按列名、按值，还有个比较特殊的"参考列过滤器"。前三类容易理解，就是看你关心的是行、列簇还是列；值过滤器则直接比对数据值，最常用。用法看代码——new 一个 RowFilter，传入比较运算符（比如小于等于）和比较器（比如行键 row2），再 setFilter 到 Scan 上即可。这五行代码，就是过滤器最典型的用法模板。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact easy-table-sm
---

## 专用过滤器

继承自 FilterBase，一些只做行筛选、只适合扫描操作；对 Get 操作限制更苛刻（包含整行或什么都不含）。

| 过滤器 | 描述 |
| --- | --- |
| **SingleColumnValueFilter** | 基于参考列过滤，只保留包含该参考列的行 |
| **SingleColumnValueExcludeFilter** | 类似前者，但排除该参考列所在的行 |
| **PrefixFilter** | 基于传入前缀过滤 |
| **PageFilter** | 按页大小将结果分行分页 |
| **KeyOnlyFilter** | 只取 KeyValue 的键，不返回实际数据 |
| **FirstKeyOnlyFilter** | 只取一行中的第一列 |

<!--
**[带读]** 专用过滤器是"开箱即用"的套路，各解决一类典型需求。挑常用的说：SingleColumnValueFilter 根据某个参考列的值决定要不要这一行，是实现条件筛选的利器；PrefixFilter 按行键前缀过滤，相当于按前缀"切片"；PageFilter 做行分页，配合循环实现翻页；KeyOnlyFilter 只取键不取值，搞"只看结构不看内容"效率极高。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact easy-table-sm
---

## 专用过滤器（续）

| 过滤器 | 描述 |
| --- | --- |
| **InclusiveStopFilter** | 把扫描起始行到终止行全部包含进结果 |
| **TimestampsFilter** | 对扫描结果的版本做细粒度控制 |
| **ColumnCountGetFilter** | 限制每行取回的最大列数 |
| **ColumnPaginationFilter** | 按列分页 |
| **RandomRowFilter** | 按 chance 值随机决定一行是否被过滤 |

<!--
**[带读]** 剩下几个也各有用途：InclusiveStopFilter 让扫描"把终止行也算进去"；TimestampsFilter 按时间戳控制保留哪些版本；ColumnCountGetFilter 限制每行最多取几列；ColumnPaginationFilter 按列分页；RandomRowFilter 按概率随机采样一部分行。这些专用过滤器，让你不必每次手写复杂的比较逻辑。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact
---

## 附加过滤器

补充特殊功能，**不依赖于过滤器自身**，却可应用在其他过滤器上：

| 过滤器 | 描述 |
| --- | --- |
| **SkipFilter** | 遇到一个需要过滤的 KeyValue 时，过滤**整行**数据 |
| **WhileMatchFilter** | 遇到一条被过滤的数据时，**放弃后面的扫描** |

- SkipFilter：从"过滤掉一个值"升级为"过滤掉整行"，可用来清理含空值的行
- WhileMatchFilter：碰到被过滤记录就提前终止扫描，适合"找到第一个不满足的就停下"的边界扫描

<!--
**[核心]** 附加过滤器很特别，它是"包在其他过滤器外面"的。SkipFilter 改变了过滤粒度——普通过滤器滤掉某个值，但 SkipFilter 会让整行都被滤掉，这对清除"含空行的脏数据"特别有效。WhileMatchFilter 则改变扫描行为——一旦碰到一条被过滤的记录，它就放弃后面的所有扫描。这适合那种"我要找到第一个不满足条件的就停"的场景。所以附加过滤器不是独立工作的，而是以装饰者的身份，增强某个已有过滤器的语义。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact
---

## 组合过滤：FilterList

实际应用需要多个过滤器共同限制结果，HBase 提供 **FilterList**：

- 构造：`FilterList(List<RowFilter>)`、`FilterList(Operator)`、`FilterList(Operator, List)`
- 添加：`void addFilter(Filter filter)`
- **Operator** 两个取值：
  - `MUST_PASS_ALL`（默认）：所有过滤器都通过才保留
  - `MUST_PASS_ONE`：任一过滤器通过即保留
- 通过控制 List 中过滤器的**顺序**精确控制执行先后

<!--
**[核心]** 单个过滤器往往不够，FilterList 让你把多个过滤器组合起来。怎么组合？由 Operator 决定"与"还是"或"：MUST_PASS_ALL 相当于 AND——所有条件都满足才要；MUST_PASS_ONE 相当于 OR——满足一个就要。默认是 AND。这就像 SQL 里的 where A and B 或 where A or B。还有个细节：list 里过滤器的**顺序**就是执行顺序，所以你能精确控制先跑哪个、后跑哪个，这对性能调优很关键。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
class: compact
---

## 8.1 小结

- 过滤器在 **region 服务器端**执行，减少数据传输
- **比较过滤器**（Row/Family/Qualifier/Value）按对象过滤；**专用过滤器**是可复用的套路；**附加过滤器**（Skip/WhileMatch）改变过滤语义；**FilterList** 用 AND/OR 组合多个过滤器
- 实际案例：`FilterList(filterRow范围, QualifierFilter)` 实现行区间 + 列名的组合过滤

<!--
**[过渡]** 我们把过滤器收个尾。它最大的价值是"把筛选下沉到服务器"，配合各类比较器、专用过滤器和 FilterList，HBase 就拥有了相当丰富的"查询表达能力"，但又不像 SQL 那么重。记住，过滤器解决的是"**筛**"。下一个高阶特性解决的是"**算**"——实时统计。我们看计数器。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.1 节。
-->

---
layout: section
class: compact
---

## 8.2 计数器

<ol class="outline">
  <li class="current">为什么需要计数器</li>
  <li>单计数器与多计数器</li>
</ol>

<!--
很多业务场景（点击流、在线广告）需要实时统计，比如"今天这个页面被点了多少次"。如果用普通的读改写，会有并发和资源占用问题。HBase 的计数器就是为此而生的。我们看它如何做到既实时又原子。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.2 节。
-->

---
class: compact
---

## 为什么需要计数器

- 若用某行的值 + Put 实现计数，为保证原子性，必须让一个客户端**独占该行的资源**
- 大量计数操作会占用大量资源；一旦客户端崩溃，其他客户端将**长期等待**
- HBase 定义**计数器**解决：既**避免资源占用**，又保证**原子性**

**特性**：
- 把某一列当作计数器使用，创建方式与创建行相同，**无需特定创建流程**（列具有动态添加特性，第一次使用即隐式创建，初始值 0）
- 增加值是 **long 类型**（不是字符串）：>0 增加、=0 不改并返回当前值、<0 减少

<!--
**[核心]** 用普通方式做计数器，麻烦在"原子性"三个字。你想安全地把一个数加一，就得先锁住那行、读完、加一、写回、解锁；一旦这中间有人崩溃，锁就死在那了，别人都得等。HBase 的计数器把这个操作变成一个**原子操作**——它自己保证"读-加-写"一气呵成，不需要你加锁。而且列是动态创建的，第一次用计数器，它就自动创建并初始化为 0。记住增加值是 long，加减或只读当前值都在一次调用里完成——这点很关键，因为拿字符串去加会得出一个吓人的大数。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.2 节。
-->

---
class: compact
---

## 单计数器

一次只操作一个计数器，需自行设定列：

```kotlin
incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
```

- `row`、`family`、`qualifier` 为列坐标，`amount` 为增加值

```java
long cnt1 = table.incrementColumnValue(row, "month", "1", 1);   // +1
long current = table.incrementColumnValue(row, "month", "1", 0); // 读当前值，不改
long cnt3 = table.incrementColumnValue(row, "month", "1", -1);  // -1
```

<!--
**[看代码]** 单计数器用 incrementColumnValue 一行搞定。它接收四个参数：行键、列簇、列名、增加值。看例子最直观：加 1 就是传 1，结果返回加完之后的新值；传 0 则只读当前值而不改变，很适合"窥探"计数；传 -1 就减一。这三个调用分别对应"自增、读当前、自减"。注意它返回的就是那个 long 型的计数结果，非常方便。这就是做实时点击计数最常用的原子写法。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.2 节。
-->

---
class: compact
---

## 多计数器

- 若对一行内多个计数器增加，逐次调用需发送多次 RPC；为此 HBase 提供 **Increment** API
- `Result increment(Increment increment)`：一次 RPC 操作一行内的多个计数器
- 构造 `Increment(byte[] row)`，用 `addColumn(family, qualifier, amount)` 添加多个计数增减

```java
Increment inc = new Increment(row);
inc.addColumn("month", "1", 1);
inc.addColumn("month", "2", 1);
inc.addColumn("month", "1", 5);   // 同一列可叠加
Result res = table.increment(inc); // 一次 RPC，返回各列的计数结果
```

<!--
**[看代码]** 当你想一次性给同一行的好几个计数器都加值，总不能一个个调用发很多次 RPC，那就用 Increment 包装起来。看例子：new 一个 Increment 指定行，然后 addColumn 依次声明要对 "month:1"、"month:2" 这些列分别加多少；注意甚至可以对同一列多次 addColumn（比如对 month:1 加了 1 又加 5，就是总共加 6）。然后一次 table.increment(inc) 就把这行所有计数器都更新了，只发一次 RPC。这在批量实时统计（比如按月份、按类型多维累加）时特别高效。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.2 节。
-->

---
layout: section
class: compact
---

## 8.3 协处理器

<ol class="outline">
  <li class="current">什么是协处理器</li>
  <li>观察者模式与终端模式</li>
  <li>加载方式与状态</li>
</ol>

<!--
最后一个高阶特性，也是最"重"的一个——协处理器。它让 HBase 的能力从"存"和"筛"进一步走向"算"，因为你能把自己写的代码注入到 region 服务器里执行。我们看它是什么、怎么分类、怎么加载。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.3 节。
-->

---
class: compact
---

## 什么是协处理器

- HBase 作为列存储数据库，很多统计函数没有直接的快速计算；于是提供**协处理器**
- 允许用户在 **region 服务器端插入自己的代码**，实现特定功能（创建二级索引、行数统计等）
- 两类（都源于 Coprocessor 类）：
  1. **观察者模式（observer）**：提供触发器，集成 `BaseRegionObserver` 等并重写方法，加载到表后"监听"预设动作，动作执行即触发钩子；如无法直接建二级索引，可在此模式下每次插入数据时自定义实现二级索引
  2. **终端模式（endpoint）**：类似关系型数据库的**存储过程**，通过 RPC 触发终端代码，如实现某些表行统计

<!--
**[核心]** 协处理器的核心思想：把原本要"把数据搬出来算"变成"把代码送进去算"。什么意思？HBase 很多统计没法直接高效算，你就写一段代码 injection 到 region 服务器里，让它在数据所在的地方执行。它分两类：观察者模式是"被动触发"的——你在表上挂了一个监听器，一旦发生 put、get 这类动作，你自己写的钩子代码就被调用，最适合做二级索引这种"只要有写入就顺手维护一份索引"的事。终端模式更像存储过程，主动通过 RPC 去调用一段代码算结果。一句话：**观察者是"有事就叫我"，终端是"我有事去找它"。**

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.3 节。
-->

---
class: compact
---

## 协处理器的执行顺序与生命周期

- **权限**：级别 `SYSTEM` > `USER`；系统级协处理器优先执行，用户级滞后执行；同级别按**序号**辨别执行顺序
- **生命周期**：所有协处理器继承 `Coprocessor`，有共同方法：
  - `start(CoprocessorEnvironment env)`：启动协处理器
  - `stop(CoprocessorEnvironment env)`：停止协处理器
- 状态封装在 `Coprocessor.State` 枚举：UNINSTALLED → INSTALLED → STARTING → ACTIVE → STOPPING → STOPPED

<!--
**[核心]** 协处理器不是随便跑的，它有权限和顺序的概念。系统级（SYSTEM）优先于用户级（USER），这说明 HBase 把内部管理逻辑的优先级压得更高；同级别之间靠序号决定先后。这点和"你会写钩子跟系统逻辑竞争"是两种心态。另外协处理器有完整的生命周期：start 启动、stop 停止，状态机从 UNINSTALLED 一路走到 STOPPED。你写协处理器时，start 里做初始化，stop 里做清理，这是基本规范。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.3 节。
-->

---
class: compact
---

## 加载方式与观察者级别

**两种加载方式：**
- **从配置加载**：在 `hbase-site.xml` 配置协处理器类位置；配置项顺序 = 加载顺序 = 执行顺序；对**每一张表**都生效。可配置 `hbase.coprocessor.master.classes`（master 级）、`hbase.coprocessor.region.classes`（region 级）、`hbase.coprocessor.wal.classes`（WAL 级）
- **从表描述加载**：通过 `HTableDescriptor.setValue()` 指定，key 以 `COPROCESSOR` 开头 + 序号；value 含类路径、类、等级；**只能对特定某张表**生效

**观察者模式三种类型**：region 级（put/get/delete）、WAL 级（WAL 操作）、master 级（建表/删表/禁用表等 DDL 操作）

- region 级观察者继承 `BaseRegionObserver`，函数以 `preDo()`/`postDo()` 成对出现，可实现 `prePut()/postPut()` 等

<!--
**[核心]** 加载协处理器有两种途径，区别在影响范围。从配置加载是**全局的**——所有表都会应用，而且配置项的顺序就是执行顺序，适合放系统级逻辑；从表描述加载是**局部的**——只对某张表生效，更精确可控。再说观察者模式的三种监听级别：region 级盯数据操作（put/get/delete），WAL 级盯日志，master 级盯 DDL 操作（建表删表这些）。实现时继承对应基类，重写诸如 prePut/postPut 这类成对方法——pre 是动作前、post 是动作后，你可以在插入数据前拦截、或在插入后顺手维护索引，这就是做二级索引的完美切入点。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.3 节。
-->

---
class: compact
---

## 协处理器示例

```java
public class RegionObserverExample extends BaseRegionObserver {
    public static final byte[] FIXED_ROW = Bytes.toBytes("@GETTIME@");
    public void preGet(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Get get, final List<KeyValue> res) throws IOException {
        if (Bytes.equals(get.getRow(), FIXED_ROW)) {        // 检查行键是否匹配
            KeyValue kv = new KeyValue(get.getRow(), FIXED_ROW, FIXED_ROW,
                                       Bytes.toBytes(System.currentTimeMillis()));
            res.add(kv);                                     // 加入一个特殊 KeyValue
            e.bypass();                                      // 之后的常用操作都被跳过
        }
    }
}
```

- 部署：把编译好的 JAR 包添加到 `hbase-env.sh` 的 `HBASE_CLASSPATH`，重启 HBase 使配置生效

<!--
**[看代码]** 这个观察者做了一件很巧的事：当用户 get 一个特定行键（@GETTIME@）时，协处理器不返回真实数据，而是返回**服务器当前时间**。看代码：preGet 里先判断行键是否等于 FIXED_ROW；若是，就构造一个 KeyValue 塞进结果，里面存的就是当前时间戳；然后 `e.bypass()` 意味着"跳过之后的正常查询流程"。结果就是，读这个特殊行时，你拿到的始终是服务器实时时间。这就是一个典型的"拦截 + 注入"的观察者用例。最后别忘了，要把编译好的 JAR 加到 HBASE_CLASSPATH 里并重启 HBase 才生效。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8.3 节。
-->

---
class: compact
---

## 想一想（自检）

**问题 1**：想"找到所有行键以 `2024-` 开头"的行，用什么过滤器最简单？

> 提示：过滤器里有个按"前缀"筛的。

**问题 2**：为什么"同行的 Put 和 Delete 不能放在同一个 batch"？

> 提示：想想它们同一行、顺序不同会怎样。

<!--
这两题把过滤器和批处理两个知识点串起来。第一题：按前缀找，最直接的就是**前缀过滤器（PrefixFilter）**，传一个前缀就行，比自己去比对简单多了。第二题：batch 里同一行的操作会按顺序执行，如果既有 put 又有 delete，先执行哪个、后执行哪个，得到的结果完全不同——顺序一变结果就变，所以 HBase 不允许把同一行的 Put 和 Delete 混在一个 batch 里。如果你能答上来，说明你对过滤器"开箱即用"的特性和批处理的"顺序敏感"都有了意识。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8 章，自检。
-->

---
class: compact
---

## 本章小结

1. **过滤器**：在 region 服务器端过滤，减少数据传输；比较/专用/附加过滤器 + FilterList（AND/OR）组合
2. **计数器**：`incrementColumnValue` 原子自增自减；`Increment` 一次 RPC 操作一行多个计数器
3. **协处理器**：把计算下沉到数据端；观察者（预/后置钩子）与终端（存储过程）；系统级优先、按序执行

<!--
**[过渡]** 我们把第八章收束。这三个高阶特性回答了同一个问题：在大数据量下，怎么让"筛、算、管"都贴近数据。过滤器把筛选交给 region，省了网络；计数器用原子操作避免锁死与资源占用，实现实时统计；协处理器更进一步，把整段业务逻辑注入数据端。它们共同让 HBase 从"存储系统"进化成"能算能筛的存储引擎"。下一章我们进入管理视角，看怎么定义 HBase 的表结构、管理表和集群。

[Sources]
- 吴斌，《大数据实时计算与应用》第 8 章，本章小结。
-->
