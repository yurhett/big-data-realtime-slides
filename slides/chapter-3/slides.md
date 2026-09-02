---
theme: easy-jyy
title: 大数据实时计算与应用 · 第3章 Kafka 环境搭建
description: Kafka 服务器搭建、多 broker 集群配置，以及基于 Maven 的开发环境与发送/接收程序示例
author: 吴斌
date: 2023-06-01
tags:
  - 大数据实时计算与应用
  - Kafka
  - 环境搭建
transition: fade-out
mdc: true
drawings:
  persist: false
---

# 第3章 Kafka 环境搭建

> 把 Kafka 真正跑起来：从服务器到开发环境

<!--
前两章我们学的是 Kafka 的概念和模型，这一章我们动起手来，把 Kafka 真正搭起来跑通。很多同学可能觉得“搭建环境”很枯燥，但它恰恰是理解一个系统最好的方式——你亲手创建 Topic、发一条消息、在另一个终端收到它，比背十遍概念都牢。这一章我们分两步：第一步在服务器上把 Kafka 单机和多 broker 集群搭起来；第二步在开发环境里引入依赖、写一个能发送/接收消息的小程序。跟着做一遍，你对之前讲的 topic、分区、producer、consumer 会有实感。

[Sources]
- 吴斌，《大数据实时计算与应用》，清华大学出版社，第 3 章。
-->

---
class: compact
---

## 本章目录

1. **3.1 服务器搭建**：下载解压、启动 Zookeeper 与 Kafka、创建 Topic、发送/接收消息、搭建多 broker 集群
2. **3.2 开发环境搭建**：Maven 依赖、配置程序、简单的发送/接收、高级别 consumer

<!--
同学们请看本章的两大步。3.1 是服务器层面，我们会一步步把 Kafka 和它依赖的 Zookeeper 启动起来，再走一遍「创建 Topic → 发消息 → 收消息」的完整流程，最后搭一个真正的多 broker 集群；3.2 是开发层面，用 Maven 引入 Kafka 的 jar 包，写一个能跑起来的生产者和消费者。学完这一章，你就拥有了一个可用的 Kafka 环境。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3 章，章节结构。
-->

---
layout: section
class: compact
---

## 3.1 服务器搭建

<ol class="outline">
  <li class="current">下载与启动</li>
  <li>创建 Topic 与收发消息</li>
  <li>多 broker 集群</li>
</ol>

<!--
我们先从服务器搭建开始。整体流程很清晰：先下载解压，再依次启动 Zookeeper 和 Kafka，然后用命令行工具创建 Topic、发送和接收消息。走通单实例之后，我们再进阶到多 broker 集群。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 先理解：Zookeeper 是干嘛的（简版）

这一章会先启动一个叫 **Zookeeper** 的东西。别被名字吓到，它特简单：

- 它就是集群里的"**管事登记处**"：谁加入了、谁还在线、谁是主，它都记着
- Kafka 靠它来**协调一堆机器**——比如"那台 broker 挂了，大家要有人顶上"
- 没有它，一群机器就群龙无首、互不相认

**为什么 Kafka 非要它？** 因为 Kafka 是分布式的、有多台 broker，需要有人帮忙"登记"和"协调"。

<!--
第一次接触 Kafka 的同学，看到要先启动 Zookeeper 可能会懵。你只要记住：Zookeeper 是集群里的"管事登记处"。它专门负责记录谁加入了、谁还活着、谁说了算，供大家协调。就像一个大社区的物业：哪户有人、哪户搬走了、楼长是谁，物业都有账。没有物业，住户之间就互不相认。Kafka 既然是很多台机器一起干活，就需要这么个"物业"来协调大家。这样理解，你就不怕它了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节，基础。
-->

---
class: compact
---

## 下载与解压

```bash
> tar -xzf kafka_2.9.2-0.8.1.1.tgz
> cd kafka_2.9.2-0.8.1.1
```

- 下载 Kafka（本例使用 0.8.1.1）并解压到本地

<!--
第一步很简单，下载发布包然后解压。这里用的是 0.8.1.1 这个较早期版本，你在实操时换成你拿到的版本号即可。解压完成后，你会看到一个包含 bin、config、lib 等目录的标准 Kafka 发布包。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 启动 Zookeeper

Kafka 用到了 Zookeeper，所以先启动一个单实例的 Zookeeper 服务：

```bash
> bin/zookeeper-server-start.sh config/zookeeper.properties &
[2013-04-22 15:01:37,495] INFO Reading configuration from:
config/zookeeper.properties (org.apache.zookeeper.server.quorum.QuorumPeerConfig)
...
```

- 命令末尾加 `&`，即可让服务在后台运行、离开控制台

<!--
Kafka 的集群协调依赖 Zookeeper，所以必须先把它启动起来。这里我们启动的是一个单实例 Zookeeper，配置来自 config/zookeeper.properties。注意命令末尾的 & 符号，它表示让这个进程在后台运行，这样你就不会一直被它占住终端。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 启动 Kafka

```bash
> bin/kafka-server-start.sh config/server.properties
[2013-04-22 15:01:47,028] INFO Verifying properties (kafka.utils.VerifiableProperties)
[2013-04-22 15:01:47,051] INFO Property socket.send.buffer.bytes is overridden to 1048576
...
```

- 启动 server 端，读取 `config/server.properties` 配置

<!--
Zookeeper 起来之后，接着启动 Kafka 服务端。它读取的是 config/server.properties 这个配置文件，里面定义了 broker.id、端口、日志目录等关键参数。启动后你会看到一大串 INFO 日志，其中有些属性会被覆盖，这都是正常现象，说明配置被加载了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 创建 Topic

```bash
> bin/kafka-topics.sh --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 1 --topic test

> bin/kafka-topics.sh --list --zookeeper localhost:2181
test
```

- 创建一个名为 `test` 的 Topic：1 个分区、1 个副本
- 除了手动创建，也可配置 broker **自动创建** Topic

<!--
服务启动后，我们要建立一个 Topic。这条命令创建了一个叫 test 的 Topic，参数里指定了副本数为 1、分区数为 1。然后用 list 命令就能看到它。这里提醒一点：除了手动建，Kafka 也可以配置成在 producer 首次写入时自动创建 Topic，生产环境里两种方式都会用到。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 发送消息

```bash
> bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
This is a message
This is another message
```

- Kafka 提供命令行 producer，从文件或标准输入读取消息并发送到服务端
- 默认每条命令发送一条消息；按 `Ctrl+C` 退出发送

<!--
Topic 建好后，我们用命令行 producer 发消息。它会连接到 broker（这里是 9092 端口），你敲入的一行行文字，就会被当作消息发送到 test 这个 Topic。看到提示符后直接输入即可，按 Ctrl+C 退出。这一步你得到的直观感受是：我发出去的东西，是“进”了 Kafka 这个管道。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 接收消息

```bash
> bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
This is a message
This is another message
```

- Kafka 提供命令行 consumer，读取消息并输出到标准输出
- `--from-beginning` 表示从最早的消息开始消费

<!--
关键的一步来了：接收消息。在另一个终端运行这个 consumer，它会去订阅 test 这个 Topic。加上 --from-beginning，它会从最早一条消息开始读，所以你能看到刚才 producer 发的那两条消息被打印出来。你可以在一个终端开 producer、另一个终端开 consumer，一边敲一边看，消息实时流动，这就是 Kafka 最简单的实物演示。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 搭建多 broker 集群：准备配置

为每个节点复制配置文件并添加参数：

```bash
> cp config/server.properties config/server-1.properties
> cp config/server.properties config/server-2.properties
```

```txt
config/server-1.properties:
    broker.id=1
    port=9093
    log.dir=/tmp/kafka-logs-1
config/server-2.properties:
    broker.id=2
    port=9094
    log.dir=/tmp/kafka-logs-2
```

- `broker.id` 在集群中唯一标注一个节点
- 同一台机器上必须用不同端口和不同日志目录，避免数据被覆盖

<!--
单机跑通了，我们来搭一个三节点集群（都在本机上）。做法很简单：复制两份 server.properties，然后给它们各自设置不同的 broker.id、端口和日志目录。注意，因为都在同一台机器上，端口和日志目录必须不一样，否则数据会被互相覆盖。broker.id 用来在集群里唯一标识这台 broker，Zookeeper 正是靠它来注册和识别每个节点。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 启动其余节点并创建 3 副本 Topic

```bash
> bin/kafka-server-start.sh config/server-1.properties &
> bin/kafka-server-start.sh config/server-2.properties &
```

```bash
> bin/kafka-topics.sh --create --zookeeper localhost:2181 \
    --replication-factor 3 --partitions 1 --topic my-replicated-topic
```

- 启动 server-1、server-2，加上之前已启动的节点，构成 3 个 broker
- 创建一个拥有 3 个副本的 Topic

<!--
现在启动刚才配置好的两个节点，加上之前启动的第一个，就组成了一个 3 broker 集群。接着我们创建一个 replication-factor 为 3 的 Topic，也就是说，这个 Topic 的数据会在三台 broker 上各存一份。这就是真正的“副本”在起作用了——你可以想象，如果其中一台挂了，数据也不会丢。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 查看集群信息：describe

```bash
> bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
Topic: my-replicated-topic PartitionCount:1 ReplicationFactor:3 Configs:
Topic: my-replicated-topic Partition: 0  Leader: 1  Replicas: 1,2,0  Isr: 1,2,0
```

- **Leader**：负责处理消息的读和写，从所有节点随机选择
- **Replicas**：列出所有副本节点，无论是否在服务中
- **Isr**：正在服务中的节点集合

<!--
这个 describe 命令很值得细看，它把集群的状态一次性展示出来。第一行是 Topic 的整体描述，分区数 1、副本因子 3。下面每一行对应一个分区，这里只有一个分区。三个字段的含义：Leader 是当前负责读写的主节点；Replicas 是全部副本节点，不管在不在线；Isr 是真正”在同步、在服务“的节点。本例里节点 1 是 Leader，Replicas 和 Isr 都是 1、2、0。看懂这三个值，你就基本理解了副本机制。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
class: compact
---

## 集群收发消息验证

```bash
> bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-replicated-topic
...
my test message 1
my test message 2

> bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning \
    --topic my-replicated-topic
...
my test message 1
my test message 2
```

- 向 3 副本 Topic 发送消息，再从 consumer 消费，验证集群可用

<!--
最后我们验证一下这个集群真的能收发消息。用 producer 发两条消息：my test message 1 和 2，再用 consumer 从头消费，能把这两条原样取回来。这说明消息被正确写入、同步到副本、并能被消费者读到。到这一步，你的 Kafka 集群环境就搭建完成了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.1 节。
-->

---
layout: section
class: compact
---

## 3.2 开发环境搭建

<ol class="outline">
  <li class="current">添加依赖与配置程序</li>
  <li>简单发送/接收</li>
  <li>高级别 consumer</li>
</ol>

<!--
服务器能用了，接下来我们进入开发环境。命令行工具适合验证，但要真正把它用到业务里，就得写代码。这一节我们用 Maven 搭建开发环境，先引入依赖，再写配置和收发程序。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.2 节。
-->

---
class: compact
---

## 添加 Maven 依赖

- 方式一：把 Kafka 安装包 `lib` 目录下的 jar 包加入项目 classpath
- 方式二：使用 Maven 管理依赖

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka_2.10</artifactId>
    <version>0.8.0</version>
</dependency>
```

- 个别依赖可能找不到，可将相应 jar 安装到本地 mvn 仓库，或直接复制到本地仓库对应目录

<!--
在开发中引用 Kafka，推荐用 Maven 管理依赖。在你的 pom.xml 里加上这段 dependency 就能拉到 kafka_2.10 这个包。实际操作中你可能会发现有两三个传到依赖找不到，这时要么用 mvn install 把它们装进本地仓库，要么手动把 jar 拷到本地仓库对应目录。这一步做完，你的项目就有 Kafka 的类可用了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.2 节。
-->

---
class: compact
---

## 配置程序：连接参数接口

用一个接口集中配置 Kafka 连接参数：

```java
package com.sohu.kafkademon;
public interface KafkaProperties {
    final static String zkConnect = "10.22.10.139:2181";
    final static String groupId = "group1";
    final static String topic = "topic1";
    final static String kafkaServerURL = "10.22.10.139";
    final static int kafkaServerPort = 9092;
    final static int kafkaProducerBufferSize = 64 * 1024;
    final static int connectionTimeOut = 20000;
    final static int reconnectInterval = 10000;
    final static String topic2 = "topic2";
    final static String topic3 = "topic3";
    final static String clientId = "SimpleConsumerDemoClient";
}
```

<!--
写正式程序前，我们建一个接口，把常用的连接参数集中起来。你看这里就有 Zookeeper 地址、group.id、topic、broker 端口、缓冲区大小、超时时间等等。把这些参数集中定义的好处是，后面 producer 和 consumer 都能复用，改配置时也只改这一处。这就是一个典型的“配置集中管理”的写法。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.2 节。
-->

---
class: compact
---

## 编写 Producer

```java
public class KafkaProducer extends Thread {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public KafkaProducer(String topic) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "10.22.10.139:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(
                new ProducerConfig(props));
        this.topic = topic;
    }
    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = new String("Message_" + messageNo);
            System.out.println("Send:" + messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            messageNo++;
            try { sleep(3000); } catch (InterruptedException e) { e.printStackTrace(); }
        }
    }
}
```

<!--
**[看代码]** 我们看这个 producer 的核心。两个关键配置：serializer.class 表明消息用字符串编码；metadata.broker.list 告诉它去哪找 broker。run 方法里是真正的发送逻辑——构造一条 “Message_N”、send 发出去，然后 sleep 3 秒再发下一条。注意它用 KeyedMessage 包装，第一个参数是 topic，第二个是消息内容。整个循环就是一个不断生产消息的线程。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.2 节。
-->

---
class: compact
---

## 编写 Consumer

```java
public class KafkaConsumer extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;
    public KafkaConsumer(String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }
    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", KafkaProperties.groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("receive: " + new String(it.next().message()));
            try { sleep(3000); } catch (InterruptedException e) { e.printStackTrace(); }
        }
    }
}
```

<!--
**[看代码]** consumer 这边就比较典型了。构造时通过 createConsumerConfig 配制了一堆 Zookeeper 相关的参数（连接地址、group.id、会话超时、自动提交 offset 的间隔）。run 方法里：用 topicCountMap 告诉它要订阅某个 topic、开几个流，然后 createMessageStreams 拿到消息流，循环里用 it.next().message() 取出消息打印。注意这里用的是高级别 API，你不需要手动管理 offset，框架会按 auto.commit.interval.ms 自动提交。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.2 节。
-->

---
class: compact
---

## 简单发送/接收：主程序

```java
package com.sohu.kafkademon;
public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {
        KafkaProducer producerThread = new KafkaProducer(KafkaProperties.topic);
        producerThread.start();
        KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.topic);
        consumerThread.start();
    }
}
```

<!--
最后是把前面两个线程启动起来。main 里分别 new 一个 KafkaProducer 和 KafkaConsumer，然后 start()。这样 producer 线程就会持续发消息，consumer 线程持续收消息，你就能看到控制台上“Send: …”和“receive: …”交替出现。这就是一个最精简的 Kafka 收发示例。至此，从服务器到开发环境，一条完整的链路就走通了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3.2 节。
-->

---
class: compact
---

## 想一想（自检）

**问题 1**：我们在一台机器上搭建"三个 broker 的集群"时，为什么三个节点的端口、日志目录必须不一样？

> 提示：想一想 `broker.id` 和"数据不能互相覆盖"。

**问题 2**：用 `describe` 命令看到一个 Topic 有 `Leader:1 Replicas:1,2,0 Isr:1,2,0`，这三项各自说明什么？

> 提示：分别对应"现在谁负责读写 / 所有的副本 / 真正在同步的副本"。

<!--
这两个问题是检查你有没有动手动脑。第一题：三个 broker 挤在同一台机器上，为了区分它们、避免数据写到同一个文件里互相覆盖，所以必须用不同的端口和不同的日志目录；而 broker.id 用来在集群里唯一标识每个节点。第二题：Leader 是现在负责读写的那个主节点（这里是节点 1）；Replicas 是所有副本节点（1、2、0），不管在不在线；Isr 是真正"在同步中"的节点。看懂这三项，就说明你理解了副本机制。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3 章，自检。
-->

---
class: compact
---

## 本章小结

1. **服务器搭建**：先启动 Zookeeper，再启动 Kafka；用命令行创建 Topic、收发消息；多 broker 集群靠 `broker.id` + 不同端口/日志目录区分
2. **开发环境搭建**：Maven 引入依赖；集中配置连接参数；Producer 持续发送、Consumer 持续订阅
3. 通过 `describe` 命令可查看 **Leader / Replicas / Isr**，理解副本与集群状态

<!--
**[过渡]** 我们把第三章收尾。环境搭建的本质，就是让 Zookeeper 协调、Kafka broker 跑起来、客户端能连上并收发消息。你亲手走一遍，会比读十遍更清楚 Topic、分区、副本、leader 这些概念在真实系统里长什么样。环境有了，下一章我们就切入 Kafka 的“消息传送”机制——数据传输的一致性级别、性能优化、主从同步这些更深入的东西。做好心里准备，下一章要动脑子了。

[Sources]
- 吴斌，《大数据实时计算与应用》第 3 章，本章小结。
-->
