# 配置 Storm 集群

## 11.1 Storm 集群框架介绍

Storm 集群遵循主/从(master/slave)结构, 和 Hadoop 等分布式计算技术类似, 语义上稍有不同。主/从结构中, 通常有一个配置中静态指定或运行时动态选举出的主节点。Storm 使用前一种实现方式。

Storm 集群由一个主节点(称为 nimbus)和一个或者多个工作节点(称为 supervisor)组成。在 nimbus 和 supervisor 节点之外, Storm 还需要一个 Apache Zookeeper 的实例, Zookeeper 实例本身可以由一个或者多个节点组成, 如图 11-1 所示。

![](images/b2fc5d591cb12e07d166a3f7fadb5c4aba9c6ab000fba6cdefe4dc2eab9db1ff.jpg)  
图11-1 Storm集群的框架

nimbus 和 supervisor 都是 Storm 提供的后台守护进程, 可以共存在同一台机器上。实际上, 可以建立一个单节点伪集群, 把 nimbus、supervisor 和 Zookeeper 进程都运行在同一台机器上。

## 11.1.1 理解 nimbus 守护进程

nimbus 守护进程的主要职责是管理、协调和监控在集群上运行的 topology，包括 topology 的发布、任务指派，以及在事件处理失败时重新指派任务。

将 topology 发布到 Strom 集群, 将预先打包成 jar 文件的 topology 和配置信息提交(submitting)到 nimbus 服务器上。一旦 nimbus 接收到了 topology 的压缩包,会将 jar 包分发到足够数量的 supervisor 节点上。当 supervisor 节点接收到了 topology 压缩文件后, nimbus 就会指派 task (Bolt 和 Spout 实例) 到每个 supervisor,并且发送信号指示 supervisor 生成足够的 Worker 来执行指派的 task。

nimbus 记录所有 supervisor 节点的状态和分配给它们的 task。如果 nimbus 发现某个 supervisor 没有上报心跳或者已经不可达，它会将故障 supervisor 分配的 task 重新分配到集群中的其他 supervisor 节点。

前面提到过,严格意义上讲 nimbus 不会引起单点故障。这个特性是因为 nimbus 并不参与 topology 的数据处理过程,它仅仅是管理 topology 的初始化、任务分发和进行监控。实际上,如果 nimbus 守护进程在 topology 运行时停止了,只要分配的 supervisor 和 Worker 健康运行,topology 一直继续数据处理。需要注意的是,在 nimbus 已经停止的情况下 supervisor 会异常终止,因为没有 nimbus 守护进程来重新指派失败这个终止的 supervisor 的任务,数据处理就会失败。

## 11.1.2 supervisor 守护进程的工作方式

supervisor 守护进程等待 nimbus 分配任务后生成并监控 Workers(JVM 进程)执行任务。supervisor 和 Worker 都是运行在不同的 JVM 进程上, 如果由 supervisor 拉起的一个 Worker 进程因为错误(或者因为 UNIX 终端的 kill-9 命令, Windows 的 tskkill 命令强制结束)异常退出, supervisor 守护进程会尝试重新生成新的 Worker 进程。

看到这里读者可能想知道 Storm 的有保障传输机制如何适应其容错模型。如果一个 Worker 甚至整个 supervisor 节点都故障了, Storm 如何保障出错时正在处理的 tuples 的传输?

答案就在 Storm 的 tuple 锚定和应答确认机制中。当打开了可靠传输的选项，传输到故障节点上的 tuples 将不会收到应答确认，Spout 会因为超时而重新发射原始 tuple。这样的过程会一直重复，直到 topology 从故障中恢复开始正常处理数据。

## 11.1.3 DRPC 服务工作机制

Storm 应用中的一个常见模式期望将 Storm 的并发性和分布式计算能力应用到“请求—响应”范式中。一个客户端进程或者应用提交了一个请求并同步地等待响应。这样的范式可能看起来和典型 topology 的高异步性、长时间运行的特点恰恰相反，Storm 具有事务处理的特性来实现这种应用场景，如图 11-2 所示。

客户端给 DRPC 服务器发送要执行的方法的名字,以及这个方法的参数,实现这个函数的 topology 使用 DRPCSpout 从 DRPC 服务器接收函数调用流。每个函数调用被 DRPC 服务器标记了一个唯一的 id。

然后这个 topology 计算结果, 在 topology 的最后一个叫作 ReturnResults 的 Bolt 会连接到 DRPC 服务器, 并且把这个调用的结果发送给 DRPC 服务器(通过那个唯一的 id 标识)。DRPC 服务器用那个唯一 id 与等待的客户端匹配上, 唤醒这个客户端并且把结果发送给它。

![](images/33998a51ee98e4aa3bbaf7e8058c08d49528c204fe0b49784a50850887f37a0a.jpg)  
图 11-2 DRPC 的工作流机制

## 11.1.4 Storm的UI简介

Storm UI 是可选功能,该功能可提供一个基于 Web 的 GUI 来监控 Storm 集群,对正在运行的 topology 有一定的管理功能。Storm UI 提供了已经发布的 topology 的统计信息,对监控 Storm 集群的运转和 topology 的功能有很大帮助,如图 11-3 所示。

![](images/7393311cbafdb324dc8cc231ebbd45e9433cd6058e6673352aeeb916d6b7692c.jpg)  
图11-3 Storm UI

Storm UI 只能报告由 nimubs 的 trhift API 获取的信息, 不会影响 topology 上其他功能。Storm UI 可以随时开关而不影响任何 topology 的运行, 在那里它完全是无状态的。它还可以用配置来进行一些简单的管理, 如开启、停止、暂停和重新均衡负载 topology。

## 11.2 在 Linux 上安装 Storm

这一节将详细描述如何在 Linux 上搭建一个 Storm 集群。请依次完成以下安装步骤。

(1) 搭建 Zookeeper 集群。

(2) 安装 Storm 依赖库。

(3) 下载并解压 Storm 发布版本。

(4) 修改 storm.yaml 配置文件。

(5) 启动 Storm 各个后台进程。

## 11.2.1 搭建 Zookeeper 集群

由于在前面 Zookeeper 部分已有相关介绍,此处不再赘述。

## 11.2.2 安装 Storm 依赖库

接下来,需要在 Nimbus 和 supervisor 机器上安装 Storm 的依赖库,具体如下。

（1）ZeroMQ 2.1.7（请勿使用 2.1.10 版本，因为该版本的一些严重 bug 会导致 Storm 集群运行时出现奇怪的问题。少数用户在 2.1.7 版本会遇到 IllegalArgumentException 的异常，此时降为 2.1.4 版本可修复这一问题）。

(2) JZMQ。

(3) Java 6。

(4) Python 2.6.6。

```txt
(5) Unzip。
```

1. 安装 ZeroMQ 2.1.7

下载后编译安装 ZeroMQ。

```shell
wget http://download.zeromq.org/zeromq-2.1.7.tar.gz
tar -xzf zeromq-2.1.7.tar.gz
cd zeromq-2.1.7
./configure
make
sudo make install
```

如果安装过程报错 uuid 找不到, 则通过如下的包安装 uuid 库。

```shell
sudo yum install e2fsprogs1 -b current
sudo yum install e2fsprogs-devel -b current
```

## 2. 安装 JZMQ

```shell
git clone https://github.com/nathanmarz/jzmq.git
cd jzmq
./autogen.sh
./configure
make
sudo make install
```

## 3. 安装Python2.6.6

(1) 下载 Python 2.6.6。

```batch
wget http://www.python.org/ftp/python/2.6.6/Python-2.6.6.tar.bz2
```

(2) 编译安装 Python 2.6.6。

```batch
tar -jxvf Python-2.6.6.tar.bz2
```

```batch
cd Python-2.6.6
./configure
make
make install
```

## (3) 测试 Python 2.6.6。

```batch
\$ python -V
Python 2.6.6
```

## 4. 安装 Unzip

(1) 如果使用 RedHat 系列 Linux 系统, 执行以下命令安装 Unzip。
apt-get install unzip

(2) 如果使用 Debian 系列 Linux 系统, 执行以下命令安装 Unzip。

```txt
yum install unzip
```

## 11.2.3 下载并解压 Storm 发布版本

下一步,需要在 Nimbus 和 supervisor 机器上安装 Storm 发行版本。

(1) 下载 Storm 发行版本, 推荐使用 Storm 0.8.1。

```txt
wget https://github.com/downloads/nathanmarz/storm/storm-0.8.1.zip
```

(2) 解压到安装目录下。

```txt
unzip storm-0.8.1.zip
```

## 11.2.4 修改 storm.yaml 配置文件

Storm 发行版本解压目录下有一个 conf/storm.yaml 文件, 用于配置 Storm。conf/storm.yaml 中的配置选项将覆盖 defaults.yaml 中的默认配置。以下选项必须在 conf/storm.yaml 中进行配置。

(1) storm. zookeeper. servers: Storm 集群使用的 Zookeeper 集群地址, 其格式如下。

```txt
storm.zookeeper.servers:
  - "111.222.333.444"
  - "555.666.777.888"
```

如果 Zookeeper 集群使用的不是默认端口,那么还需要 storm.zookeeper.port 选项。

(2) storm. local. dir: Nimbus 和 Supervisor 进程用于存储少量状态, 如 jars、confs 等的本地磁盘目录, 需要提前创建该目录并给予足够的访问权限, 然后在 storm. yaml 中配置该目录。

```txt
storm.local.dir:/home/admin/storm/workdir"
```

(3) java. library. path: Storm 使用的本地库 (ZeroMQ 和 JZMQ) 加载路径，默认为 "/usr/local/lib: /opt/local/lib: /usr/lib"，一般来说 ZeroMQ 和 JZMQ 默认安装在 /usr/local/lib 下，因此不需要配置。

(4) nimbus.host: Storm 集群 Nimbus 机器地址, 各个 supervisor 工作节点需要知道哪个机器是 Nimbus, 以便下载 Topologies 的 jars、confs 等文件。

```txt
nimbus.host:"111.222.333.444"
```

(5) supervisor.slots.ports: 对于每个 supervisor 工作节点, 需要配置该工作节点可以运行的 Worker 数量。每个 Worker 占用一个单独的端口用于接收消息, 该配置选项用于定义哪些端口是可被 Worker 使用的。默认情况下, 每个节点上可运行 4 个 Workers, 分别在 6700、6701、6702 和 6703 端口。

```asm
supervisor.slots.ports:
    -6700
    -6701
    -6702
    -6703
```

## 11.2.5 启动 Storm 后台进程

最后一步,启动 Storm 的所有后台进程。和 Zookeeper 一样,Storm 也是快速失败(fail-fast)的系统,这样 Storm 才能在任意时刻被停止,并且当进程重启后被正确地恢复执行。这也是为什么 Storm 不在进程内保存状态的原因,即使 Nimbus 或 supervisors 被重启,运行中的 topologies 不会受到影响。

以下是启动 Storm 各个后台进程的方式。

(1) Nimbus: 在 Storm 主控节点上运行"bin/storm nimbus >/dev/null 2>&1 &"启动 Nimbus 后台程序,并放到后台执行。

(2) supervisor: 在 Storm 各个工作节点上运行"bin/storm supervisor >/dev/null 2>&1 &"启动 supervisor 后台程序,并放到后台执行。

(3) UI: 在 Storm 主控节点上运行"bin/storm ui >/dev/null 2>&1 &"启动 UI 后台程序,并放到后台执行。启动后可以通过 http://{nimbus host}:8080 观察集群的 Worker 资源使用情况,topologies 的运行状态等信息。

## 注意：

① Storm 后台进程被启动后, 将在 Storm 安装部署目录下的 logs/子目录下生成各个进程的日志文件。

② 经测试, Storm UI 必须和 Storm nimbus 部署在同一台机器上, 否则 UI 无法正常工作, 因为 UI 进程会检查本机是否存在 nimbus 链接。

③ 为了方便使用,可以将 Bin/Storm 加入系统环境变量中。

至此，Storm 集群已经部署、配置完毕，可以向集群提交拓扑运行。

## 11.3 将 topology 提交到集群上

向集群提交任务分为以下几步。

(1) 启动 Storm topology。

storm jar allmycode.jar org.me.MyTopology arg1 arg2 arg3

其中，allmycode.jar 是包含 topology 实现代码的 jar 包，org.me.MyTopology 的 main 方法是 topology 的入口，arg1、arg2 和 arg3 为 org.me.MyTopology 执行时需要传入的参数。

(2) 停止 Storm topology。

storm kill {toponame}

其中，{toponame}为 topology 提交到 Storm 集群时指定的 topology 任务名称。

## 本章小结

在本章中,介绍了安装和配置 Storm 集群的必需步骤,以及如何用 Storm 的守护进程和命令行工具来管理和运行 topology。

第 12 章将对 Trident——一个在 Storm 事务处理和状态管理基础上的高级别抽象技术进行介绍。

## 习题

（1）请试着在本机上按照本章介绍的方法搭建 Storm 集群。

(2) Hadoop 的 MapReduce 与 Storm 的 topology 有什么不一样的地方?

(3) Nimbus 与 hadoop 的 jobtracer 作用是否类似?

(4) Nimbus 和 supervisor 之间的所有协调工作由谁来完成?

(5) 一个 topology 由哪两部分组成?

(6) Storm HA 模式如果机器意外停止,是如何处理任务的?

(7) Storm 如何运行一个 topology?

(8) Spout 类里面最重要的方法是 nextTuple, 它的作用是什么?

(9) Storm 里面有几种类型的 stream grouping, 分别是什么?

(10) 如何构建 topology?
