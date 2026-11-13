# 理论与图示来源

理论主线采用Google Dataflow论文；Flink实现固定为release-2.3。各页讲稿的[Sources]标注对应来源。教学数据与原创例图见course目录。

## 论文、规范与官方文档

- https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/（第1、3、5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/overview/（第1、2、4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/（第1、2、4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/stateful-stream-processing/（第1、4、5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/（第1、2、5讲）
- https://www.omg.org/spec/UML/2.5.1/PDF（第1、2、4、5讲）
- https://beam.apache.org/documentation/programming-guide/#windowing（第1、3讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/back_pressure/（第2、5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/（第3、4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/（第3、4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/time/（第3讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/concepts/dynamic_tables/（第3、4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/（第4、5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/checkpointing/（第4、5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/（第4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/sql/queries/joins/（第4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/libs/cep/（第4讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/large_state_tuning/（第4讲）
- https://lamport.azurewebsites.net/pubs/chandy.pdf（第5讲）
- https://arxiv.org/abs/1506.08603（第5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpoints/（第5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/savepoints/（第5讲）
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/ha/overview/（第5讲）

## 使用方式

论文组织概念；框架文档用于核对具体行为。UML依据OMG 2.5.1规范，源码由PlantUML本地渲染。Dataflow的What/Where/When/How与Flink具体API分别讲解。所有性能数值均为题设参数；订单记录为教学构造数据。

## 第1至4讲重构对应关系（2026-11-13）

- 状态TTL：采用处理时间，专项选择OnCreateAndWrite与NeverReturnExpired；其语义见state文档。订单事件时间定时器与TTL分别推演。
- 周期有界乱序水位线：按固定版本BoundedOutOfOrdernessWatermarks实现的M−B−1ms规则推演；每条后发射是教学安排。
- 窗口清理：按窗口maxTimestamp+allowedLateness计算；生命周期、触发器、结果消息协议分别说明。
- 并行资源：共享Slot例子限定同作业、同共享组且资源足够；物理连接区分Forward与重新分区。
- 查询：Interval Join的键与时间谓词、动态表变更、SQL分组与窗口、CEP正向匹配分别配独立示例。
- 图源：course/story/lesson-01..04.json保存158幅SVG场景参数；course/diagrams保存UML PlantUML与Mermaid源码。页面讲稿中的[Sources]提供逐页来源。

补充固定版本入口：

- https://nightlies.apache.org/flink/flink-docs-release-2.3/api/java/org/apache/flink/api/common/eventtime/BoundedOutOfOrdernessWatermarks.html
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/joining/
- https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/sql/queries/overview/
