# 分布式实时计算：五次课

以 Google Dataflow 模型组织理论，用 Apache Flink 2.3 解释执行机制。五次讲授，每次135分钟，面向基础薄弱的学生。

| 课次 | 主题 | 页数 | 源稿 |
| --- | --- | --- | --- |
| 1 | 数据流与计算模型 | 50 | [正文与讲稿](slides/lesson-01/slides.md) |
| 2 | 主从架构与并行执行 | 50 | [正文与讲稿](slides/lesson-02/slides.md) |
| 3 | 时间、窗口与结果修正 | 51 | [正文与讲稿](slides/lesson-03/slides.md) |
| 4 | 状态与流式查询 | 55 | [正文与讲稿](slides/lesson-04/slides.md) |
| 5 | 分布式快照与故障恢复 | 54 | [正文与讲稿](slides/lesson-05/slides.md) |

共260页，含各讲附录。五讲均采用机制图、连续数据推演、课堂口述讲稿和方案设计练习。第1至4讲共206页，配158幅可编辑SVG场景及UML、Mermaid图。每页包含讲稿与来源，题目和答案分开。

## 本地观看与编辑

```bash
pnpm install
pnpm build
pnpm preview
```

打开 [课程首页](http://127.0.0.1:3000/)，按左右方向键翻页。首页只列五次新课；原教材章节位于独立的 `/reference/` 页面。

```bash
pnpm dev -- lesson-01
```

开发服务默认使用3030端口，支持保存后更新及 `/presenter/` 演示者视图。生产构建未配置 `PRESENTER_TOKEN` 时隐藏演示者入口。

## 数据、图示与维护

- [教师使用指引](docs/course-guide.md)：时长、关键推演、图示与提问。
- [案例数据](course/data/cases.json)：原始投递、窗口专项、执行能力、Join及恢复参数。
- [数据说明](course/README.md)：不同专项数据的边界与时间口径。
- [来源清单](docs/sources.md)：论文、Flink固定版本文档和UML标准。
- [图示源码](course/diagrams/)：六类PlantUML及可编辑SVG。

```bash
pnpm check:course
pnpm check
pnpm diagrams
```

`check:course`独立复算金额、窗口、水位线、Join、热点与恢复，并核对页面、讲稿、图示及旧版删除情况。`diagrams`需要Java，使用固定版本PlantUML并校验SHA-256；首次运行将工具下载到忽略目录 `tmp/tools/`。生成的SVG已随源码提供，正常构建不需要Java。

`pnpm build`构建课件、组织课程与参考入口，再核对发布结构。七次课及其草稿、专用考试和小论文资料已删除。原始教材、正式教学文件与原chapter章节保留。

## 构建与部署

内容使用 easy-slides / easy-jyy。`slides/`存放Markdown源稿，`dist/`为生成站点，`scripts/preview.mjs`提供本地预览。现有GitHub Actions继续使用Cloudflare Pages部署；本轮未远程发布。
