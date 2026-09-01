# 《大数据实时计算与应用》课程课件

这是一个独立的 easy-slides 内容仓库。课程文字、图片、讲稿和部署配置保存在本仓库；构建依赖固定到 [easy-slides](https://git.xaria.tk/yulonger/easy-slides) 的确定提交。

基于吴斌主编，清华大学出版社《大数据实时计算与应用》一书。

## 在 VS Code 中实时编辑

1. 安装 Node.js 20+ 与 pnpm。
2. 在本目录执行 `pnpm install`。
3. 用 VS Code 打开本目录：`code .`。
4. 接受工作区推荐扩展，至少安装 **Slidev** 与 **Vue - Official**。
5. 打开 `slides/chapter-1/slides.md`。
6. 按 `Cmd+Shift+P`，选择 `Tasks: Run Task`，再选择 `easy-slides: 实时预览章节` 并选取章节。
7. 浏览器打开终端显示的地址。保存 Markdown 或替换图片后，页面会自动热更新。

也可以直接使用终端：

```bash
pnpm dev -- chapter-1
```

演示者模式与提词器：

```bash
cp .env.example .env
# 编辑 .env 中的 PRESENTER_TOKEN
pnpm present -- chapter-1
```

## 内容位置

每章一个目录 `slides/chapter-<n>/`，内部结构统一：

- `slides.md`：幻灯片正文
- `assets/figures/`：本章示意图（由 `_ref/chapters/Chapter<n>/images/` 裁选并转 PNG）
- 每页提词器（演示者视图讲稿）：对应幻灯片末尾的 HTML 注释，含 `[Sources]` 页码

所有章节共享的排版样式由 `easy-slides` 主题提供；章节目录不再复制相同的 `style.css`。只有某一章确有独立视觉需求时，才在该目录添加局部样式。

| 章节 | 主题 | 目录 |
| ---- | ---- | ---- |
| 1 | 分布式实时计算系统 | `slides/chapter-1/` |
| 2 | 初识 Kafka | `slides/chapter-2/` |
| 3 | Kafka 环境搭建 | `slides/chapter-3/` |
| 4 | Kafka 消息传送 | `slides/chapter-4/` |
| 5 | Zookeeper 开发 | `slides/chapter-5/` |
| 6 | 初识 HBase | `slides/chapter-6/` |
| 7 | HBase 基础操作 | `slides/chapter-7/` |
| 8 | HBase 高阶特性 | `slides/chapter-8/` |
| 9 | 管理 HBase | `slides/chapter-9/` |
| 10 | 初识 Storm | `slides/chapter-10/` |
| 11 | 配置 Storm 集群 | `slides/chapter-11/` |
| 12 | Trident 和 Trident-ML | `slides/chapter-12/` |
| 13 | DRPC 模式 | `slides/chapter-13/` |
| 14 | Storm 实战 | `slides/chapter-14/` |

原稿素材在 `_ref/`：`_ref/Chapter<n>.pdf` 为原 PDF，`_ref/chapters/Chapter<n>/` 为 MinerU 抽取的 Markdown 与 `images/`。

## Cloudflare Pages 部署

推送到 `main` 后，GitHub Actions 会构建全部课件并把 `dist/` 发布到 Cloudflare Pages；Pull Request 只执行构建检查。首次启用时需完成：

1. 在 Cloudflare Pages 创建名为 `big-data-realtime-slides` 的 Direct Upload 项目，Production branch 设为 `main`。
2. 在 GitHub 仓库的 **Settings → Secrets and variables → Actions** 中添加：
   - `CLOUDFLARE_API_TOKEN`：具备 Cloudflare Pages 编辑权限的 API Token
   - `CLOUDFLARE_ACCOUNT_ID`：Cloudflare Account ID
   - `PRESENTER_TOKEN`（可选）：演示者入口口令；未设置时生产站隐藏演示者入口

工作流使用 Node.js 22、pnpm 11 和固定提交的 easy-slides 引擎。Cloudflare Pages 的构建产物目录为 `dist/`。
