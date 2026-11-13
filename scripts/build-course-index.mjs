import { readFile, writeFile, mkdir } from 'node:fs/promises';
import config from '../easy-slides.config.mjs';
const file = new URL('../dist/index.html', import.meta.url);
const original = await readFile(file, 'utf8');
const cards = [...original.matchAll(/<article class="deck-card">[\s\S]*?<\/article>/g)].map(m => m[0]);
const descriptions = [
 '50 页 · 事件身份、按键累计、UML 与数据流方案。',
 '50 页 · 子任务与 Slot、部署、反压及热点推演。',
 '51 页 · 时间窗口、水位线、迟到修正与输出版本。',
 '55 页 · 订单状态机、Join、SQL 与状态扩容。',
 '54 页 · 屏障传播、故障重放与端到端方案设计。',
];
const lessons = cards.filter(card => /href="[^\"]*\/lesson-0[1-5]\//.test(card)).map(card => {
 const n = Number(card.match(/lesson-0([1-5])/)[1]);
 return card.replace(/<p class="description">[\s\S]*?<\/p>/, `<p class="description">${descriptions[n-1]}</p>`);
});
let chapters = cards.filter(card => /href="[^\"]*\/chapter-\d+\//.test(card));
if (!chapters.length) {
  const previous = await readFile(new URL('../dist/reference/index.html', import.meta.url), 'utf8').catch(() => '');
  chapters = [...previous.matchAll(/<article class="deck-card">[\s\S]*?<\/article>/g)].map(m => m[0]);
}
if (lessons.length !== 5) throw new Error(`Expected five current decks, found ${lessons.length}`);
const update = (html, content) => html.replace(/(<section class="deck-list"[^>]*>)[\s\S]*?(<\/section>)/, `$1\n${content}\n$2`);
let homepage = update(original, lessons.join('\n')).replace('MARKDOWN PRESENTATIONS', '分布式实时计算 · 五次课').replace(/<title>[\s\S]*?<\/title>/, `<title>${config.title}</title>`).replace(/<h1>[\s\S]*?<\/h1>/, `<h1>${config.title}</h1>`).replace(/<p class="lede">[\s\S]*?<\/p>/, '<p class="lede">Dataflow 模型与 Flink 2.3 · 每讲 135 分钟</p>');
if (!homepage.includes('href="./reference/"')) homepage = homepage.replace('</main>', '<p style="margin-top:40px"><a href="./reference/">教材章节参考 →</a></p></main>');
await writeFile(file, homepage);
await mkdir(new URL('../dist/reference/', import.meta.url), { recursive: true });
await writeFile(new URL('../dist/reference/index.html', import.meta.url), update(original, chapters.join('\n')).replace(/<h1>[\s\S]*?<\/h1>/, '<h1>教材章节参考</h1>').replace(/<p class="lede">[\s\S]*?<\/p>/, '<p class="lede"><a href="../">返回五次课课程首页</a></p>'));
console.log('Course index: five lessons; textbook chapters at /reference/.');
