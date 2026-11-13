import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { cp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';

const slug = process.argv[2] || 'lesson-05';
const dir = path.resolve('node_modules/@easy-slides/cli/dist-cli');
const { checkProject } = await import(pathToFileURL(path.join(dir, 'check.js')));
const { runSlidev } = await import(pathToFileURL(path.join(dir, 'lib/run-slidev.js')));
const { resolveTheme } = await import(pathToFileURL(path.join(dir, 'lib/theme.js')));
const { decks, config } = await checkProject(process.cwd());
const deck = decks.find(d => d.slug === slug);
if (!deck) throw new Error(`Unknown lecture: ${slug}`);
const out = path.resolve('dist', slug);
// A lecture rebuild must not retain chunks from earlier versions of that lecture.
await rm(out, { recursive: true, force: true });
await runSlidev(['build', deck.entry, '--theme', resolveTheme(config.theme), '--out', out, '--base', `/${slug}/`, '--router-mode', 'hash']);
await cp(path.join(deck.directory, 'assets'), path.join(out, 'assets'), { recursive: true, force: true });
const html = await readFile(path.join(out, 'index.html'), 'utf8');
for (const route of ['presenter', 'overview', 'notes-edit']) {
  await mkdir(path.join(out, route), { recursive: true });
  await writeFile(path.join(out, route, 'index.html'), html.replace('</head>', `<script>location.hash="#/${route}"</script></head>`));
}
console.log(`Built ${slug}; other lectures unchanged.`);
