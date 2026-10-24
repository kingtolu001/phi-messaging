import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import './diagram-evidence.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const read = file => fs.readFileSync(path.join(root, file));
const json = file => JSON.parse(read(file).toString('utf8').replace(/^\uFEFF/, ''));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const require = (condition, message) => { if (!condition) throw new Error(message); };
const validation = json('docs/diagrams/validation.json');
require(validation.runs.length === 3, 'Expected three finalized diagrams');
for (const run of validation.runs) {
  const receipt = json(run.receipt);
  require(receipt.ok && receipt.status === 'pass', 'Failed finalize receipt: ' + run.name);
  for (const gate of ['validate', 'deliver', 'check', 'browser-check']) {
    require(receipt.gates[gate] === 'pass' && run.gates[gate] === 'pass', 'Gate not passed: ' + run.name + '/' + gate);
  }
  const spec = `docs/diagrams/${run.name}.${receipt.type}.json`;
  const html = `docs/diagrams/${run.name}.html`;
  require(hash(read(spec)) === receipt.specification.sha256 && receipt.specification.sha256 === run.source_sha256, 'Source drift: ' + run.name);
  require(hash(read(html)) === receipt.artifact.sha256 && receipt.artifact.sha256 === run.html_sha256, 'HTML drift: ' + run.name);
  require(json(spec).meta.output === html, 'Wrong declared output: ' + run.name);
  require(receipt.browserCheck.ran === true, 'Browser check did not run: ' + run.name);
  console.log('PASS current source/HTML/four native gates: ' + run.name);
}
const exports = json('docs/diagrams/exports.json');
require(exports.records.length === 6, 'Expected six native static exports');
for (const record of exports.records) {
  const bytes = read(record.file);
  require(record.canonical && hash(bytes) === record.sha256 && bytes.length === record.bytes, 'Export drift: ' + record.file);
  const html = record.file.replace(/\.(png|svg)$/, '.html');
  require(hash(read(html)) === record.html_sha256, 'Export belongs to an old HTML: ' + record.file);
}
// Check authored navigation, not Archify's upstream examples embedded in JS.
for (const file of ['README.md', 'docs/diagrams/README.md', 'docs/diagrams/index.html']) {
  const text = read(file).toString('utf8');
  const links = file.endsWith('.html') ? [...text.matchAll(/(?:href|src)="([^"]+)"/g)].map(m => m[1])
    : [...text.matchAll(/\]\(([^)]+)\)/g)].map(m => m[1]);
  for (const link of links) {
    if (/^(?:https?:|#)/.test(link)) continue;
    const target = path.resolve(root, path.dirname(file), link.split('#')[0]);
    require(fs.existsSync(target), 'Broken local link: ' + file + ' -> ' + link);
  }
}
console.log('PASS six export bindings and authored local links');
