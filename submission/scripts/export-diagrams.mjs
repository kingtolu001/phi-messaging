import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const tool = process.env.ARCHIFY_HOME || path.resolve(root, '../.tools/skills/archify');
const { ChromeVisualBrowser, findChrome } = await import(pathToFileURL(path.join(tool, 'bin/visual-check.mjs')));
const chrome = findChrome();
if (!chrome) throw new Error('Chrome/Chromium or Edge is required for native viewer exports');
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const records = [];
for (const name of ['runtime', 'send', 'recovery']) {
  const artifact = path.join(root, 'docs/diagrams', name + '.html');
  const input = fs.readFileSync(artifact);
  const provenance = JSON.parse(fs.readFileSync(artifact.replace('.html', '.delivery.json')));
  if (provenance.status !== 'current' || provenance.artifact.sha256 !== hash(input)) {
    throw new Error('Current Archify delivery provenance required: ' + name);
  }
  const browser = new ChromeVisualBrowser(chrome, { shutdownTimeoutMs: 5000 });
  try {
    await browser.inspect({ artifactPath: artifact, width: 1600, height: 1000, theme: 'light' });
    const session = await browser.sessionPromise;
    for (const format of ['svg', 'png']) {
      // Use Archify's own canonical exporter. Capture its final download Blob
      // in memory rather than sending a download into the user's Downloads.
      const result = await browser.cdp.send('Runtime.evaluate', {
        expression: `(async function () {
          let blob;
          const create = URL.createObjectURL;
          const click = HTMLAnchorElement.prototype.click;
          URL.createObjectURL = function (value) { blob = value; return create.call(this, value); };
          HTMLAnchorElement.prototype.click = function () { if (!this.download) click.call(this); };
          try {
            await Archify.exportMenu.run(${JSON.stringify(format)});
            const html = document.documentElement;
            if (html.getAttribute('data-last-export-error') || !blob || html.getAttribute('data-last-export-canonical') !== 'true') {
              throw new Error(html.getAttribute('data-last-export-error') || 'Canonical export not confirmed');
            }
            const data = await new Promise((resolve, reject) => {
              const reader = new FileReader(); reader.onload = () => resolve(reader.result);
              reader.onerror = () => reject(reader.error); reader.readAsDataURL(blob);
            });
            return { data, bytes: blob.size, mime: blob.type,
              width: html.getAttribute('data-last-export-width'), height: html.getAttribute('data-last-export-height') };
          } finally { URL.createObjectURL = create; HTMLAnchorElement.prototype.click = click; }
        })()`,
        awaitPromise: true, returnByValue: true,
      }, session, 60000);
      if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
      const value = result.result.value;
      const bytes = Buffer.from(value.data.split(',')[1], 'base64');
      if (bytes.length !== value.bytes) throw new Error('Export byte count mismatch');
      if (format === 'png' && !bytes.subarray(0, 8).equals(Buffer.from('89504e470d0a1a0a', 'hex'))) throw new Error('Invalid PNG');
      if (format === 'svg' && !bytes.toString('utf8').includes('<svg')) throw new Error('Invalid SVG');
      const output = path.join(root, 'docs/diagrams', name + '.' + format);
      fs.writeFileSync(output, bytes);
      records.push({ file: path.relative(root, output).replaceAll('\\', '/'), format,
        canonical: true, bytes: bytes.length, sha256: hash(bytes), html_sha256: hash(input),
        width: value.width ? Number(value.width) : null, height: value.height ? Number(value.height) : null });
      console.log('Exported ' + name + '.' + format);
    }
  } finally {
    await browser.close();
  }
}
fs.writeFileSync(path.join(root, 'docs/diagrams/exports.json'), JSON.stringify({
  method: 'Archify native exportMenu.run in a local headless browser; no HTML edits', records,
}, null, 2) + '\n');
