import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const htmlPath = path.resolve(__dirname, '../src/agentloom/hub/static/index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const match = html.match(/<script>([\s\S]*?)<\/script>/);
assert.ok(match, 'Expected <script> block in index.html');
const scriptCode = match[1];

// Stub minimal browser globals so functions can be evaluated cleanly in Node
const sandbox = {
  console,
  Math,
  Date,
  JSON,
  Array,
  Object,
  String,
  Number,
  RegExp,
  Map,
  Set,
  Promise,
  localStorage: {
    getItem: () => null,
    setItem: () => {},
    removeItem: () => {},
  },
  document: {
    addEventListener: () => {},
    getElementById: () => ({ addEventListener: () => {}, style: {}, classList: { add: () => {}, remove: () => {} } }),
    querySelectorAll: () => [],
    createElement: () => ({ style: {}, classList: { add: () => {}, remove: () => {} }, setAttribute: () => {} }),
  },
  window: {},
  navigator: {},
  location: { protocol: 'http:', host: '127.0.0.1:3000' },
  performance: { now: () => 0 },
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
};

vm.createContext(sandbox);
vm.runInContext(scriptCode, sandbox);

// 1. Verify XSS escaping in Markdown renderer
const xssOut = sandbox.formatContent('<img src=x onerror="alert(1)"> **ok**');
assert.ok(!xssOut.includes('<img'), 'HTML tags must be escaped');
assert.ok(xssOut.includes('&lt;img'), 'Escaped tag should remain visible as text');
assert.ok(xssOut.includes('<strong>ok</strong>'), 'Markdown bold should render');

// 2. Verify code-block isolation and email skipping for @mentions
const mdOut = sandbox.formatContent(
  'Contact dev@example.com or run `npm i @types/node`\n```bash\n@ignored-node\n```\nPing @worker-a'
);
assert.ok(!mdOut.includes('>@types/node</span>'), 'Inline code @types must not turn into mention');
assert.ok(!mdOut.includes('">@ignored-node</span>'), 'Fenced code @mention must not turn into mention');
assert.ok(!mdOut.includes('">@example</span>'), 'Email domain must not turn into mention');
assert.ok(mdOut.includes('>@worker-a</span>'), 'Real @mention should be highlighted');
assert.equal((mdOut.match(/class="mention"/g) || []).length, 1, 'Only @worker-a should have mention class');

// 3. Verify dynamic node profile reads meta safely and rejects invalid CSS colors
sandbox.agents = {
  'custom-vps': {
    online: true,
    meta: { region: 'Tokyo · Linode', color: '#38bdf8', mono: 'jp', backend: 'cli:claude' },
  },
  'bad-color-node': {
    online: true,
    meta: { color: 'red;position:fixed', mono: 'bc' },
  },
};
const prof = sandbox.getNodeProfile('custom-vps', 'agent');
assert.equal(prof.color, '#38bdf8');
assert.equal(prof.mono, 'JP');
assert.equal(prof.region, 'Tokyo · Linode');

const tpls = sandbox.buildDynamicTemplates(Object.entries(sandbox.agents));
assert.ok(tpls.length >= 2 && tpls[0].text.includes('@custom-vps'), 'Dynamic templates should include online nodes');

const badProf = sandbox.getNodeProfile('bad-color-node', 'agent');
assert.match(badProf.color, /^#[0-9a-f]{6}$/i, 'Invalid hex color must fall back to safe palette');

// 4. Verify V2 streaming accumulator, artifact bar, and approval card XSS escaping
assert.equal(sandbox.appendStreamChunk('node-1', 'hello ', 's1'), 'hello ');
assert.equal(sandbox.appendStreamChunk('node-1', 'world', 's1'), 'hello world');
assert.equal(sandbox.appendStreamChunk('node-1', 'fresh', 's2'), 'fresh');

const artHtml = sandbox.renderArtifactBarHtml([
  { id: 'art_1', filename: '<script>x</script>.log', size_bytes: 2048, sha256: 'abc' },
]);
assert.ok(!artHtml.includes('<script>x'), 'Artifact filename must be escaped');
assert.ok(artHtml.includes('2.0 KB'), 'Artifact size formatting should work');

const aprHtml = sandbox.renderApprovalCardsHtml(
  [{ id: 'apr_1', action: 'rm -rf', requester: 'alice', target_agent: 'node-1', summary: '<img src=x>', status: 'pending' }],
  true
);
assert.ok(!aprHtml.includes('<img'), 'Approval summary must be escaped');
assert.ok(aprHtml.includes('批准执行'), 'Admin view should include approve button');

const aprOpHtml = sandbox.renderApprovalCardsHtml(
  [{ id: 'apr_1', action: 'rm -rf', requester: 'alice', target_agent: 'node-1', summary: 'clean', status: 'pending' }],
  false
);
assert.ok(!aprOpHtml.includes('批准执行'), 'Non-admin view must not include approve button');

// 5. Ensure no private host/node strings remain hardcoded in index.html
assert.ok(!html.includes('us-hermes'), 'index.html should not hardcode us-hermes');
assert.ok(!html.includes('sg-hermes'), 'index.html should not hardcode sg-hermes');
assert.ok(!html.includes('/root/.secrets'), 'index.html should not hardcode /root/.secrets');

console.log('ALL UI SCRIPT CHECKS PASSED');
