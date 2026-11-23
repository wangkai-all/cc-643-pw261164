"""无浏览器环境下的前端冒烟测试。

用最小 DOM 桩执行 web/app.js，拦截 fetch 返回真实接口数据，验证：
页面能渲染榜单、补取过程、版本表与对照，且渲染过程中不抛异常。
不覆盖样式，只覆盖脚本与数据契约的一致性。
"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

NODE_AVAILABLE = True
try:
    import subprocess

    subprocess.run(["node", "--version"], capture_output=True, check=True, timeout=10)
except Exception:  # pragma: no cover
    NODE_AVAILABLE = False


STUB = r"""
// 极简 DOM/browser 桩：只实现 app.js 用到的 API。
class ClassList {
  constructor(node) { this.node = node; }
  set(value) { this.node._className = String(value || ""); }
  toString() { return this.node._className; }
}
class Node {
  constructor(tag, ns) {
    this.tagName = String(tag).toUpperCase();
    this.children = [];
    this.attributes = {};
    this.listeners = {};
    this.parentNode = null;
    this._text = "";
    this.className = "";
    this.value = "";
    this._className = "";
    this.checked = false;
    this.dataset = {};
    this.hidden = false;
    this.id = "";
  }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() {
    if (this.children.length) return this.children.map((c) => c.textContent).join("");
    return this._text;
  }
  set innerHTML(v) { this._text = String(v); this.children = []; }
  get innerHTML() { return this._text; }
  append(...items) {
    for (const item of items) {
      const node = typeof item === "string" ? new TextNode(item) : item;
      if (!node) continue;
      node.parentNode = this;
      this.children.push(node);
    }
  }
  appendChild(item) { this.append(item); return item; }
  replaceChildren(...items) { this.children = []; this._text = ""; this.append(...items); }
  remove() {
    if (!this.parentNode) return;
    const i = this.parentNode.children.indexOf(this);
    if (i >= 0) this.parentNode.children.splice(i, 1);
  }
  setAttribute(k, v) {
    this.attributes[k] = String(v);
    if (k === "id") this.id = String(v);
    if (k === "value") this.value = String(v);
    if (k === "class") this.className = String(v);
    if (k === "checked") this.checked = true;
  }
  getAttribute(k) { return this.attributes[k] ?? null; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  dispatch(type, event) { for (const fn of this.listeners[type] || []) fn(event || {}); }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  focus() {}
  reset() { this._reset = true; }
}
class TextNode {
  constructor(text) { this.text = String(text); this.parentNode = null; }
  get textContent() { return this.text; }
  get children() { return []; }
}

const REGISTRY = {};
function ensure(id) {
  if (!REGISTRY[id]) { const n = new Node("div"); n.id = id; REGISTRY[id] = n; }
  return REGISTRY[id];
}
const document = {
  createElement: (tag) => new Node(tag),
  createTextNode: (t) => new TextNode(t),
  body: new Node("body"),
  querySelector(sel) {
    if (sel === ".toast") return null;
    const m = sel.match(/^#([\w-]+)/);
    return m ? ensure(m[1]) : null;
  },
  querySelectorAll(sel) {
    const boxes = REGISTRY["stores"];
    if (!boxes) return [];
    return boxes.children.filter(
      (n) => n.tagName === "LABEL" && n.children.some((c) => c.checked)
    ).map((n) => n.children.find((c) => c.tagName === "INPUT")).filter(Boolean);
  },
};
// 预先建好表单节点，模拟 index.html 的结构。
for (const id of ["status","mode","n","stores","refresh","publish","board","compare","shard-select",
  "entry-form","delete-form","delete-result","seed","import","shards","batches","explain",
  "explain-legend","before","after","compare-btn","versions","version-detail","subtitle",
  "entry-hint","entry-title","board-title","data-title","explain-title","versions-title"]) {
  ensure(id);
}
ensure("mode").value = "strict";
ensure("n").value = "5";

const BASE = process.env.TEST_BASE;
async function fetchImpl(path, opts) {
  const url = path.startsWith("http") ? path : BASE + path;
  const res = await globalThis.__realFetch(url, opts);
  const body = await res.text();
  return {
    ok: res.ok,
    status: res.status,
    async json() { return JSON.parse(body); },
  };
}
globalThis.localStorage = { getItem: () => null, setItem: () => {} };
"""

# 说明：桩里的 setTimeout 必须用真实计时器。若像早期版本那样把延时压到
# 数毫秒，collect() 会在首个 fetch 返回前就执行，看到的永远是空页面。


@unittest.skipUnless(NODE_AVAILABLE, "需要 node 运行前端冒烟测试")
class FrontendSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from rankapp.server import Api, Handler
        from rankapp.store import Store
        from rankapp.seed import write_sample_files, sample_path
        from rankapp.ingest import parse_file

        cls.tmp = tempfile.TemporaryDirectory()
        cls.store = Store(Path(cls.tmp.name) / "ui.sqlite3")
        write_sample_files()
        for sid in ("S1", "S2", "S3"):
            parsed = parse_file(sample_path(sid))
            cls.store.ingest_batch(
                shard_id=parsed.shard_id,
                batch_id=parsed.batch_id,
                file_sha256=parsed.file_sha256,
                records=parsed.records,
                file_name=parsed.file_name,
            )
        cls.store.publish(mode="strict", limit_n=5, stores=["A", "B"], note="v1")
        cls.store.add_entry(
            shard_id="S1", store="A", sku="G-103", qty=1, amount="30", kind="sale",
            record_id="ui-add",
        )
        cls.store.publish(mode="strict", limit_n=5, stores=["A", "B"], note="v2")
        cls.store.publish(mode="keep_ties", limit_n=5, stores=["A", "B"], note="v3")
        handler = type("H", (Handler,), {"api": Api(cls.store)})
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.store.close()
        cls.tmp.cleanup()

    def test_app_js_renders_against_live_api(self):
        harness = r"""
process.env.TEST_BASE = "http://127.0.0.1:%d";
const realFetch = globalThis.fetch;
globalThis.__realFetch = realFetch;
%s
const fs = require("fs");
const vm = require("vm");
const ctx = { document, window: { addEventListener() {} }, console, fetch: fetchImpl,
  URLSearchParams, FormData: class { constructor(){ this._d = new Map(); } entries(){ return this._d.entries(); } },
  Node, TextNode, setTimeout: globalThis.setTimeout, clearTimeout() {}, Math, JSON, Number, Object, String, Error };
ctx.globalThis = ctx;
vm.createContext(ctx);
const code = fs.readFileSync(%r, "utf8");
const errors = [];
process.on("unhandledRejection", (e) => errors.push(String(e && e.stack || e)));
vm.runInContext(code, ctx, { filename: "app.js" });
// 轮询等待首次渲染完成（状态栏出现“已按”），避免用固定延时误判。
const deadline = Date.now() + 20000;
(async () => {
  while (Date.now() < deadline) {
    if (document.querySelector("#status").textContent.includes("已按")) break;
    await new Promise((r) => setTimeout(r, 25));
  }
  await new Promise((r) => setTimeout(r, 150)); // 让最后一个响应收尾
  const board = document.querySelector("#board");
  const explain = document.querySelector("#explain");
  const versions = document.querySelector("#versions");
  const out = {
    boardText: collect(board),
    explainText: collect(explain),
    versionsText: collect(versions),
    status: document.querySelector("#status").textContent,
    errors,
  };
  out.hasBoard = out.boardText.includes("门店 A");
  out.hasFetched = out.explainText.includes("补取");
  out.hasRounds = out.explainText.includes("第 1 轮");
  out.hasCrossShard = out.explainText.includes("G-001") && out.boardText.includes("G-001");
  out.hasVersions = out.versionsText.includes("v1") || out.versionsText.includes("v");
  out.hasCompare = collect(document.querySelector("#compare")).includes("入榜");
  console.log("RESULT " + JSON.stringify(out));
  process.exit(0);
})();
function collect(node) {
  if (!node) return "";
  let text = node.textContent || "";
  for (const c of node.children) text += " " + collect(c);
  return text.replace(/\s+/g, " ").trim();
}
""" % (self.port, STUB, str(ROOT / "web" / "app.js"))
        result = subprocess.run(
            ["node", "-e", harness], capture_output=True, text=True, timeout=60, cwd=str(ROOT)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        line = [l for l in result.stdout.splitlines() if l.startswith("RESULT ")]
        self.assertTrue(line, result.stdout + result.stderr)
        payload = json.loads(line[-1][len("RESULT "):])
        self.assertEqual(payload["errors"], [])
        self.assertTrue(payload["hasBoard"], payload["boardText"][:400])
        self.assertTrue(payload["hasFetched"], payload["explainText"][:600])
        self.assertTrue(payload["hasRounds"], payload["explainText"][:600])
        # G-001 在各分片都排不进局部前 5，只能靠跨片补取上榜；页面必须同时
        # 在榜单与补取说明里出现它。
        self.assertTrue(payload["hasCrossShard"], payload["explainText"][:800])
        self.assertTrue(payload["hasVersions"], payload["versionsText"][:400])
        self.assertTrue(payload["hasCompare"], "对照面板未渲染入榜/出榜")
        self.assertIn("已按", payload["status"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
