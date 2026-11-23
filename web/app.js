"use strict";

// ---------------------------------------------------------------------------
// 小工具：所有文本都走 textContent，避免把明细里的内容当 HTML 解释。
// ---------------------------------------------------------------------------
const $ = (sel, root = document) => root.querySelector(sel);

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else if (key === "html") node.innerHTML = value;
    else if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else if (key === "dataset") {
      for (const [k, v] of Object.entries(value)) node.dataset[k] = v;
    } else node.setAttribute(key, value === true ? "" : String(value));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

const fmtMoney = (value) => {
  if (value === null || value === undefined || value === "") return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  return n.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

let toastTimer = null;
function toast(message, bad = false) {
  let node = $(".toast");
  if (!node) {
    node = el("div", { class: "toast", role: "status" });
    document.body.append(node);
  }
  node.textContent = message;
  node.className = "toast show" + (bad ? " bad" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { node.className = "toast"; }, bad ? 5200 : 3200);
}

async function api(path, options) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    ...options,
  });
  let payload = null;
  try { payload = await res.json(); } catch (_) { payload = null; }
  if (!res.ok) {
    const message = (payload && payload.error) || `请求失败（HTTP ${res.status}）`;
    const err = new Error(message);
    err.payload = payload;
    throw err;
  }
  return payload;
}

function setStatus(text, kind = "") {
  const node = $("#status");
  node.textContent = text;
  node.className = "status" + (kind ? " " + kind : "");
}

// ---------------------------------------------------------------------------
// 全局状态
// ---------------------------------------------------------------------------
const app = {
  state: null,
  current: null,     // 当前即时榜单
  lastPublished: null,
  compare: null,
  selectedStores: new Set(),
};

function selectedStoresFromDom() {
  return [...document.querySelectorAll("#stores input[type=checkbox]:checked")].map((c) => c.value);
}

function queryString() {
  const params = new URLSearchParams();
  params.set("mode", $("#mode").value);
  params.set("n", $("#n").value || "5");
  for (const s of selectedStoresFromDom()) params.append("stores", s);
  return params.toString();
}

// ---------------------------------------------------------------------------
// 数据加载与渲染
// ---------------------------------------------------------------------------
async function loadState() {
  const state = await api("/api/state");
  app.state = state;
  renderStoreChoices(state);
  renderShards(state);
  renderSelects(state);
  renderBatches(state);
  setStatus(
    `明细 ${state.summary.records} 条 · 批次 ${state.summary.batches} 个 · 版本 ${state.summary.versions} 个` +
      (state.summary.latest_version ? ` · 最新 v${state.summary.latest_version}` : ""),
    state.summary.latest_version ? "ok" : ""
  );
  return state;
}

function renderStoreChoices(state) {
  const box = $("#stores");
  const previous = app.selectedStores.size
    ? app.selectedStores
    : new Set(state.stores.length ? state.stores : []);
  box.querySelectorAll("label").forEach((n) => n.remove());
  if (!state.stores.length) {
    box.append(el("span", { class: "small muted", text: "暂无门店" }));
    return;
  }
  for (const store of state.stores) {
    const input = el("input", { type: "checkbox", value: store });
    input.checked = previous.has(store);
    input.addEventListener("change", () => {
      app.selectedStores = new Set(selectedStoresFromDom());
    });
    box.append(el("label", {}, input, store));
  }
  app.selectedStores = new Set(selectedStoresFromDom());
}

function renderSelects(state) {
  const shardSelect = $("#shard-select");
  const keep = shardSelect.value;
  shardSelect.replaceChildren(
    ...state.shards.map((s) =>
      el("option", {
        value: s.shard_id,
        text: `${s.shard_id} · ${s.label}${s.active ? "" : "（暂停）"}`,
      })
    )
  );
  if (keep) shardSelect.value = keep;

  for (const [id, label] of [["#before", "旧版"], ["#after", "新版"]]) {
    const sel = $(id);
    const current = sel.value;
    sel.replaceChildren(
      ...state.versions.map((v) =>
        el("option", {
          value: String(v.version_no),
          text: `v${v.version_no} · ${v.mode === "strict" ? "严格" : "同额"} · N=${v.limit_n} · ${v.created_at.replace("T", " ").replace("+00:00", " UTC")}`,
        })
      )
    );
    if (current) sel.value = current;
  }
  const versions = state.versions;
  if (!$("#after").value && versions.length) $("#after").value = String(versions[0].version_no);
  if (!$("#before").value && versions.length > 1) $("#before").value = String(versions[1].version_no);
}

function renderShards(state) {
  const box = $("#shards");
  box.replaceChildren();
  for (const shard of state.shards) {
    const toggle = el("button", {
      class: "btn",
      type: "button",
      text: shard.active ? "暂时退出" : "恢复上线",
      onclick: async () => {
        try {
          await api("/api/shard", {
            method: "POST",
            body: JSON.stringify({ shard_id: shard.shard_id, active: !shard.active }),
          });
          toast(shard.active ? `${shard.shard_id} 已暂时退出，发布将被拒绝` : `${shard.shard_id} 已恢复上线`);
          await refreshAll();
        } catch (err) {
          toast(err.message, true);
        }
      },
    });
    box.append(
      el(
        "div",
        { class: "shard-row" },
        el(
          "div",
          { class: "meta" },
          el("b", { text: `${shard.shard_id} · ${shard.label}` }),
          el("span", {
            text: `${shard.records} 条明细 · ${shard.batches} 个批次${
              shard.last_batch ? " · 最近 " + String(shard.last_batch).replace("T", " ").replace("+00:00", " UTC") : ""
            }`,
          })
        ),
        el(
          "div",
          { class: "controls" },
          el("span", {
            class: "shard-state " + (shard.active ? "on" : "off"),
            text: shard.active ? "在线" : "暂时退出",
          }),
          toggle
        )
      )
    );
  }
}

function renderBatches(state) {
  const box = $("#batches");
  if (!state.batches.length) {
    box.replaceChildren(el("div", { class: "empty", text: "尚未导入批次。可使用右侧按钮载入演示明细或导入文件。" }));
    return;
  }
  const rows = state.batches.map((b) =>
    el(
      "tr",
      {},
      el("td", { text: b.batch_id }),
      el("td", { text: b.shard_id }),
      el("td", { text: b.file_name || "—" }),
      el("td", { class: "num", text: String(b.record_count) }),
      el("td", { class: "num", text: String(b.inserted_count) }),
      el(
        "td",
        {},
        el("span", {
          class: "tag " + (b.status === "accepted" ? "fetched" : b.status === "duplicate" ? "" : "tie"),
          text: { accepted: "已入库", duplicate: "重复（未累加）", conflict: "冲突（拒绝）" }[b.status] || b.status,
        })
      ),
      el("td", { class: "small muted", text: b.message }),
      el("td", { class: "small muted", text: String(b.received_at).replace("T", " ").replace("+00:00", "Z") })
    )
  );
  const table = el(
    "table",
    {},
    el(
      "thead",
      {},
      el(
        "tr",
        {},
        el("th", { text: "批次号" }),
        el("th", { text: "分片" }),
        el("th", { text: "文件" }),
        el("th", { class: "num", text: "声明条数" }),
        el("th", { class: "num", text: "实际写入" }),
        el("th", { text: "状态" }),
        el("th", { text: "说明" }),
        el("th", { text: "接收时间" })
      )
    ),
    el("tbody", {}, ...rows)
  );
  box.replaceChildren(table);
}

// ---------------------------------------------------------------------------
// 榜单
// ---------------------------------------------------------------------------
async function computeRank() {
  const stores = selectedStoresFromDom();
  if (!stores.length) {
    toast("请至少选择一个门店", true);
    return;
  }
  setStatus("正在计算榜单…");
  try {
    const data = await api("/api/rank?" + queryString());
    app.current = data;
    renderBoard(data);
    renderExplain(data);
    const missing = data.missing_shards || [];
    if (missing.length) {
      setStatus("分片未就绪：" + missing.map((m) => `${m.shard_id}（${m.reason}）`).join("；"), "error");
    } else {
      setStatus(
        `已按 ${data.mode === "strict" ? "严格取 N" : "保留末位同额"} 计算 N=${data.n} · 候选 ${data.result.totals.candidates} 件 / 补取桶 ${data.result.totals.fetched_buckets} 个`,
        "ok"
      );
    }
  } catch (err) {
    setStatus(err.message, "error");
    toast(err.message, true);
  }
}

function renderBoard(data) {
  const box = $("#board");
  box.replaceChildren();
  if (data.missing_shards && data.missing_shards.length) {
    box.append(
      el(
        "div",
        { class: "verdict warn" },
        el("span", { text: "⚠" }),
        el("span", {
          text:
            "以下分片未参与： " +
            data.missing_shards.map((m) => `${m.shard_id}（${m.reason}）`).join("；") +
            "。当前结果不能发布。",
        })
      )
    );
  }
  if (data.uninitialized_shards && data.uninitialized_shards.length) {
    box.append(
      el(
        "div",
        { class: "verdict" },
        el("span", { text: "ℹ" }),
        el("span", {
          text:
            "以下分片从未导入明细，视为未启用、不参与计算： " +
            data.uninitialized_shards.map((m) => m.shard_id).join("、"),
        })
      )
    );
  }
  const verification = data.result.verification;
  const verdictText = verification.ok
    ? "补取结果与全量分组排序逐项一致（每个门店的前 N 都已对照）。"
    : "补取结果与全量分组排序不一致，已拒绝发布：" + verification.mismatches.join("；");
  box.append(
    el(
      "div",
      { class: "verdict" + (verification.ok ? "" : " bad") },
      el("span", { text: verification.ok ? "✓" : "✕" }),
      el("span", { text: verdictText })
    )
  );

  const stores = Object.keys(data.result.entries_by_store);
  for (const store of stores) {
    const entries = data.result.entries_by_store[store] || [];
    const report = data.result.reports[store] || {};
    const fetched = entries.filter((e) => (report.fetched_skus || []).some((f) => f.sku === e.sku));
    const card = el("div", { class: "store-card" });
    card.append(
      el(
        "header",
        {},
        el("strong", { text: `门店 ${store}` }),
        el("span", {
          text: `阈值 T=${fmtMoney(report.nth_net)} · 未发现上界 U=${fmtMoney(report.upper_bound)}` +
            ` · 局部候选 ${report.local_candidate_count} → 候选 ${report.candidate_count}` +
            (fetched.length ? ` · 其中补取 ${fetched.length} 件` : ""),
        })
      )
    );
    if (!entries.length) {
      card.append(el("div", { class: "empty", text: "该门店暂无正净额商品。" }));
    } else {
      card.append(
        el(
          "table",
          {},
          el(
            "thead",
            {},
            el(
              "tr",
              {},
              el("th", { text: "名次" }),
              el("th", { text: "商品" }),
              el("th", { class: "num", text: "净销售额" }),
              el("th", { class: "num", text: "明细" }),
              el("th", { text: "说明" })
            )
          ),
          el(
            "tbody",
            {},
            ...entries.map((e) =>
              el(
                "tr",
                { class: e.tie ? "tie" : "" },
                el("td", {}, el("span", { class: "rank-badge", text: String(e.rank) })),
                el("td", { text: e.sku }),
                el("td", { class: "num", text: fmtMoney(e.net) }),
                el("td", { class: "num", text: String(e.count) }),
                el(
                  "td",
                  {},
                  e.tie ? el("span", { class: "tag tie", text: "末位同额" }) : null,
                  fetched.some((f) => f.sku === e.sku)
                    ? el("span", { class: "tag fetched", text: "补取" })
                    : null,
                  fetched.some((f) => f.sku === e.sku)
                    ? el("span", {
                        class: "small muted",
                        text:
                          " " +
                          Object.entries(
                            (report.fetched_skus.find((f) => f.sku === e.sku) || {}).per_shard || {}
                          )
                            .map(([sid, v]) => `${sid} ${fmtMoney(v)}`)
                            .join(" + "),
                      })
                    : null,
                  data.mode === "keep_ties" && e.entry_kind === "tie_tier" && !e.tie
                    ? el("span", { class: "tag", text: "同额档" })
                    : null
                )
              )
            )
          )
        )
      );
    }
    box.append(card);
  }
  if (data.mode === "keep_ties") {
    box.append(
      el("p", {
        class: "hint",
        text: "保留末位同额：取净额不低于第 N 名的全部商品，因此条数可能多于 N；净额 ≤ 0 的商品不上榜。",
      })
    );
  } else {
    box.append(
      el("p", {
        class: "hint",
        text: "严格取 N：按净额降序取前 N，遇到同额按商品编号升序；与末位同额但编号更大的商品会落榜并标出。",
      })
    );
  }
}

// ---------------------------------------------------------------------------
// 补取过程
// ---------------------------------------------------------------------------
function renderExplain(data) {
  const box = $("#explain");
  box.replaceChildren();
  const reports = data.result.reports || {};
  for (const [store, rep] of Object.entries(reports)) {
    const roundsBox = el("div", {});
    for (const round of rep.rounds) {
      const cutoffs = Object.entries(round.cutoffs || {})
        .map(([sid, value]) => `${sid}=${fmtMoney(value)}`)
        .join("，");
      const bound = Number(round.upper_bound);
      const nth = round.nth_net === null ? NaN : Number(round.nth_net);
      const comparison = round.stop ? (Number.isFinite(nth) && nth > bound ? "T > U，停止" : "已穷尽，停止") : "T ≤ U，继续";
      roundsBox.append(
        el(
          "div",
          { class: "round" + (round.stop ? " stop" : "") },
          el(
            "div",
            { class: "round-head" },
            el("b", { text: `第 ${round.round_no} 轮` }),
            el("span", { class: "kpi", text: `阈值 t=${fmtMoney(round.threshold_used)}` }),
            el("span", { class: "kpi", text: `局部第 N 大值：${cutoffs || "无分片"}` }),
            el("span", { class: "kpi", text: `补取 +${round.new_buckets} 桶 / +${round.new_records} 条` }),
            el("span", { class: "kpi", text: `新候选 ${round.new_candidates}` }),
            el("span", { class: "tag " + (round.stop ? "strict" : "tie"), text: round.stop ? "停止" : "继续" })
          ),
          el(
            "div",
            { class: "equation" },
            el("code", { text: `T=${fmtMoney(round.nth_net)}` }),
            el("span", { text: "vs" }),
            el("code", { text: `U(t)=${fmtMoney(round.upper_bound)}` }),
            el("span", { class: "cmp" + (comparison.startsWith("T ≤") ? " bad" : ""), text: comparison }),
            el("span", { text: `· 候选 ${round.candidate_count} 件 · 入榜 ${round.entries_count} 条` })
          ),
          el("p", { class: "reason" + (round.stop ? " stop" : ""), text: round.reason })
        )
      );
    }

    const localTopRows = (rep.local_top || []).map((c) =>
      el(
        "tr",
        {},
        el("td", { text: c.sku }),
        el("td", { class: "num", text: fmtMoney(c.local_sum) }),
        el(
          "td",
          { class: "small muted" },
          Object.entries(c.per_shard || {})
            .map(([sid, value]) => `${sid} ${fmtMoney(value)}`)
            .join(" · ")
        )
      )
    );
    const fetchedRows = (rep.fetched_skus || []).map((f) =>
      el(
        "tr",
        {},
        el("td", { text: f.sku }),
        el("td", { class: "num", text: fmtMoney(f.net) }),
        el("td", { class: "num", text: String(f.count) }),
        el("td", { class: "small muted", text: f.reason })
      )
    );

    box.append(
      el(
        "div",
        { class: "store-explain" },
        el(
          "header",
          {},
          el("b", { text: `门店 ${store}` }),
          el("span", { class: "kv", text: `局部候选 ${rep.local_candidate_count} 件` }),
          el("span", { class: "kv", text: `补取后候选 ${rep.candidate_count} 件` }),
          el("span", { class: "kv", text: `补取桶 ${rep.fetched_bucket_count} 个 / 明细 ${rep.fetched_record_count} 条` }),
          el("span", { class: "kv", text: `上界 U=${fmtMoney(rep.upper_bound)}` }),
          el("span", { class: "kv", text: `入榜最低净额 ${fmtMoney(rep.nth_net)}` })
        ),
        el(
          "div",
          { class: "body" },
          el(
            "div",
            {},
            el("div", { class: "subhead", text: "停止条件（逐轮）" }),
            roundsBox
          ),
          el(
            "div",
            { class: "grid-2" },
            el(
              "div",
              {},
              el("div", { class: "subhead", text: `分片主动汇报的局部前 N 名（各片局部分数之和，前 ${localTopRows.length} 条）` }),
              localTopRows.length
                ? el(
                    "table",
                    {},
                    el(
                      "thead",
                      {},
                      el(
                        "tr",
                        {},
                        el("th", { text: "商品" }),
                        el("th", { class: "num", text: "已汇报局部分数之和" }),
                        el("th", { text: "各分片局部分数" })
                      )
                    ),
                    el("tbody", {}, ...localTopRows)
                  )
                : el("div", { class: "empty", text: "无局部候选" })
            ),
            el(
              "div",
              {},
              el("div", { class: "subhead", text: "阈值补取到的跨片候选" }),
              fetchedRows.length
                ? el(
                    "table",
                    {},
                    el(
                      "thead",
                      {},
                      el(
                        "tr",
                        {},
                        el("th", { text: "商品" }),
                        el("th", { class: "num", text: "全局净额" }),
                        el("th", { class: "num", text: "明细" }),
                        el("th", { text: "原因" })
                      )
                    ),
                    el("tbody", {}, ...fetchedRows)
                  )
                : el("div", { class: "empty", text: "本轮无需额外补取" })
            )
          ),
          rep.notes && rep.notes.length
            ? el(
                "div",
                { class: "equation" },
                ...rep.notes.map((n) => el("span", { text: "· " + n }))
              )
            : null
        )
      )
    );
  }
  if (!box.children.length) {
    box.append(el("div", { class: "empty", text: "计算榜单后在此展开每个门店的补取轮次。" }));
  }
}

// ---------------------------------------------------------------------------
// 对照
// ---------------------------------------------------------------------------
async function runCompare() {
  const before = $("#before").value;
  const after = $("#after").value;
  if (!after) {
    toast("暂无可对照的版本", true);
    return;
  }
  const params = new URLSearchParams();
  if (before) params.set("before", before);
  params.set("after", after);
  try {
    const data = await api("/api/compare?" + params.toString());
    app.compare = data;
    renderCompare(data);
  } catch (err) {
    toast(err.message, true);
  }
}

function renderCompare(data) {
  const box = $("#compare");
  box.replaceChildren();
  const head = el(
    "div",
    { class: "panel-head sub" },
    el("h3", { text: `对照：v${data.before.version_no ?? "—"} → v${data.after.version_no}` }),
    el("span", {
      class: "small muted",
      text: `入榜 ${data.entered.length} · 出榜 ${data.exited.length} · 名次/净额变化 ${data.moved.length} · 未变 ${data.unchanged}`,
    })
  );
  const section = (title, items, render) =>
    el(
      "div",
      {},
      el("div", { class: "subhead", text: title }),
      items.length ? el("div", { class: "kv-list" }, ...items.map(render)) : el("div", { class: "empty", text: "无" })
    );
  box.append(
    head,
    el(
      "div",
      { class: "grid-2" },
      section("入榜", data.entered, (e) =>
        el("span", { class: "chip enter", text: `${e.store}/${e.sku} · #${e.rank} · ${fmtMoney(e.net)}` })
      ),
      section("出榜", data.exited, (e) =>
        el("span", { class: "chip exit", text: `${e.store}/${e.sku} · 原 #${e.rank} · ${fmtMoney(e.net)}` })
      )
    ),
    section("名次 / 净额变化", data.moved, (m) => {
      const delta = m.delta_rank;
      const cls = delta > 0 ? "delta-up" : delta < 0 ? "delta-down" : "";
      const arrow = delta > 0 ? `↑${delta}` : delta < 0 ? `↓${-delta}` : "持平";
      return el(
        "span",
        { class: "chip" },
        `${m.store}/${m.sku} · #${m.before_rank}→#${m.after_rank} · `,
        el("span", { class: cls, text: arrow }),
        ` · 净额 ${fmtMoney(m.before_net)}→${fmtMoney(m.after_net)}`
      );
    })
  );
}

// ---------------------------------------------------------------------------
// 版本
// ---------------------------------------------------------------------------
function renderVersions(state) {
  const box = $("#versions");
  if (!state.versions.length) {
    box.replaceChildren(el("div", { class: "empty", text: "尚无已发布版本。计算榜单后点“发布为新版本”。" }));
    return;
  }
  const rows = state.versions.map((v) =>
    el(
      "tr",
      {},
      el("td", {}, el("button", { class: "btn", type: "button", text: `v${v.version_no}`, onclick: () => showVersion(v.version_no) })),
      el("td", { text: v.mode === "strict" ? "严格取 N" : "保留末位同额" }),
      el("td", { class: "num", text: String(v.limit_n) }),
      el("td", { text: v.stores.join("、") }),
      el("td", { class: "num", text: String(v.record_count) }),
      el("td", { text: v.shard_ids.join("、") }),
      el(
        "td",
        {},
        el("span", { class: "tag " + (v.verification_ok ? "fetched" : "tie"), text: v.verification_ok ? "已对照" : "未对照" })
      ),
      el("td", { class: "small muted", text: String(v.created_at).replace("T", " ").replace("+00:00", "Z") }),
      el("td", { class: "small muted", text: v.note || "—" })
    )
  );
  box.replaceChildren(
    el(
      "table",
      {},
      el(
        "thead",
        {},
        el(
          "tr",
          {},
          el("th", { text: "版本" }),
          el("th", { text: "规则" }),
          el("th", { class: "num", text: "N" }),
          el("th", { text: "门店" }),
          el("th", { class: "num", text: "输入明细" }),
          el("th", { text: "分片" }),
          el("th", { text: "对照" }),
          el("th", { text: "发布时间" }),
          el("th", { text: "备注" })
        )
      ),
      el("tbody", {}, ...rows)
    )
  );
}

async function showVersion(no) {
  try {
    const v = await api(`/api/version/${no}`);
    const detail = $("#version-detail");
    detail.replaceChildren();
    const basis = v.basis || {};
    const range = basis.input_range || {};
    const totals = basis.totals || {};
    detail.append(
      el(
        "div",
        { class: "basis" },
        el(
          "dl",
          {},
          el("dt", { text: "版本" }),
          el("dd", { text: `v${v.version_no} · ${v.mode} · N=${v.limit_n} · ${v.stores.join("、")}` }),
          el("dt", { text: "记录摘要" }),
          el("dd", { text: v.record_digest }),
          el("dt", { text: "输入分片" }),
          el("dd", { text: (range.shard_ids || []).join("、") }),
          el("dt", { text: "输入批次" }),
          el("dd", {
            text: Object.entries(range.batch_ids || {}).map(([sid, list]) => `${sid}: ${(list || []).join(", ") || "（网页录入）"}`).join("；"),
          }),
          el("dt", { text: "明细条数 / 候选" }),
          el("dd", {
            text: `${Object.values(range.record_ids || {}).reduce((a, b) => a + (b || []).length, 0)} 条 · 候选 ${totals.candidates ?? "—"} 件 · 补取轮次 ${totals.rounds ?? "—"}`,
          }),
          el("dt", { text: "输入范围（按分片）" }),
          el("dd", {
            text: Object.entries(range.record_ids || {}).map(([sid, ids]) => `${sid}: ${(ids || []).length} 条`).join("；"),
          })
        )
      )
    );
    const board = el("div", { class: "board" });
    for (const [store, entries] of Object.entries(v.result.entries_by_store || {})) {
      board.append(
        el(
          "div",
          { class: "store-card" },
          el("header", {}, el("strong", { text: `门店 ${store}` }), el("span", { text: `${entries.length} 条` })),
          el(
            "table",
            {},
            el("thead", {}, el("tr", {}, el("th", { text: "名次" }), el("th", { text: "商品" }), el("th", { class: "num", text: "净额" }))),
            el(
              "tbody",
              {},
              ...entries.map((e) =>
                el(
                  "tr",
                  {},
                  el("td", {}, el("span", { class: "rank-badge", text: String(e.rank) })),
                  el("td", { text: e.sku }),
                  el("td", { class: "num", text: fmtMoney(e.net) })
                )
              )
            )
          )
        )
      );
    }
    detail.append(board);
  } catch (err) {
    toast(err.message, true);
  }
}

// ---------------------------------------------------------------------------
// 交互
// ---------------------------------------------------------------------------
async function refreshAll() {
  await loadState();
  renderVersions(app.state);
  await computeRank();
  const versions = app.state.versions;
  if (versions.length >= 2) {
    await runCompare();
  } else {
    $("#compare").replaceChildren();
  }
}

function formData(form) {
  const out = {};
  for (const [k, value] of new FormData(form).entries()) out[k] = value;
  return out;
}

function attachHandlers() {
  $("#refresh").addEventListener("click", computeRank);
  $("#mode").addEventListener("change", computeRank);
  $("#n").addEventListener("change", computeRank);

  $("#entry-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = formData(event.target);
    data.qty = Number(data.qty);
    try {
      const result = await api("/api/entries", { method: "POST", body: JSON.stringify(data) });
      toast(`已写入 ${result.kind} · ${result.store}/${result.sku} · ${result.amount}`);
      event.target.querySelector('[name="note"]').value = "";
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $("#delete-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = formData(event.target);
    try {
      const result = await api("/api/delete", { method: "POST", body: JSON.stringify(data) });
      const pre = $("#delete-result");
      pre.hidden = false;
      pre.textContent =
        `${result.message}\n剩余明细 ${result.remaining_records} 条，剩余净额 ${fmtMoney(result.remaining_net)}\n` +
        `状态：${result.state === "absent" ? "商品已不存在（无任何明细）" : result.state === "zero" ? "净额为零（仍有明细）" : "仍有净额"}`;
      event.target.reset();
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $("#seed").addEventListener("click", async () => {
    try {
      const result = await api("/api/seed", { method: "POST", body: JSON.stringify({}) });
      toast(`已载入演示明细并发布 v${result.version_no}`);
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $("#import").addEventListener("click", async () => {
    if (!app.state) return;
    const files = (app.state.samples || []).filter((n) => /\.(json|jsonl|ndjson|csv)$/i.test(n));
    if (!files.length) {
      toast("sample_shards 下没有可导入文件", true);
      return;
    }
    try {
      const result = await api("/api/batches", {
        method: "POST",
        body: JSON.stringify({ files, operator: "web" }),
      });
      const duplicates = result.results.filter((r) => r.status === "duplicate").length;
      toast(
        `导入完成：${result.results.length} 个文件，重复跳过 ${duplicates} 个（不会重复累加）`,
        false
      );
      await refreshAll();
    } catch (err) {
      toast(err.message, true);
    }
  });

  $("#publish").addEventListener("click", async () => {
    const stores = selectedStoresFromDom();
    if (!stores.length) {
      toast("请至少选择一个门店", true);
      return;
    }
    try {
      const result = await api("/api/publish", {
        method: "POST",
        body: JSON.stringify({
          mode: $("#mode").value,
          n: Number($("#n").value || 5),
          stores,
          note: "网页发布",
        }),
      });
      app.lastPublished = result;
      toast(`已发布 v${result.version_no}`);
      await refreshAll();
      await showVersion(result.version_no);
    } catch (err) {
      if (err.payload && err.payload.missing_shards) {
        toast(
          "拒绝发布：" +
            err.payload.missing_shards.map((m) => `${m.shard_id}（${m.reason}）`).join("；"),
          true
        );
      } else {
        toast(err.message, true);
      }
    }
  });

  $("#compare-btn").addEventListener("click", runCompare);
}

async function main() {
  attachHandlers();
  try {
    await refreshAll();
  } catch (err) {
    setStatus("加载失败：" + err.message, "error");
  }
}

main();
