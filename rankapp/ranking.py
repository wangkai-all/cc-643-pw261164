"""榜单核心：净额聚合、严格/同额两种取 N 规则、跨片候选补取与停止条件。

数据模型
========

* 明细按分片存放，分片之间互不相交：``record_id`` 全库唯一，一条明细只属于
  一个分片。因此同一 ``(门店, 商品)`` 在各分片的局部净额可以相加，得到全局
  净额的一部分。
* 分片只回答局部问题：某门店的局部桶列表（商品、局部净额、条数）。它不保存
  也不返回任何榜单结果；榜单由应用一次次局部查询拼出，再与全量分组排序对照。

跨片累计为什么会漏，以及如何保证全局正确
==========================================

只取每片局部前 N 名会漏掉“每片都不突出、跨片相加才上榜”的商品。设门店在某
分片的局部桶降序排列，``z_i`` 为该分片的**局部第 N 大值**（桶数不足 N 取最小
桶值，无桶取 0）。没有出现在该片前 N 的 SKU 满足 ``局部净额 <= z_i``。

补取轮次里，应用向每个分片索取所有 ``净额 >= t`` 的局部桶，并对新出现的 SKU
按键补齐它在**全部**分片的桶（得到精确全局净额）。于是任何仍未被发现的 SKU：
每片都不在前 N（``<= z_i``），且每片都 ``< t``；金额精确到分，所以每片
``<= min(z_i, t - 0.01)``。由此

    未发现 SKU 的全局净额 <= U(t) := Σ_i max(0, min(z_i, t - 0.01))

候选集都有精确全局净额，记第 N 名净额为 ``T``（不足 N 件时取最小候选净额）。
当 ``T > U(t)`` 时，未发现商品的净额严格小于 T：既进不了前 N，也不可能与
末位同额（保留末位同额需要严格不等式）。这就是**停止补取的条件**。

否则把 t 下调到下一个更小的局部桶净额，再补取、再判定；t 严格递减且来自有限
集合，最后一步以 0.01 为阈值取走所有正额桶（此后未发现商品每片均 <= 0，
U = 0）。所以算法必然终止。停止时结果与全量分组排序逐项一致，页面逐步展开
每轮的 z_i、t、U(t)、T、补取数量与停止理由。

取 N 规则
=========

* ``strict``：按 (净额降序, 商品编号升序) 取前 N。商品编号稳定打破平局；
  与末位同额但编号更大的商品落榜，``tie`` 字段标记该情形。不会多取。
* ``keep_ties``：先求第 N 大值 T，再取所有净额 >= T 的商品，绝不把同一净额的
  并列截断，条数可能多于 N。净额 <= 0 的商品永不上榜（否则并列末位会无限
  扩张，失去“榜”的意义），页面与接口均明确展示这一约束。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .money import money_str, q2

TIE_STRICT = "strict"
TIE_KEEP = "keep_ties"
MODES = (TIE_STRICT, TIE_KEEP)
ZERO = Decimal("0")
CENT = Decimal("0.01")


@dataclass(frozen=True)
class Record:
    record_id: str
    store: str
    sku: str
    qty: int
    amount: Decimal
    kind: str = "sale"
    batch_id: str = ""
    ts: str = ""
    note: str = ""


@dataclass(frozen=True)
class Bucket:
    """分片内的局部聚合桶：某门店某商品的局部净额与明细条数。"""

    store: str
    sku: str
    net: Decimal
    count: int = 0

    @property
    def key(self) -> Tuple[str, str]:
        return (self.store, self.sku)


@dataclass(frozen=True)
class ShardSlice:
    """某一版榜单所用分片的不可变快照，供复现与对照。"""

    shard_id: str
    label: str
    batches: Tuple[str, ...]
    records: Tuple[Record, ...]
    buckets: Tuple[Bucket, ...]
    min_net: Decimal
    max_net: Decimal
    record_count: int

    @property
    def bucket_count(self) -> int:
        return len(self.buckets)

    @classmethod
    def build(
        cls,
        shard_id: str,
        label: str,
        batches: Iterable[str],
        records: Iterable[Record],
    ) -> "ShardSlice":
        recs = tuple(sorted(records, key=lambda r: r.record_id))
        nets: Dict[Tuple[str, str], Decimal] = {}
        counts: Dict[Tuple[str, str], int] = {}
        for r in recs:
            key = (r.store, r.sku)
            nets[key] = nets.get(key, ZERO) + r.amount
            counts[key] = counts.get(key, 0) + 1
        buckets = tuple(
            Bucket(store=k[0], sku=k[1], net=q2(v), count=counts[k])
            for k, v in sorted(nets.items())
        )
        ordered = sorted((b.net for b in buckets), reverse=True)
        return cls(
            shard_id=shard_id,
            label=label,
            batches=tuple(sorted(batches)),
            records=recs,
            buckets=buckets,
            min_net=min(ordered) if ordered else ZERO,
            max_net=max(ordered) if ordered else ZERO,
            record_count=len(recs),
        )

    def store_buckets(self, store: str) -> List[Bucket]:
        return [b for b in self.buckets if b.store == store]

    def cutoff_for(self, store: str, n: int) -> Decimal:
        """局部第 N 大值；不足 N 个桶取最小桶值；没有桶为 0。"""
        nets = sorted((b.net for b in self.store_buckets(store)), reverse=True)
        if not nets:
            return ZERO
        return nets[min(n, len(nets)) - 1]

    def digest(self) -> Dict[str, object]:
        return {
            "shard_id": self.shard_id,
            "label": self.label,
            "batches": list(self.batches),
            "record_count": self.record_count,
            "bucket_count": self.bucket_count,
            "min_net": money_str(self.min_net),
            "max_net": money_str(self.max_net),
            "records_digest": records_digest(self.records),
        }


@dataclass
class RankEntry:
    rank: int
    sku: str
    store: str
    net: Decimal
    count: int
    tier: int
    tie: bool
    entry_kind: str  # strict / tie_tier


@dataclass
class ScoreLine:
    store: str
    sku: str
    net: Decimal
    source: str
    count: int = 0


@dataclass
class FetchRound:
    round_no: int
    threshold_used: Optional[Decimal]
    next_threshold: Optional[Decimal]
    cutoffs: Dict[str, str]
    upper_bound: Decimal
    candidate_count: int
    nth_net: Optional[Decimal]
    entries_count: int
    new_buckets: int
    new_records: int
    new_candidates: int
    stop: bool
    reason: str


@dataclass
class StoreReport:
    store: str
    local_top: List[Dict[str, object]]
    local_candidate_count: int
    fetched_skus: List[Dict[str, object]]
    fetched_bucket_count: int
    fetched_record_count: int
    candidate_count: int
    nth_net: Optional[Decimal]
    upper_bound: Decimal
    threshold: Optional[Decimal]
    entries: List[RankEntry]
    stop_reason: str
    rounds: List[FetchRound]
    notes: List[str] = field(default_factory=list)


@dataclass
class Verification:
    ok: bool
    mode: str
    n: int
    entries_equal: bool
    mismatches: List[str]
    per_store: Dict[str, Dict[str, object]]
    full_sorted: Dict[str, List[Dict[str, object]]]

    def to_dict(self) -> Dict[str, object]:
        return {
            "ok": self.ok,
            "mode": self.mode,
            "n": self.n,
            "entries_equal": self.entries_equal,
            "mismatches": self.mismatches,
            "per_store": self.per_store,
            "full_sorted": self.full_sorted,
        }


@dataclass
class RankResult:
    mode: str
    limit: int
    entries_by_store: Dict[str, List[RankEntry]]
    reports: Dict[str, StoreReport]
    verification: Verification
    totals: Dict[str, object]
    shards: List[Dict[str, object]]


# ---------------------------------------------------------------------------
# 基础运算
# ---------------------------------------------------------------------------

def records_digest(records: Sequence[Record]) -> str:
    import hashlib

    h = hashlib.sha256()
    for r in sorted(records, key=lambda x: x.record_id):
        h.update(
            "|".join(
                [
                    r.record_id,
                    r.store,
                    r.sku,
                    str(r.qty),
                    money_str(r.amount),
                    r.kind,
                    r.batch_id,
                    r.ts,
                ]
            ).encode("utf-8")
        )
        h.update(b"\n")
    return h.hexdigest()


def aggregate(
    records: Iterable[Record], stores: Optional[Iterable[str]] = None
) -> Dict[Tuple[str, str], Tuple[Decimal, int]]:
    allowed = set(stores) if stores is not None else None
    acc: Dict[Tuple[str, str], Tuple[Decimal, int]] = {}
    for r in records:
        if allowed is not None and r.store not in allowed:
            continue
        key = (r.store, r.sku)
        net, cnt = acc.get(key, (ZERO, 0))
        acc[key] = (net + r.amount, cnt + 1)
    return {k: (q2(v), c) for k, (v, c) in sorted(acc.items())}


def _mark_strict_ties(entries: List[RankEntry], ordered: List[ScoreLine], n: int) -> None:
    if not entries or len(entries) < n:
        for e in entries:
            e.tie = False
        return
    cutoff = entries[-1].net
    last_sku = entries[-1].sku
    tie_present = any(s.net == cutoff and s.sku > last_sku for s in ordered)
    for e in entries:
        e.tie = tie_present and e.net == cutoff


def strict_entries(scored: List[ScoreLine], n: int) -> List[RankEntry]:
    """严格取 N：净额降序，商品编号升序稳定打破平局；只取正净额。"""
    ordered = sorted((s for s in scored if s.net > 0), key=lambda s: (-s.net, s.sku))
    out: List[RankEntry] = []
    prev_net: Optional[Decimal] = None
    tier = 0
    for s in ordered:
        if len(out) >= n:
            break
        if prev_net is None or s.net != prev_net:
            tier += 1
            prev_net = s.net
        out.append(
            RankEntry(
                rank=len(out) + 1,
                sku=s.sku,
                store=s.store,
                net=s.net,
                count=s.count,
                tier=tier,
                tie=False,
                entry_kind="strict",
            )
        )
    _mark_strict_ties(out, ordered, n)
    return out


def keep_tie_entries(
    scored: List[ScoreLine], n: int
) -> Tuple[List[RankEntry], Optional[Decimal]]:
    """保留末位同额：取净额 >= 第 N 大值且 > 0 的全部商品。"""
    ordered = sorted((s for s in scored if s.net > 0), key=lambda s: (-s.net, s.sku))
    if not ordered:
        return [], None
    threshold = ordered[min(n, len(ordered)) - 1].net
    entries: List[RankEntry] = []
    prev_net: Optional[Decimal] = None
    tier = 0
    tie_count = sum(1 for s in ordered if s.net == threshold)
    for s in ordered:
        if s.net < threshold:
            break
        if prev_net is None or s.net != prev_net:
            tier += 1
            prev_net = s.net
        entries.append(
            RankEntry(
                rank=len(entries) + 1,
                sku=s.sku,
                store=s.store,
                net=s.net,
                count=s.count,
                tier=tier,
                tie=(s.net == threshold and tie_count > 1),
                entry_kind="tie_tier",
            )
        )
    return entries, threshold


# ---------------------------------------------------------------------------
# 分片补取
# ---------------------------------------------------------------------------

class _ShardView:
    """只能通过局部桶查询观察分片；模拟分片边界，防止偷偷读全量排序。"""

    def __init__(self, slices: Sequence[ShardSlice], store: str):
        self.store_shards = [s for s in slices if s.store_buckets(store)]
        self.by_shard: Dict[str, List[Bucket]] = {
            s.shard_id: sorted(s.store_buckets(store), key=lambda b: (-b.net, b.sku))
            for s in self.store_shards
        }
        self.fetched: Dict[Tuple[str, str], Bucket] = {}
        self.point_queries = 0
        self.range_queries = 0

    def local_top(self, n: int) -> Dict[str, Dict[str, object]]:
        out: Dict[str, Dict[str, object]] = {}
        for sid, rows in self.by_shard.items():
            for b in rows[:n]:
                item = out.setdefault(
                    b.sku,
                    {"sku": b.sku, "local_sum": ZERO, "per_shard": {}, "count": 0},
                )
                item["local_sum"] = q2(Decimal(item["local_sum"]) + b.net)
                item["per_shard"][sid] = money_str(b.net)
                item["count"] = int(item["count"]) + b.count
        return out

    def capture_key(self, sku: str) -> int:
        """按键补取：向所有分片查询该 SKU 的局部桶，得到精确全局净额。"""
        self.point_queries += 1
        added = 0
        for sid, rows in self.by_shard.items():
            key = (sid, sku)
            if key in self.fetched:
                continue
            for b in rows:
                if b.sku == sku:
                    self.fetched[key] = b
                    added += 1
                    break
        return added

    def capture_at_least(self, threshold: Decimal) -> List[str]:
        """向每个分片索取净额 >= threshold 的桶；返回首次拿到桶的 SKU。"""
        self.range_queries += 1
        seen: List[str] = []
        for sid, rows in self.by_shard.items():
            for b in rows:
                if b.net < threshold or b.net <= 0:
                    continue
                key = (sid, b.sku)
                if key in self.fetched:
                    continue
                self.fetched[key] = b
                seen.append(b.sku)
        return sorted(set(seen))

    def exact(self) -> Dict[str, Tuple[Decimal, int]]:
        out: Dict[str, Tuple[Decimal, int]] = {}
        for (_sid, sku), b in self.fetched.items():
            net, cnt = out.get(sku, (ZERO, 0))
            out[sku] = (q2(net + b.net), cnt + b.count)
        return out

    def has_unfetched_positive(self) -> bool:
        for sid, rows in self.by_shard.items():
            for b in rows:
                if b.net > 0 and (sid, b.sku) not in self.fetched:
                    return True
        return False


def _candidate_lines(view: _ShardView, store: str) -> List[ScoreLine]:
    return [
        ScoreLine(store=store, sku=sku, net=net, source="candidate", count=cnt)
        for sku, (net, cnt) in sorted(view.exact().items())
    ]


def _entries_for(
    mode: str, candidates: List[ScoreLine], n: int
) -> Tuple[List[RankEntry], Optional[Decimal]]:
    if mode == TIE_STRICT:
        entries = strict_entries(candidates, n)
        return entries, (entries[-1].net if entries else None)
    return keep_tie_entries(candidates, n)


def _rank_store(
    *,
    mode: str,
    n: int,
    store: str,
    shards: Sequence[ShardSlice],
    sample_limit: int,
) -> Tuple[StoreReport, List[RankEntry]]:
    view = _ShardView(shards, store)
    notes: List[str] = []
    z: Dict[str, Decimal] = {}
    for sh in view.store_shards:
        z[sh.shard_id] = sh.cutoff_for(store, n)

    initial = view.local_top(n)
    for sku, item in initial.items():
        if len(item["per_shard"]) > 1:
            notes.append(
                "%s 在 %d 个分片都进入局部前 %d，单看一片的局部分数会低估它"
                % (sku, len(item["per_shard"]), n)
            )
    initial_skus = set(initial)
    for sku in sorted(initial_skus):
        view.capture_key(sku)

    all_positive = sorted(
        {b.net for rows in view.by_shard.values() for b in rows if b.net > 0},
        reverse=True,
    )

    def upper_bound(t: Optional[Decimal]) -> Decimal:
        """本轮阈值 t 下，未发现 SKU 全局净额的上界。"""
        if t is None:
            cap_t = ZERO
        else:
            cap_t = max(t - CENT, ZERO)
        total = ZERO
        for sid in view.by_shard:
            total += max(min(z[sid], cap_t), ZERO)
        return q2(total)

    def jump_threshold(target: Optional[Decimal]) -> Decimal:
        """选下一轮阈值，使下一轮补取结束后必然可以停止。

        ``U(t)`` 随 t 单调不减，因此在所有正额局部桶净额里二分找**最大**的
        t，使得 ``U(t) < target``。下一轮按该 t 补取后，新增候选只会让 T
        不变或变大，于是 ``T_new >= target > U(t)``，停止条件必然成立。
        找不到这样的 t（连最小金额都不满足），就直接取走全部正额桶。
        """
        if target is None or not all_positive:
            return CENT
        ascending = sorted(set(all_positive))
        lo, hi = 0, len(ascending) - 1
        best: Optional[Decimal] = None
        while lo <= hi:
            mid = (lo + hi) // 2
            if upper_bound(ascending[mid]) < target:
                best = ascending[mid]
                lo = mid + 1
            else:
                hi = mid - 1
        if best is None or best <= 0:
            return CENT
        return best

    threshold: Optional[Decimal] = max(z.values()) if z else None
    if (threshold is None or threshold <= 0) and all_positive:
        # 局部第 N 大值可能因负桶而为非正，但仍可能存在正额桶需要补取。
        threshold = all_positive[0]
    if threshold is not None and threshold <= 0:
        threshold = None

    cutoffs = {sid: money_str(v) for sid, v in z.items()}
    rounds: List[FetchRound] = []
    stop_reason = ""
    entries: List[RankEntry] = []
    nth: Optional[Decimal] = None
    bound = upper_bound(threshold)
    max_rounds = 200
    stopped = False

    for round_no in range(1, max_rounds + 1):
        used = threshold
        before = dict(view.fetched)
        new_candidates = 0
        if threshold is not None and threshold > 0:
            for sku in view.capture_at_least(threshold):
                view.capture_key(sku)
        new_keys = [k for k in view.fetched if k not in before]
        new_buckets = len(new_keys)
        new_records = sum(view.fetched[k].count for k in new_keys)
        for sku in {k[1] for k in new_keys}:
            if sku not in initial_skus:
                new_candidates += 1

        lines = _candidate_lines(view, store)
        entries, nth = _entries_for(mode, lines, n)
        bound = upper_bound(threshold)
        exhausted = not view.has_unfetched_positive()
        positive_candidates = sum(1 for s in lines if s.net > 0)
        # 候选不足 N 件时，“候选里的第 N 名”没有意义：未发现商品可能补足榜单，
        # 必须继续补取，直到全部正额桶都取到或候选达到 N 件。
        enough = positive_candidates >= n
        nxt: Optional[Decimal] = None

        # 候选达到 N 件后，第 N 名 T 只会因补取而变大；若按本轮阈值 t 的
        # 上界都已低于 T，就无需再补，直接停止。
        if enough and nth is not None and nth > bound:
            stop = True
            reason = (
                "T=%s > U(t)=%s：所有未发现商品在每个分片都不超过局部第 N 大值，"
                "且本轮已取走各分片净额 >= %s 的桶，每片都低于该阈值，"
                "累计净额上界为 %s，严格小于 T，不可能进前 %d、也不可能与末位同额；"
                "停止补取。"
                % (
                    money_str(nth),
                    money_str(bound),
                    money_str(used) if used is not None else "无",
                    money_str(bound),
                    n,
                )
            )
        elif exhausted:
            stop = True
            reason = (
                "所有正额局部桶均已补取，U(t)=%s，候选覆盖全部正净额商品；"
                "现有数据只有 %d 件正净额商品，不足或刚好满足 N=%d，按实际给出。"
                % (money_str(bound), len(entries), n)
            )
        elif used is not None and used <= CENT:
            stop = True
            reason = (
                "阈值已到最小正金额 0.01 并取走所有正额桶，U(t)=%s；"
                "没有更细的桶可补，按全部正额商品给出结果。" % money_str(bound)
            )
        else:
            stop = False
            nxt = jump_threshold(nth)
            reason = (
                "T=%s 未超过 U(t)=%s，可能有跨片累计商品尚未发现；"
                "把阈值从 %s 下调到 %s 继续补取（该阈值下补取后 U(t') < T，必可停止）。"
                % (
                    money_str(nth) if nth is not None else "无",
                    money_str(bound),
                    money_str(used) if used is not None else "无",
                    money_str(nxt) if nxt is not None else "0.01",
                )
            )
            threshold = nxt

        rounds.append(
            FetchRound(
                round_no=round_no,
                threshold_used=used,
                next_threshold=threshold,
                cutoffs=dict(cutoffs),
                upper_bound=bound,
                candidate_count=len(view.exact()),
                nth_net=nth,
                entries_count=len(entries),
                new_buckets=new_buckets,
                new_records=new_records,
                new_candidates=new_candidates,
                stop=stop,
                reason=reason,
            )
        )
        if stop:
            stop_reason = reason
            stopped = True
            break

    if not stopped:  # pragma: no cover - 阈值有限，理论上不会到这里
        for sku in list(view.exact()):
            view.capture_key(sku)
        entries, nth = _entries_for(mode, _candidate_lines(view, store), n)
        stop_reason = "达到轮次上限，已补取全部候选，按现有数据给出结果。"

    fetched_details: List[Dict[str, object]] = []
    for sku, (net, cnt) in sorted(
        view.exact().items(), key=lambda kv: (-kv[1][0], kv[0])
    ):
        if sku in initial_skus:
            continue
        details = {
            "sku": sku,
            "net": money_str(net),
            "count": cnt,
            "reason": "跨片累计/阈值补取",
            "per_shard": {
                sid: money_str(b.net)
                for (sid, s), b in sorted(view.fetched.items())
                if s == sku
            },
        }
        fetched_details.append(details)
        if len(details["per_shard"]) > 1:
            notes.append(
                "%s 在 %d 个分片都有净额，局部都不拔尖，跨片累计后补取为 %.2f"
                % (sku, len(details["per_shard"]), float(net))
            )

    local_top = sorted(
        (
            {
                "store": store,
                "sku": item["sku"],
                "local_sum": money_str(item["local_sum"]),
                "per_shard": item["per_shard"],
                "count": item["count"],
            }
            for item in initial.values()
        ),
        key=lambda x: (-Decimal(x["local_sum"]), x["sku"]),
    )
    report = StoreReport(
        store=store,
        local_top=local_top[:sample_limit],
        local_candidate_count=len(initial_skus),
        fetched_skus=fetched_details[:sample_limit],
        fetched_bucket_count=len(view.fetched),
        fetched_record_count=sum(b.count for b in view.fetched.values()),
        candidate_count=len(view.exact()),
        nth_net=nth,
        upper_bound=bound,
        threshold=threshold,
        entries=entries,
        stop_reason=stop_reason,
        rounds=[
            # 展示每个 SKU 的局部候选与补取来源：初筛的局部候选（含跨片局部和）
            # 与“只为凑够 N 而补取”的候选分开列出。
            r for r in rounds
        ],
        notes=notes,
    )
    return report, entries


# ---------------------------------------------------------------------------
# 对外入口
# ---------------------------------------------------------------------------

def full_ranking(
    records: Sequence[Record], stores: Sequence[str], limit: int = 100
) -> Dict[str, List[Dict[str, object]]]:
    """全量分组排序（不做任何分片假设），用于页面上的对照与自证。"""
    agg = aggregate(records, stores)
    out: Dict[str, List[Dict[str, object]]] = {}
    for store in sorted(set(stores)):
        rows = [
            (sku, net, cnt)
            for (st, sku), (net, cnt) in agg.items()
            if st == store and net > 0
        ]
        rows.sort(key=lambda x: (-x[1], x[0]))
        out[store] = [
            {"rank": i + 1, "sku": sku, "net": money_str(net), "count": cnt}
            for i, (sku, net, cnt) in enumerate(rows[:limit])
        ]
    return out


def rank(
    mode: str,
    n: int,
    records: Sequence[Record],
    shards: Sequence[ShardSlice],
    stores: Sequence[str],
    *,
    sample_limit: int = 10,
) -> RankResult:
    """对给定快照做分片式排名，并用全量分组排序独立重算对照。"""
    if mode not in MODES:
        raise ValueError("未知取 N 规则：%s" % mode)
    if n <= 0:
        raise ValueError("N 必须为正整数")
    store_list = sorted(set(stores))
    if not store_list:
        raise ValueError("请选择至少一个门店")

    global_agg = aggregate(records, store_list)
    reports: Dict[str, StoreReport] = {}
    entries_by_store: Dict[str, List[RankEntry]] = {}
    mismatches: List[str] = []
    per_store_verify: Dict[str, Dict[str, object]] = {}
    full_sorted: Dict[str, List[Dict[str, object]]] = {}

    for store in store_list:
        report, entries = _rank_store(
            mode=mode, n=n, store=store, shards=shards, sample_limit=sample_limit
        )
        reports[store] = report
        entries_by_store[store] = entries

        truth_source = [
            ScoreLine(store=st, sku=sku, net=net, source="truth", count=cnt)
            for (st, sku), (net, cnt) in global_agg.items()
            if st == store
        ]
        if mode == TIE_STRICT:
            truth = strict_entries(truth_source, n)
        else:
            truth, _thr = keep_tie_entries(truth_source, n)
        truth_keys = [(e.sku, e.net) for e in truth]
        got_keys = [(e.sku, e.net) for e in entries]
        equal = truth_keys == got_keys
        per_store_verify[store] = {
            "ok": equal,
            "truth_top": [{"sku": k[0], "net": money_str(k[1])} for k in truth_keys],
            "reported_top": [{"sku": k[0], "net": money_str(k[1])} for k in got_keys],
            "truth_count": len(truth),
            "reported_count": len(entries),
            "local_candidate_count": report.local_candidate_count,
            "candidate_count": report.candidate_count,
            "rounds": len(report.rounds),
        }
        if not equal:
            mismatches.append(
                "%s：补取结果与全量分组排序不一致（报告 %d 条，全量 %d 条）"
                % (store, len(entries), len(truth))
            )
        full_sorted[store] = full_ranking(records, [store], limit=100)[store]

    verification = Verification(
        ok=not mismatches,
        mode=mode,
        n=n,
        entries_equal=not mismatches,
        mismatches=mismatches,
        per_store=per_store_verify,
        full_sorted=full_sorted,
    )
    totals = {
        "entries": sum(len(v) for v in entries_by_store.values()),
        "stores": len(store_list),
        "buckets": len(global_agg),
        "records": len(records),
        "rounds": sum(len(r.rounds) for r in reports.values()),
        "fetched_buckets": sum(r.fetched_bucket_count for r in reports.values()),
        "fetched_records": sum(r.fetched_record_count for r in reports.values()),
        "local_candidates": sum(r.local_candidate_count for r in reports.values()),
        "candidates": sum(r.candidate_count for r in reports.values()),
        "mode": mode,
        "n": n,
    }
    return RankResult(
        mode=mode,
        limit=n,
        entries_by_store=entries_by_store,
        reports=reports,
        verification=verification,
        totals=totals,
        shards=[s.digest() for s in shards],
    )
