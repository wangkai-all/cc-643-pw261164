#!/usr/bin/env python3
"""演示完整操作流程：导入 → 新增 → 更正 → 补位，并逐版与全量分组排序对照。

用法：
    python3 scripts/demo_flow.py [--db var/demo.sqlite3] [--json var/demo_report.json]

脚本会新建（或复用）一个数据库，走与应用完全相同的存储与排名代码，
不写入任何预设榜单结果；每一步的结果都由当前明细即时算出。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rankapp.money import money_str  # noqa: E402
from rankapp.ranking import (  # noqa: E402
    TIE_KEEP,
    TIE_STRICT,
    ScoreLine,
    aggregate,
    full_ranking,
    keep_tie_entries,
    strict_entries,
)
from rankapp.seed import SAMPLES, write_sample_files, sample_path  # noqa: E402
from rankapp.ingest import parse_file  # noqa: E402
from rankapp.store import Store  # noqa: E402


def import_samples(store: Store):
    write_sample_files()
    out = []
    for sid in SAMPLES:
        parsed = parse_file(sample_path(sid))
        out.append(
            store.ingest_batch(
                shard_id=parsed.shard_id,
                batch_id=parsed.batch_id,
                file_sha256=parsed.file_sha256,
                records=parsed.records,
                file_name=parsed.file_name,
                operator="demo",
            )
        )
    return out


def who(store: Store, n: int, mode: str, stores, label: str) -> dict:
    v = store.latest_version()
    entries = {
        st: [(e["rank"], e["sku"], e["net"]) for e in v["result"]["entries_by_store"].get(st, [])]
        for st in stores
    }
    # 全量分组排序独立重算（不经分片、不经发布结果）。
    agg = aggregate(store.active_records(), stores)
    truth_n = {}
    for st in stores:
        rows = [
            ScoreLine(store=st, sku=sku, net=net, source="truth", count=cnt)
            for (store_name, sku), (net, cnt) in agg.items()
            if store_name == st
        ]
        picked = strict_entries(rows, n) if mode == TIE_STRICT else keep_tie_entries(rows, n)[0]
        truth_n[st] = [(e.rank, e.sku, money_str(e.net)) for e in picked]
    ok = all(
        [(sku, net) for _r, sku, net in entries[st]]
        == [(sku, net) for _r, sku, net in truth_n[st]]
        for st in stores
    )
    print(
        "%s → v%s（%s）%s"
        % (
            label,
            v["version_no"],
            "严格取 N" if mode == TIE_STRICT else "保留末位同额",
            "✓ 与全量分组排序一致" if ok else "✗ 不一致",
        )
    )
    for st in stores:
        print("   %s: %s" % (st, "，".join("#%d %s %s" % row for row in entries[st])))
    return {"version_no": v["version_no"], "entries": entries, "truth": truth_n, "match": ok}


def show_compare(store: Store, before: int, after: int):
    diff = store.compare_versions(before, after)
    print(
        "   对照 v%d→v%d：入榜 %s；出榜 %s；名次变化 %d 条"
        % (
            diff["before"]["version_no"],
            diff["after"]["version_no"],
            ["%s/%s" % (e["store"], e["sku"]) for e in diff["entered"]] or "无",
            ["%s/%s" % (e["store"], e["sku"]) for e in diff["exited"]] or "无",
            len(diff["moved"]),
        )
    )
    for m in diff["moved"]:
        print(
            "     %s/%s  #%d→#%d  净额 %s→%s"
            % (m["store"], m["sku"], m["before_rank"], m["after_rank"], m["before_net"], m["after_net"])
        )
    return diff


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(ROOT / "var" / "demo.sqlite3"))
    parser.add_argument("--json", default=str(ROOT / "var" / "demo_report.json"))
    parser.add_argument("--n", type=int, default=5)
    args = parser.parse_args(argv)

    db = Path(args.db)
    if db.exists():
        db.unlink()
    for suffix in ("-wal", "-shm"):
        extra = Path(str(db) + suffix)
        if extra.exists():
            extra.unlink()
    store = Store(db)
    report = {"n": args.n, "steps": []}

    ingested = import_samples(store)
    print("① 导入三份分片明细：", "，".join("%s %s" % (r["shard_id"], r["status"]) for r in ingested))

    v1 = store.publish(mode=TIE_STRICT, limit_n=args.n, stores=["A", "B"], note="基线")
    report["steps"].append(who(store, args.n, TIE_STRICT, ["A", "B"], "② 基线发布"))

    # 新增：录入一笔销售，使 A/G-103 从榜外进入。
    added = store.add_entry(
        shard_id="S1", store="A", sku="G-103", qty=1, amount="20.00",
        kind="sale", note="演示·新增销售", operator="demo",
    )
    v2 = store.publish(mode=TIE_STRICT, limit_n=args.n, stores=["A", "B"], note="新增后")
    report["steps"].append(who(store, args.n, TIE_STRICT, ["A", "B"], "③ 新增销售 %s" % added["record_id"]))
    diff12 = show_compare(store, v1.version_no, v2.version_no)

    # 历史更正：发现 G-103 多记，冲减 40，令其跌出榜单、由榜外商品补位。
    fixed = store.add_entry(
        shard_id="S3", store="A", sku="G-103", qty=-1, amount="-40.00",
        kind="correction", note="演示·历史更正", operator="demo",
    )
    v3 = store.publish(mode=TIE_STRICT, limit_n=args.n, stores=["A", "B"], note="更正后")
    report["steps"].append(who(store, args.n, TIE_STRICT, ["A", "B"], "④ 历史更正 %s" % fixed["record_id"]))
    diff23 = show_compare(store, v2.version_no, v3.version_no)

    # 同一份明细，用保留末位同额规则再看一遍（B 店 N 名处有并列）。
    tie = store.publish(mode=TIE_KEEP, limit_n=args.n, stores=["A", "B"], note="保留末位同额")
    report["steps"].append(who(store, args.n, TIE_KEEP, ["A", "B"], "⑤ 保留末位同额"))
    show_compare(store, v3.version_no, tie.version_no)

    # 每版榜单的输入范围。
    versions = []
    for row in sorted(store.versions(), key=lambda r: r["version_no"]):
        detail = store.version(row["version_no"])
        versions.append(
            {
                "version_no": detail["version_no"],
                "mode": detail["mode"],
                "n": detail["limit_n"],
                "shards": detail["basis"]["shard_ids"],
                "batches": detail["basis"]["batches"],
                "records": {k: len(v) for k, v in detail["basis"]["input_range"]["record_ids"].items()},
                "record_digest": detail["record_digest"],
                "verification_ok": detail["verification_ok"],
            }
        )
    report["versions"] = versions
    report["compare_1_2"] = diff12
    report["compare_2_3"] = diff23

    print("\n⑥ 各版本保存的输入范围：")
    for v in versions:
        print(
            "   v%d %s N=%d 分片 %s 明细 %s 摘要 %s… 对照 %s"
            % (
                v["version_no"],
                v["mode"],
                v["n"],
                "/".join(v["shards"]),
                v["records"],
                v["record_digest"][:12],
                "通过" if v["verification_ok"] else "未通过",
            )
        )

    out = Path(args.json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("\n报告已写入 %s" % out)
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
