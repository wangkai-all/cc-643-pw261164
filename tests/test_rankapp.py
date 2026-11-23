"""测试：核心算法、存储不变量、HTTP 接口与完整操作流程。"""

from __future__ import annotations

import json
import os
import random
import sys
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rankapp.ingest import parse_file, file_sha256  # noqa: E402
from rankapp.money import D, money_str  # noqa: E402
from rankapp.ranking import (  # noqa: E402
    Record,
    ShardSlice,
    TIE_KEEP,
    TIE_STRICT,
    aggregate,
    full_ranking,
    keep_tie_entries,
    rank,
    strict_entries,
    ScoreLine,
)
from rankapp.store import SnapshotIncomplete, Store  # noqa: E402


def rec(rid, store, sku, amount, shard="S1", kind="sale", qty=1, batch=None):
    return Record(
        record_id=rid,
        store=store,
        sku=sku,
        qty=qty,
        amount=Decimal(str(amount)),
        kind=kind,
        batch_id=batch or ("batch-" + shard),
    )


def make_shards(records, shard_ids=("S1", "S2", "S3")):
    out = []
    for sid in shard_ids:
        rows = [r for r in records if r.record_id.startswith(sid + "-")]
        out.append(ShardSlice.build(sid, sid, sorted({r.batch_id for r in rows}), rows))
    return out


def brute(records, n, mode, store):
    agg = aggregate(records, [store])
    lines = [
        ScoreLine(store=store, sku=sku, net=net, source="truth", count=cnt)
        for (st, sku), (net, cnt) in agg.items()
        if st == store
    ]
    if mode == TIE_STRICT:
        return [(e.sku, e.net) for e in strict_entries(lines, n)]
    return [(e.sku, e.net) for e in keep_tie_entries(lines, n)[0]]


class RankingTests(unittest.TestCase):
    def test_cross_shard_cumulative_candidate_is_fetched(self):
        # G-001 在每个分片都排不进局部前 2，但跨片累计 390 进入全局前 2。
        records = [
            rec("S1-1", "A", "G-001", 120),
            rec("S1-2", "A", "G-002", 400),
            rec("S1-3", "A", "G-003", 250),
            rec("S1-4", "A", "G-009", 240),
            rec("S2-1", "A", "G-001", 130),
            rec("S2-2", "A", "G-004", 245),
            rec("S2-3", "A", "G-010", 235),
            rec("S3-1", "A", "G-001", 140),
            rec("S3-2", "A", "G-005", 230),
            rec("S3-3", "A", "G-011", 225),
        ]
        shards = make_shards(records)
        for mode in (TIE_STRICT, TIE_KEEP):
            result = rank(mode, 2, records, shards, ["A"])
            self.assertTrue(result.verification.ok, result.verification.mismatches)
            got = [(e.sku, e.net) for e in result.entries_by_store["A"]]
            self.assertEqual(got, brute(records, 2, mode, "A"))
            self.assertIn("G-001", [g[0] for g in got])
            report = result.reports["A"]
            # 补取过程必须可见：局部候选没有 G-001，补取后才出现。
            self.assertNotIn("G-001", [c["sku"] for c in report.local_top])
            self.assertIn("G-001", [c["sku"] for c in report.fetched_skus])
            self.assertGreaterEqual(len(report.rounds), 1)
            self.assertGreater(report.candidate_count, report.local_candidate_count)
            self.assertTrue(any("跨片" in n or "累计" in n for n in report.notes))

    def test_strict_tie_break_is_stable_by_sku(self):
        records = [rec("S1-1", "A", "B", 100), rec("S1-2", "A", "A", 100), rec("S1-3", "A", "C", 90)]
        shards = make_shards(records)
        result = rank(TIE_STRICT, 1, records, shards, ["A"])
        entries = result.entries_by_store["A"]
        self.assertEqual([e.sku for e in entries], ["A"])
        self.assertTrue(entries[0].tie, "同额被截断时应标记")
        result2 = rank(TIE_KEEP, 1, records, shards, ["A"])
        self.assertEqual([e.sku for e in result2.entries_by_store["A"]], ["A", "B"])

    def test_keep_ties_never_truncates_a_tied_tail(self):
        records = [
            rec("S1-1", "A", "P1", 300),
            rec("S1-2", "A", "P2", 200),
            rec("S1-3", "A", "P3", 100),
            rec("S1-4", "A", "P4", 100),
            rec("S1-5", "A", "P5", 100),
        ]
        shards = make_shards(records)
        # 第 3 名是 100，与 P4/P5 并列：同额模式必须全部保留（多于 N）。
        result = rank(TIE_KEEP, 3, records, shards, ["A"])
        skus = [e.sku for e in result.entries_by_store["A"]]
        self.assertEqual(skus, ["P1", "P2", "P3", "P4", "P5"])
        self.assertTrue(all(e.tie for e in result.entries_by_store["A"][2:]))
        strict = rank(TIE_STRICT, 3, records, shards, ["A"])
        self.assertEqual([e.sku for e in strict.entries_by_store["A"]], ["P1", "P2", "P3"])
        self.assertTrue(strict.entries_by_store["A"][-1].tie, "严格模式应标记末位被同额截断")

    def test_non_positive_never_ranks(self):
        records = [
            rec("S1-1", "A", "N1", -50),
            rec("S1-2", "A", "Z0", 0),
            rec("S1-3", "A", "P1", 10),
        ]
        shards = make_shards(records)
        for mode in (TIE_STRICT, TIE_KEEP):
            result = rank(mode, 5, records, shards, ["A"])
            self.assertEqual([e.sku for e in result.entries_by_store["A"]], ["P1"])

    def test_fewer_positive_than_n(self):
        records = [rec("S1-1", "A", "P1", 10), rec("S2-1", "A", "P2", 5)]
        shards = make_shards(records)
        result = rank(TIE_STRICT, 10, records, shards, ["A"])
        self.assertEqual(len(result.entries_by_store["A"]), 2)
        self.assertTrue(result.reports["A"].rounds[-1].stop)
        self.assertTrue(result.verification.ok)

    def test_empty_shard_and_missing_store(self):
        records = [rec("S1-1", "A", "P1", 10)]
        shards = make_shards(records)
        result = rank(TIE_STRICT, 3, records, shards, ["A"])
        self.assertTrue(result.verification.ok)
        self.assertEqual(result.reports["A"].local_candidate_count, 1)

    def test_randomized_against_full_sort(self):
        rng = random.Random(20261123)
        for _ in range(300):
            n_shards = rng.randint(1, 4)
            n_skus = rng.randint(1, 12)
            n = rng.randint(1, 6)
            stores = rng.sample(["A", "B"], rng.randint(1, 2))
            records = []
            for s in range(n_shards):
                for i in range(rng.randint(0, 10)):
                    records.append(
                        rec(
                            "S%d-%d" % (s + 1, i),
                            rng.choice(stores),
                            "K%02d" % rng.randrange(n_skus),
                            rng.choice([-5, 0, 5, 10, 10, 20, 20, 30, 45, 60]),
                        )
                    )
            shards = make_shards(records, tuple("S%d" % (i + 1) for i in range(n_shards)))
            for mode in (TIE_STRICT, TIE_KEEP):
                result = rank(mode, n, records, shards, stores)
                self.assertTrue(result.verification.ok, (mode, result.verification.mismatches))
                for store in stores:
                    got = [(e.sku, e.net) for e in result.entries_by_store[store]]
                    self.assertEqual(got, brute(records, n, mode, store))

    def test_full_ranking_matches_entries(self):
        records = [rec("S1-1", "A", "P1", 30), rec("S1-2", "A", "P2", 20), rec("S2-1", "A", "P1", 25)]
        full = full_ranking(records, ["A"], limit=10)["A"]
        self.assertEqual([row["sku"] for row in full], ["P1", "P2"])
        self.assertEqual(full[0]["net"], "55.00")


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "rank.sqlite3"
        self.store = Store(self.db)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _load_samples(self):
        from rankapp.seed import write_sample_files, sample_path

        write_sample_files()
        for sid in ("S1", "S2", "S3"):
            parsed = parse_file(sample_path(sid))
            self.store.ingest_batch(
                shard_id=parsed.shard_id,
                batch_id=parsed.batch_id,
                file_sha256=parsed.file_sha256,
                records=parsed.records,
                file_name=parsed.file_name,
            )

    def test_batch_reupload_is_idempotent(self):
        rows = [rec("S1-1", "A", "P1", 10)]
        first = self.store.ingest_batch(
            shard_id="S1", batch_id="b1", file_sha256="hash-a", records=rows
        )
        second = self.store.ingest_batch(
            shard_id="S1", batch_id="b1", file_sha256="hash-a", records=rows
        )
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(second["status"], "duplicate")
        self.assertEqual(self.store.state_summary()["records"], 1)
        # 换批次号重传同一批数据：行级去重，仍不重复累加。
        third = self.store.ingest_batch(
            shard_id="S1", batch_id="b1-copy", file_sha256="hash-b", records=rows
        )
        self.assertEqual(third["inserted"], 0)
        self.assertEqual(self.store.state_summary()["records"], 1)

    def test_conflicting_batch_is_rejected(self):
        self.store.ingest_batch(
            shard_id="S1", batch_id="b1", file_sha256="hash-a", records=[rec("S1-1", "A", "P1", 10)]
        )
        conflict = self.store.ingest_batch(
            shard_id="S1", batch_id="b1", file_sha256="hash-b", records=[rec("S1-2", "A", "P2", 10)]
        )
        self.assertEqual(conflict["status"], "conflict")
        self.assertEqual(self.store.state_summary()["records"], 1)

    def test_invalid_batch_writes_nothing(self):
        bad = [rec("S1-1", "A", "P1", 10), rec("S1-2", "A", "P2", 10, kind="return", qty=2)]
        with self.assertRaises(ValueError):
            self.store.ingest_batch(shard_id="S1", batch_id="bad", file_sha256="h", records=bad)
        self.assertEqual(self.store.state_summary()["records"], 0)

    def test_unused_shard_does_not_block_but_lost_data_does(self):
        # 从未导入明细的分片视为未启用，不阻断发布（小规模部署只用部分分片）。
        rows = [rec("S1-1", "A", "P1", 10), rec("S1-2", "A", "P2", 5)]
        self.store.ingest_batch(shard_id="S1", batch_id="only", file_sha256="h", records=rows)
        slices, missing, uninit = self.store.build_snapshot()
        self.assertEqual(missing, [])
        self.assertEqual(sorted(u["shard_id"] for u in uninit), ["S2", "S3"])
        self.store.publish(mode=TIE_STRICT, limit_n=1, stores=["A"])
        self.assertEqual(self.store.state_summary()["versions"], 1)

        # 曾参与过计算的分片，若有效明细被清空，则阻断发布。
        self.store.delete_record("S1-1")
        self.store.delete_record("S1-2")
        slices, missing, uninit = self.store.build_snapshot()
        self.assertEqual([m["shard_id"] for m in missing], ["S1"])
        with self.assertRaises(SnapshotIncomplete):
            self.store.publish(mode=TIE_STRICT, limit_n=1, stores=["A"])
        self.assertEqual(self.store.state_summary()["versions"], 1)

    def test_shard_offline_blocks_publish(self):
        self._load_samples()
        self.store.publish(mode=TIE_STRICT, limit_n=3, stores=["A"])
        self.store.set_shard_active("S2", False)
        with self.assertRaises(SnapshotIncomplete) as ctx:
            self.store.publish(mode=TIE_STRICT, limit_n=3, stores=["A"])
        self.assertEqual(ctx.exception.missing[0]["shard_id"], "S2")
        # 版本数没有增加，界面不会出现“半份榜单”。
        self.assertEqual(self.store.state_summary()["versions"], 1)
        self.store.set_shard_active("S2", True)
        self.store.publish(mode=TIE_STRICT, limit_n=3, stores=["A"])
        self.assertEqual(self.store.state_summary()["versions"], 2)

    def test_publish_records_input_range(self):
        self._load_samples()
        outcome = self.store.publish(mode=TIE_STRICT, limit_n=3, stores=["A"])
        version = self.store.version(outcome.version_no)
        self.assertEqual(sorted(version["record_ids"]), ["S1", "S2", "S3"])
        self.assertEqual(version["basis"]["mode"], TIE_STRICT)
        self.assertEqual(version["basis"]["n"], 3)
        self.assertEqual(
            sum(len(v) for v in version["record_ids"].values()), self.store.state_summary()["records"]
        )
        self.assertTrue(version["record_digest"])

    def test_delete_distinguishes_absent_and_zero(self):
        self.store.add_entry(
            shard_id="S1", store="A", sku="G-1", qty=1, amount="100", kind="sale", record_id="e1"
        )
        self.store.add_entry(
            shard_id="S1", store="A", sku="G-1", qty=-1, amount="-100", kind="return", record_id="e2"
        )
        zero = self.store.delete_record("e1")
        self.assertEqual(zero["state"], "present")  # e2 仍在，净额 -100
        absent = self.store.delete_record("e2")
        self.assertEqual(absent["state"], "absent")
        self.assertEqual(absent["remaining_records"], 0)

        self.store.add_entry(
            shard_id="S1", store="A", sku="G-2", qty=1, amount="50", kind="sale", record_id="e3"
        )
        self.store.add_entry(
            shard_id="S1", store="A", sku="G-2", qty=-1, amount="-50", kind="return", record_id="e4"
        )
        self.store.add_entry(
            shard_id="S1", store="A", sku="G-2", qty=1, amount="0", kind="correction", record_id="e5"
        )
        result = self.store.delete_record("e5")
        self.assertEqual(result["state"], "zero")
        self.assertEqual(result["remaining_net"], "0.00")

    def test_full_flow_entry_correction_promotion(self):
        """完整流程：录入 → 更正 → 减少后补位，每一版都与全量对照。"""
        for sid in ("S1", "S2", "S3"):
            self.store.ingest_batch(
                shard_id=sid,
                batch_id="seed-" + sid,
                file_sha256="hash-" + sid,
                records=[
                    rec("%s-base" % sid, "A", "G-100", 100),
                    rec("%s-b" % sid, "A", "G-200", 80),
                ],
            )
        for i in range(1, 6):
            self.store.add_entry(
                shard_id="S3",
                store="A",
                sku="G-30%d" % i,
                qty=1,
                amount=str(10 * i),
                kind="sale",
                record_id="fill-%d" % i,
            )
        v1 = self.store.publish(mode=TIE_STRICT, limit_n=3, stores=["A"])
        top1 = [e["sku"] for e in v1.result["entries_by_store"]["A"]]
        self.assertEqual(top1, ["G-100", "G-200", "G-305"])

        # 历史更正：把 G-305 改小（模拟在其他分片发现漏记退货）。
        self.store.add_entry(
            shard_id="S2",
            store="A",
            sku="G-305",
            qty=-1,
            amount="-40",
            kind="correction",
            record_id="fix-305",
        )
        v2 = self.store.publish(mode=TIE_STRICT, limit_n=3, stores=["A"])
        top2 = [e["sku"] for e in v2.result["entries_by_store"]["A"]]
        self.assertNotIn("G-305", top2)
        self.assertIn("G-304", top2, "减少后应由原未入榜商品补位")

        diff = self.store.compare_versions(v1.version_no, v2.version_no)
        self.assertEqual([e["sku"] for e in diff["entered"]], ["G-304"])
        self.assertEqual([e["sku"] for e in diff["exited"]], ["G-305"])

        # 每一版都用全量分组排序复核。
        for vno in (v1.version_no, v2.version_no):
            version = self.store.version(vno)
            self.assertTrue(version["verification_ok"])
            self.assertTrue(version["result"]["verification"]["ok"])

    def test_concurrent_publish_never_partial(self):
        for sid in ("S1", "S2", "S3"):
            self.store.ingest_batch(
                shard_id=sid,
                batch_id="c-" + sid,
                file_sha256="h-" + sid,
                records=[rec("%s-%d" % (sid, i), "A", "P-" + sid, 10 + i) for i in range(3)],
            )
        errors = []

        def work():
            try:
                self.store.publish(mode=TIE_STRICT, limit_n=2, stores=["A"])
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=work) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.store.state_summary()["versions"], 6)
        numbers = [v["version_no"] for v in self.store.versions()]
        self.assertEqual(numbers, list(range(6, 0, -1)))
        for v in self.store.versions():
            detail = self.store.version(v["version_no"])
            self.assertTrue(detail["result"]["verification"]["ok"])


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from http.server import ThreadingHTTPServer
        from rankapp.server import Api, Handler

        cls.tmp = tempfile.TemporaryDirectory()
        cls.store = Store(Path(cls.tmp.name) / "api.sqlite3")
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

    def request(self, method, path, body=None):
        import urllib.error
        import urllib.request

        url = "http://127.0.0.1:%d%s" % (self.port, path)
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_http_flow(self):
        status, payload = self.request("POST", "/api/batches", {"files": ["s1.json", "s2.json", "s3.json"]})
        self.assertEqual(status, 200, payload)
        self.assertEqual(len(payload["results"]), 3)

        status, payload = self.request(
            "POST", "/api/entries",
            {"store": "A", "sku": "G-003", "qty": -1, "amount": "-310", "kind": "return", "shard_id": "S2"},
        )
        self.assertEqual(status, 200, payload)

        status, rank_payload = self.request("GET", "/api/rank?mode=strict&n=3&stores=A")
        self.assertEqual(status, 200)
        self.assertTrue(rank_payload["result"]["verification"]["ok"])
        report = rank_payload["result"]["reports"]["A"]
        self.assertTrue(report["rounds"], "补取轮次必须展开")
        self.assertTrue(report["stop_reason"])

        status, published = self.request("POST", "/api/publish", {"mode": "strict", "n": 3, "stores": ["A"]})
        self.assertEqual(status, 200, published)
        version_no = published["version_no"]

        status, version = self.request("GET", "/api/version/%d" % version_no)
        self.assertEqual(status, 200)
        self.assertTrue(version["basis"]["input_range"]["record_ids"])

        status, dup = self.request("POST", "/api/batches", {"files": ["s1.json"]})
        self.assertEqual(dup["results"][0]["status"], "duplicate")

        status, err = self.request("POST", "/api/publish", {"mode": "strict", "n": 3, "stores": ["Z"]})
        self.assertEqual(status, 400)

        status, bad = self.request("GET", "/api/rank?mode=nope&n=3&stores=A")
        self.assertEqual(status, 400)

    def test_static_index_and_csp(self):
        import urllib.request

        with urllib.request.urlopen("http://127.0.0.1:%d/" % self.port) as resp:
            html = resp.read().decode("utf-8")
            self.assertIn("门店商品销售榜", html)
            self.assertIn("Content-Security-Policy", resp.headers)
        with urllib.request.urlopen("http://127.0.0.1:%d/static/app.js" % self.port) as resp:
            self.assertEqual(resp.headers["Content-Type"], "text/javascript; charset=utf-8")


if __name__ == "__main__":
    unittest.main(verbosity=2)
