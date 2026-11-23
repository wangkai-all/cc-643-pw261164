"""演示数据：三份互不相交的分片明细文件（仅明细，不含任何榜单结果）。

演示文件同时用于“文件导入”按钮；``seed_demo`` 也走同一套文件解析与批次
入库通道，因此用同一文件重复导入会被判为 duplicate，而不是重复累加。
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "sample_shards"

# 门店 A：G-001 在三片都不拔尖，跨片累计后才进前 5；G-002 是稳定第一。
# 门店 B：用来说明“减少后补位”与“末位同额”。
SAMPLES = {
    # 门店 A：G-001 在三个分片里都排不进局部前 5（每片各有 5 件更高的商品），
    # 但 120+130+140=390 跨片累计后是全局第一 —— 只汇报局部分数会漏掉它。
    "S1": {
        "batch_id": "S1-2026-11-01",
        "records": [
            ("S1-0001", "A", "G-001", 1, "120.00", "sale", "2026-11-01"),
            ("S1-0002", "A", "G-101", 1, "300.00", "sale", "2026-11-01"),
            ("S1-0003", "A", "G-102", 1, "290.00", "sale", "2026-11-01"),
            ("S1-0004", "A", "G-103", 1, "280.00", "sale", "2026-11-01"),
            ("S1-0005", "A", "G-104", 1, "270.00", "sale", "2026-11-02"),
            ("S1-0006", "A", "G-105", 1, "260.00", "sale", "2026-11-02"),
            ("S1-0007", "B", "H-001", 3, "300.00", "sale", "2026-11-01"),
            ("S1-0008", "B", "H-002", 1, "100.00", "sale", "2026-11-01"),
            ("S1-0009", "B", "H-003", 1, "100.00", "sale", "2026-11-02"),
        ],
    },
    "S2": {
        "batch_id": "S2-2026-11-01",
        "records": [
            ("S2-0001", "A", "G-001", 1, "130.00", "sale", "2026-11-03"),
            ("S2-0002", "A", "G-201", 1, "295.00", "sale", "2026-11-03"),
            ("S2-0003", "A", "G-202", 1, "285.00", "sale", "2026-11-03"),
            ("S2-0004", "A", "G-203", 1, "275.00", "sale", "2026-11-03"),
            ("S2-0005", "A", "G-204", 1, "265.00", "sale", "2026-11-04"),
            ("S2-0006", "A", "G-205", 1, "255.00", "sale", "2026-11-04"),
            ("S2-0007", "B", "H-004", 1, "100.00", "sale", "2026-11-03"),
            ("S2-0008", "B", "H-005", 1, "90.00", "sale", "2026-11-03"),
            ("S2-0009", "B", "H-005", -1, "-30.00", "return", "2026-11-04"),
        ],
    },
    "S3": {
        "batch_id": "S3-2026-11-01",
        "records": [
            ("S3-0001", "A", "G-001", 1, "140.00", "sale", "2026-11-05"),
            ("S3-0002", "A", "G-301", 1, "250.00", "sale", "2026-11-05"),
            ("S3-0003", "A", "G-302", 1, "240.00", "sale", "2026-11-05"),
            ("S3-0004", "A", "G-303", 1, "230.00", "sale", "2026-11-05"),
            ("S3-0005", "A", "G-304", 1, "220.00", "sale", "2026-11-05"),
            ("S3-0006", "A", "G-305", 1, "210.00", "sale", "2026-11-05"),
            ("S3-0007", "B", "H-002", 1, "80.00", "sale", "2026-11-05"),
            ("S3-0008", "B", "H-003", 2, "200.00", "sale", "2026-11-05"),
            ("S3-0009", "B", "H-006", 1, "60.00", "sale", "2026-11-05"),
        ],
    },
}


def sample_path(shard_id: str, directory: Path | None = None) -> Path:
    return (directory or SAMPLE_DIR) / ("%s.json" % shard_id.lower())


def write_sample_files(directory: Path | None = None) -> list:
    directory = directory or SAMPLE_DIR
    directory.mkdir(parents=True, exist_ok=True)
    written = []
    for shard_id, spec in SAMPLES.items():
        path = sample_path(shard_id, directory)
        payload = {
            "shard_id": shard_id,
            "batch_id": spec["batch_id"],
            "records": [
                {
                    "record_id": rid,
                    "store": store,
                    "sku": sku,
                    "qty": qty,
                    "amount": amount,
                    "kind": kind,
                    "ts": ts,
                }
                for rid, store, sku, qty, amount, kind, ts in spec["records"]
            ],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(path)
    return written


def seed_demo(store) -> dict:
    """把三份演示文件按正式解析与入库通道写入，并发布第一版榜单。"""
    from .ingest import parse_file

    write_sample_files()
    results = []
    for shard_id in SAMPLES:
        parsed = parse_file(sample_path(shard_id))
        results.append(
            store.ingest_batch(
                shard_id=parsed.shard_id,
                batch_id=parsed.batch_id,
                file_sha256=parsed.file_sha256,
                records=parsed.records,
                file_name=parsed.file_name,
                operator="seed",
            )
        )
    if store.latest_version() is None:
        outcome = store.publish(mode="strict", limit_n=5, stores=["A", "B"], note="演示数据首版")
        version_no = outcome.version_no
    else:
        version_no = store.latest_version()["version_no"]
    return {
        "batches": results,
        "version_no": version_no,
        "summary": store.state_summary(),
    }


if __name__ == "__main__":  # pragma: no cover
    for p in write_sample_files():
        print(p)
