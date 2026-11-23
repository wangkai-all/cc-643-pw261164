"""分片明细文件读取：JSON / JSONL / CSV，计算摘要以便幂等重传。

文件本身视为不可信输入：只做结构校验，不做任何代码执行；金额一律经
``money.D`` 解析，拒绝 NaN/Infinity。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .money import D, parse_int
from .ranking import Record

ALLOWED_KINDS = {"sale", "return", "correction"}


@dataclass
class ParsedBatch:
    shard_id: str
    batch_id: str
    file_name: str
    file_sha256: str
    records: List[Record]
    meta: Dict[str, object]


def file_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _record_from_obj(obj: Dict[str, object], *, file_name: str, idx: int) -> Record:
    def field(name: str, default: Optional[str] = None) -> str:
        value = obj.get(name, default)
        if value is None:
            raise ValueError("%s 第 %d 条缺少字段 %s" % (file_name, idx + 1, name))
        return str(value).strip()

    kind = (str(obj.get("kind") or obj.get("type") or "sale")).strip()
    if kind in {"退货", "return", "refund", "refunded"}:
        kind = "return"
    elif kind in {"销售", "sale", "sell"}:
        kind = "sale"
    elif kind in {"更正", "correction", "adjust"}:
        kind = "correction"
    if kind not in ALLOWED_KINDS:
        raise ValueError("%s 第 %d 条类型不可识别：%r" % (file_name, idx + 1, kind))
    qty = parse_int(obj.get("qty", obj.get("quantity", 0)), "数量")
    amount = D(obj.get("amount", obj.get("net", 0)))
    if kind == "sale" and qty <= 0:
        raise ValueError("%s 第 %d 条销售数量必须为正" % (file_name, idx + 1))
    if kind == "return" and qty > 0:
        raise ValueError("%s 第 %d 条退货数量必须为负" % (file_name, idx + 1))
    rid = str(obj.get("record_id") or obj.get("id") or "").strip()
    if not rid:
        rid = "%s-%04d" % (Path(file_name).stem, idx + 1)
    return Record(
        record_id=rid,
        store=field("store"),
        sku=field("sku"),
        qty=qty,
        amount=amount,
        kind=kind,
        batch_id=str(obj.get("batch_id") or "").strip(),
        ts=str(obj.get("ts") or obj.get("date") or "").strip(),
        note=str(obj.get("note") or "").strip(),
    )


def parse_file(path: str | Path, shard_id: Optional[str] = None, batch_id: Optional[str] = None) -> ParsedBatch:
    p = Path(path)
    data = p.read_bytes()
    digest = file_sha256(data)
    text = data.decode("utf-8-sig")
    meta: Dict[str, object] = {}
    raw_records: List[Dict[str, object]] = []
    suffix = p.suffix.lower()
    if suffix == ".json":
        payload = json.loads(text)
        if isinstance(payload, dict):
            meta = {k: v for k, v in payload.items() if k != "records"}
            raw_records = list(payload.get("records") or [])
        elif isinstance(payload, list):
            raw_records = list(payload)
        else:
            raise ValueError("%s 不是对象或数组" % p.name)
    elif suffix in {".jsonl", ".ndjson"}:
        raw_records = [json.loads(line) for line in text.splitlines() if line.strip()]
    elif suffix == ".csv":
        reader = csv.DictReader(io.StringIO(text))
        raw_records = [dict(row) for row in reader]
    else:
        raise ValueError("不支持的文件类型：%s（仅 .json/.jsonl/.csv）" % suffix)
    if not raw_records:
        raise ValueError("%s 中没有明细" % p.name)
    records = [_record_from_obj(o, file_name=p.name, idx=i) for i, o in enumerate(raw_records)]
    resolved_shard = (
        shard_id
        or str(meta.get("shard_id") or "").strip()
        or (records[0].batch_id.split(":")[0] if records[0].batch_id else "")
    )
    if not resolved_shard:
        raise ValueError("无法确定分片编号：请在文件 meta 中提供 shard_id，或显式传入")
    resolved_batch = (
        batch_id
        or str(meta.get("batch_id") or "").strip()
        or "batch-%s" % digest[:16]
    )
    normalized: List[Record] = []
    for r in records:
        row_batch = resolved_batch
        if r.batch_id:
            if ":" in r.batch_id:
                r_shard, r_batch = r.batch_id.split(":", 1)
                if r_shard != resolved_shard:
                    raise ValueError(
                        "明细 %s 标明的分片 %s 与批次分片 %s 不一致"
                        % (r.record_id, r_shard, resolved_shard)
                    )
                row_batch = r_batch
            else:
                row_batch = r.batch_id
        normalized.append(
            Record(
                record_id=r.record_id,
                store=r.store,
                sku=r.sku,
                qty=r.qty,
                amount=r.amount,
                kind=r.kind,
                batch_id=row_batch,
                ts=r.ts,
                note=r.note,
            )
        )
    return ParsedBatch(
        shard_id=resolved_shard,
        batch_id=resolved_batch,
        file_name=p.name,
        file_sha256=digest,
        records=normalized,
        meta={k: v for k, v in meta.items() if k not in {"shard_id", "batch_id"}},
    )
