"""明细存储：SQLite，批次幂等入库、原子发布版本、输入范围快照。

三条不变量
==========

1. **批次不重复累加**：``batches`` 以 (分片, 文件摘要, 批次号) 去重；同一批
   数据在任何时刻重传都只入库一次，且返回 duplicate 而不是静默成功。同批次
   号但内容摘要不同则判为 conflict 并拒绝写入，避免覆盖式重传。行级还以
   ``record_id`` 去重，换个批次号重传同一批数据也不会重复累加。
2. **不发布半份榜单**：验证明细通过后才写入；启用中的分片若没有已接受的
   明细（未初始化）或被标记为暂时退出（``active=0``），发布会被拒绝并列出
   待同步分片，绝不生成只含部分分片的榜单。发布全程持有同一把锁，版本写入
   在单个事务内完成。
3. **每版榜单都保存输入范围**：``rank_versions`` 记录分片、批次号、逐条记录
   编号与整体摘要、参数（模式/N/门店）和完整结果；版本只追加不修改，可随时
   复算与对照。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .money import D, money_str, q2
from .ranking import (
    MODES,
    RankResult,
    Record,
    ShardSlice,
    TIE_KEEP,
    TIE_STRICT,
    aggregate,
    keep_tie_entries,
    rank,
    records_digest,
    strict_entries,
    ScoreLine,
)

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS shards (
    shard_id      TEXT PRIMARY KEY,
    label         TEXT NOT NULL,
    active        INTEGER NOT NULL DEFAULT 1,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    shard_id      TEXT NOT NULL REFERENCES shards(shard_id),
    batch_id      TEXT NOT NULL,
    file_sha256   TEXT NOT NULL,
    file_name     TEXT NOT NULL DEFAULT '',
    record_count  INTEGER NOT NULL,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    accepted      INTEGER NOT NULL DEFAULT 0,
    duplicate     INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL,
    message       TEXT NOT NULL DEFAULT '',
    received_at   TEXT NOT NULL,
    UNIQUE (shard_id, file_sha256, batch_id)
);

CREATE TABLE IF NOT EXISTS records (
    record_id     TEXT PRIMARY KEY,
    shard_id      TEXT NOT NULL REFERENCES shards(shard_id),
    batch_pk      INTEGER NOT NULL DEFAULT 0,
    batch_id      TEXT NOT NULL DEFAULT '',
    store         TEXT NOT NULL,
    sku           TEXT NOT NULL,
    qty           INTEGER NOT NULL,
    amount        TEXT NOT NULL,
    kind          TEXT NOT NULL,
    ts            TEXT NOT NULL DEFAULT '',
    note          TEXT NOT NULL DEFAULT '',
    active        INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS records_key ON records(store, sku);
CREATE INDEX IF NOT EXISTS records_shard ON records(shard_id, active);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      TEXT NOT NULL UNIQUE,
    op            TEXT NOT NULL,
    shard_id      TEXT NOT NULL DEFAULT '',
    batch_id      TEXT NOT NULL DEFAULT '',
    record_id     TEXT NOT NULL DEFAULT '',
    store         TEXT NOT NULL DEFAULT '',
    sku           TEXT NOT NULL DEFAULT '',
    qty           INTEGER NOT NULL DEFAULT 0,
    amount        TEXT NOT NULL DEFAULT '0.00',
    kind          TEXT NOT NULL DEFAULT '',
    payload       TEXT NOT NULL DEFAULT '{}',
    operator      TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rank_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version_no      INTEGER NOT NULL UNIQUE,
    mode            TEXT NOT NULL,
    limit_n         INTEGER NOT NULL,
    stores          TEXT NOT NULL,
    shard_ids       TEXT NOT NULL,
    batch_ids       TEXT NOT NULL,
    record_ids      TEXT NOT NULL,
    record_digest   TEXT NOT NULL,
    result_json     TEXT NOT NULL,
    basis_json      TEXT NOT NULL,
    verification_ok INTEGER NOT NULL,
    created_at      TEXT NOT NULL,
    note            TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

SHARD_SEED = (
    ("S1", "门店明细·华东片"),
    ("S2", "门店明细·华南片"),
    ("S3", "门店明细·补录片"),
)

KINDS = ("sale", "return", "correction")


def now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_event_id(prefix: str) -> str:
    return "%s-%s" % (prefix, uuid.uuid4().hex[:16])


class SnapshotIncomplete(Exception):
    """分片未就绪或暂时退出，拒绝基于不完整输入发布榜单。"""

    def __init__(self, missing: List[Dict[str, str]]):
        self.missing = missing
        super().__init__(
            "输入不完整，拒绝发布半份榜单："
            + "；".join("%s(%s)" % (m["shard_id"], m["reason"]) for m in missing)
        )


class DuplicateRecord(Exception):
    pass


@dataclass
class PublishOutcome:
    version_no: int
    result: Dict[str, object]
    basis: Dict[str, object]
    created_at: str


class Store:
    """线程安全的本地 SQLite 存储。"""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            for shard_id, label in SHARD_SEED:
                self._conn.execute(
                    "INSERT OR IGNORE INTO shards(shard_id,label,active,updated_at) VALUES(?,?,1,?)",
                    (shard_id, label, now_iso()),
                )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # -- 分片 ------------------------------------------------------------
    def list_shards(self) -> List[Dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT s.shard_id, s.label, s.active, s.updated_at,
                          (SELECT COUNT(*) FROM records r WHERE r.shard_id=s.shard_id AND r.active=1) AS records,
                          (SELECT COUNT(*) FROM batches b WHERE b.shard_id=s.shard_id AND b.accepted=1) AS batches,
                          (SELECT MAX(received_at) FROM batches b WHERE b.shard_id=s.shard_id AND b.accepted=1) AS last_batch
                   FROM shards s ORDER BY s.shard_id"""
            ).fetchall()
        return [dict(r) for r in rows]

    def set_shard_active(self, shard_id: str, active: bool) -> Dict[str, object]:
        with self._tx() as c:
            cur = c.execute(
                "UPDATE shards SET active=?, updated_at=? WHERE shard_id=?",
                (1 if active else 0, now_iso(), shard_id),
            )
            if cur.rowcount == 0:
                raise KeyError("分片不存在：%s" % shard_id)
            c.execute(
                "INSERT INTO events(event_id,op,shard_id,payload,created_at) VALUES(?,?,?,?,?)",
                (
                    _new_event_id("shard"),
                    "shard_online" if active else "shard_offline",
                    shard_id,
                    json.dumps({"active": bool(active)}),
                    now_iso(),
                ),
            )
        return {"shard_id": shard_id, "active": bool(active)}

    def ensure_shard(self, shard_id: str, label: Optional[str] = None) -> None:
        if not shard_id or not shard_id.strip():
            raise ValueError("分片编号不能为空")
        with self._tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO shards(shard_id,label,active,updated_at) VALUES(?,?,1,?)",
                (shard_id.strip(), (label or shard_id).strip(), now_iso()),
            )

    # -- 批次 ------------------------------------------------------------
    def ingest_batch(
        self,
        *,
        shard_id: str,
        batch_id: str,
        file_sha256: str,
        records: Sequence[Record],
        file_name: str = "",
        operator: str = "",
    ) -> Dict[str, object]:
        """整批校验 + 入库；任何一条明细不合法，整批拒绝，绝不部分写入。"""
        if not batch_id or not batch_id.strip():
            raise ValueError("批次号不能为空")
        batch_id = batch_id.strip()
        if not file_sha256:
            raise ValueError("缺少文件摘要")
        self.ensure_shard(shard_id)
        seen: Dict[str, Record] = {}
        for r in records:
            if r.record_id in seen:
                raise ValueError("批次内明细编号重复：%s" % r.record_id)
            seen[r.record_id] = r
            if not r.store.strip():
                raise ValueError("明细 %s 缺少门店" % r.record_id)
            if not r.sku.strip():
                raise ValueError("明细 %s 缺少商品编号" % r.record_id)
            if r.kind not in KINDS:
                raise ValueError("明细 %s 类型不可识别：%s" % (r.record_id, r.kind))
            if r.kind == "sale" and r.qty <= 0:
                raise ValueError("销售明细 %s 数量必须为正" % r.record_id)
            if r.kind == "return" and r.qty > 0:
                raise ValueError("退货明细 %s 数量必须为负（或金额为负）" % r.record_id)

        with self._lock:
            # 1) 完全相同的批次：幂等返回，绝不重复累加。
            row = self._conn.execute(
                "SELECT * FROM batches WHERE shard_id=? AND file_sha256=? AND batch_id=?",
                (shard_id, file_sha256, batch_id),
            ).fetchone()
            if row is not None:
                self._log_event(
                    "batch_duplicate",
                    shard_id=shard_id,
                    batch_id=batch_id,
                    payload={"file_sha256": file_sha256, "record_count": len(records)},
                    operator=operator,
                )
                return {
                    "shard_id": shard_id,
                    "batch_id": batch_id,
                    "status": "duplicate",
                    "accepted": 0,
                    "duplicate": 1,
                    "inserted": 0,
                    "message": "该批次已入库，本次未重复累加",
                }
            # 2) 同批次号但内容不同：拒绝，避免覆盖式重传。
            row = self._conn.execute(
                "SELECT * FROM batches WHERE shard_id=? AND batch_id=? AND file_sha256<>?",
                (shard_id, batch_id, file_sha256),
            ).fetchone()
            if row is not None:
                self._log_event(
                    "batch_conflict",
                    shard_id=shard_id,
                    batch_id=batch_id,
                    payload={"file_sha256": file_sha256, "existing_sha256": row["file_sha256"]},
                    operator=operator,
                )
                return {
                    "shard_id": shard_id,
                    "batch_id": batch_id,
                    "status": "conflict",
                    "accepted": 0,
                    "duplicate": 0,
                    "inserted": 0,
                    "message": "同批次号但内容摘要不同，已拒绝以免覆盖；请换新批次号或核对后重传",
                }
            with self._tx() as c:
                batch_pk = c.execute(
                    "INSERT INTO batches(shard_id,batch_id,file_sha256,file_name,record_count,"
                    "inserted_count,accepted,duplicate,status,message,received_at) "
                    "VALUES(?,?,?,?,?,0,0,0,?,?,?)",
                    (
                        shard_id,
                        batch_id,
                        file_sha256,
                        file_name,
                        len(records),
                        "pending",
                        "整批校验通过，正在入库",
                        now_iso(),
                    ),
                ).lastrowid
                inserted = 0
                skipped = 0
                for r in records:
                    if c.execute("SELECT 1 FROM records WHERE record_id=?", (r.record_id,)).fetchone():
                        skipped += 1
                        continue
                    c.execute(
                        "INSERT INTO records(record_id,shard_id,batch_pk,batch_id,store,sku,qty,amount,kind,ts,note,active) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,1)",
                        (
                            r.record_id,
                            shard_id,
                            batch_pk,
                            batch_id,
                            r.store.strip(),
                            r.sku.strip(),
                            r.qty,
                            money_str(r.amount),
                            r.kind,
                            r.ts,
                            r.note,
                        ),
                    )
                    inserted += 1
                message = "入库 %d 条（批次 %d 条，已存在跳过 %d 条）" % (
                    inserted,
                    len(records),
                    skipped,
                )
                c.execute(
                    "UPDATE batches SET accepted=1,inserted_count=?,status=?,message=? WHERE id=?",
                    (inserted, "accepted", message, batch_pk),
                )
                c.execute(
                    "INSERT INTO events(event_id,op,shard_id,batch_id,payload,operator,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        _new_event_id("acc"),
                        "batch_accepted",
                        shard_id,
                        batch_id,
                        json.dumps(
                            {
                                "inserted": inserted,
                                "skipped_existing": skipped,
                                "record_count": len(records),
                                "file_name": file_name,
                            }
                        ),
                        operator,
                        now_iso(),
                    ),
                )
            return {
                "shard_id": shard_id,
                "batch_id": batch_id,
                "status": "accepted",
                "accepted": 1,
                "duplicate": 0,
                "inserted": inserted,
                "skipped_existing": skipped,
                "message": message,
            }

    def _log_event(
        self,
        op: str,
        *,
        shard_id: str = "",
        batch_id: str = "",
        record_id: str = "",
        store: str = "",
        sku: str = "",
        qty: int = 0,
        amount: str = "0.00",
        kind: str = "",
        payload: Optional[Dict[str, object]] = None,
        operator: str = "",
    ) -> str:
        event_id = _new_event_id(op)
        with self._tx() as c:
            c.execute(
                "INSERT INTO events(event_id,op,shard_id,batch_id,record_id,store,sku,qty,amount,kind,"
                "payload,operator,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    op,
                    shard_id,
                    batch_id,
                    record_id,
                    store,
                    sku,
                    qty,
                    amount,
                    kind,
                    json.dumps(payload or {}, ensure_ascii=False),
                    operator,
                    now_iso(),
                ),
            )
        return event_id

    def list_batches(self, limit: int = 100) -> List[Dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    # -- 明细 ------------------------------------------------------------
    def add_entry(
        self,
        *,
        shard_id: str,
        store: str,
        sku: str,
        qty: int,
        amount,
        kind: str,
        record_id: str = "",
        ts: str = "",
        note: str = "",
        operator: str = "",
    ) -> Dict[str, object]:
        """网页录入销售、退货或历史更正；以“事件 + 明细”的形式追加。"""
        if kind not in KINDS:
            raise ValueError("类型只能是 sale / return / correction")
        if not store.strip() or not sku.strip():
            raise ValueError("门店与商品编号不能为空")
        if kind == "sale" and qty <= 0:
            raise ValueError("销售数量必须为正")
        if kind == "return" and qty > 0:
            raise ValueError("退货数量必须为负（或金额为负）")
        amt = q2(amount)
        rid = (record_id or "").strip() or "web-%s" % uuid.uuid4().hex[:12]
        self.ensure_shard(shard_id)
        with self._tx() as c:
            if c.execute("SELECT 1 FROM records WHERE record_id=?", (rid,)).fetchone():
                raise ValueError("明细编号已存在：%s" % rid)
            c.execute(
                "INSERT INTO records(record_id,shard_id,batch_pk,batch_id,store,sku,qty,amount,kind,ts,note,active) "
                "VALUES(?,?,0,'web',?,?,?,?,?,?,?,1)",
                (rid, shard_id, store.strip(), sku.strip(), qty, money_str(amt), kind, ts, note),
            )
            event_id = _new_event_id(kind)
            c.execute(
                "INSERT INTO events(event_id,op,shard_id,batch_id,record_id,store,sku,qty,amount,kind,"
                "payload,operator,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    kind,
                    shard_id,
                    "web",
                    rid,
                    store.strip(),
                    sku.strip(),
                    qty,
                    money_str(amt),
                    kind,
                    json.dumps({"note": note, "ts": ts}, ensure_ascii=False),
                    operator,
                    now_iso(),
                ),
            )
        return {
            "record_id": rid,
            "event_id": event_id,
            "store": store.strip(),
            "sku": sku.strip(),
            "qty": qty,
            "amount": money_str(amt),
            "kind": kind,
            "status": "recorded",
        }

    def delete_record(self, record_id: str, operator: str = "") -> Dict[str, object]:
        """删除一条明细（软删，保留审计），并区分“商品已不存在”与“净额为零”。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM records WHERE record_id=? AND active=1", (record_id,)
            ).fetchone()
            if row is None:
                raise KeyError("明细不存在或已删除：%s" % record_id)
            store, sku = row["store"], row["sku"]
            with self._tx() as c:
                c.execute("UPDATE records SET active=0 WHERE record_id=?", (record_id,))
                remain = c.execute(
                    "SELECT amount FROM records WHERE active=1 AND store=? AND sku=?",
                    (store, sku),
                ).fetchall()
            net = q2(sum((D(r["amount"]) for r in remain), Decimal("0")))
            remain_count = len(remain)
            if remain_count == 0:
                state = "absent"
                message = "商品 %s 已无任何明细（已不存在）" % sku
            elif net == 0:
                state = "zero"
                message = "商品 %s 仍有 %d 条明细，净额为零" % (sku, remain_count)
            else:
                state = "present"
                message = "商品 %s 仍有 %d 条明细，净额 %s" % (sku, remain_count, money_str(net))
            self._log_event(
                "record_deleted",
                shard_id=row["shard_id"],
                record_id=record_id,
                store=store,
                sku=sku,
                amount=money_str(row["amount"]),
                kind=row["kind"],
                payload={
                    "state": state,
                    "remaining_records": remain_count,
                    "remaining_net": money_str(net),
                },
                operator=operator,
            )
        return {
            "record_id": record_id,
            "store": store,
            "sku": sku,
            "state": state,
            "remaining_records": remain_count,
            "remaining_net": money_str(net),
            "message": message,
        }

    def _row_to_record(self, r: sqlite3.Row) -> Record:
        return Record(
            record_id=r["record_id"],
            store=r["store"],
            sku=r["sku"],
            qty=int(r["qty"]),
            amount=Decimal(r["amount"]),
            kind=r["kind"],
            batch_id=r["batch_id"] or "",
            ts=r["ts"] or "",
            note=r["note"] or "",
        )

    def active_records(self) -> List[Record]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records WHERE active=1 ORDER BY record_id"
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def recent_records(self, limit: int = 30) -> List[Dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records WHERE active=1 ORDER BY rowid DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {
                "record_id": r["record_id"],
                "shard_id": r["shard_id"],
                "store": r["store"],
                "sku": r["sku"],
                "qty": r["qty"],
                "amount": r["amount"],
                "kind": r["kind"],
                "note": r["note"],
                "batch_id": r["batch_id"],
                "ts": r["ts"],
            }
            for r in rows
        ]

    def record(self, record_id: str) -> Optional[Dict[str, object]]:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM records WHERE record_id=?", (record_id,)
            ).fetchone()
        return dict(r) if r else None

    def events(self, limit: int = 100) -> List[Dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def stores(self) -> List[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT store FROM records WHERE active=1 ORDER BY store"
            ).fetchall()
        return [r["store"] for r in rows]

    # -- 快照 ------------------------------------------------------------
    def shard_ever_had_data(self, shard_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT (SELECT COUNT(*) FROM batches WHERE shard_id=?) + "
                "(SELECT COUNT(*) FROM records WHERE shard_id=?) AS n",
                (shard_id, shard_id),
            ).fetchone()
        return bool(row and int(row["n"]) > 0)

    def build_snapshot(
        self,
    ) -> Tuple[List[ShardSlice], List[Dict[str, str]], List[Dict[str, str]]]:
        """按当前有效明细构造分片快照。

        返回 (快照, 阻断发布的分片, 未启用分片)：

        * 被标记为暂时退出（active=0）的分片**阻断发布**：它曾参与榜单，
          缺席就意味着半份输入。
        * 启用中、曾经有过数据但现在没有任何有效明细的分片同样阻断（数据缺失
          或被清空）。
        * 从未有过任何批次与明细的分片视为“未启用”，不参与计算也不阻断；
          这允许在只启用部分分片的小规模部署里发布完整榜单。
        """
        shards = self.list_shards()
        slices: List[ShardSlice] = []
        missing: List[Dict[str, str]] = []
        uninitialized: List[Dict[str, str]] = []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records WHERE active=1 ORDER BY record_id"
            ).fetchall()
        grouped: Dict[str, List[Record]] = {}
        for r in rows:
            grouped.setdefault(r["shard_id"], []).append(self._row_to_record(r))
        for sh in shards:
            sid = str(sh["shard_id"])
            recs = grouped.get(sid, [])
            if not sh["active"]:
                missing.append(
                    {
                        "shard_id": sid,
                        "label": str(sh["label"]),
                        "reason": "暂时退出（已暂停参与发布），拒绝出半份榜单",
                    }
                )
                continue
            if not recs:
                if self.shard_ever_had_data(sid):
                    missing.append(
                        {
                            "shard_id": sid,
                            "label": str(sh["label"]),
                            "reason": "曾参与计算，但当前没有有效明细，输入不完整",
                        }
                    )
                else:
                    uninitialized.append(
                        {
                            "shard_id": sid,
                            "label": str(sh["label"]),
                            "reason": "从未导入明细，未启用",
                        }
                    )
                continue
            batches = sorted({r.batch_id for r in recs if r.batch_id and r.batch_id != "web"})
            slices.append(ShardSlice.build(sid, str(sh["label"]), batches, recs))
        return slices, missing, uninitialized

    # -- 版本 ------------------------------------------------------------
    def _next_version_no(self) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(version_no),0)+1 AS n FROM rank_versions"
        ).fetchone()
        return int(row["n"])

    def publish(
        self,
        *,
        mode: str,
        limit_n: int,
        stores: Sequence[str],
        note: str = "",
    ) -> PublishOutcome:
        """计算并原子发布一个榜单版本；输入不完整或对照不通过则抛错、不写版本。"""
        if mode not in MODES:
            raise ValueError("未知取 N 规则")
        if limit_n <= 0:
            raise ValueError("N 必须为正整数")
        store_list = sorted({s for s in stores if s})
        if not store_list:
            raise ValueError("请选择至少一个门店")
        with self._lock:
            slices, missing, uninitialized = self.build_snapshot()
            if missing:
                raise SnapshotIncomplete(missing)
            if not slices:
                raise ValueError("没有任何分片提供明细，无法发布")
            records = [r for s in slices for r in s.records]
            available = {r.store for r in records}
            absent_stores = [s for s in store_list if s not in available]
            if absent_stores:
                raise ValueError("所选门店在分片中没有任何明细：%s" % "、".join(absent_stores))
            result = rank(mode, limit_n, records, slices, store_list)
            if not result.verification.ok:
                raise ValueError(
                    "补取结果未通过全量分组排序对照，拒绝发布："
                    + "；".join(result.verification.mismatches)
                )
            version_no = self._next_version_no()
            created_at = now_iso()
            payload = result_json(result)
            basis = {
                "mode": mode,
                "n": limit_n,
                "stores": store_list,
                "shard_ids": [s.shard_id for s in slices],
                "batches": {s.shard_id: list(s.batches) for s in slices},
                "records_per_shard": {s.shard_id: s.record_count for s in slices},
                "record_digest": records_digest(records),
                "shard_digests": [s.digest() for s in slices],
                "verification": result.verification.to_dict(),
                "totals": result.totals,
                "input_range": {
                    "record_ids": {s.shard_id: [r.record_id for r in s.records] for s in slices},
                    "shard_ids": [s.shard_id for s in slices],
                    "batch_ids": {s.shard_id: list(s.batches) for s in slices},
                },
            }
            with self._tx() as c:
                c.execute(
                    "INSERT INTO rank_versions(version_no,mode,limit_n,stores,shard_ids,batch_ids,"
                    "record_ids,record_digest,result_json,basis_json,verification_ok,created_at,note) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        version_no,
                        mode,
                        limit_n,
                        json.dumps(store_list, ensure_ascii=False),
                        json.dumps([s.shard_id for s in slices], ensure_ascii=False),
                        json.dumps(
                            {s.shard_id: list(s.batches) for s in slices}, ensure_ascii=False
                        ),
                        json.dumps(
                            {s.shard_id: [r.record_id for r in s.records] for s in slices},
                            ensure_ascii=False,
                        ),
                        records_digest(records),
                        json.dumps(payload, ensure_ascii=False),
                        json.dumps(basis, ensure_ascii=False, default=str),
                        1,
                        created_at,
                        note,
                    ),
                )
            self._log_event(
                "version_published",
                payload={
                    "version_no": version_no,
                    "mode": mode,
                    "n": limit_n,
                    "stores": store_list,
                },
            )
        return PublishOutcome(
            version_no=version_no, result=payload, basis=basis, created_at=created_at
        )

    def versions(self, limit: int = 100) -> List[Dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,version_no,mode,limit_n,stores,shard_ids,batch_ids,record_ids,"
                "record_digest,verification_ok,created_at,note "
                "FROM rank_versions ORDER BY version_no DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["stores"] = json.loads(d["stores"])
            d["shard_ids"] = json.loads(d["shard_ids"])
            d["batch_ids"] = json.loads(d["batch_ids"])
            d["record_ids"] = json.loads(d["record_ids"])
            d["verification_ok"] = bool(d["verification_ok"])
            d["record_count"] = sum(len(v) for v in d["record_ids"].values())
            out.append(d)
        return out

    def version(self, version_no: int) -> Optional[Dict[str, object]]:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM rank_versions WHERE version_no=?", (version_no,)
            ).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["stores"] = json.loads(d["stores"])
        d["shard_ids"] = json.loads(d["shard_ids"])
        d["batch_ids"] = json.loads(d["batch_ids"])
        d["record_ids"] = json.loads(d["record_ids"])
        d["result"] = json.loads(d["result_json"])
        d["basis"] = json.loads(d["basis_json"])
        d["verification_ok"] = bool(d["verification_ok"])
        return d

    def latest_version(self) -> Optional[Dict[str, object]]:
        with self._lock:
            r = self._conn.execute("SELECT MAX(version_no) AS v FROM rank_versions").fetchone()
        if not r or r["v"] is None:
            return None
        return self.version(int(r["v"]))

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            r = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def set_meta(self, key: str, value: str) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # -- 对照 ------------------------------------------------------------
    def compare_versions(
        self, before_no: Optional[int] = None, after_no: Optional[int] = None
    ) -> Dict[str, object]:
        """比较两版榜单：入榜、出榜、名次变化（按 门店/商品 匹配）。"""
        with self._lock:
            if after_no is None:
                row = self._conn.execute(
                    "SELECT MAX(version_no) AS v FROM rank_versions"
                ).fetchone()
                after_no = int(row["v"]) if row and row["v"] is not None else None
            if before_no is None and after_no is not None:
                row = self._conn.execute(
                    "SELECT MAX(version_no) AS v FROM rank_versions WHERE version_no < ?",
                    (after_no,),
                ).fetchone()
                before_no = int(row["v"]) if row and row["v"] is not None else None
        after = self.version(after_no) if after_no is not None else None
        if after is None:
            raise KeyError("目标版本不存在")
        before = self.version(before_no) if before_no is not None else None

        def flatten(version: Optional[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
            out: Dict[str, Dict[str, object]] = {}
            if not version:
                return out
            for store, entries in (version["result"].get("entries_by_store") or {}).items():
                for e in entries:
                    item = dict(e)
                    item["store"] = store
                    out["%s/%s" % (store, e["sku"])] = item
            return out

        bmap = flatten(before)
        amap = flatten(after)
        entered = [amap[k] for k in sorted(set(amap) - set(bmap))]
        exited = [bmap[k] for k in sorted(set(bmap) - set(amap))]
        moved = []
        for k in sorted(set(amap) & set(bmap)):
            b, a = bmap[k], amap[k]
            delta_rank = int(b["rank"]) - int(a["rank"])
            delta_net = q2(Decimal(a["net"]) - Decimal(b["net"]))
            if delta_rank != 0 or delta_net != 0:
                moved.append(
                    {
                        "store": a["store"],
                        "sku": a["sku"],
                        "before_rank": b["rank"],
                        "after_rank": a["rank"],
                        "before_net": b["net"],
                        "after_net": a["net"],
                        "delta_rank": delta_rank,
                        "delta_net": money_str(delta_net),
                    }
                )
        moved.sort(key=lambda x: (-abs(x["delta_rank"]), x["sku"]))
        return {
            "before": {
                "version_no": before["version_no"] if before else None,
                "created_at": before["created_at"] if before else None,
                "mode": before["mode"] if before else None,
                "n": before["limit_n"] if before else None,
            },
            "after": {
                "version_no": after["version_no"],
                "created_at": after["created_at"],
                "mode": after["mode"],
                "n": after["limit_n"],
            },
            "entered": entered,
            "exited": exited,
            "moved": moved,
            "unchanged": len(set(amap) & set(bmap)) - len(moved),
        }

    def state_summary(self) -> Dict[str, object]:
        with self._lock:
            row = self._conn.execute(
                """SELECT (SELECT COUNT(*) FROM records WHERE active=1) AS records,
                          (SELECT COUNT(*) FROM batches WHERE accepted=1) AS batches,
                          (SELECT COUNT(DISTINCT store||'/'||sku) FROM records WHERE active=1) AS pairs,
                          (SELECT COUNT(*) FROM rank_versions) AS versions"""
            ).fetchone()
            maxv = self._conn.execute(
                "SELECT MAX(version_no) AS v FROM rank_versions"
            ).fetchone()
        return {
            "records": int(row["records"]),
            "batches": int(row["batches"]),
            "pairs": int(row["pairs"]),
            "versions": int(row["versions"]),
            "latest_version": int(maxv["v"]) if maxv and maxv["v"] is not None else None,
        }


def result_json(result: RankResult) -> Dict[str, object]:
    """把 RankResult 序列化为网页使用的 JSON（含补取过程与对照）。"""

    def entry_dict(e) -> Dict[str, object]:
        return {
            "rank": e.rank,
            "store": e.store,
            "sku": e.sku,
            "net": money_str(e.net),
            "count": e.count,
            "tier": e.tier,
            "tie": e.tie,
            "entry_kind": e.entry_kind,
        }

    reports: Dict[str, object] = {}
    for store, rep in result.reports.items():
        reports[store] = {
            "store": rep.store,
            "local_candidate_count": rep.local_candidate_count,
            "candidate_count": rep.candidate_count,
            "fetched_bucket_count": rep.fetched_bucket_count,
            "fetched_record_count": rep.fetched_record_count,
            "nth_net": money_str(rep.nth_net) if rep.nth_net is not None else None,
            "upper_bound": money_str(rep.upper_bound),
            "threshold": money_str(rep.threshold) if rep.threshold is not None else None,
            "stop_reason": rep.stop_reason,
            "local_top": rep.local_top,
            "fetched_skus": rep.fetched_skus,
            "rounds": [
                {
                    "round_no": r.round_no,
                    "threshold_used": (
                        money_str(r.threshold_used) if r.threshold_used is not None else None
                    ),
                    "next_threshold": (
                        money_str(r.next_threshold) if r.next_threshold is not None else None
                    ),
                    "cutoffs": r.cutoffs,
                    "upper_bound": money_str(r.upper_bound),
                    "candidate_count": r.candidate_count,
                    "nth_net": money_str(r.nth_net) if r.nth_net is not None else None,
                    "entries_count": r.entries_count,
                    "new_buckets": r.new_buckets,
                    "new_records": r.new_records,
                    "new_candidates": r.new_candidates,
                    "stop": r.stop,
                    "reason": r.reason,
                }
                for r in rep.rounds
            ],
            "notes": rep.notes,
            "entries": [entry_dict(e) for e in rep.entries],
        }
    return {
        "mode": result.mode,
        "n": result.limit,
        "entries_by_store": {
            store: [entry_dict(e) for e in entries]
            for store, entries in result.entries_by_store.items()
        },
        "reports": reports,
        "verification": result.verification.to_dict(),
        "totals": result.totals,
        "shards": result.shards,
    }
