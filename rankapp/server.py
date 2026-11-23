"""HTTP 应用：标准库 ThreadingHTTPServer，无第三方依赖。

路由
====

GET  /                          网页（录入、榜单、补取过程、版本对照）
GET  /api/state                 分片、批次、明细、版本、事件概要
GET  /api/rank?mode&n&stores    对当前明细即时计算榜单（不落版本）
GET  /api/version/<no>          某版榜单（含输入范围）
GET  /api/compare?before&after  两版对照
POST /api/entries               录入销售 / 退货 / 历史更正
POST /api/delete                删除一条明细
POST /api/batches               导入明细文件（幂等）
POST /api/publish               发布榜单版本
POST /api/shard                 分片上线 / 暂时退出
POST /api/seed                  生成一份演示数据（可选，非榜单结果）
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from . import __version__
from .ingest import parse_file
from .money import D, money_str, parse_int, q2
from .store import Store, SnapshotIncomplete, result_json

ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = ROOT / "web"
SAMPLE_DIR = ROOT / "sample_shards"


class ValidationError(Exception):
    pass


def _parse_stores(values) -> list:
    if isinstance(values, str):
        values = [values]
    out = []
    for v in values or []:
        for part in str(v).replace("，", ",").split(","):
            part = part.strip()
            if part and part not in out:
                out.append(part)
    return out


def _validate_mode(mode: str) -> str:
    if mode not in {"strict", "keep_ties"}:
        raise ValidationError("取 N 规则只能是 strict 或 keep_ties")
    return mode


class Api:
    def __init__(self, store: Store):
        self.store = store

    def handle(self, method: str, path: str, query: Dict[str, list], body: Optional[Dict]) -> Tuple[int, object]:
        body = body or {}
        if method == "GET" and path in {"/api/state", "/api/state/"}:
            return 200, self.state()
        if method == "GET" and path in {"/api/rank", "/api/rank/"}:
            return 200, self.rank_query(query)
        if method == "GET" and path.startswith("/api/version/"):
            no = parse_int(path.rsplit("/", 1)[-1], "版本号")
            v = self.store.version(no)
            if v is None:
                raise KeyError("版本不存在：%d" % no)
            return 200, v
        if method == "GET" and path in {"/api/compare", "/api/compare/"}:
            before = query.get("before", [None])[0]
            after = query.get("after", [None])[0]
            return 200, self.store.compare_versions(
                parse_int(before, "起始版本") if before else None,
                parse_int(after, "目标版本") if after else None,
            )
        if method == "GET" and path in {"/api/health", "/api/health/"}:
            return 200, {"ok": True, "version": __version__}
        if method == "POST" and path in {"/api/entries", "/api/entries/"}:
            return 200, self.add_entry(body)
        if method == "POST" and path in {"/api/delete", "/api/delete/"}:
            return 200, self.store.delete_record(
                _str(body, "record_id"), str(body.get("operator") or "")
            )
        if method == "POST" and path in {"/api/batches", "/api/batches/"}:
            return 200, self.import_batches(body)
        if method == "POST" and path in {"/api/publish", "/api/publish/"}:
            return 200, self.publish(body)
        if method == "POST" and path in {"/api/shard", "/api/shard/"}:
            active = body.get("active")
            if not isinstance(active, bool):
                raise ValidationError("active 必须是 true/false")
            return 200, self.store.set_shard_active(_str(body, "shard_id"), active)
        if method == "POST" and path in {"/api/seed", "/api/seed/"}:
            return 200, self.seed(force=bool(body.get("force")))
        if method == "POST" and path in {"/api/events", "/api/events/"}:
            return 200, {"events": self.store.events(limit=parse_int(body.get("limit", 100)))}
        raise KeyError("未知接口：%s %s" % (method, path))

    # -- 只读 ------------------------------------------------------------
    def state(self) -> Dict[str, object]:
        return {
            "version": __version__,
            "summary": self.store.state_summary(),
            "shards": self.store.list_shards(),
            "stores": self.store.stores(),
            "batches": self.store.list_batches(limit=50),
            "records": self.store.recent_records(limit=40),
            "versions": self.store.versions(limit=30),
            "events": self.store.events(limit=40),
            "latest": self._latest_summary(),
            "samples": sorted(p.name for p in SAMPLE_DIR.glob("*") if p.is_file()) if SAMPLE_DIR.exists() else [],
        }

    def _latest_summary(self) -> Optional[Dict[str, object]]:
        v = self.store.latest_version()
        if v is None:
            return None
        return {
            "version_no": v["version_no"],
            "mode": v["mode"],
            "n": v["limit_n"],
            "stores": v["stores"],
            "created_at": v["created_at"],
            "verification_ok": v["verification_ok"],
        }

    def rank_query(self, query: Dict[str, list]) -> Dict[str, object]:
        mode = _validate_mode(query.get("mode", ["strict"])[0])
        n = parse_int(query.get("n", ["5"])[0], "N")
        if n <= 0 or n > 1000:
            raise ValidationError("N 需在 1..1000 之间")
        stores = _parse_stores(query.get("stores", []))
        if not stores:
            stores = self.store.stores()
        if not stores:
            raise ValidationError("暂无门店数据，请先导入或录入明细")
        slices, missing, uninitialized = self.store.build_snapshot()
        from .ranking import rank

        records = [r for s in slices for r in s.records]
        if not records:
            raise ValidationError("没有任何分片提供明细，请先导入或录入")
        result = rank(mode, n, records, slices, stores)
        return {
            "mode": mode,
            "n": n,
            "stores": stores,
            "missing_shards": missing,
            "uninitialized_shards": uninitialized,
            "can_publish": bool(slices) and not missing,
            "result": result_json(result),
        }

    # -- 写入 ------------------------------------------------------------
    def add_entry(self, body: Dict[str, object]) -> Dict[str, object]:
        kind = str(body.get("kind") or "sale")
        if kind not in {"sale", "return", "correction"}:
            raise ValidationError("类型只能是 sale / return / correction")
        store = _str(body, "store")
        sku = _str(body, "sku")
        qty = parse_int(body.get("qty", 1), "数量")
        amount = D(body.get("amount"))
        note = str(body.get("note") or "")
        ts = str(body.get("ts") or "")
        shard_id = str(body.get("shard_id") or "S1")
        operator = str(body.get("operator") or "")
        return self.store.add_entry(
            shard_id=shard_id,
            store=store,
            sku=sku,
            qty=qty,
            amount=amount,
            kind=kind,
            record_id=str(body.get("record_id") or ""),
            ts=ts,
            note=note,
            operator=operator,
        )

    def import_batches(self, body: Dict[str, object]) -> Dict[str, object]:
        """导入文件：接受 sample_shards 内的文件名，或任意本地路径。

        先全部解析校验，再逐批入库；某一批校验失败时不会写入任何一批，
        避免“传了一半”。
        """
        names = body.get("files") or []
        if isinstance(names, str):
            names = [names]
        if not names:
            raise ValidationError("请提供至少一个文件")
        parsed_batches = []
        for name in names:
            name = str(name).strip()
            if not name:
                continue
            if name.startswith("sample:"):
                path = SAMPLE_DIR / name.split(":", 1)[1]
            else:
                candidate = Path(name)
                path = candidate if candidate.is_absolute() else (SAMPLE_DIR / candidate)
            if not path.exists():
                raise ValidationError("文件不存在：%s" % name)
            parsed_batches.append(parse_file(path))
        if not parsed_batches:
            raise ValidationError("没有可导入的文件")
        results = []
        for parsed in parsed_batches:
            results.append(
                self.store.ingest_batch(
                    shard_id=parsed.shard_id,
                    batch_id=str(body.get("batch_id") or parsed.batch_id),
                    file_sha256=parsed.file_sha256,
                    records=parsed.records,
                    file_name=parsed.file_name,
                    operator=str(body.get("operator") or ""),
                )
            )
        return {"results": results, "summary": self.store.state_summary()}

    def publish(self, body: Dict[str, object]) -> Dict[str, object]:
        mode = _validate_mode(str(body.get("mode") or "strict"))
        n = parse_int(body.get("n", 5), "N")
        if n <= 0 or n > 1000:
            raise ValidationError("N 需在 1..1000 之间")
        stores = _parse_stores(body.get("stores") or [])
        stores = stores or self.store.stores()
        if not stores:
            raise ValidationError("暂无门店数据，无法发布")
        outcome = self.store.publish(
            mode=mode, limit_n=n, stores=stores, note=str(body.get("note") or "")
        )
        return {
            "version_no": outcome.version_no,
            "created_at": outcome.created_at,
            "result": outcome.result,
            "basis": outcome.basis,
        }

    # -- 演示数据 --------------------------------------------------------
    def seed(self, force: bool = False) -> Dict[str, object]:
        summary = self.store.state_summary()
        if summary["versions"] and not force:
            raise ValidationError("已有发布版本，若确需重置演示数据请加 force")
        if summary["records"] and not force:
            raise ValidationError("已有明细，若确需重置演示数据请加 force")
        from .seed import seed_demo

        return seed_demo(self.store)


class Handler(BaseHTTPRequestHandler):
    server_version = "RankApp/" + __version__
    api: Api

    def log_message(self, fmt: str, *args) -> None:  # 静默常规访问日志
        if os.environ.get("RANKAPP_DEBUG"):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status: int, payload, content_type: str = "application/json; charset=utf-8") -> None:
        if isinstance(payload, (dict, list)):
            data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        elif isinstance(payload, bytes):
            data = payload
        else:
            data = str(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        )
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _error(self, status: int, message: str, extra: Optional[dict] = None) -> None:
        payload = {"error": message}
        if extra:
            payload.update(extra)
        self._send(status, payload)

    def _read_json(self) -> Dict[str, object]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > 2_000_000:
            raise ValidationError("请求体过大")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError("请求体不是合法 JSON：%s" % exc) from exc
        if not isinstance(data, dict):
            raise ValidationError("请求体必须是 JSON 对象")
        return data

    def _guard(self, fn: Callable[[], None]) -> None:
        try:
            fn()
        except ValidationError as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        except SnapshotIncomplete as exc:
            self._error(HTTPStatus.CONFLICT, str(exc), {"missing_shards": exc.missing})
        except KeyError as exc:
            self._error(HTTPStatus.NOT_FOUND, str(exc).strip("'\""))
        except ValueError as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        except Exception:  # pragma: no cover
            traceback.print_exc()
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "服务器内部错误，详见终端日志")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path in {"/", "/index.html"}:
            return self._guard(self._serve_static)
        if path.startswith("/static/"):
            return self._guard(lambda: self._serve_asset(path))
        def run():
            query = parse_qs(parsed.query, keep_blank_values=True)
            status, payload = self.api.handle("GET", path, query, None)
            self._send(status, payload)
        self._guard(run)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        def run():
            body = self._read_json()
            query = parse_qs(parsed.query, keep_blank_values=True)
            status, payload = self.api.handle("POST", parsed.path, query, body)
            self._send(status, payload)

        self._guard(run)

    def _serve_static(self) -> None:
        index = WEB_DIR / "index.html"
        if not index.exists():
            raise KeyError("网页资源缺失：web/index.html")
        self._send(HTTPStatus.OK, index.read_bytes(), "text/html; charset=utf-8")

    def _serve_asset(self, path: str) -> None:
        rel = path[len("/static/"):]
        target = (WEB_DIR / rel).resolve()
        if WEB_DIR.resolve() not in target.parents and target != WEB_DIR.resolve():
            raise ValidationError("非法资源路径")
        if not target.is_file():
            raise KeyError("资源不存在：%s" % rel)
        ctype = {
            ".js": "text/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
            ".json": "application/json; charset=utf-8",
        }.get(target.suffix, "application/octet-stream")
        self._send(HTTPStatus.OK, target.read_bytes(), ctype)


def _str(body: Dict[str, object], key: str) -> str:
    value = body.get(key)
    if value is None or str(value).strip() == "":
        raise ValidationError("缺少字段 %s" % key)
    return str(value).strip()


def run(host: str = "127.0.0.1", port: int = 8765, db_path: str = "var/rankapp.sqlite3") -> None:
    store = Store(db_path)
    handler_cls = type("BoundHandler", (Handler,), {"api": Api(store)})
    server = ThreadingHTTPServer((host, port), handler_cls)
    print("商品销售榜已启动： http://%s:%d/  (数据库 %s)" % (host, port, db_path))
    print("按 Ctrl+C 停止。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        server.server_close()
        store.close()
