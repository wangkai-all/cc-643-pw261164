"""命令行入口：python -m rankapp [--port 8765] [--db var/rankapp.sqlite3]"""

from __future__ import annotations

import argparse
import sys

from .server import run


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="rankapp", description="门店商品销售榜（本地应用）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default="var/rankapp.sqlite3")
    args = parser.parse_args(argv)
    run(host=args.host, port=args.port, db_path=args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
