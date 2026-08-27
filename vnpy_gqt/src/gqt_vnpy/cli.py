from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .backtest import run_research, write_result
from .config import ResearchConfig
from .data_import import import_to_vnpy


def _date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gqt-vnpy")
    commands = parser.add_subparsers(dest="command", required=True)

    import_command = commands.add_parser("import-freqtrade", help="import existing Feather candles")
    import_command.add_argument("--source", required=True)
    import_command.add_argument("--symbol", choices=("BTCUSDT", "ETHUSDT"), required=True)
    import_command.add_argument("--timeframe", choices=("15m", "1h", "4h"), required=True)

    backtest = commands.add_parser("backtest", help="run isolated walk-forward backtests")
    backtest.add_argument("--config", required=True)
    backtest.add_argument("--start", type=_date, required=True)
    backtest.add_argument("--end", type=_date, required=True)
    backtest.add_argument("--output", default="results")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "import-freqtrade":
        report = import_to_vnpy(args.source, args.symbol, args.timeframe)
        print(json.dumps(report.__dict__, ensure_ascii=False, indent=2))
        return 0
    if args.command == "backtest":
        config = ResearchConfig.load(Path(args.config))
        result = run_research(config, args.start, args.end)
        output = write_result(result, args.output)
        print(json.dumps({"output": str(output), **result}, ensure_ascii=False, indent=2, default=str))
        return 0 if result["portfolio_eligible"] else 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
