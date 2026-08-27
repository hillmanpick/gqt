#!/usr/bin/env python3
"""Export settled event-contract predictions as JSONL training records.

The exporter is read-only. It keeps every settled BTC/ETH prediction as one
supervised sample so an external agent/LLM can learn from both correct and
incorrect direction calls.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


SUPPORTED_SYMBOLS = {"BTCUSDT", "ETHUSDT"}


def sqlite_uri(path: Path) -> str:
    return "file:" + str(path).replace("\\", "/") + "?mode=ro&immutable=1"


def parse_features(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def timestamp_text(value: int | None) -> str | None:
    if value is None:
        return None
    seconds = value / 1000 if value > 10_000_000_000 else value
    return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).isoformat()


def outcome_direction(move_percent: float | None) -> str:
    if move_percent is None or abs(move_percent) <= 0.000001:
        return "flat"
    return "up" if move_percent > 0 else "down"


def row_to_record(row: sqlite3.Row) -> dict[str, Any]:
    features = parse_features(row["features_json"])
    predicted_direction = row["direction"]
    realized_direction = outcome_direction(row["move_percent"])
    correct = row["result"] == "win"
    return {
        "id": row["id"],
        "created_at": timestamp_text(row["created_at"]),
        "open_time": timestamp_text(row["open_time"]),
        "close_time": timestamp_text(row["close_time"]),
        "settled_at": timestamp_text(row["settled_at"]),
        "symbol": row["symbol"],
        "horizon_minutes": row["horizon_minutes"],
        "predicted_direction": predicted_direction,
        "realized_direction": realized_direction,
        "correct": correct,
        "result": row["result"],
        "confidence": row["confidence"],
        "score": row["score"],
        "stake_amount": row["stake_amount"],
        "entry_price": row["entry_price"],
        "expiry_price": row["expiry_price"],
        "move_percent": row["move_percent"],
        "virtual_pnl": row["virtual_pnl"],
        "features": features,
        "label": {
            "direction": realized_direction,
            "would_win_up": realized_direction == "up",
            "would_win_down": realized_direction == "down",
        },
    }


def record_to_chat(record: dict[str, Any]) -> dict[str, Any]:
    user_payload = {
        "symbol": record["symbol"],
        "horizon_minutes": record["horizon_minutes"],
        "entry_price": record["entry_price"],
        "score": record["score"],
        "confidence": record["confidence"],
        "features": record["features"],
    }
    assistant_payload = {
        "direction": record["realized_direction"],
        "correct_previous_prediction": record["correct"],
        "move_percent": record["move_percent"],
    }
    return {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an event-contract direction agent. "
                    "Predict whether price will finish up or down at expiry."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False, separators=(",", ":")),
            },
            {
                "role": "assistant",
                "content": json.dumps(
                    assistant_payload, ensure_ascii=False, separators=(",", ":")
                ),
            },
        ],
        "metadata": {
            "id": record["id"],
            "result": record["result"],
            "virtual_pnl": record["virtual_pnl"],
        },
    }


def export(args: argparse.Namespace) -> int:
    connection = sqlite3.connect(sqlite_uri(args.sqlite), uri=True)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT id, created_at, symbol, horizon_minutes, open_time, close_time,
               direction, confidence, score, stake_amount, entry_price,
               expiry_price, status, result, move_percent, virtual_pnl,
               features_json, settled_at
          FROM event_prediction_tickets
         WHERE status = 'settled'
           AND result IN ('win', 'loss', 'tie')
           AND symbol IN ('BTCUSDT', 'ETHUSDT')
           AND horizon_minutes IN (30, 60)
         ORDER BY created_at ASC, symbol ASC, horizon_minutes ASC
        """
    )

    count = 0
    with args.output.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            if row["symbol"] not in SUPPORTED_SYMBOLS:
                continue
            record = row_to_record(row)
            if args.format == "chat":
                record = record_to_chat(record)
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
            count += 1

    print(f"exported {count} records to {args.output}")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite", type=Path, help="Path to event_predictions.sqlite")
    parser.add_argument("output", type=Path, help="Output JSONL path")
    parser.add_argument(
        "--format",
        choices=["record", "chat"],
        default="record",
        help="record keeps structured samples; chat emits messages-style records",
    )
    args = parser.parse_args(argv)
    if not args.sqlite.exists():
        parser.error(f"SQLite file not found: {args.sqlite}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    return export(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
