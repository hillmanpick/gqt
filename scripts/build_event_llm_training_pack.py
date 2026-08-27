#!/usr/bin/env python3
"""Build an event-contract LLM training pack from local GQT SQLite data.

The script is read-only against SQLite and writes generated artifacts under an
ignored local output directory by default. It keeps sample volume high: every
settled BTC/ETH 10m/30m/60m ticket becomes a supervised label, while factor
reports are generated separately so strategy changes can be measured instead of
hand-waved. Active horizons remain separate so their payout and performance
statistics are not blended.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


SUPPORTED_SYMBOLS = ("BTCUSDT", "ETHUSDT")
SUPPORTED_HORIZONS = (10, 30, 60)
CURRENT_STRATEGY = "event_reinvest_cycle_v1"
KNOWN_STRATEGIES = (
    "legacy_raw",
    "direction_dataset_v2",
    "direction_dataset_v3",
    "direction_dataset_v4",
    "direction_dataset_v5",
    "direction_dataset_v6",
    "direction_dataset_v7",
    "direction_dataset_v8",
    "direction_dataset_v9",
    "direction_dataset_v10",
    "direction_dataset_v10_1",
    "direction_dataset_v10_2",
    "event_reinvest_cycle_v1",
)
DEFAULT_OUTPUT_DIR = Path("data/event_llm_training")

FACTOR_DESCRIPTIONS = {
    "ret3": "3-minute raw return before prediction.",
    "ret10": "10-minute raw return before prediction.",
    "ret30": "30-minute raw return before prediction.",
    "ret60": "60-minute raw return before prediction.",
    "volatility": "Mean absolute 1m return over the recent window.",
    "momentum3": "3-minute return normalized by recent volatility.",
    "momentum10": "10-minute return normalized by recent volatility.",
    "momentum30": "30-minute return normalized by recent volatility.",
    "momentum60": "60-minute return normalized by recent volatility.",
    "ema_short": "Short EMA bias, roughly 8/21.",
    "ema_mid": "Mid EMA bias, roughly 13/34.",
    "ema_long": "Long EMA bias, roughly 21/55.",
    "rsi": "14-period RSI.",
    "rsi_trend": "RSI transformed around 50 and clipped to [-1, 1].",
    "volume_ratio": "Recent volume relative to its trailing average.",
    "volume_bias": "Momentum-signed volume confirmation.",
    "breakout": "Position versus recent high/low range.",
    "long_short_bias": "Binance long/short ratio transformed around neutral.",
    "funding_bias": "Funding-rate contrarian bias.",
    "sentiment": "Combined long/short and funding bias.",
    "snapshot_change_percent": "Exchange 24h change percentage at prediction time.",
    "raw_score": "Original deterministic score before direction calibration.",
    "final_score": "Score signed by the strategy's final direction.",
    "score": "Ticket score written to the virtual order.",
    "confidence": "Ticket confidence written to the virtual order.",
    "open_hour_utc": "UTC hour when the event opened.",
    "open_day_of_week_utc": "UTC day of week, Monday=0.",
}

LEAKY_OR_NON_NUMERIC_FEATURES = {
    "strategy_version",
    "raw_direction",
    "final_direction",
    "direction_flipped",
    "direction_reason",
    "horizon_minutes",
}


def default_runtime_database() -> Path | None:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if not local_app_data:
        return None
    return (
        Path(local_app_data)
        / "HillmanPick"
        / "GQT Trader"
        / "data"
        / "trading"
        / "user_data"
        / "event_predictions.sqlite"
    )


def sqlite_uri(path: Path) -> str:
    return "file:" + str(path.resolve()).replace("\\", "/") + "?mode=ro&immutable=1"


def timestamp_seconds(value: int | float | None) -> float | None:
    if value is None:
        return None
    if value > 10_000_000_000:
        return float(value) / 1000.0
    return float(value)


def timestamp_text(value: int | float | None) -> str | None:
    seconds = timestamp_seconds(value)
    if seconds is None:
        return None
    return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).isoformat()


def safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def parse_features(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def realized_direction(move_percent: float | None) -> str:
    if move_percent is None or abs(move_percent) <= 0.000001:
        return "flat"
    return "up" if move_percent > 0 else "down"


def strategy_version(features: dict[str, Any]) -> str:
    value = features.get("strategy_version")
    return value if isinstance(value, str) and value else "legacy_raw"


def load_rows(database: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(sqlite_uri(database), uri=True)
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
           AND horizon_minutes IN (10, 30, 60)
         ORDER BY created_at ASC, symbol ASC, horizon_minutes ASC
        """
    ).fetchall()
    records: list[dict[str, Any]] = []
    for row in rows:
        features = parse_features(row["features_json"])
        move_percent = safe_float(row["move_percent"])
        direction = realized_direction(move_percent)
        created_seconds = timestamp_seconds(row["created_at"])
        open_seconds = timestamp_seconds(row["open_time"])
        open_dt = (
            dt.datetime.fromtimestamp(open_seconds, tz=dt.timezone.utc)
            if open_seconds is not None
            else None
        )
        normalized_features = dict(features)
        normalized_features["score"] = safe_float(row["score"])
        normalized_features["confidence"] = safe_float(row["confidence"])
        if open_dt is not None:
            normalized_features["open_hour_utc"] = open_dt.hour
            normalized_features["open_day_of_week_utc"] = open_dt.weekday()
        records.append(
            {
                "id": row["id"],
                "created_at": timestamp_text(row["created_at"]),
                "created_seconds": created_seconds,
                "open_time": timestamp_text(row["open_time"]),
                "close_time": timestamp_text(row["close_time"]),
                "settled_at": timestamp_text(row["settled_at"]),
                "symbol": row["symbol"],
                "horizon_minutes": int(row["horizon_minutes"]),
                "baseline_direction": row["direction"],
                "baseline_correct": row["result"] == "win",
                "result": row["result"],
                "confidence": safe_float(row["confidence"]),
                "score": safe_float(row["score"]),
                "stake_amount": safe_float(row["stake_amount"]),
                "entry_price": safe_float(row["entry_price"]),
                "expiry_price": safe_float(row["expiry_price"]),
                "move_percent": move_percent,
                "virtual_pnl": safe_float(row["virtual_pnl"]),
                "features": normalized_features,
                "strategy_version": strategy_version(features),
                "label_direction": direction,
                "would_win_up": direction == "up",
                "would_win_down": direction == "down",
            }
        )
    return records


def filter_records(records: list[dict[str, Any]], strategy: str) -> list[dict[str, Any]]:
    if strategy == "all":
        return records
    if strategy == "current":
        return [record for record in records if record["strategy_version"] == CURRENT_STRATEGY]
    return [record for record in records if record["strategy_version"] == strategy]


def factor_payload(record: dict[str, Any]) -> dict[str, Any]:
    features = record["features"]
    payload: dict[str, Any] = {}
    for key in sorted(features):
        value = features[key]
        if key in {"horizon_minutes"}:
            continue
        if isinstance(value, float):
            payload[key] = round(value, 8)
        else:
            payload[key] = value
    return payload


def record_json(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "task": "binance_event_contract_direction",
        "id": record["id"],
        "input": {
            "symbol": record["symbol"],
            "horizon_minutes": record["horizon_minutes"],
            "open_time": record["open_time"],
            "entry_price": record["entry_price"],
            "baseline": {
                "strategy_version": record["strategy_version"],
                "direction": record["baseline_direction"],
                "score": record["score"],
                "confidence": record["confidence"],
            },
            "factors": factor_payload(record),
        },
        "output": {
            "direction": record["label_direction"],
            "move_percent": record["move_percent"],
            "would_win_up": record["would_win_up"],
            "would_win_down": record["would_win_down"],
        },
        "metadata": {
            "result": record["result"],
            "virtual_pnl": record["virtual_pnl"],
            "close_time": record["close_time"],
            "settled_at": record["settled_at"],
        },
    }


def chat_json(record: dict[str, Any]) -> dict[str, Any]:
    sample = record_json(record)
    user_payload = sample["input"]
    assistant_payload = sample["output"]
    return {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a BTC/ETH event-contract direction model. "
                    "Given only pre-expiry market factors, answer JSON with "
                    "the realized expiry direction: up, down, or flat."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    user_payload, ensure_ascii=False, separators=(",", ":")
                ),
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
            "symbol": record["symbol"],
            "horizon_minutes": record["horizon_minutes"],
            "strategy_version": record["strategy_version"],
        },
    }


def split_chronologically(
    records: list[dict[str, Any]], train_ratio: float, validation_ratio: float
) -> dict[str, list[dict[str, Any]]]:
    ordered = sorted(records, key=lambda record: (record["created_seconds"] or 0, record["id"]))
    total = len(ordered)
    train_end = int(total * train_ratio)
    validation_end = int(total * (train_ratio + validation_ratio))
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "test": ordered[validation_end:],
    }


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
            count += 1
    return count


def pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def breakeven_rate(horizon_minutes: int) -> float:
    return 1.0 / (1.0 + (0.80 if horizon_minutes == 10 else 0.85))


def aggregate(records: Iterable[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[str(record[key])].append(record)
    output: dict[str, dict[str, Any]] = {}
    for name, items in sorted(groups.items()):
        decisive = [item for item in items if item["label_direction"] in {"up", "down"}]
        baseline_wins = sum(1 for item in decisive if item["baseline_correct"])
        up_count = sum(1 for item in decisive if item["label_direction"] == "up")
        output[name] = {
            "total": len(items),
            "decisive": len(decisive),
            "baseline_wins": baseline_wins,
            "baseline_win_rate": baseline_wins / len(decisive) if decisive else None,
            "realized_up_count": up_count,
            "realized_up_rate": up_count / len(decisive) if decisive else None,
            "baseline_virtual_pnl": round(
                sum((item["virtual_pnl"] or 0.0) for item in items), 8
            ),
        }
    return output


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return None
    return cov / math.sqrt(var_x * var_y)


def quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * q
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return ordered[int(index)]
    weight = index - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def numeric_factor_names(records: list[dict[str, Any]]) -> list[str]:
    names: set[str] = set()
    for record in records:
        for key, value in record["features"].items():
            if key in LEAKY_OR_NON_NUMERIC_FEATURES:
                continue
            if safe_float(value) is not None:
                names.add(key)
    return sorted(names)


def factor_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    decisive = [record for record in records if record["label_direction"] in {"up", "down"}]
    factor_names = numeric_factor_names(decisive)
    by_horizon: dict[str, list[dict[str, Any]]] = {}
    for horizon in SUPPORTED_HORIZONS:
        subset = [record for record in decisive if record["horizon_minutes"] == horizon]
        rows: list[dict[str, Any]] = []
        for factor in factor_names:
            pairs: list[tuple[float, float]] = []
            for record in subset:
                value = safe_float(record["features"].get(factor))
                if value is None:
                    continue
                label = 1.0 if record["label_direction"] == "up" else -1.0
                pairs.append((value, label))
            if len(pairs) < 50:
                continue
            xs = [pair[0] for pair in pairs]
            ys = [pair[1] for pair in pairs]
            q20 = quantile(xs, 0.20)
            q80 = quantile(xs, 0.80)
            low = [pair for pair in pairs if pair[0] <= q20]
            high = [pair for pair in pairs if pair[0] >= q80]
            low_up = sum(1 for _, label in low if label > 0) / len(low) if low else None
            high_up = sum(1 for _, label in high if label > 0) / len(high) if high else None
            edge = (high_up - low_up) if high_up is not None and low_up is not None else None
            corr = pearson(xs, ys)
            rows.append(
                {
                    "factor": factor,
                    "description": FACTOR_DESCRIPTIONS.get(factor, ""),
                    "samples": len(pairs),
                    "correlation_to_up": corr,
                    "low_20pct_threshold": q20,
                    "high_20pct_threshold": q80,
                    "low_bucket_up_rate": low_up,
                    "high_bucket_up_rate": high_up,
                    "high_minus_low_up_rate": edge,
                    "high_values_favor": (
                        "up"
                        if edge is not None and edge > 0
                        else "down"
                        if edge is not None and edge < 0
                        else "neutral"
                    ),
                }
            )
        rows.sort(
            key=lambda row: max(
                abs(row["correlation_to_up"] or 0.0),
                abs(row["high_minus_low_up_rate"] or 0.0),
            ),
            reverse=True,
        )
        by_horizon[f"{horizon}m"] = rows
    return {"by_horizon": by_horizon}


def direction_baseline_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    decisive = [record for record in records if record["label_direction"] in {"up", "down"}]
    output: dict[str, Any] = {}
    for horizon in SUPPORTED_HORIZONS:
        subset = [record for record in decisive if record["horizon_minutes"] == horizon]
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in subset:
            features = record["features"]
            raw_direction = features.get("raw_direction") or "missing"
            final_direction = features.get("final_direction") or record["baseline_direction"]
            direction_reason = features.get("direction_reason") or "missing"
            groups[f"raw={raw_direction}|final={final_direction}|reason={direction_reason}"].append(
                record
            )
        output[f"{horizon}m"] = []
        for name, items in sorted(groups.items(), key=lambda item: len(item[1]), reverse=True):
            up_count = sum(1 for item in items if item["label_direction"] == "up")
            baseline_wins = sum(1 for item in items if item["baseline_correct"])
            output[f"{horizon}m"].append(
                {
                    "group": name,
                    "samples": len(items),
                    "realized_up_rate": up_count / len(items) if items else None,
                    "baseline_win_rate": baseline_wins / len(items) if items else None,
                }
            )
    return output


def make_manifest(
    database: Path,
    selected_records: list[dict[str, Any]],
    split_records: dict[str, list[dict[str, Any]]],
    strategy_filter: str,
    format_choice: str,
) -> dict[str, Any]:
    strategies = Counter(record["strategy_version"] for record in selected_records)
    horizons = Counter(record["horizon_minutes"] for record in selected_records)
    symbols = Counter(record["symbol"] for record in selected_records)
    created = [record["created_at"] for record in selected_records if record["created_at"]]
    return {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "source_sqlite": str(database),
        "supported_symbols": list(SUPPORTED_SYMBOLS),
        "supported_horizons": list(SUPPORTED_HORIZONS),
        "strategy_filter": strategy_filter,
        "current_strategy": CURRENT_STRATEGY,
        "format": format_choice,
        "total_records": len(selected_records),
        "created_range": {
            "first": min(created) if created else None,
            "last": max(created) if created else None,
        },
        "split_counts": {name: len(rows) for name, rows in split_records.items()},
        "strategy_counts": dict(strategies),
        "horizon_counts": {str(key): value for key, value in sorted(horizons.items())},
        "symbol_counts": dict(symbols),
        "by_horizon": aggregate(selected_records, "horizon_minutes"),
        "by_symbol": aggregate(selected_records, "symbol"),
        "payout_breakeven": {
            "10m": breakeven_rate(10),
            "30m": breakeven_rate(30),
            "60m": breakeven_rate(60),
        },
        "factor_schema": {
            key: FACTOR_DESCRIPTIONS.get(key, "")
            for key in numeric_factor_names(selected_records)
        },
        "label": {
            "direction": "realized expiry direction from move_percent",
            "up": "expiry_price > entry_price",
            "down": "expiry_price < entry_price",
            "flat": "absolute move_percent <= 0.000001",
        },
    }


def markdown_report(
    manifest: dict[str, Any],
    factor_report: dict[str, Any],
    direction_report: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append("# Event Contract LLM Training Pack")
    lines.append("")
    lines.append(f"- Generated: `{manifest['generated_at']}`")
    lines.append(f"- Source: `{manifest['source_sqlite']}`")
    lines.append(f"- Strategy filter: `{manifest['strategy_filter']}`")
    lines.append(f"- Total records: `{manifest['total_records']}`")
    lines.append(f"- Created range: `{manifest['created_range']['first']}` to `{manifest['created_range']['last']}`")
    lines.append("")
    lines.append("## Split counts")
    lines.append("")
    lines.append("| split | records |")
    lines.append("|---|---:|")
    for name, count in manifest["split_counts"].items():
        lines.append(f"| {name} | {count} |")
    lines.append("")
    lines.append("## Baseline performance by horizon")
    lines.append("")
    lines.append("| horizon | records | baseline win rate | realized up rate | baseline pnl | breakeven |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for horizon in ("10", "30", "60"):
        row = manifest["by_horizon"].get(horizon)
        if not row:
            continue
        key = f"{horizon}m"
        win_rate = row["baseline_win_rate"]
        up_rate = row["realized_up_rate"]
        lines.append(
            "| "
            + f"{key} | {row['total']} | "
            + f"{pct(win_rate) if win_rate is not None else '--'} | "
            + f"{pct(up_rate) if up_rate is not None else '--'} | "
            + f"{row['baseline_virtual_pnl']:+.2f} | "
            + f"{pct(manifest['payout_breakeven'][key])} |"
        )
    lines.append("")
    lines.append("## Top factor signals by horizon")
    lines.append("")
    for horizon, rows in factor_report["by_horizon"].items():
        lines.append(f"### {horizon}")
        lines.append("")
        lines.append("| factor | samples | corr_to_up | high-low up-rate edge | high values favor |")
        lines.append("|---|---:|---:|---:|---|")
        for row in rows[:12]:
            corr = row["correlation_to_up"]
            edge = row["high_minus_low_up_rate"]
            corr_text = f"{corr:+.4f}" if corr is not None else "--"
            edge_text = f"{edge:+.2%}" if edge is not None else "--"
            lines.append(
                f"| {row['factor']} | {row['samples']} | {corr_text} | "
                f"{edge_text} | {row['high_values_favor']} |"
            )
        lines.append("")
    lines.append("## Direction baseline groups")
    lines.append("")
    for horizon, rows in direction_report.items():
        lines.append(f"### {horizon}")
        lines.append("")
        lines.append("| group | samples | realized up rate | baseline win rate |")
        lines.append("|---|---:|---:|---:|")
        for row in rows[:8]:
            up_rate = row["realized_up_rate"]
            win_rate = row["baseline_win_rate"]
            lines.append(
                f"| `{row['group']}` | {row['samples']} | "
                f"{pct(up_rate) if up_rate is not None else '--'} | "
                f"{pct(win_rate) if win_rate is not None else '--'} |"
            )
        lines.append("")
    lines.append("## Practical notes")
    lines.append("")
    lines.append("- Use the train and validation JSONL files for model training, and keep test JSONL untouched for later scoring.")
    lines.append("- Do not commit generated JSONL files; they are local data artifacts under an ignored directory.")
    lines.append("- Regenerate this pack after collecting more settled tickets; the script uses chronological splits to reduce leakage.")
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> int:
    if not args.sqlite.exists():
        raise SystemExit(f"SQLite file not found: {args.sqlite}")
    if args.train_ratio <= 0 or args.validation_ratio <= 0:
        raise SystemExit("train and validation ratios must be positive")
    if args.train_ratio + args.validation_ratio >= 1:
        raise SystemExit("train_ratio + validation_ratio must be < 1")

    all_records = load_rows(args.sqlite)
    selected = filter_records(all_records, args.strategy)
    if not selected:
        raise SystemExit(f"No records matched strategy filter: {args.strategy}")
    if args.max_records and len(selected) > args.max_records:
        selected = selected[-args.max_records :]

    split_records = split_chronologically(selected, args.train_ratio, args.validation_ratio)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    manifest = make_manifest(args.sqlite, selected, split_records, args.strategy, args.format)
    factor_report = factor_stats(selected)
    direction_report = direction_baseline_stats(selected)

    files: dict[str, str] = {}
    if args.format in {"record", "both"}:
        for split, rows in split_records.items():
            path = args.output_dir / f"event_{split}.record.jsonl"
            write_jsonl(path, (record_json(row) for row in rows))
            files[f"{split}_record_jsonl"] = str(path)
    if args.format in {"chat", "both"}:
        for split, rows in split_records.items():
            path = args.output_dir / f"event_{split}.chat.jsonl"
            write_jsonl(path, (chat_json(row) for row in rows))
            files[f"{split}_chat_jsonl"] = str(path)

    manifest["files"] = files
    manifest_path = args.output_dir / "manifest.json"
    factor_path = args.output_dir / "factor_report.json"
    direction_path = args.output_dir / "direction_baselines.json"
    markdown_path = args.output_dir / "factor_report.md"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    factor_path.write_text(
        json.dumps(factor_report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    direction_path.write_text(
        json.dumps(direction_report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    markdown_path.write_text(
        markdown_report(manifest, factor_report, direction_report), encoding="utf-8"
    )

    print(f"records={len(selected)}")
    print(f"output_dir={args.output_dir}")
    for name, path in files.items():
        print(f"{name}={path}")
    print(f"manifest={manifest_path}")
    print(f"factor_report={markdown_path}")
    return 0


def main(argv: list[str]) -> int:
    default_db = default_runtime_database()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "sqlite",
        nargs="?",
        type=Path,
        default=default_db,
        help="Path to event_predictions.sqlite. Defaults to the desktop runtime DB.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated local training artifacts.",
    )
    parser.add_argument(
        "--strategy",
        default="all",
        choices=["all", "current", *KNOWN_STRATEGIES],
        help="Use all labels for training by default; current filters to the active strategy.",
    )
    parser.add_argument(
        "--format",
        choices=["record", "chat", "both"],
        default="both",
        help="record is structured JSONL; chat is messages-style JSONL.",
    )
    parser.add_argument("--train-ratio", type=float, default=0.70)
    parser.add_argument("--validation-ratio", type=float, default=0.15)
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Optional cap using the most recent N selected records.",
    )
    args = parser.parse_args(argv)
    if args.sqlite is None:
        parser.error("SQLite path was not provided and LOCALAPPDATA is unavailable")
    return build(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
