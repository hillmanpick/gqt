#!/usr/bin/env python3
import argparse
import json
import sqlite3
from pathlib import Path


def pct(value):
    return f"{value:.2f}%"


def connect(path):
    if not path.exists():
        raise SystemExit(f"database not found: {path}")
    return sqlite3.connect(path)


def rows(connection, sql, params=()):
    connection.row_factory = sqlite3.Row
    return connection.execute(sql, params).fetchall()


def main():
    parser = argparse.ArgumentParser(description="Export GQT event prediction review stats.")
    parser.add_argument("database", type=Path)
    parser.add_argument("--loss-limit", type=int, default=20)
    args = parser.parse_args()

    connection = connect(args.database)
    totals = rows(
        connection,
        """
        SELECT horizon_minutes,
               COUNT(*) AS total,
               SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
               SUM(CASE WHEN result = 'tie' THEN 1 ELSE 0 END) AS ties,
               AVG(confidence) AS avg_confidence,
               AVG(move_percent) AS avg_move_percent,
               SUM(COALESCE(virtual_pnl, 0.0)) AS virtual_pnl
        FROM event_prediction_tickets
        WHERE status = 'settled'
        GROUP BY horizon_minutes
        ORDER BY horizon_minutes
        """,
    )
    open_count = rows(
        connection,
        "SELECT COUNT(*) AS count FROM event_prediction_tickets WHERE status = 'open'",
    )[0]["count"]
    open_exposure = rows(
        connection,
        "SELECT COALESCE(SUM(stake_amount), 0.0) AS value FROM event_prediction_tickets WHERE status = 'open'",
    )[0]["value"]
    realized_pnl = rows(
        connection,
        "SELECT COALESCE(SUM(virtual_pnl), 0.0) AS value FROM event_prediction_tickets WHERE status = 'settled'",
    )[0]["value"]

    print(f"open tickets: {open_count}")
    print(f"bankroll: start=200.00 stake=5.00 realized_pnl={realized_pnl:+.2f} equity={200.0 + realized_pnl:.2f} open_exposure={open_exposure:.2f} available={200.0 + realized_pnl - open_exposure:.2f}")
    print("settled stats:")
    if not totals:
        print("  no settled tickets yet")
    for row in totals:
        decisive = (row["wins"] or 0) + (row["losses"] or 0)
        win_rate = (row["wins"] or 0) / decisive * 100 if decisive else 0.0
        print(
            f"  {row['horizon_minutes']}m total={row['total']} "
            f"win={row['wins'] or 0} loss={row['losses'] or 0} tie={row['ties'] or 0} "
            f"win_rate={pct(win_rate)} avg_conf={pct((row['avg_confidence'] or 0) * 100)} "
            f"avg_move={pct(row['avg_move_percent'] or 0)} pnl={row['virtual_pnl'] or 0:+.2f}"
        )

    losses = rows(
        connection,
        """
        SELECT symbol, horizon_minutes, direction, confidence, score, stake_amount,
               move_percent, virtual_pnl,
               features_json, review
        FROM event_prediction_tickets
        WHERE status = 'settled' AND result = 'loss'
        ORDER BY close_time DESC
        LIMIT ?
        """,
        (args.loss_limit,),
    )
    print("recent losses:")
    if not losses:
        print("  none")
    for row in losses:
        features = json.loads(row["features_json"])
        compact = {
            key: round(features.get(key, 0.0), 4)
            for key in [
                "momentum3",
                "momentum10",
                "momentum30",
                "momentum60",
                "ema_short",
                "ema_mid",
                "ema_long",
                "volume_bias",
                "sentiment",
            ]
        }
        print(
            f"  {row['symbol']} {row['horizon_minutes']}m {row['direction']} "
            f"conf={pct(row['confidence'] * 100)} score={row['score']:+.3f} "
            f"stake={row['stake_amount']:.2f} pnl={row['virtual_pnl'] or 0:+.2f} "
            f"move={pct(row['move_percent'] or 0)} features={compact} review={row['review']}"
        )


if __name__ == "__main__":
    main()
