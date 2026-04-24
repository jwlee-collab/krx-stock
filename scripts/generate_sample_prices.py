#!/usr/bin/env python3
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path


def main() -> None:
    out = Path("data/sample_daily_prices.csv")
    out.parent.mkdir(parents=True, exist_ok=True)

    symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM"]
    start = date(2025, 1, 2)
    days = 90

    rng = random.Random(42)
    prices = {s: 100.0 + (i * 12.0) for i, s in enumerate(symbols)}

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "date", "open", "high", "low", "close", "volume"])
        for d in range(days):
            dt = start + timedelta(days=d)
            if dt.weekday() >= 5:
                continue
            for s in symbols:
                prev = prices[s]
                shock = rng.uniform(-0.02, 0.02)
                close = max(1.0, prev * (1.0 + shock))
                high = max(close, prev) * (1.0 + rng.uniform(0.0, 0.01))
                low = min(close, prev) * (1.0 - rng.uniform(0.0, 0.01))
                open_ = prev * (1.0 + rng.uniform(-0.005, 0.005))
                volume = int(1_000_000 * (1.0 + rng.uniform(-0.35, 0.35)))
                prices[s] = close
                w.writerow([s, dt.isoformat(), f"{open_:.4f}", f"{high:.4f}", f"{low:.4f}", f"{close:.4f}", volume])

    print(out)


if __name__ == "__main__":
    main()
