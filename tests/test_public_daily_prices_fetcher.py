from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from scripts.fetch_public_daily_prices import parse_naver_daily_chart_xml, write_daily_prices_csv


class PublicDailyPricesFetcherTests(unittest.TestCase):
    def test_parse_naver_daily_chart_xml_filters_dates_and_null_rows(self) -> None:
        xml = """<?xml version="1.0" encoding="EUC-KR" ?>
        <protocol>
          <chartdata symbol="005930" timeframe="day">
            <item data="20260506|254000|270000|251000|266000|53097996" />
            <item data="20260507|272000|277000|260000|271500|41404687" />
            <item data="20260508|null|null|null|268500|25696964" />
          </chartdata>
        </protocol>
        """
        rows = parse_naver_daily_chart_xml(xml, "5930", start_date="2026-05-07", end_date="2026-05-08")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].symbol, "005930")
        self.assertEqual(rows[0].date, "2026-05-07")
        self.assertEqual(rows[0].close, 271500.0)
        self.assertGreater(rows[0].traded_value, 0.0)

    def test_write_daily_prices_csv_uses_loader_compatible_columns(self) -> None:
        xml = """<protocol><chartdata><item data="20260507|100|110|90|105|1000" /></chartdata></protocol>"""
        rows = parse_naver_daily_chart_xml(xml, "005930")
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "daily_prices.csv"
            write_daily_prices_csv(out, rows)
            with out.open(newline="", encoding="utf-8") as f:
                loaded = list(csv.DictReader(f))
            self.assertEqual(loaded[0]["date"], "2026-05-07")
            self.assertEqual(loaded[0]["symbol"], "005930")
            self.assertIn("traded_value", loaded[0])


if __name__ == "__main__":
    unittest.main()
