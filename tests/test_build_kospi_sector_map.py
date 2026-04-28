from __future__ import annotations

import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


class BuildKospiSectorMapTest(unittest.TestCase):
    def test_manual_source_builds_same_unique_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            universe = tmp / "universe.csv"
            out = tmp / "sector_map.csv"

            with universe.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["symbol", "name"])
                w.writeheader()
                w.writerow({"symbol": "5930", "name": "삼성전자"})
                w.writerow({"symbol": "005930", "name": ""})
                w.writerow({"symbol": "000660", "name": "SK하이닉스"})

            cmd = [
                "python",
                "scripts/build_kospi_sector_map.py",
                "--universe-file",
                str(universe),
                "--output",
                str(out),
                "--source",
                "manual",
                "--overwrite",
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)

            with out.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["symbol"], "000660")
            self.assertEqual(rows[1]["symbol"], "005930")
            self.assertEqual(rows[1]["name"], "삼성전자")
            self.assertEqual(rows[0]["mapping_status"], "unknown")
            self.assertEqual(rows[1]["sector"], "UNKNOWN")

            summary_line = [line for line in proc.stdout.splitlines() if line.startswith("KOSPI_SECTOR_MAP_JSON=")][-1]
            payload = json.loads(summary_line.split("=", 1)[1])
            self.assertEqual(payload["universe_rows"], 2)
            self.assertEqual(payload["output_rows"], 2)
            self.assertEqual(payload["unknown_rows"], 2)
            self.assertAlmostEqual(payload["unknown_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
