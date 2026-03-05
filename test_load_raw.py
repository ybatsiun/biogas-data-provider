"""
test_load_raw.py
================
Verifies that the electricity load API data is passed through to energy.csv
without corruption.

Fixed window: 2026-01-15 00:00 – 11:00 UTC (same 12-record span used by the
other test files).  The load API returns ISO 8601 UTC timestamps (Z suffix is
genuine UTC), so NO timezone shift should occur.

Key contract being tested:
  • load API "date" field is already UTC → parsed with parse_iso(), floor_hour()
  • 'electricity_load' field → load_mw column (exact float)
  • 'forecasted_load' field → load_forecast_mw column (exact float)

Strategy:
  1. Fetch 12 raw load records from the real API (aggregation_type=avg,
     aggregation_timeframe=hour).
  2. Apply fetch.py's transform logic (imported directly).
  3. Assert transformed keys, value preservation, and sanity bounds.
"""

import sys
import unittest
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from fetch import (
    LOAD_URL,
    INSTRAT_HEADERS,
    floor_hour,
    fmt_utc,
    parse_iso,
)

# ---------------------------------------------------------------------------
# Fixed test window
# ---------------------------------------------------------------------------
DATE_FROM_INSTRAT = "15-01-2026T00:00:00Z"
DATE_TO_INSTRAT   = "15-01-2026T12:00:00Z"  # exclusive → 12 records

EXPECTED_UTC_KEYS = [
    "2026-01-15T00:00:00+00:00",
    "2026-01-15T01:00:00+00:00",
    "2026-01-15T02:00:00+00:00",
    "2026-01-15T03:00:00+00:00",
    "2026-01-15T04:00:00+00:00",
    "2026-01-15T05:00:00+00:00",
    "2026-01-15T06:00:00+00:00",
    "2026-01-15T07:00:00+00:00",
    "2026-01-15T08:00:00+00:00",
    "2026-01-15T09:00:00+00:00",
    "2026-01-15T10:00:00+00:00",
    "2026-01-15T11:00:00+00:00",
]


def fetch_raw_load() -> list[dict]:
    params = {
        "date_from":            DATE_FROM_INSTRAT,
        "date_to":              DATE_TO_INSTRAT,
        "aggregation_type":     "avg",
        "aggregation_timeframe": "hour",
    }
    resp = requests.get(LOAD_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def transform_load(raw: list[dict]) -> dict:
    """Exact same logic as fetch_load() in fetch.py."""
    result = {}
    for row in raw:
        ts_utc = floor_hour(parse_iso(row["date"]))
        key = fmt_utc(ts_utc)
        result[key] = {
            "load_mw":          row.get("electricity_load", ""),
            "load_forecast_mw": row.get("forecasted_load", ""),
        }
    return result


class TestLoadRaw(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.raw = fetch_raw_load()
        cls.transformed = transform_load(cls.raw)
        cls.load_keys = list(cls.transformed.keys())

    # ------------------------------------------------------------------
    # 1. Shape checks
    # ------------------------------------------------------------------

    def test_api_returns_12_records(self):
        self.assertEqual(len(self.raw), 12, [r["date"] for r in self.raw])

    def test_load_keys_are_utc(self):
        """Load dates carry a genuine Z suffix — no timezone shift should occur."""
        for row in self.raw:
            raw_utc_key = fmt_utc(floor_hour(parse_iso(row["date"])))
            with self.subTest(raw_date=row["date"]):
                self.assertIn(raw_utc_key, self.transformed)

    def test_transformed_contains_expected_keys(self):
        self.assertEqual(sorted(self.transformed.keys()), sorted(EXPECTED_UTC_KEYS))

    # ------------------------------------------------------------------
    # 2. Value correctness – raw API values survive transformation
    # ------------------------------------------------------------------

    def _assert_load_matches_raw(self, utc_key: str, out_field: str, raw_field: str):
        raw_row = next(r for r in self.raw if r["date"].replace("Z", "+00:00") == utc_key)
        raw_val = raw_row.get(raw_field, "")
        api_val = self.transformed[utc_key][out_field]
        if raw_val == "" and (api_val == "" or api_val is None):
            return
        self.assertAlmostEqual(
            float(raw_val), float(api_val), places=6,
            msg=f"{out_field} mismatch at {utc_key}: raw={raw_val}  transformed={api_val}",
        )

    def test_load_mw_preserved_all_hours(self):
        for key in self.load_keys:
            with self.subTest(key=key):
                self._assert_load_matches_raw(key, "load_mw", "electricity_load")

    def test_load_forecast_mw_preserved_all_hours(self):
        for key in self.load_keys:
            with self.subTest(key=key):
                self._assert_load_matches_raw(key, "load_forecast_mw", "forecasted_load")

    # ------------------------------------------------------------------
    # 3. Sanity: load values are physically plausible for the Polish grid
    # ------------------------------------------------------------------

    def test_load_mw_positive(self):
        for key in self.load_keys:
            val = self.transformed[key]["load_mw"]
            if val != "":
                with self.subTest(key=key):
                    self.assertGreater(float(val), 0.0)

    def test_load_forecast_mw_positive(self):
        for key in self.load_keys:
            val = self.transformed[key]["load_forecast_mw"]
            if val != "":
                with self.subTest(key=key):
                    self.assertGreater(float(val), 0.0)

    def test_load_mw_plausible_range(self):
        """Polish grid load is typically 10 000 – 28 000 MW."""
        for key in self.load_keys:
            val = self.transformed[key]["load_mw"]
            if val != "":
                with self.subTest(key=key):
                    self.assertGreater(float(val), 1_000.0)
                    self.assertLess(float(val), 30_000.0)

    def test_load_forecast_mw_plausible_range(self):
        for key in self.load_keys:
            val = self.transformed[key]["load_forecast_mw"]
            if val != "":
                with self.subTest(key=key):
                    self.assertGreater(float(val), 1_000.0)
                    self.assertLess(float(val), 30_000.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
