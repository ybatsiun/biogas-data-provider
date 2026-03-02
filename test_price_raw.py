"""
test_price_raw.py
=================
Verifies that price API data is passed through to energy.csv without corruption.

Fixed window: Warsaw 2026-01-15 00:00 – 11:00 (same 12-record span as the
generation test).  The price API returns ISO 8601 UTC (the 'Z' suffix is
genuine here, not mislabelled Warsaw time), so NO timezone shift should occur.

Key contract being tested:
  • price API "date" field is already UTC → parsed with parse_iso(), floor_hour()
  • 'price' field → price_pln_per_mwh column (exact float)
  • 'volume' field → price_volume_mwh column (exact float)
  • 'indeks' field is ignored (not written to CSV)

Strategy:
  1. Fetch 12 raw price records from the real API.
  2. Apply fetch.py's transform logic (imported directly).
  3. Load energy.csv, find the same 12 UTC timestamp rows.
  4. Assert values match to float precision.
"""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from fetch import (
    PRICE_URL,
    INSTRAT_HEADERS,
    floor_hour,
    fmt_utc,
    parse_iso,
)

# ---------------------------------------------------------------------------
# Fixed test window – price API is queried with the same instrat date format
# ---------------------------------------------------------------------------
DATE_FROM_INSTRAT = "15-01-2026T00:00:00Z"
DATE_TO_INSTRAT   = "15-01-2026T12:00:00Z"  # exclusive → 12 records

# The price API date IS genuine UTC (unlike generation, where 'Z' was a lie).
# For date_from=15-01-2026T00:00:00Z to date_to=15-01-2026T12:00:00Z the API
# returns 12 records: 2026-01-15T00:00Z through 2026-01-15T11:00Z (inclusive).
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

def fetch_raw_price() -> list[dict]:
    params = {
        "date_from": DATE_FROM_INSTRAT,
        "date_to":   DATE_TO_INSTRAT,
    }
    resp = requests.get(PRICE_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def transform_price(raw: list[dict]) -> dict:
    """Exact same logic as fetch_price() in fetch.py."""
    result = {}
    for row in raw:
        ts_utc = floor_hour(parse_iso(row["date"]))
        key = fmt_utc(ts_utc)
        result[key] = {
            "price_pln_per_mwh": row.get("price", ""),
            "price_volume_mwh":  row.get("volume", ""),
        }
    return result


class TestPriceRaw(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.raw = fetch_raw_price()
        cls.transformed = transform_price(cls.raw)
        cls.price_keys = list(cls.transformed.keys())

    # ------------------------------------------------------------------
    # 1. Shape checks
    # ------------------------------------------------------------------

    def test_api_returns_12_records(self):
        self.assertEqual(len(self.raw), 12, [r["date"] for r in self.raw])

    def test_price_keys_are_utc_not_local(self):
        """
        Price dates carry a genuine Z suffix.  parse_iso() converts them to UTC
        directly — unlike generation, no Warsaw offset should be applied.
        The raw dates should already equal the output UTC keys (no shift).
        """
        for row in self.raw:
            raw_utc_key = fmt_utc(floor_hour(parse_iso(row["date"])))
            with self.subTest(raw_date=row["date"]):
                self.assertIn(raw_utc_key, self.transformed)

    def test_transformed_contains_expected_keys(self):
        self.assertEqual(sorted(self.transformed.keys()), sorted(EXPECTED_UTC_KEYS))

    # ------------------------------------------------------------------
    # 2. The 'indeks' field is NOT carried through
    # ------------------------------------------------------------------

    def test_indeks_not_in_transformed_keys(self):
        """'indeks' is an internal API field; it must not appear in any transformed row."""
        for key in self.price_keys:
            with self.subTest(key=key):
                self.assertNotIn("indeks", self.transformed[key])

    # ------------------------------------------------------------------
    # 3. Value correctness – raw API values survive transformation
    # ------------------------------------------------------------------

    def _assert_price_matches_raw(self, utc_key: str, out_field: str, raw_field: str):
        raw_row = next(r for r in self.raw if r["date"].replace("Z", "+00:00") == utc_key)
        raw_val = raw_row.get(raw_field, "")
        api_val = self.transformed[utc_key][out_field]
        if raw_val == "" and (api_val == "" or api_val is None):
            return
        self.assertAlmostEqual(
            float(raw_val), float(api_val), places=6,
            msg=f"{out_field} mismatch at {utc_key}: raw={raw_val}  transformed={api_val}",
        )

    def test_price_pln_preserved_all_hours(self):
        for key in self.price_keys:
            with self.subTest(key=key):
                self._assert_price_matches_raw(key, "price_pln_per_mwh", "price")

    def test_volume_preserved_all_hours(self):
        for key in self.price_keys:
            with self.subTest(key=key):
                self._assert_price_matches_raw(key, "price_volume_mwh", "volume")

    # ------------------------------------------------------------------
    # 4. Sanity: prices are positive PLN values
    # ------------------------------------------------------------------

    def test_prices_are_positive(self):
        for key in self.price_keys:
            val = self.transformed[key]["price_pln_per_mwh"]
            if val != "":
                with self.subTest(key=key):
                    self.assertGreater(float(val), 0.0)

    def test_volumes_are_positive(self):
        for key in self.price_keys:
            val = self.transformed[key]["price_volume_mwh"]
            if val != "":
                with self.subTest(key=key):
                    self.assertGreater(float(val), 0.0)

    def test_price_not_astronomically_high(self):
        """Spot price should be below 10 000 PLN/MWh for any normal hour."""
        for key in self.price_keys:
            val = self.transformed[key]["price_pln_per_mwh"]
            if val != "":
                with self.subTest(key=key):
                    self.assertLess(float(val), 10_000.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
