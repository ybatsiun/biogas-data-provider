"""
test_generation_raw.py
======================
Verifies that the generation API response is parsed correctly into UTC
timestamps and that values are not corrupted during transformation.

Fixed window: 2026-01-15 00:00 – 11:00 UTC (12 records).

The instrat ENTSO-E generation API returns genuine UTC timestamps with a
proper Z suffix.  fetch.py uses parse_iso() to parse them directly — the
same approach as the price API.  No timezone shift is applied.

Strategy:
  1. Fetch the raw JSON from the real API (no mocking).
  2. Apply the same transformation fetch.py uses (imported directly).
  3. Assert: output UTC keys match the raw API dates exactly (no shift).
  4. Assert: every numeric field is preserved to full float precision.
"""

import csv
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Make fetch.py importable without running main()
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
from fetch import (
    GENERATION_URL,
    INSTRAT_HEADERS,
    floor_hour,
    fmt_utc,
    parse_iso,
)

# ---------------------------------------------------------------------------
# Fixed test window
# ---------------------------------------------------------------------------
DATE_FROM_INSTRAT = "15-01-2026T00:00:00Z"
DATE_TO_INSTRAT   = "15-01-2026T12:00:00Z"  # exclusive → 12 records (UTC 00:00–11:00)

# Expected UTC keys: the API returns genuine UTC, so they match the raw dates directly.
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

GEN_FIELDS = [
    "gen_biomass", "gen_gas", "gen_hard_coal", "gen_hydro",
    "gen_lignite", "gen_other", "gen_solar", "gen_wind_onshore",
    "gen_energy_storage",
]

def fetch_raw_generation() -> list[dict]:
    params = {
        "date_from": DATE_FROM_INSTRAT,
        "date_to":   DATE_TO_INSTRAT,
        "aggregation_type": "avg",
        "aggregation_timeframe": "hour",
    }
    resp = requests.get(GENERATION_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def transform_generation(raw: list[dict]) -> dict:
    """Exact same logic as fetch_generation() in fetch.py."""
    result = {}
    for row in raw:
        ts_utc = floor_hour(parse_iso(row["date"]))
        key = fmt_utc(ts_utc)
        result[key] = {
            "gen_biomass":        row.get("biomass", ""),
            "gen_gas":            row.get("gas", ""),
            "gen_hard_coal":      row.get("hard_coal", ""),
            "gen_hydro":          row.get("hydro", ""),
            "gen_lignite":        row.get("lignite", ""),
            "gen_other":          row.get("other", ""),
            "gen_solar":          row.get("solar", ""),
            "gen_wind_onshore":   row.get("wind_onshore", ""),
            "gen_energy_storage": row.get("energy_storage", ""),
        }
    return result


class TestGenerationRaw(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.raw = fetch_raw_generation()
        cls.transformed = transform_generation(cls.raw)

    # ------------------------------------------------------------------
    # 1. Basic shape checks
    # ------------------------------------------------------------------

    def test_api_returned_12_records(self):
        """The API returns records [date_from, date_to) – 12 records for a 12-hour window."""
        self.assertEqual(len(self.raw), 12, [r["date"] for r in self.raw])

    def test_transformed_keys_match_expected_utc(self):
        """12 UTC keys must match the expected list exactly."""
        self.assertEqual(sorted(self.transformed.keys()), sorted(EXPECTED_UTC_KEYS))

    # ------------------------------------------------------------------
    # 2. Timestamp correctness – no shift applied
    # ------------------------------------------------------------------

    def test_midnight_utc_key_equals_raw_date(self):
        """The raw record '2026-01-15T00:00:00Z' must produce key '2026-01-15T00:00:00+00:00'."""
        midnight_record = next(r for r in self.raw if r["date"] == "2026-01-15T00:00:00Z")
        key = fmt_utc(floor_hour(parse_iso(midnight_record["date"])))
        self.assertEqual(key, "2026-01-15T00:00:00+00:00")

    def test_timestamps_are_not_shifted(self):
        """
        The API returns genuine UTC.  Each raw 'Z' date must map to the
        identical UTC key — no hour offset should be applied.
        """
        for raw_row in self.raw:
            expected_key = raw_row["date"].replace("Z", "+00:00")
            with self.subTest(raw_date=raw_row["date"]):
                self.assertIn(
                    expected_key, self.transformed,
                    f"Expected key {expected_key} not found — an unwanted "
                    "timezone shift may have been applied.",
                )

    # ------------------------------------------------------------------
    # 3. Value correctness – raw API values survive transformation
    # ------------------------------------------------------------------

    def _assert_field_matches_raw(self, utc_key: str, field: str, raw_field: str):
        raw_row = next(
            r for r in self.raw
            if r["date"].replace("Z", "+00:00") == utc_key
        )
        raw_val = raw_row.get(raw_field, "")
        api_val = self.transformed[utc_key][field]
        if raw_val == "" and api_val == "":
            return
        self.assertAlmostEqual(
            float(raw_val), float(api_val), places=6,
            msg=f"{field} mismatch at {utc_key}: raw={raw_val}  transformed={api_val}",
        )

    def test_biomass_preserved_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_raw(key, "gen_biomass", "biomass")

    def test_gas_preserved_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_raw(key, "gen_gas", "gas")

    def test_hard_coal_preserved_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_raw(key, "gen_hard_coal", "hard_coal")

    def test_wind_onshore_preserved_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_raw(key, "gen_wind_onshore", "wind_onshore")

    def test_all_generation_fields_preserved(self):
        """Umbrella: all 9 generation fields for all 12 hours match the raw API values."""
        field_map = {
            "gen_biomass": "biomass", "gen_gas": "gas",
            "gen_hard_coal": "hard_coal", "gen_hydro": "hydro",
            "gen_lignite": "lignite", "gen_other": "other",
            "gen_solar": "solar", "gen_wind_onshore": "wind_onshore",
            "gen_energy_storage": "energy_storage",
        }
        for key in EXPECTED_UTC_KEYS:
            for out_field, raw_field in field_map.items():
                with self.subTest(key=key, field=out_field):
                    self._assert_field_matches_raw(key, out_field, raw_field)

    # ------------------------------------------------------------------
    # 4. Sanity: plausible MW values
    # ------------------------------------------------------------------

    def test_generation_values_are_non_negative(self):
        for key in EXPECTED_UTC_KEYS:
            for field in GEN_FIELDS:
                val = self.transformed[key][field]
                if val != "":
                    with self.subTest(key=key, field=field):
                        self.assertGreaterEqual(float(val), 0.0)

    def test_hard_coal_is_dominant_source_at_midnight(self):
        """At UTC midnight in January, hard_coal should far exceed biomass."""
        key     = "2026-01-15T00:00:00+00:00"
        coal    = float(self.transformed[key]["gen_hard_coal"])
        biomass = float(self.transformed[key]["gen_biomass"])
        self.assertGreater(coal, biomass)


if __name__ == "__main__":
    unittest.main(verbosity=2)
