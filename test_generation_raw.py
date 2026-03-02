"""
test_generation_raw.py
======================
Verifies that the generation API response is transformed correctly into UTC
timestamps and that the values written to energy.csv match the raw wire data.

Fixed window: 2026-01-15 00:00 – 10:00 Warsaw local time (UTC+1 in January).
That covers UTC hours 2026-01-14T23:00 through 2026-01-15T09:00 (11 records).

Why this window matters:
  The generation API returns dates with a bogus "Z" suffix that actually
  means Warsaw local time, not UTC.  fetch.py strips the Z, treats the
  datetime as Warsaw local, converts to UTC via warsaw_to_utc(), then
  floor_hour()s to the hour.  One wrong sign (e.g. +1 instead of -1) would
  shift every timestamp by 2 hours and corrupt every value in the CSV.

Strategy:
  1. Fetch the raw JSON from the real API (no mocking – we want the actual
     numbers the script would have seen).
  2. Apply the same transformation fetch.py uses (imported directly).
  3. Load energy.csv, filter to the UTC window we expect.
  4. Assert: keys match, and every numeric field matches to the full float
     precision present in the CSV.
"""

import csv
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
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
    warsaw_to_utc,
)

# ---------------------------------------------------------------------------
# Fixed test window (Warsaw local time – what the API accepts)
# ---------------------------------------------------------------------------
DATE_FROM_INSTRAT = "15-01-2026T00:00:00Z"
DATE_TO_INSTRAT   = "15-01-2026T12:00:00Z"   # exclusive → 12 records (Warsaw 00:00–11:00)

# Expected UTC keys after transformation (January is CET = UTC+1, so -1h).
# Warsaw 00:00 → UTC 23:00 previous day, …, Warsaw 11:00 → UTC 10:00 same day.
EXPECTED_UTC_KEYS = [
    "2026-01-14T23:00:00+00:00",  # Warsaw 00:00
    "2026-01-15T00:00:00+00:00",  # Warsaw 01:00
    "2026-01-15T01:00:00+00:00",  # Warsaw 02:00
    "2026-01-15T02:00:00+00:00",  # Warsaw 03:00
    "2026-01-15T03:00:00+00:00",  # Warsaw 04:00
    "2026-01-15T04:00:00+00:00",  # Warsaw 05:00
    "2026-01-15T05:00:00+00:00",  # Warsaw 06:00
    "2026-01-15T06:00:00+00:00",  # Warsaw 07:00
    "2026-01-15T07:00:00+00:00",  # Warsaw 08:00
    "2026-01-15T08:00:00+00:00",  # Warsaw 09:00
    "2026-01-15T09:00:00+00:00",  # Warsaw 10:00
    "2026-01-15T10:00:00+00:00",  # Warsaw 11:00
]

GEN_FIELDS = [
    "gen_biomass", "gen_gas", "gen_hard_coal", "gen_hydro",
    "gen_lignite", "gen_other", "gen_solar", "gen_wind_onshore",
    "gen_energy_storage",
]

CSV_PATH = Path(__file__).parent / "energy.csv"


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
        dt_naive = datetime.fromisoformat(row["date"].replace("Z", ""))
        ts_utc = floor_hour(warsaw_to_utc(dt_naive))
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


def load_csv_rows(keys: list[str]) -> dict:
    """Load energy.csv rows for a specific set of UTC timestamp keys."""
    rows = {}
    with open(CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            if row["timestamp_utc"] in keys:
                rows[row["timestamp_utc"]] = row
    return rows


class TestGenerationRaw(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.raw = fetch_raw_generation()
        cls.transformed = transform_generation(cls.raw)
        cls.csv_rows = load_csv_rows(EXPECTED_UTC_KEYS)

    # ------------------------------------------------------------------
    # 1. Basic shape checks
    # ------------------------------------------------------------------

    def test_api_returned_12_records(self):
        """The API returns records [date_from, date_to) – 12 records for a 12-hour window."""
        self.assertEqual(len(self.raw), 12, [r["date"] for r in self.raw])

    def test_transformed_keys_match_expected_utc(self):
        """All 12 Warsaw timestamps must shift to the right UTC keys."""
        self.assertEqual(sorted(self.transformed.keys()), sorted(EXPECTED_UTC_KEYS))

    def test_csv_has_all_expected_rows(self):
        """energy.csv must contain rows for every expected UTC key."""
        missing = set(EXPECTED_UTC_KEYS) - set(self.csv_rows.keys())
        self.assertEqual(missing, set(), f"Missing rows in CSV: {missing}")

    # ------------------------------------------------------------------
    # 2. Timezone correctness – the core invariant
    # ------------------------------------------------------------------

    def test_warsaw_midnight_maps_to_utc_23_prev_day(self):
        """Warsaw 00:00 on Jan 15 must become 2026-01-14T23:00:00+00:00 (CET=UTC+1)."""
        # The raw record whose date is "2026-01-15T00:00:00Z" (really Warsaw 00:00)
        warsaw_midnight_record = next(
            r for r in self.raw if r["date"] == "2026-01-15T00:00:00Z"
        )
        dt_naive = datetime.fromisoformat(warsaw_midnight_record["date"].replace("Z", ""))
        utc_key  = fmt_utc(floor_hour(warsaw_to_utc(dt_naive)))
        self.assertEqual(utc_key, "2026-01-14T23:00:00+00:00")

    def test_timestamps_are_shifted_not_naive_utc(self):
        """
        The raw 'Z' dates are Warsaw local time mislabelled as UTC.
        If fetch.py naively trusted the Z, each key would be the raw date
        string, but the correct UTC key is 1 hour earlier (CET = UTC+1).

        For each raw record we verify:
          - the correctly shifted UTC key IS present in the transformed dict
          - the naive UTC key (raw date as-is) is NOT the same as the
            correct key (i.e. the shift was actually applied)
        """
        from datetime import timedelta
        for raw_row in self.raw:
            naive_utc_str = raw_row["date"].replace("Z", "+00:00")
            naive_dt      = datetime.fromisoformat(naive_utc_str)
            correct_dt    = naive_dt - timedelta(hours=1)
            correct_key   = fmt_utc(correct_dt.replace(tzinfo=timezone.utc))
            with self.subTest(raw_date=raw_row["date"]):
                self.assertIn(
                    correct_key, self.transformed,
                    f"Shifted UTC key {correct_key} missing — timezone "
                    f"conversion may be wrong (naive would give {naive_utc_str}).",
                )
                self.assertNotEqual(
                    naive_utc_str, correct_key,
                    "Naive UTC equals correct UTC — test is degenerate "
                    "(Warsaw == UTC, impossible in January).",
                )

    # ------------------------------------------------------------------
    # 3. Value correctness – transformed dict matches CSV exactly
    # ------------------------------------------------------------------

    def _assert_field_matches_csv(self, utc_key: str, field: str):
        csv_val_str = self.csv_rows[utc_key][field]
        api_val     = self.transformed[utc_key][field]
        # Both can be "" if the API returned None
        if csv_val_str == "" and api_val == "":
            return
        self.assertNotEqual(csv_val_str, "", f"{field} is blank in CSV for {utc_key}")
        self.assertNotEqual(api_val,     "", f"{field} is blank in transformed for {utc_key}")
        self.assertAlmostEqual(
            float(csv_val_str), float(api_val), places=6,
            msg=f"{field} mismatch at {utc_key}: CSV={csv_val_str}  API={api_val}",
        )

    def test_biomass_matches_csv_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_csv(key, "gen_biomass")

    def test_gas_matches_csv_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_csv(key, "gen_gas")

    def test_hard_coal_matches_csv_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_csv(key, "gen_hard_coal")

    def test_wind_onshore_matches_csv_for_all_hours(self):
        for key in EXPECTED_UTC_KEYS:
            with self.subTest(key=key):
                self._assert_field_matches_csv(key, "gen_wind_onshore")

    def test_all_generation_fields_match_csv(self):
        """Umbrella: every generation field for every expected hour matches CSV."""
        for key in EXPECTED_UTC_KEYS:
            for field in GEN_FIELDS:
                with self.subTest(key=key, field=field):
                    self._assert_field_matches_csv(key, field)

    # ------------------------------------------------------------------
    # 4. No data corruption – values are plausible MW figures
    # ------------------------------------------------------------------

    def test_generation_values_are_positive(self):
        for key in EXPECTED_UTC_KEYS:
            for field in GEN_FIELDS:
                val = self.transformed[key][field]
                if val != "":
                    with self.subTest(key=key, field=field):
                        self.assertGreaterEqual(float(val), 0.0)

    def test_hard_coal_is_dominant_source_in_january(self):
        """In January nights hard_coal should exceed biomass (sanity check)."""
        midnight_utc = "2026-01-14T23:00:00+00:00"
        coal    = float(self.transformed[midnight_utc]["gen_hard_coal"])
        biomass = float(self.transformed[midnight_utc]["gen_biomass"])
        self.assertGreater(coal, biomass)


if __name__ == "__main__":
    unittest.main(verbosity=2)
