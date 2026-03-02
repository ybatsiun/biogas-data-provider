"""
test_solar_raw.py
=================
Verifies that open-meteo solar irradiance data is converted from UNIX timestamps
to UTC keys correctly and that values are written to energy.csv unchanged.

Fixed window: 2026-01-15, restricted to the first 12 hours (00:00–11:00 UTC).
open-meteo returns 24 records per day; we take the first 12.

Key contract being tested:
  • The API returns UNIX timestamps (seconds since epoch, UTC).
  • fetch.py converts them with datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    then floor_hour() — this must produce the correct UTC ISO key.
  • The 'shortwave_radiation' value is stored as-is in solar_radiation_wm2.
  • None values from the API become "" in the CSV.

Strategy:
  1. Fetch the real API response for 2026-01-15.
  2. Apply the same transformation logic from fetch.py.
  3. Load energy.csv and compare the 12 solar values.
"""

import csv
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from fetch import SOLAR_URL, floor_hour, fmt_utc

# ---------------------------------------------------------------------------
# Fixed test window
# ---------------------------------------------------------------------------
DATE_STR = "2026-01-15"

# 12 UTC hours we will check (first half of the day)
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

CSV_PATH = Path(__file__).parent / "energy.csv"


def fetch_raw_solar() -> dict:
    """Returns the raw JSON dict from open-meteo."""
    params = {
        "latitude": 52,
        "longitude": 20,
        "start_date": DATE_STR,
        "end_date": DATE_STR,
        "hourly": "shortwave_radiation",
        "format": "json",
        "timeformat": "unixtime",
    }
    resp = requests.get(SOLAR_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def transform_solar(raw: dict) -> dict:
    """Exact same logic as fetch_solar() in fetch.py."""
    times  = raw["hourly"]["time"]
    values = raw["hourly"]["shortwave_radiation"]
    result = {}
    for unix_ts, val in zip(times, values):
        ts_utc = floor_hour(datetime.fromtimestamp(unix_ts, tz=timezone.utc))
        key    = fmt_utc(ts_utc)
        result[key] = {"solar_radiation_wm2": val if val is not None else ""}
    return result


def load_csv_rows(keys: list[str]) -> dict:
    rows = {}
    with open(CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            if row["timestamp_utc"] in keys:
                rows[row["timestamp_utc"]] = row
    return rows


class TestSolarRaw(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.raw        = fetch_raw_solar()
        cls.transformed = transform_solar(cls.raw)
        cls.csv_rows   = load_csv_rows(EXPECTED_UTC_KEYS)

    # ------------------------------------------------------------------
    # 1. Shape checks
    # ------------------------------------------------------------------

    def test_api_returns_24_records_for_full_day(self):
        """open-meteo always returns one record per UTC hour for the requested day."""
        self.assertEqual(len(self.raw["hourly"]["time"]), 24)

    def test_transformed_has_24_keys(self):
        self.assertEqual(len(self.transformed), 24)

    def test_all_12_expected_keys_present(self):
        missing = set(EXPECTED_UTC_KEYS) - set(self.transformed.keys())
        self.assertEqual(missing, set(), f"Missing keys: {missing}")

    def test_csv_has_all_expected_rows(self):
        missing = set(EXPECTED_UTC_KEYS) - set(self.csv_rows.keys())
        self.assertEqual(missing, set(), f"Missing CSV rows: {missing}")

    # ------------------------------------------------------------------
    # 2. Unix → UTC conversion correctness
    # ------------------------------------------------------------------

    def test_first_unix_timestamp_maps_to_midnight_utc(self):
        """
        open-meteo day starts at Unix 1768435200 = 2026-01-15T00:00:00Z.
        The transformation must produce exactly that UTC key.
        """
        first_unix = self.raw["hourly"]["time"][0]
        expected   = datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(
            datetime.fromtimestamp(first_unix, tz=timezone.utc),
            expected,
            f"First Unix timestamp {first_unix} does not map to 2026-01-15T00:00:00Z",
        )

    def test_unix_timestamps_are_hourly_spaced(self):
        """Consecutive unix timestamps must differ by exactly 3600 seconds."""
        times = self.raw["hourly"]["time"]
        for i in range(1, len(times)):
            with self.subTest(i=i):
                self.assertEqual(times[i] - times[i - 1], 3600,
                    f"Gap between record {i-1} and {i} is not 3600s")

    def test_transformed_keys_are_hourly_utc(self):
        """Every key must be an exact-hour UTC string."""
        for key in self.transformed:
            dt = datetime.fromisoformat(key)
            with self.subTest(key=key):
                self.assertEqual(dt.minute, 0)
                self.assertEqual(dt.second, 0)
                self.assertIsNotNone(dt.tzinfo)

    # ------------------------------------------------------------------
    # 3. Value correctness – matches CSV
    # ------------------------------------------------------------------

    def test_solar_values_match_csv_all_12_hours(self):
        for key in EXPECTED_UTC_KEYS:
            csv_val = self.csv_rows[key]["solar_radiation_wm2"]
            api_val = self.transformed[key]["solar_radiation_wm2"]
            with self.subTest(key=key):
                if api_val == "" or api_val is None:
                    self.assertEqual(csv_val, "",
                        f"API returned None but CSV has {csv_val!r} at {key}")
                else:
                    self.assertAlmostEqual(
                        float(csv_val), float(api_val), places=6,
                        msg=f"solar_radiation_wm2 mismatch at {key}: "
                            f"CSV={csv_val}  API={api_val}",
                    )

    # ------------------------------------------------------------------
    # 4. Sanity: values are plausible W/m² for Warsaw in January
    # ------------------------------------------------------------------

    def test_nighttime_hours_have_zero_irradiance(self):
        """Hours 00:00–06:00 UTC in January Warsaw are before civil dawn → 0 W/m²."""
        night_keys = EXPECTED_UTC_KEYS[:7]  # 00:00–06:00 UTC
        for key in night_keys:
            val = self.transformed[key]["solar_radiation_wm2"]
            with self.subTest(key=key):
                if val != "":
                    self.assertEqual(float(val), 0.0,
                        f"Expected 0 W/m² at night ({key}) but got {val}")

    def test_daytime_hours_have_nonzero_irradiance(self):
        """Hours 07:00–10:00 UTC in January Warsaw should have some solar radiation."""
        daytime_keys = [
            "2026-01-15T07:00:00+00:00",
            "2026-01-15T08:00:00+00:00",
            "2026-01-15T09:00:00+00:00",
            "2026-01-15T10:00:00+00:00",
        ]
        for key in daytime_keys:
            val = self.transformed[key]["solar_radiation_wm2"]
            with self.subTest(key=key):
                self.assertNotEqual(val, "",
                    f"solar_radiation_wm2 is None/blank at {key}")
                self.assertGreater(float(val), 0.0,
                    f"Expected >0 W/m² during daylight at {key} but got {val}")

    def test_irradiance_below_physical_maximum(self):
        """Top-of-atmosphere solar irradiance is ~1361 W/m²; surface must be lower."""
        for key in EXPECTED_UTC_KEYS:
            val = self.transformed[key]["solar_radiation_wm2"]
            if val != "":
                with self.subTest(key=key):
                    self.assertLessEqual(float(val), 1361.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
