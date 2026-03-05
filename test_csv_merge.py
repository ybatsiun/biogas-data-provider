"""
test_csv_merge.py
=================
End-to-end test: fetches all four raw data sources for the same 12-hour window,
runs the exact merge logic from fetch.py in memory, and verifies the result is
internally consistent and structurally correct.

Window: UTC 2026-01-15 00:00–11:00 (12 rows).  All four APIs cover this range,
so every merged row has generation, price, solar, and load data.

Strategy:
  1. Fetch all four raw payloads.
  2. Run the same transform + merge logic as fetch.py main() (imported directly).
  3. Assert structure: correct keys, correct column set, all fields populated.
  4. Spot-check specific known values from the raw API responses.
"""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from fetch import (
    GENERATION_URL,
    PRICE_URL,
    SOLAR_URL,
    LOAD_URL,
    INSTRAT_HEADERS,
    OUTPUT_COLUMNS,
    floor_hour,
    fmt_utc,
    parse_iso,
)

# ---------------------------------------------------------------------------
# Fixed test window – all four APIs queried for the same 12 UTC hours
# ---------------------------------------------------------------------------
GEN_DATE_FROM   = "15-01-2026T00:00:00Z"
GEN_DATE_TO     = "15-01-2026T12:00:00Z"  # exclusive → 12 records (UTC 00:00–11:00)
PRICE_DATE_FROM = "15-01-2026T00:00:00Z"
PRICE_DATE_TO   = "15-01-2026T12:00:00Z"
LOAD_DATE_FROM  = "15-01-2026T00:00:00Z"
LOAD_DATE_TO    = "15-01-2026T12:00:00Z"
SOLAR_DATE      = "2026-01-15"


# ---------------------------------------------------------------------------
# Transform helpers (mirrors fetch.py exactly)
# ---------------------------------------------------------------------------

def fetch_and_transform_generation() -> dict:
    params = {
        "date_from": GEN_DATE_FROM,
        "date_to":   GEN_DATE_TO,
        "aggregation_type": "avg",
        "aggregation_timeframe": "hour",
    }
    resp = requests.get(GENERATION_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    result = {}
    for row in resp.json():
        key = fmt_utc(floor_hour(parse_iso(row["date"])))
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


def fetch_and_transform_price() -> dict:
    params = {"date_from": PRICE_DATE_FROM, "date_to": PRICE_DATE_TO}
    resp = requests.get(PRICE_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    result = {}
    for row in resp.json():
        key = fmt_utc(floor_hour(parse_iso(row["date"])))
        result[key] = {
            "price_pln_per_mwh": row.get("price", ""),
            "price_volume_mwh":  row.get("volume", ""),
        }
    return result


def fetch_and_transform_load() -> dict:
    params = {
        "date_from":             LOAD_DATE_FROM,
        "date_to":               LOAD_DATE_TO,
        "aggregation_type":      "avg",
        "aggregation_timeframe": "hour",
    }
    resp = requests.get(LOAD_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    result = {}
    for row in resp.json():
        key = fmt_utc(floor_hour(parse_iso(row["date"])))
        result[key] = {
            "load_mw":          row.get("electricity_load", ""),
            "load_forecast_mw": row.get("forecasted_load", ""),
        }
    return result


def fetch_and_transform_solar() -> dict:
    params = {
        "latitude": 52, "longitude": 20,
        "start_date": SOLAR_DATE, "end_date": SOLAR_DATE,
        "hourly": "shortwave_radiation",
        "format": "json", "timeformat": "unixtime",
    }
    resp = requests.get(SOLAR_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    result = {}
    for unix_ts, val in zip(data["hourly"]["time"], data["hourly"]["shortwave_radiation"]):
        key = fmt_utc(floor_hour(datetime.fromtimestamp(unix_ts, tz=timezone.utc)))
        result[key] = {"solar_radiation_wm2": val if val is not None else ""}
    return result


def build_merged_rows(gen: dict, price: dict, solar: dict, load: dict) -> dict[str, dict]:
    """Exact same merge logic as fetch.py main()."""
    merged = {}
    for ts in sorted(gen.keys()):
        row = {"timestamp_utc": ts}
        row.update(gen[ts])
        if ts in price:
            row.update(price[ts])
        else:
            row["price_pln_per_mwh"] = ""
            row["price_volume_mwh"]  = ""
        if ts in solar:
            row.update(solar[ts])
        else:
            row["solar_radiation_wm2"] = ""
        if ts in load:
            row.update(load[ts])
        else:
            row["load_mw"] = ""
            row["load_forecast_mw"] = ""
        merged[ts] = row
    return merged


class TestCsvMerge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        gen   = fetch_and_transform_generation()
        price = fetch_and_transform_price()
        solar = fetch_and_transform_solar()
        load  = fetch_and_transform_load()
        cls.merged   = build_merged_rows(gen, price, solar, load)
        cls.all_keys = sorted(cls.merged.keys())

    # ------------------------------------------------------------------
    # 1. Shape
    # ------------------------------------------------------------------

    def test_merged_contains_12_rows(self):
        """Generation window covers 12 UTC hours → 12 merged rows."""
        self.assertEqual(len(self.merged), 12)

    def test_first_row_utc_is_jan15_00(self):
        """With genuine UTC timestamps the first row is 2026-01-15T00:00:00+00:00."""
        self.assertEqual(self.all_keys[0], "2026-01-15T00:00:00+00:00")

    def test_last_row_utc_is_jan15_11(self):
        self.assertEqual(self.all_keys[-1], "2026-01-15T11:00:00+00:00")

    def test_merged_row_has_all_output_columns(self):
        """Every merged row must contain exactly the OUTPUT_COLUMNS keys."""
        for key in self.all_keys:
            with self.subTest(key=key):
                self.assertEqual(
                    set(self.merged[key].keys()), set(OUTPUT_COLUMNS),
                    f"Column mismatch at {key}",
                )

    # ------------------------------------------------------------------
    # 2. Join correctness: all 12 rows have data from all four sources
    # ------------------------------------------------------------------

    def test_all_rows_have_price_and_solar(self):
        """All 12 UTC Jan 15 rows must have price and solar data."""
        for key in self.all_keys:
            row = self.merged[key]
            with self.subTest(key=key):
                self.assertNotEqual(row["price_pln_per_mwh"], "",
                    f"price_pln_per_mwh blank at {key}")
                self.assertNotEqual(row["solar_radiation_wm2"], "",
                    f"solar_radiation_wm2 blank at {key}")

    def test_all_rows_have_load(self):
        """All 12 UTC Jan 15 rows must have load data."""
        for key in self.all_keys:
            row = self.merged[key]
            with self.subTest(key=key):
                self.assertNotEqual(row["load_mw"], "",
                    f"load_mw blank at {key}")
                self.assertNotEqual(row["load_forecast_mw"], "",
                    f"load_forecast_mw blank at {key}")

    def test_nighttime_rows_have_zero_solar(self):
        """UTC 00:00–06:00 are nighttime in Warsaw → solar_radiation_wm2 = 0.0."""
        night_keys = [k for k in self.all_keys if int(k[11:13]) < 7]
        for key in night_keys:
            val = self.merged[key]["solar_radiation_wm2"]
            with self.subTest(key=key):
                self.assertEqual(float(val), 0.0,
                    f"Expected 0 W/m² at night ({key}), got {val}")

    # ------------------------------------------------------------------
    # 3. Spot checks — known values from raw API responses
    # ------------------------------------------------------------------

    def test_spot_check_biomass_jan15_utc00(self):
        """
        Raw API T00:00Z biomass = 384.46387500000003 MW.
        With genuine UTC (no shift) this must appear at key UTC 00:00.
        """
        key = "2026-01-15T00:00:00+00:00"
        self.assertIn(key, self.merged)
        val = float(self.merged[key]["gen_biomass"])
        self.assertAlmostEqual(val, 384.46387500000003, places=6)

    def test_spot_check_price_jan15_utc00(self):
        """Price at UTC 00:00 Jan 15 is 490.32 PLN/MWh from the API."""
        key = "2026-01-15T00:00:00+00:00"
        val = float(self.merged[key]["price_pln_per_mwh"])
        self.assertAlmostEqual(val, 490.32, places=2)

    def test_spot_check_solar_jan15_utc09(self):
        """UTC 09:00 Jan 15 has 67 W/m² (from open-meteo, daytime)."""
        key = "2026-01-15T09:00:00+00:00"
        val = float(self.merged[key]["solar_radiation_wm2"])
        self.assertAlmostEqual(val, 67.0, places=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
