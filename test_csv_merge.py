"""
test_csv_merge.py
=================
End-to-end test: fetches all three raw data sources for the same 12-hour window,
runs the exact merge logic from fetch.py in memory, and compares every field of
every resulting row against the corresponding row in energy.csv.

This is the highest-confidence test: it catches any corruption introduced by
the merge itself (wrong join key, field name mismatch, column ordering, etc.)
regardless of whether individual source tests pass.

Window:
  • Generation: Warsaw 2026-01-15 00:00–11:00 → UTC 2026-01-14T23:00 – 2026-01-15T10:00
  • Price: UTC 2026-01-15T00:00–11:00 (real UTC)
  • Solar: UTC 2026-01-15T00:00–11:00 (first 12 h of day)

Overlap: the rows that have ALL THREE sources joined are UTC 00:00–10:00 on Jan 15
(11 rows).  The earliest generation row (UTC 23:00 Jan 14) has no price/solar
match because those APIs start at UTC 00:00 Jan 15 → price/solar columns are "".

Strategy:
  1. Fetch all three raw payloads.
  2. Run fetch_generation / fetch_price / fetch_solar transform logic (from fetch.py).
  3. Build an in-memory merged row dict (same logic as main()).
  4. Load energy.csv and compare field-by-field for every merged row.
"""

import csv
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
    INSTRAT_HEADERS,
    OUTPUT_COLUMNS,
    floor_hour,
    fmt_utc,
    parse_iso,
    warsaw_to_utc,
)

# ---------------------------------------------------------------------------
# Fixed test window
# ---------------------------------------------------------------------------
GEN_DATE_FROM   = "15-01-2026T00:00:00Z"
GEN_DATE_TO     = "15-01-2026T12:00:00Z"  # exclusive → 12 Warsaw hours (00:00–11:00)
# Price/solar must cover UTC 2026-01-14T23:00 (= Warsaw 00:00 Jan 15, the first
# generation row after timezone shift).  We start from Jan 14 to pick it up.
PRICE_DATE_FROM = "14-01-2026T23:00:00Z"
PRICE_DATE_TO   = "15-01-2026T12:00:00Z"
SOLAR_DATE_FROM = "2026-01-14"
SOLAR_DATE_TO   = "2026-01-15"

CSV_PATH = Path(__file__).parent / "energy.csv"


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
        dt_naive = datetime.fromisoformat(row["date"].replace("Z", ""))
        key = fmt_utc(floor_hour(warsaw_to_utc(dt_naive)))
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


def fetch_and_transform_solar() -> dict:
    params = {
        "latitude": 52, "longitude": 20,
        "start_date": SOLAR_DATE_FROM, "end_date": SOLAR_DATE_TO,
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


def build_merged_rows(gen: dict, price: dict, solar: dict) -> dict[str, dict]:
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
        merged[ts] = row
    return merged


def load_csv_rows(keys: list[str]) -> dict:
    rows = {}
    with open(CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            if row["timestamp_utc"] in keys:
                rows[row["timestamp_utc"]] = row
    return rows


class TestCsvMerge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        gen   = fetch_and_transform_generation()
        price = fetch_and_transform_price()
        solar = fetch_and_transform_solar()
        cls.merged    = build_merged_rows(gen, price, solar)
        cls.csv_rows  = load_csv_rows(list(cls.merged.keys()))
        cls.all_keys  = sorted(cls.merged.keys())

    # ------------------------------------------------------------------
    # 1. Shape: merged rows exist in CSV
    # ------------------------------------------------------------------

    def test_merged_contains_12_rows(self):
        """Generation window covers 12 Warsaw hours → 12 merged rows."""
        self.assertEqual(len(self.merged), 12)

    def test_csv_contains_all_merged_rows(self):
        missing = set(self.all_keys) - set(self.csv_rows.keys())
        self.assertEqual(missing, set(), f"CSV missing rows: {missing}")

    def test_output_columns_match_csv_header(self):
        """energy.csv header must exactly match the OUTPUT_COLUMNS list from fetch.py."""
        with open(CSV_PATH, newline="") as f:
            header = next(csv.reader(f))
        self.assertEqual(header, OUTPUT_COLUMNS)

    # ------------------------------------------------------------------
    # 2. Join correctness: first row has no price/solar (UTC Jan 14)
    # ------------------------------------------------------------------

    def test_first_row_utc_is_jan14_23(self):
        """The first merged row is generation-only (UTC 23:00 Jan 14)."""
        self.assertEqual(self.all_keys[0], "2026-01-14T23:00:00+00:00")

    def test_first_row_has_price_data(self):
        """
        UTC Jan 14 23:00 is within the price window (we fetch from Jan 14 23:00).
        The price API covers this hour, so price fields must be populated.
        """
        row = self.merged["2026-01-14T23:00:00+00:00"]
        self.assertNotEqual(row["price_pln_per_mwh"], "",
            "First row should have price data (we fetch from Jan 14 UTC)")

    def test_first_row_has_solar_zero(self):
        """
        UTC Jan 14 23:00 is nighttime in Warsaw → solar_radiation_wm2 = 0.0.
        open-meteo returns 0.0 (not None) for nighttime hours.
        """
        row = self.merged["2026-01-14T23:00:00+00:00"]
        # We fetch solar from Jan 14, so the key should be present with 0.0
        val = row["solar_radiation_wm2"]
        self.assertNotEqual(val, "", "solar_radiation_wm2 should not be blank at UTC Jan 14 23:00")
        self.assertEqual(float(val), 0.0,
            f"Expected 0 W/m² at Jan 14 23:00 UTC (nighttime), got {val}")

    def test_all_rows_have_price_and_solar(self):
        """All rows in our window must have price and solar data."""
        for key in self.all_keys:
            row = self.merged[key]
            with self.subTest(key=key):
                self.assertNotEqual(row["price_pln_per_mwh"], "",
                    f"price_pln_per_mwh blank at {key}")
                self.assertNotEqual(row["solar_radiation_wm2"], "",
                    f"solar_radiation_wm2 blank at {key}")

    # ------------------------------------------------------------------
    # 3. Field-by-field match between merged output and CSV
    # ------------------------------------------------------------------

    def _assert_field(self, key: str, field: str):
        merged_val = self.merged[key].get(field, "")
        csv_val    = self.csv_rows[key][field]
        if merged_val == "" and csv_val == "":
            return
        if merged_val == "" or csv_val == "":
            self.fail(
                f"{field} at {key}: merged={merged_val!r}  csv={csv_val!r} "
                "(one is blank, other is not)"
            )
        self.assertAlmostEqual(
            float(csv_val), float(merged_val), places=6,
            msg=f"{field} mismatch at {key}: CSV={csv_val}  merged={merged_val}",
        )

    def test_all_columns_all_rows_match_csv(self):
        """
        Master assertion: every data column in every merged row must equal
        the corresponding value in energy.csv, to 6 decimal places.
        """
        data_columns = [c for c in OUTPUT_COLUMNS if c != "timestamp_utc"]
        for key in self.all_keys:
            for col in data_columns:
                with self.subTest(key=key, col=col):
                    self._assert_field(key, col)

    # ------------------------------------------------------------------
    # 4. Specific spot checks for known values (regression anchors)
    # ------------------------------------------------------------------

    def test_spot_check_hard_coal_jan15_utc00(self):
        """
        Warsaw 01:00 = UTC 00:00 Jan 15.
        Raw API biomass for that Warsaw hour: 381.55362499999995 MW.
        Validate this exact value survives into the merged row.
        """
        key = "2026-01-15T00:00:00+00:00"
        self.assertIn(key, self.merged)
        val = float(self.merged[key]["gen_biomass"])
        self.assertAlmostEqual(val, 381.55362499999995, places=6)

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
