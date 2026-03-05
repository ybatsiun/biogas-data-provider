"""
Fetches Polish energy data from four sources and merges into a single hourly CSV.

Sources:
  1. Generation (instrat ENTSO-E): MW per source, hourly avg
  2. Day-ahead price (instrat RDN): PLN/MWh + traded volume MWh, hourly
  3. Solar irradiance (open-meteo archive): W/m² at lat=52 lon=20, hourly
  4. Electricity load (instrat): actual + forecasted consumption MW, hourly avg

Usage:
  python fetch.py --date-from 2025-02-01 --date-to 2026-02-01 --output energy.csv
"""

import argparse
import csv
import sys
from datetime import datetime, timezone

import requests

GENERATION_URL = "https://energy-api.instrat.pl/api/energy/production_entsoe"
PRICE_URL = "https://energy-api.instrat.pl/api/prices/energy_price_rdn_hourly"
SOLAR_URL = "https://archive-api.open-meteo.com/v1/archive"
LOAD_URL = "https://energy-api.instrat.pl/api/energy/load"

INSTRAT_HEADERS = {
    "accept": "*/*",
    "origin": "https://energy.instrat.pl",
    "referer": "https://energy.instrat.pl/",
    "user-agent": "Mozilla/5.0",
}

OUTPUT_COLUMNS = [
    "timestamp_utc",
    "gen_biomass",
    "gen_gas",
    "gen_hard_coal",
    "gen_hydro",
    "gen_lignite",
    "gen_other",
    "gen_solar",
    "gen_wind_onshore",
    "gen_energy_storage",
    "price_pln_per_mwh",
    "price_volume_mwh",
    "solar_radiation_wm2",
    "load_mw",
    "load_forecast_mw",
]


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def instrat_fmt(dt: datetime) -> str:
    return dt.strftime("%d-%m-%YT%H:%M:%SZ")


def parse_iso(s: str) -> datetime:
    """Parse ISO 8601 timestamp with Z suffix to UTC datetime."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def floor_hour(dt: datetime) -> datetime:
    """Truncate a datetime to the hour."""
    return dt.replace(minute=0, second=0, microsecond=0)



def fmt_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def fetch_generation(date_from: datetime, date_to: datetime) -> dict:
    """Returns dict of timestamp_utc_str -> generation field dict."""
    print("Fetching generation data...", flush=True)
    params = {
        "date_from": instrat_fmt(date_from),
        "date_to": instrat_fmt(date_to),
        "aggregation_type": "avg",
        "aggregation_timeframe": "hour",
    }
    resp = requests.get(GENERATION_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        print("  WARNING: generation API returned no records", file=sys.stderr)
        return {}

    result = {}
    for row in data:
        ts_utc = floor_hour(parse_iso(row["date"]))
        key = fmt_utc(ts_utc)
        result[key] = {
            "gen_biomass": row.get("biomass", ""),
            "gen_gas": row.get("gas", ""),
            "gen_hard_coal": row.get("hard_coal", ""),
            "gen_hydro": row.get("hydro", ""),
            "gen_lignite": row.get("lignite", ""),
            "gen_other": row.get("other", ""),
            "gen_solar": row.get("solar", ""),
            "gen_wind_onshore": row.get("wind_onshore", ""),
            "gen_energy_storage": row.get("energy_storage", ""),
        }

    print(f"  Got {len(result)} hourly generation records", flush=True)
    return result


def fetch_price(date_from: datetime, date_to: datetime) -> dict:
    """Returns dict of timestamp_utc_str -> price field dict."""
    print("Fetching price data...", flush=True)
    params = {
        "date_from": instrat_fmt(date_from),
        "date_to": instrat_fmt(date_to),
    }
    resp = requests.get(PRICE_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        print("  WARNING: price API returned no records", file=sys.stderr)
        return {}

    result = {}
    for row in data:
        ts_utc = floor_hour(parse_iso(row["date"]))
        key = fmt_utc(ts_utc)
        result[key] = {
            "price_pln_per_mwh": row.get("price", ""),
            "price_volume_mwh": row.get("volume", ""),
        }

    print(f"  Got {len(result)} hourly price records", flush=True)
    return result


def fetch_load(date_from: datetime, date_to: datetime) -> dict:
    """Returns dict of timestamp_utc_str -> load field dict."""
    print("Fetching electricity load data...", flush=True)
    params = {
        "date_from": instrat_fmt(date_from),
        "date_to": instrat_fmt(date_to),
        "aggregation_type": "avg",
        "aggregation_timeframe": "hour",
    }
    resp = requests.get(LOAD_URL, params=params, headers=INSTRAT_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        print("  WARNING: load API returned no records", file=sys.stderr)
        return {}

    result = {}
    for row in data:
        ts_utc = floor_hour(parse_iso(row["date"]))
        key = fmt_utc(ts_utc)
        result[key] = {
            "load_mw": row.get("electricity_load", ""),
            "load_forecast_mw": row.get("forecasted_load", ""),
        }

    print(f"  Got {len(result)} hourly load records", flush=True)
    return result


def fetch_solar(date_from: datetime, date_to: datetime) -> dict:
    """Returns dict of timestamp_utc_str -> solar field dict."""
    print("Fetching solar irradiance data...", flush=True)
    params = {
        "latitude": 52,
        "longitude": 20,
        "start_date": date_from.strftime("%Y-%m-%d"),
        "end_date": date_to.strftime("%Y-%m-%d"),
        "hourly": "shortwave_radiation",
        "format": "json",
        "timeformat": "unixtime",
    }
    resp = requests.get(SOLAR_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    times = data["hourly"]["time"]
    values = data["hourly"]["shortwave_radiation"]

    result = {}
    for unix_ts, val in zip(times, values):
        ts_utc = floor_hour(datetime.fromtimestamp(unix_ts, tz=timezone.utc))
        key = fmt_utc(ts_utc)
        result[key] = {"solar_radiation_wm2": val if val is not None else ""}

    print(f"  Got {len(result)} hourly irradiance records", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description="Fetch and merge Polish energy data")
    parser.add_argument("--date-from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--date-to", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--output", default="energy.csv", help="Output CSV file path")
    args = parser.parse_args()

    date_from = parse_date(args.date_from)
    date_to = parse_date(args.date_to)

    gen = fetch_generation(date_from, date_to)
    price = fetch_price(date_from, date_to)
    solar = fetch_solar(date_from, date_to)
    load = fetch_load(date_from, date_to)

    # Generation is the spine; all timestamps come from it
    all_timestamps = sorted(gen.keys())

    null_price = 0
    null_solar = 0
    null_load = 0

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for ts in all_timestamps:
            row = {"timestamp_utc": ts}
            row.update(gen[ts])
            if ts in price:
                row.update(price[ts])
            else:
                row["price_pln_per_mwh"] = ""
                row["price_volume_mwh"] = ""
                null_price += 1
            if ts in solar:
                row.update(solar[ts])
            else:
                row["solar_radiation_wm2"] = ""
                null_solar += 1
            if ts in load:
                row.update(load[ts])
            else:
                row["load_mw"] = ""
                row["load_forecast_mw"] = ""
                null_load += 1
            writer.writerow(row)

    print(f"\nWrote {len(all_timestamps)} rows to {args.output}")
    if null_price:
        print(f"  price_pln_per_mwh / price_volume_mwh: {null_price} nulls (price API coverage gap)")
    if null_solar:
        print(f"  solar_radiation_wm2: {null_solar} nulls")
    if null_load:
        print(f"  load_mw / load_forecast_mw: {null_load} nulls")


if __name__ == "__main__":
    main()
