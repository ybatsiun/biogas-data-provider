# CLAUDE.md — biogas-data-provider

## What this repo is

A standalone data pipeline that fetches hourly Polish energy market data from
three public APIs and merges it into a single flat CSV (`energy.csv`).

It is a **data provider** for the wider biogas project — it supplies the
external market context (grid generation mix, electricity prices, solar
irradiance) that the biogas tracker app uses for analytics and decision
support.

## Why it exists

Biogas plants operate in the context of the broader energy grid. Knowing the
current generation mix (how much coal, wind, solar is on the grid), the
day-ahead electricity price, and solar irradiance helps answer questions like:

- Is it a good time to feed energy into the grid or store it?
- How does biogas generation correlate with grid price spikes?
- What is the renewable share on the grid right now?

This repo collects that context data in a reusable, versioned CSV format.

## Data sources

| Source | API | What it provides |
|---|---|---|
| ENTSO-E via instrat.pl | `energy-api.instrat.pl/api/energy/production_entsoe` | Generation by source (MW), hourly avg of 15-min data |
| RDN via instrat.pl | `energy-api.instrat.pl/api/prices/energy_price_rdn_hourly` | Day-ahead electricity price (PLN/MWh) + traded volume |
| open-meteo archive | `archive-api.open-meteo.com/v1/archive` | Solar irradiance at Warsaw (lat=52, lon=20), W/m² |

All three APIs return genuine UTC timestamps. Generation and price come from
the instrat.pl energy dashboard; solar from the open-meteo historical archive.

## Key implementation notes

- `fetch.py` is the single entry point — run it with a date range to produce `energy.csv`
- Generation timestamps from instrat use a `Z` suffix that represents real UTC
- All three sources are left-joined on the generation spine (generation timestamps
  drive the row set; price and solar fill in where available)
- Price data availability starts late 2025 — earlier rows have nulls

## Test suite

Four test files hit the live APIs against a fixed 12-hour window (2026-01-15)
and verify that no data is corrupted during transformation or merging.
Tests compare transformed output directly against raw API responses — no mocking.

```bash
source venv/bin/activate
python3 -m pytest test_generation_raw.py test_price_raw.py test_solar_raw.py test_csv_merge.py -v
```

## Running

```bash
source venv/bin/activate
python3 fetch.py --date-from 2025-01-01 --date-to 2026-03-01 --output energy.csv
```
