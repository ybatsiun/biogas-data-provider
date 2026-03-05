# biogas-data-provider

Fetches and merges hourly Polish energy data from four sources into a single CSV.

## Sources

| Data | Provider | Unit |
|---|---|---|
| Grid generation by source | ENTSO-E via instrat.pl | MW (avg per hour) |
| Day-ahead electricity price | RDN via instrat.pl | PLN/MWh |
| Solar irradiance (lat=52, lon=20) | open-meteo archive | W/m² |
| Electricity load (actual + forecast) | instrat.pl | MW (avg per hour) |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install pytest
```

## Usage

```bash
python3 fetch.py --date-from 2025-02-01 --date-to 2026-02-01 --output energy.csv
```

## Output schema

| Column | Unit | Description |
|---|---|---|
| `timestamp_utc` | ISO 8601 UTC | Hour start |
| `gen_biomass` | MW | Biomass generation |
| `gen_gas` | MW | Gas generation |
| `gen_hard_coal` | MW | Hard coal generation |
| `gen_hydro` | MW | Hydro generation |
| `gen_lignite` | MW | Lignite generation |
| `gen_other` | MW | Other sources |
| `gen_solar` | MW | Solar PV fed to grid |
| `gen_wind_onshore` | MW | Onshore wind |
| `gen_energy_storage` | MW | Energy storage (discharge) |
| `price_pln_per_mwh` | PLN/MWh | Day-ahead market price |
| `price_volume_mwh` | MWh | Traded volume |
| `solar_radiation_wm2` | W/m² | Shortwave irradiance at Warsaw |
| `load_mw` | MW | Actual electricity consumption |
| `load_forecast_mw` | MW | Forecasted electricity consumption |

Generation and load values are server-side hourly averages of the underlying 15-min data.
Price data availability starts from late 2025 — earlier rows will have nulls in price columns.

## Tests

The test suite fetches a small fixed window (12 hours, 2026-01-15) from the live APIs.
No mocking — actual wire data is used.

```bash
source venv/bin/activate
python3 -m pytest test_generation_raw.py test_price_raw.py test_solar_raw.py test_load_raw.py test_csv_merge.py -v
```

| File | What it checks |
|---|---|
| `test_generation_raw.py` | Generation timestamps are genuine UTC (no shift); all 9 MW fields preserved |
| `test_price_raw.py` | Price API dates are genuine UTC; `price`/`volume` pass through exactly |
| `test_solar_raw.py` | UNIX→UTC key conversion; nighttime = 0 W/m²; daytime > 0 |
| `test_load_raw.py` | Load API UTC parsing; `electricity_load`/`forecasted_load` pass through exactly |
| `test_csv_merge.py` | End-to-end: all four sources merged in-memory, structure and spot values verified |

All five files are independent — run any single one in isolation if needed:

```bash
source venv/bin/activate
python3 -m pytest test_csv_merge.py -v
```

**Requirements:** Tests make real HTTP requests to `energy-api.instrat.pl` and
`archive-api.open-meteo.com`, so an internet connection is required.
