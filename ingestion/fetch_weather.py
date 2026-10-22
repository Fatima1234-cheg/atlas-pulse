"""Ingestion Open-Meteo archive → couche Bronze (Parquet).

Boucle année par année pour éviter les timeouts et permettre
l'ingestion incrémentale (les années déjà téléchargées sont ignorées).
"""
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

CITIES = {
    "casablanca": (33.5731, -7.5898),
    "rabat":      (34.0209, -6.8416),
    "marrakech":  (31.6295, -7.9811),
    "tanger":     (35.7595, -5.8340),
    "agadir":     (30.4278, -9.5981),
}

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = "temperature_2m,apparent_temperature,precipitation,wind_speed_10m,wind_gusts_10m,relative_humidity_2m,pressure_msl"

START_YEAR = 2020
# L'API archive a un délai de ~6 jours : on s'arrête à J-6
CUTOFF = date.today() - timedelta(days=6)


def fetch_city(city: str, lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": HOURLY_VARS,
        "timezone": "Africa/Casablanca",
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=120)
    r.raise_for_status()
    payload = r.json()

    df = pd.DataFrame(payload["hourly"])
    df["time"] = pd.to_datetime(df["time"]).astype("datetime64[us]")  # compat Spark
    df["city"] = city
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def write_bronze(df: pd.DataFrame, city: str, period_label: str) -> Path:
    out_dir = Path(f"data/bronze/city={city}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"weather_{period_label}.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def already_ingested(city: str, period_label: str) -> bool:
    return (Path(f"data/bronze/city={city}") / f"weather_{period_label}.parquet").exists()


def main():
    current_year = date.today().year
    print(f"Ingestion {START_YEAR} → {current_year} (coupure à J-6)")

    total_calls = 0
    total_rows = 0

    for city, (lat, lon) in CITIES.items():
        print(f"\n=== {city} ===")
        for year in range(START_YEAR, current_year + 1):
            period_label = str(year)

            if already_ingested(city, period_label):
                print(f"  {year} → déjà présent, ignoré")
                continue

            start = f"{year}-01-01"
            if year == current_year:
                end = CUTOFF.isoformat()
            else:
                end = f"{year}-12-31"

            try:
                df = fetch_city(city, lat, lon, start, end)
                path = write_bronze(df, city, period_label)
                total_calls += 1
                total_rows += len(df)
                print(f"  {year} → {len(df):6d} lignes → {path.name}")
                time.sleep(1)  # politesse envers l'API
            except Exception as e:
                print(f"  {year} → ERREUR : {e}")
                # On continue avec l'année suivante, on ne perd pas le reste

    print(f"\n=== Résumé ===")
    print(f"  Appels API effectués : {total_calls}")
    print(f"  Lignes récupérées    : {total_rows}")


if __name__ == "__main__":
    main()