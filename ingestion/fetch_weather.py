"""Ingestion Open-Meteo archive → couche Bronze (Parquet)."""
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
HOURLY_VARS = "temperature_2m,precipitation,wind_speed_10m"

def fetch_city(city: str, lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": HOURLY_VARS,
        "timezone": "Africa/Casablanca",
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()

    df = pd.DataFrame(payload["hourly"])
    df["time"] = pd.to_datetime(df["time"])
   
    df["time"] = pd.to_datetime(df["time"]).astype("datetime64[us]")
    df["city"] = city
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df

def write_bronze(df: pd.DataFrame, city: str) -> Path:
    out_dir = Path(f"data/bronze/city={city}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"weather_{date.today().isoformat()}.parquet"
    df.to_parquet(out_file, index=False)
    return out_file

def main():
    # On s'arrête 6 jours avant aujourd'hui pour éviter le délai de l'API archive
    end = (date.today() - timedelta(days=6)).isoformat()
    start = (date.today() - timedelta(days=13)).isoformat()
    print(f"Ingestion du {start} au {end} (période sûre pour l'archive)")

    for city, (lat, lon) in CITIES.items():
        df = fetch_city(city, lat, lon, start, end)
        path = write_bronze(df, city)
        print(f"  {city:12s} -> {len(df):4d} lignes -> {path}")

if __name__ == "__main__":
    main()