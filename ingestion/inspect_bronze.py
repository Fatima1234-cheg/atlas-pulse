"""Inspection qualité de la couche Bronze (lecture seule)."""
import pandas as pd
from pathlib import Path

BRONZE_DIR = Path("data/bronze")


def inspect_file(path: Path) -> dict:
    df = pd.read_parquet(path)
    city = path.parent.name.replace("city=", "")

    # Nombre d'heures attendues entre le min et le max
    expected_hours = len(pd.date_range(df["time"].min(), df["time"].max(), freq="h"))
    actual_hours = df["time"].nunique()

    return {
        "city": city,
        "rows": len(df),
        "dup (city,time)": int(df.duplicated(["city", "time"]).sum()),
        "start": df["time"].min(),
        "end": df["time"].max(),
        "hours": actual_hours,
        "expected": expected_hours,
        "missing_hours": expected_hours - actual_hours,
        "temp_nulls": int(df["temperature_2m"].isna().sum()),
        "precip_nulls": int(df["precipitation"].isna().sum()),
        "temp_min": round(df["temperature_2m"].min(), 1),
        "temp_max": round(df["temperature_2m"].max(), 1),
        "precip_min": round(df["precipitation"].min(), 2),
        "precip_max": round(df["precipitation"].max(), 2),
    }


def main():
    files = sorted(BRONZE_DIR.rglob("*.parquet"))
    if not files:
        print("Aucun fichier Parquet trouvé dans", BRONZE_DIR)
        return

    report = pd.DataFrame([inspect_file(f) for f in files])
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print(report.to_string(index=False))


if __name__ == "__main__":
    main()