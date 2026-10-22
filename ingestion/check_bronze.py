"""Vérification rapide de la couche Bronze."""
import pandas as pd
from pathlib import Path

files = list(Path("data/bronze").rglob("*.parquet"))
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

print(f"Fichiers Parquet : {len(files)}")
print(f"Lignes totales   : {len(df):,}")
print(f"Villes           : {df['city'].nunique()} → {sorted(df['city'].unique())}")
print(f"Période          : {df['time'].min()} → {df['time'].max()}")
print(f"Colonnes         : {list(df.columns)}")
print(f"Doublons (city,time) : {df.duplicated(['city','time']).sum()}")
print(f"Nulls temperature_2m : {df['temperature_2m'].isna().sum()}")
print(f"Nulls precipitation  : {df['precipitation'].isna().sum()}")