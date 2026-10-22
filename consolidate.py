import pandas as pd
import glob
import os

files = glob.glob('data/silver/city=*/part-*.parquet')
dfs = []
for f in files:
    df = pd.read_parquet(f)
    # On récupère le nom de la ville depuis le nom du dossier
    city = f.split('city=')[1].split(os.sep)[0]
    df['city'] = city
    dfs.append(df)

final_df = pd.concat(dfs, ignore_index=True)
print("Colonnes du fichier :", list(final_df.columns))
final_df.to_parquet('data/silver_consolidated.parquet', index=False)
print("Fichier consolidé créé avec succès !")