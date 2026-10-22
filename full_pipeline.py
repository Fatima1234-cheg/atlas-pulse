import subprocess
import pandas as pd
import glob
import os
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Téléchargement des données
print("--- 1. Téléchargement des données (Patientez...) ---")
subprocess.run(["python", "-m", "ingestion.fetch_weather"], check=True)

# 2. Transformation avec PySpark
print("--- 2. Transformation avec PySpark ---")
subprocess.run(["docker", "compose", "exec", "spark", "python", "-m", "spark_jobs.bronze_to_silver"], check=True)

# 3. Consolidation et ajout de la colonne 'city'
print("--- 3. Consolidation des fichiers ---")
files = glob.glob('data/silver/city=*/part-*.parquet')
dfs = []
for f in files:
    df = pd.read_parquet(f)
    city = f.split('city=')[1].split(os.sep)[0]
    df['city'] = city
    dfs.append(df)
final_df = pd.concat(dfs, ignore_index=True)
print("Colonnes prêtes :", list(final_df.columns))
final_df.to_parquet('data/silver_consolidated.parquet', index=False)

# 4. Envoi DIRECT vers BigQuery (Plus besoin de Cloud Shell !)
print("--- 4. Envoi vers BigQuery ---")
key_path = r"C:\Users\lessa\atlas-pulse\gcp-key.json" # Votre clé de compte de service
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(project="plasma-streamer-480323-h7", credentials=credentials)

table_id = "plasma-streamer-480323-h7.atlas_pulse_silver.weather_clean"
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE", # Ecrase l'ancienne table
    source_format=bigquery.SourceFormat.PARQUET,
)

with open("data/silver_consolidated.parquet", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result() # Attend la fin du chargement
print("\n✅ SUCCÈS ! La table BigQuery a été mise à jour avec la colonne 'city'.")