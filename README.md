# AtlasPulse — Morocco Climate Intelligence

Pipeline Data Engineering qui collecte et analyse les données météo horaires
de 5 villes marocaines (Casablanca, Rabat, Marrakech, Tanger, Agadir) pour
détecter les épisodes de chaleur et les fortes précipitations.

## Problème résolu

Une entreprise avec des équipes terrain au Maroc veut pouvoir répondre à :

- Quelles villes connaissent les épisodes de chaleur les plus importants ?
- Comment évoluent les températures et les précipitations depuis 2020 ?
- Quels événements météo nécessitent une surveillance particulière ?

Ce projet transforme des données météo brutes en indicateurs exploitables,
via un pipeline de données fiable de bout en bout.

## Stack technique

| Composant | Outil |
|---|---|
| Ingestion | Python (requests + pandas) → API Open-Meteo |
| Stockage | Parquet (couches Bronze / Silver) |
| Transformation | PySpark 3.5 (Docker, mode `local[*]`) |
| Orchestration | Airflow *(à venir)* |
| Entrepôt | BigQuery Sandbox + dbt *(à venir)* |
| Visualisation | Power BI *(à venir)* |

## Chiffres actuels

| Métrique | Valeur |
|---|---|
| Période | 2020-01-01 → 2026-10-16 (J-6) |
| Villes | 5 (Casablanca, Rabat, Marrakech, Tanger, Agadir) |
| Volume Bronze | **297 720 lignes horaires** |
| Fichiers Parquet | 35 (5 villes × 7 années) |
| Appels API | 35 (ingestion incrémentale par année) |
| Temps de traitement Silver | **34,7 s** sur 297 720 lignes |
| Qualité Bronze | 0 doublon, 0 null |
| Qualité Silver | 0 doublon, 0 valeur aberrante |

## Architecture
Open-Meteo API
│ (Python, boucle année par année, ingestion incrémentale)
▼
data/bronze/city=.../weather_YYYY.parquet (35 fichiers, 297 720 lignes)
│ (PySpark, Docker local[])
▼
data/silver/city=.../part-.parquet (données typées et validées)
data/silver/quality_checks/run_id=.../ (logs à chaque exécution)

## Structure du projet
atlas-pulse/
├── ingestion/
│ ├── fetch_weather.py # Ingestion API → Bronze
│ ├── inspect_bronze.py # Inspection qualité Bronze
│ └── check_bronze.py # Vérification rapide Bronze
├── spark_jobs/
│ └── bronze_to_silver.py # Transformation Bronze → Silver
├── data/
│ ├── bronze/ # Parquet bruts (ignoré par Git)
│ └── silver/ # Parquet nettoyés (ignoré par Git)
├── docs/
│ └── api_notes.md # Notes sur l'API Open-Meteo
├── Dockerfile # Image PySpark + Java 17
├── docker-compose.yml # Service Spark conteneurisé
└── requirements.txt

## Lancer le pipeline

### Prérequis

- Python 3.11+
- Docker Desktop (avec WSL 2 activé sur Windows)

### Ingestion (local, ~5–8 min)

```bash
pip install -r requirements.txt
python -m ingestion.fetch_weather
Le script boucle année par année. Relancer ne re-télécharge pas les années
déjà présentes dans data/bronze/ (ingestion incrémentale).

Transformation Silver (Docker PySpark, ~35 s)
bash
docker compose build
docker compose up -d
docker compose exec spark python -m spark_jobs.bronze_to_silver
Qualité des données
À chaque exécution de la couche Silver, un log est écrit dans
data/silver/quality_checks/run_id=.../ avec :

rows_received : lignes lues depuis Bronze

duplicates_removed : doublons supprimés sur (city, time)

rows_rejected : lignes rejetées (température hors [-20, 60] °C, précipitations < 0)

rows_clean : lignes écrites dans Silver

Règles de qualité appliquées
Dédoublonnage sur (city, time)

Température valide : -20 °C ≤ temperature_2m ≤ 60 °C

Précipitations valides : precipitation ≥ 0

Indicateurs ajoutés : is_heatwave_candidate (temp ≥ 40 °C), is_heavy_rain (precip ≥ 10 mm)

Reproductibilité
Le pipeline est entièrement conteneurisé (PySpark + Java 17) et ne nécessite
aucune installation locale de Java ou de Spark. La couche Bronze est
immuable : la couche Silver peut être recalculée à volonté à partir de Bronze.