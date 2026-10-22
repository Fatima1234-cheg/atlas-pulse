# AtlasPulse --- Morocco Climate Intelligence

**AtlasPulse** est un projet de Data Engineering qui collecte, stocke,
transforme et contrôle des données météorologiques horaires pour cinq
villes marocaines. L'objectif est de construire un pipeline
reproductible permettant d'étudier les épisodes de chaleur et les fortes
précipitations à partir de données historiques.

> **État du projet :** l'ingestion Python vers la couche Bronze et la
> transformation PySpark vers Silver sont implémentées. L'orchestration
> avec Airflow, l'entrepôt BigQuery, les modèles dbt et le dashboard
> Power BI sont prévus et ne sont pas encore intégrés.

------------------------------------------------------------------------

## Sommaire

-   [Contexte et objectifs](#contexte-et-objectifs)
-   [Périmètre des données](#périmètre-des-données)
-   [Stack technique](#stack-technique)
-   [Résultats actuels](#résultats-actuels)
-   [Architecture du pipeline](#architecture-du-pipeline)
-   [Structure du dépôt](#structure-du-dépôt)
-   [Fonctionnement du pipeline](#fonctionnement-du-pipeline)
-   [Installation et exécution](#installation-et-exécution)
-   [Contrôles de qualité](#contrôles-de-qualité)
-   [Reproductibilité et reprise](#reproductibilité-et-reprise)
-   [Limites et précautions
    d'interprétation](#limites-et-précautions-dinterprétation)
-   [Roadmap](#roadmap)
-   [Pistes d'analyse](#pistes-danalyse)
-   [Auteur](#auteur)

------------------------------------------------------------------------

## Contexte et objectifs

Les organisations qui disposent d'équipes terrain peuvent avoir besoin
de suivre les conditions météorologiques dans plusieurs régions du
Maroc. AtlasPulse vise à transformer des données météo brutes en données
structurées et contrôlées, prêtes pour l'analyse.

Le projet cherche notamment à répondre aux questions suivantes :

-   Quelles villes enregistrent les températures horaires les plus
    élevées ?
-   Comment les températures et les précipitations évoluent-elles sur la
    période étudiée ?
-   Quand et où observe-t-on des températures élevées ou des
    précipitations importantes ?
-   Les données ingérées sont-elles complètes, cohérentes et sans
    doublons ?
-   Les traitements peuvent-ils être relancés de façon reproductible
    sans télécharger inutilement toutes les données ?

### Objectifs Data Engineering

-   Ingestion de données depuis une API externe.
-   Stockage des données brutes dans un format colonne, Parquet.
-   Organisation des données selon une architecture en couches Bronze et
    Silver.
-   Transformation et validation distribuées avec PySpark.
-   Traçabilité des contrôles de qualité à chaque exécution.
-   Préparation du pipeline à une orchestration future avec Apache
    Airflow.

------------------------------------------------------------------------

## Périmètre des données

Les données sont récupérées depuis l'API météorologique
[Open-Meteo](https://open-meteo.com/).

  Élément                 Valeur
  ----------------------- ------------------------------------------------
  Villes                  Casablanca, Rabat, Marrakech, Tanger et Agadir
  Période observée        Du 2020-01-01 au 2026-10-16
  Granularité             Horaire
  Variables principales   Température à 2 m et précipitations
  Format de stockage      Parquet
  Couches disponibles     Bronze et Silver

La période exacte dépend des données effectivement renvoyées par l'API
et des fichiers présents localement. Les résultats peuvent donc évoluer
lorsque de nouvelles données sont ingérées.

------------------------------------------------------------------------

## Stack technique

  Composant                   Technologie                    Statut
  --------------------------- ------------------------------ ------------
  Source de données           API Open-Meteo                 Implémenté
  Ingestion                   Python, `requests`, `pandas`   Implémenté
  Stockage                    Parquet                        Implémenté
  Organisation des données    Bronze / Silver                Implémenté
  Transformation              PySpark 3.5                    Implémenté
  Environnement d'exécution   Docker, Java 17                Implémenté
  Contrôles de qualité        PySpark et logs Parquet        Implémenté
  Orchestration               Apache Airflow                 À venir
  Entrepôt analytique         BigQuery Sandbox               À venir
  Transformation analytique   dbt                            À venir
  Visualisation               Power BI                       À venir

------------------------------------------------------------------------

## Résultats actuels

Les mesures ci-dessous correspondent à l'état du projet au moment de la
rédaction du README.

  Indicateur                                                             Résultat
  ------------------------------------------------------- -----------------------
  Villes couvertes                                                              5
  Années couvertes                                                    2020 à 2026
  Lignes dans Bronze                                                      297 720
  Fichiers Parquet Bronze                                                      35
  Appels API réalisés pour l'ingestion initiale                                35
  Temps mesuré pour la transformation Silver                Environ 34,7 secondes
  Doublons détectés dans Bronze lors du contrôle                                0
  Valeurs nulles détectées dans Bronze lors du contrôle                         0
  Doublons restants dans Silver après traitement                                0
  Valeurs hors des limites configurées dans Silver                              0

**À noter :** le temps de traitement dépend de la machine, de Docker,
des ressources disponibles et du volume traité. Les contrôles indiquant
zéro anomalie décrivent les données examinées lors de l'exécution
concernée ; ils ne garantissent pas que toutes les données futures
seront exemptes d'anomalies.

------------------------------------------------------------------------

## Architecture du pipeline

``` text
                   ┌─────────────────────────┐
                   │      Open-Meteo API     │
                   │  Données météo horaires │
                   └────────────┬────────────┘
                                │
                                ▼
                   ┌─────────────────────────┐
                   │   Ingestion Python      │
                   │ requests + pandas       │
                   │ Traitement par année    │
                   └────────────┬────────────┘
                                │
                                ▼
          ┌────────────────────────────────────────┐
          │              BRONZE                    │
          │          data/bronze/                  │
          │ Données ingérées au format Parquet     │
          │ 5 villes × fichiers annuels            │
          └────────────────────┬───────────────────┘
                               │
                               ▼
                   ┌─────────────────────────┐
                   │     PySpark 3.5         │
                   │     dans Docker         │
                   │ Nettoyage et validation │
                   └────────────┬────────────┘
                                │
                                ▼
          ┌────────────────────────────────────────┐
          │               SILVER                   │
          │           data/silver/                 │
          │ Données typées, nettoyées et validées  │
          │ Indicateurs météo calculés             │
          └────────────────────┬───────────────────┘
                               │
                 ┌─────────────┴──────────────┐
                 ▼                            ▼
      ┌────────────────────┐      ┌─────────────────────┐
      │ Logs de qualité    │      │ Analyses futures    │
      │ quality_checks/    │      │ Airflow, dbt,       │
      │                    │      │ BigQuery, Power BI  │
      └────────────────────┘      └─────────────────────┘
```

### Couche Bronze --- données ingérées

La couche Bronze conserve les données récupérées depuis l'API dans des
fichiers Parquet. Les données sont organisées par ville et par année
afin de faciliter leur inspection et leur traitement.

Chemin : `data/bronze/`

Principes : - conserver une couche source distincte des données
transformées ; - éviter de re-télécharger les années déjà présentes lors
d'une relance normale ; - permettre de reconstruire Silver à partir de
Bronze ; - faciliter les vérifications avant transformation.

### Couche Silver --- données nettoyées

La couche Silver est produite par un job PySpark. Elle applique les
règles de qualité, supprime les doublons sur la clé `(city, time)`,
vérifie les limites métier configurées et ajoute des indicateurs utiles
à l'analyse.

Chemin : `data/silver/`

Les fichiers de contrôle sont enregistrés séparément dans
`data/silver/quality_checks/`.

### Couche Gold --- à venir

Une couche Gold pourra contenir des tables agrégées pour l'analyse
métier, par exemple : - statistiques quotidiennes et mensuelles par
ville ; - nombre d'heures dépassant un seuil de température ; - cumul de
précipitations par jour ; - classement des villes selon des indicateurs
définis.

La couche Gold n'est pas encore incluse dans le pipeline actuel.

------------------------------------------------------------------------

## Structure du dépôt

``` text
atlas-pulse/
├── ingestion/
│   ├── fetch_weather.py       # Ingestion API vers Bronze
│   ├── inspect_bronze.py     # Inspection détaillée de Bronze
│   └── check_bronze.py       # Contrôle rapide de Bronze
├── spark_jobs/
│   └── bronze_to_silver.py   # Transformation Bronze vers Silver
├── dags/                     # DAGs Airflow — à venir
├── dbt/                      # Modèles analytiques — à venir
├── data/                     # Données locales, ignorées par Git
│   ├── bronze/
│   └── silver/
│       └── quality_checks/
├── docs/
│   └── api_notes.md          # Notes sur l'API Open-Meteo
├── Dockerfile                # Environnement PySpark et Java 17
├── docker-compose.yml         # Configuration du service Spark
├── requirements.txt
└── README.md
```

Les fichiers et dossiers peuvent varier légèrement selon l'évolution du
dépôt.

------------------------------------------------------------------------

## Installation et exécution

### Prérequis

-   Python 3.11 ou version compatible avec les dépendances du projet.
-   Docker Desktop installé et démarré.
-   WSL 2 activé sous Windows si nécessaire pour Docker Desktop.
-   Git pour cloner le dépôt.

Aucune installation locale de Java ou de Spark n'est nécessaire pour le
job Spark si l'image Docker du projet contient bien ces dépendances.

### 1. Cloner le dépôt

``` bash
git clone <URL_DU_DEPOT>
cd atlas-pulse
```

Remplacez `<URL_DU_DEPOT>` par l'URL réelle du dépôt Git.

### 2. Installer les dépendances Python

Sous Windows PowerShell :

``` powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Sous macOS ou Linux :

``` bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Exécuter l'ingestion

À la racine du dépôt :

``` bash
python -m ingestion.fetch_weather
```

Le script récupère les données par année et écrit les fichiers Parquet
dans `data/bronze/`. Lors d'une relance, les années déjà présentes sont
ignorées selon la logique d'ingestion mise en place. Si une année
échoue, relancez le script après avoir vérifié la cause de l'erreur.

**Durée indicative :** environ 5 à 8 minutes, selon l'accès réseau et
les réponses de l'API.

### 4. Vérifier la couche Bronze

Pour une vérification rapide :

``` bash
python -m ingestion.check_bronze
```

Pour une inspection plus détaillée :

``` bash
python -m ingestion.inspect_bronze
```

### 5. Construire et démarrer le service Spark

Assurez-vous que Docker Desktop est en cours d'exécution.

``` bash
docker compose build
docker compose up -d
```

Vérifiez l'état des conteneurs :

``` bash
docker compose ps
```

### 6. Transformer Bronze vers Silver

``` bash
docker compose exec spark python -m spark_jobs.bronze_to_silver
```

Cette commande suppose que le service du fichier `docker-compose.yml`
s'appelle `spark` et que le dépôt est disponible dans le conteneur au
chemin prévu par sa configuration.

### 7. Consulter les logs de qualité

La commande suivante lit les fichiers Parquet de contrôle depuis
l'environnement où `pandas` est installé et où le dossier `data/` est
accessible :

``` bash
python -c "import glob, pandas as pd; files = glob.glob('data/silver/quality_checks/**/*.parquet', recursive=True); print(pd.concat([pd.read_parquet(f) for f in files], ignore_index=True).to_string(index=False) if files else 'Aucun fichier de contrôle trouvé.')"
```

Si les données sont uniquement accessibles dans le conteneur, exécutez
la commande dans ce conteneur ou adaptez le chemin au volume monté dans
`docker-compose.yml`.

### 8. Arrêter le service Spark

``` bash
docker compose down
```

Cette commande arrête et supprime les conteneurs du projet sans
supprimer normalement les fichiers de données présents dans les
répertoires montés sur l'hôte.

------------------------------------------------------------------------

## Contrôles de qualité

Les contrôles sont appliqués pendant la transformation Bronze vers
Silver. Ils servent à rendre les données plus fiables et à garder une
trace des opérations réalisées.

### Règles appliquées

  Contrôle         Règle
  ---------------- --------------------------------------------------------
  Unicité          Déduplication sur `(city, time)`
  Température      Valeur comprise entre -20 °C et 60 °C
  Précipitations   Valeur supérieure ou égale à 0 mm
  Traçabilité      Écriture d'un journal de qualité pour chaque exécution

### Indicateurs calculés

  -----------------------------------------------------------------------
  Indicateur                          Définition actuelle
  ----------------------------------- -----------------------------------
  `is_heatwave_candidate`             `true` si la température horaire
                                      est supérieure ou égale à 40 °C

  `is_heavy_rain`                     `true` si les précipitations
                                      horaires sont supérieures ou égales
                                      à 10 mm
  -----------------------------------------------------------------------

Ces indicateurs signalent des **candidats** à des événements nécessitant
une attention particulière. Une température horaire de 40 °C ne suffit
pas, à elle seule, à définir une vague de chaleur météorologique : une
définition officielle peut nécessiter plusieurs jours consécutifs et des
seuils adaptés à la région. De même, le seuil de 10 mm est une règle de
projet et non une classification universelle des fortes précipitations.

### Journal de qualité

Chaque exécution écrit un journal horodaté dans un dossier de la forme :

``` text
data/silver/quality_checks/run_id=<timestamp>/
```

Le journal contient notamment les métriques suivantes :

  -----------------------------------------------------------------------
  Métrique                            Signification
  ----------------------------------- -----------------------------------
  `rows_received`                     Nombre de lignes lues depuis Bronze

  `duplicates_removed`                Nombre de doublons supprimés

  `rows_rejected`                     Nombre de lignes rejetées par les
                                      règles configurées

  `rows_clean`                        Nombre de lignes écrites dans
                                      Silver
  -----------------------------------------------------------------------

Les noms exacts et le contenu des colonnes dépendent de l'implémentation
du job PySpark.

------------------------------------------------------------------------

## Reproductibilité et reprise

Le projet a été conçu pour séparer l'ingestion des transformations :

-   **Bronze est conservée** comme couche de données source.
-   **Silver peut être recalculée** à partir de Bronze sans interroger à
    nouveau l'API, tant que les fichiers Bronze nécessaires sont
    disponibles.
-   **Les exécutions sont traçables** grâce aux journaux de qualité
    horodatés.
-   **PySpark est conteneurisé** avec Docker afin de limiter les
    différences entre environnements.
-   **L'ingestion est relançable** pour compléter les années manquantes
    selon la logique du script.

La reproductibilité dépend aussi de la version des scripts, des
dépendances, de la configuration Docker et des données sources. Pour
renforcer ce point, il est recommandé de versionner les dépendances, de
documenter la configuration et d'ajouter des tests automatisés.

------------------------------------------------------------------------

## Limites et précautions d'interprétation

-   Les données sont limitées aux cinq villes configurées.
-   Les indicateurs de chaleur et de précipitations utilisent des seuils
    définis pour ce projet ; ils ne constituent pas des alertes
    météorologiques officielles.
-   Les contrôles de qualité vérifient les règles programmées, mais ne
    garantissent pas à eux seuls l'exactitude météorologique de chaque
    observation.
-   Les temps d'exécution dépendent de la machine et de la configuration
    Docker.
-   Airflow, BigQuery, dbt et Power BI sont des évolutions prévues tant
    qu'ils ne sont pas effectivement intégrés au dépôt.
-   Les données locales ne sont pas versionnées dans Git ; il faut
    exécuter l'ingestion pour les recréer.

------------------------------------------------------------------------

## Roadmap

-   [x] Récupération des données horaires depuis Open-Meteo.
-   [x] Stockage des données en Parquet dans Bronze.
-   [x] Transformation Bronze vers Silver avec PySpark.
-   [x] Déduplication et contrôles de qualité.
-   [x] Journaux de qualité par exécution.
-   [ ] Ajouter des tests automatisés pour l'ingestion et les
    transformations.
-   [ ] Orchestrer le pipeline avec Apache Airflow.
-   [ ] Construire des modèles analytiques Gold avec dbt.
-   [ ] Charger les données dans BigQuery Sandbox.
-   [ ] Créer un dashboard Power BI pour les tendances et événements
    météo.
-   [ ] Ajouter une documentation d'exploitation et des exemples
    d'analyses.

------------------------------------------------------------------------

## Pistes d'analyse

Une fois la couche analytique et le dashboard développés, AtlasPulse
pourra servir à explorer :

-   les températures maximales par ville et par mois ;
-   le nombre d'heures au-dessus du seuil de 40 °C ;
-   les cumuls de précipitations journaliers et mensuels ;
-   la fréquence des observations dépassant les seuils configurés ;
-   la comparaison des tendances entre villes ;
-   la complétude et la qualité des données par période.

Ces analyses décrivent les données disponibles. Toute conclusion sur une
tendance climatique à long terme nécessite une méthodologie appropriée,
une période suffisamment longue et une prise en compte des limites de la
source.

------------------------------------------------------------------------

## Auteur

**Fatima Chegdani**\
Élève ingénieure en Génie Informatique et Intelligence Artificielle ---
ENSA Safi

-   LinkedIn : [Fatima
    Chegdani](https://www.linkedin.com/in/fatima-chegdani-44b840274)
-   GitHub : ajoutez ici l'URL réelle du dépôt AtlasPulse.

------------------------------------------------------------------------

*AtlasPulse est un projet personnel de Data Engineering réalisé à des
fins d'apprentissage et de démonstration technique.*
