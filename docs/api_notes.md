# Notes sur l'API Open-Meteo

## Endpoint archive (historique)

- URL testée : `archive-api.open-meteo.com/v1/archive`
- Période testée : 2024-01-01 → 2024-01-02 (Casablanca)
- Nombre de lignes attendues : 48 (2 jours × 24 h)
- Nombre de lignes obtenues : [à compléter]
- Présence de `null` : [oui/non, sur quelles variables]
- Coordonnées renvoyées : [lat/lon effectives]
- Fuseau horaire : Africa/Casablanca (GMT+1)

## Endpoint forecast (prévisions)

- URL testée : `api.open-meteo.com/v1/forecast`
- Période : 3 jours à partir d'aujourd'hui
- 72 lignes, aucune valeur `null`
- Températures plausibles (15–24 °C en octobre)

## Délai des données archive

- L'API historique a un délai d'environ 5 jours (parfois 5–7) sur les données les plus récentes [reference:0][reference:1].
- Pour les jours récents, il faut utiliser l'endpoint forecast avec le paramètre `past_days` [reference:2].
## Inspection couche Bronze (8 jours × 5 villes)

- Total lignes : 960
- Doublons (city, time) : 0
- Heures manquantes : 0
- Nulls temperature_2m : 0
- Nulls precipitation : 0
- Températures plausibles : oui (min 12–17 °C, max 24–36 °C selon la ville)
- Précipitations : toutes ≥ 0

### Règles de qualité pour Silver (déduites de cette inspection)
- Filtrer temperature_2m entre -20 et 60 °C (rejeter si hors bornes)
- Filtrer precipitation >= 0 (rejeter les valeurs négatives)
- Dédoublonner sur (city, time)
- Vérifier la continuité horaire (pas d'heure manquante)