# Page Power BI « Alertes temps réel » (voie rapide)

Le rapport `pulmaire.pbix` contient trois pages alimentées par Trino (ODBC, DSN `Trino_Lakehouse`) :
Stratégique, Opérationnel et Analytique. Cette page ajoute la vue temps réel, lue directement dans la
base OLTP `plumair_ops` remplie par NiFi (processeur `Store_Alert_PostgreSQL`).

## 1. Source de données (DirectQuery)

1. Obtenir les données > Base de données PostgreSQL.
2. Serveur `localhost:5433`, base `plumair_ops`, mode **DirectQuery**.
3. Identifiants de base de données : ceux du service `postgres-ops` dans `infrastructure/docker-compose.yml`.
4. Sélectionner la vue `public.v_alert_latency`.

## 2. Mesures DAX

```DAX
Nb alertes (5 min) =
CALCULATE (
    COUNTROWS ( v_alert_latency ),
    v_alert_latency[inserted_at] >= UTCNOW () - TIME ( 0, 5, 0 )
)

Latence pipeline moyenne (ms) =
AVERAGE ( v_alert_latency[pipeline_latency_ms] )

Latence pipeline max (ms) =
MAX ( v_alert_latency[pipeline_latency_ms] )

Fraîcheur dernière alerte (s) =
DATEDIFF ( MAX ( v_alert_latency[meta_ingested_at] ), UTCNOW (), SECOND )
```

`pipeline_latency_ms` mesure le trajet NiFi -> PostgreSQL (de `_meta_ingested_at`, posé après le
ControlRate, jusqu'à l'insertion). Le délai d'affichage s'y ajoute : au plus une période de
rafraîchissement automatique de la page.

## 3. Visuels

- Cartes : `Nb alertes (5 min)`, `Latence pipeline moyenne (ms)`, `Latence pipeline max (ms)`.
  Les percentiles (tableau plus bas) se calculent côté SQL : les fonctions de percentile DAX ne sont
  pas toujours traduites en DirectQuery.
- Tableau trié par `inserted_at` décroissant : `station_id`, `measurement_time`, `pm25_ugm3`,
  `meta_ingested_at`, `inserted_at`, `pipeline_latency_ms`.
- Histogramme groupé : nombre d'alertes par `station_id`.

## 4. Actualisation automatique

Format de la page > Actualisation de la page > activé, type « Actualisation automatique de la page »,
intervalle **1 seconde**. L'option n'est disponible que pour une source en DirectQuery.

## Mesures relevées (24/10/2026, rejeu du dataset à 300 msg/s)

| Indicateur | Valeur |
|---|---|
| Alertes reçues dans PostgreSQL | 4 667 |
| Latence NiFi -> PostgreSQL, médiane | 68 ms |
| Latence NiFi -> PostgreSQL, p95 | 169 ms |
| Latence NiFi -> PostgreSQL, p99 | 251 ms |
| Latence NiFi -> PostgreSQL, maximum | 492 ms |

Requête de contrôle : `infrastructure/postgres-ops/latency_report.sql`.
