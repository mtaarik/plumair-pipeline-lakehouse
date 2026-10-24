-- Mesure de la latence de la voie rapide (base plumair_ops)
-- docker exec -i plumair-ops-db psql -U ops -d plumair_ops < infrastructure/postgres-ops/latency_report.sql

-- Distribution : horodatage NiFi (_meta_ingested_at) -> ligne disponible dans PostgreSQL
SELECT
    count(*)                                                               AS nb_alertes,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY pipeline_latency_ms)::numeric, 1) AS p50_ms,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY pipeline_latency_ms)::numeric, 1) AS p95_ms,
    round(percentile_cont(0.99) WITHIN GROUP (ORDER BY pipeline_latency_ms)::numeric, 1) AS p99_ms,
    round(max(pipeline_latency_ms)::numeric, 1)                            AS max_ms
FROM v_alert_latency;

-- Dernières alertes reçues
SELECT station_id, measurement_time, pm25_ugm3, meta_ingested_at, inserted_at,
       round(pipeline_latency_ms::numeric, 1) AS latency_ms
FROM v_alert_latency
ORDER BY inserted_at DESC
LIMIT 10;
