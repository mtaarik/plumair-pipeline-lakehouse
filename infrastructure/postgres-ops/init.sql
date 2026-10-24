-- ==============================================================================
-- Base OLTP de la voie rapide (Speed Layer)
-- Alimentée en continu par NiFi (PutDatabaseRecord) dès qu'une mesure dépasse le
-- seuil d'alerte, et lue par Power BI en DirectQuery + Automatic Page Refresh.
-- Exécuté une seule fois, à la création du volume plumair-ops-data.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS realtime_alerts (
    alert_pk          BIGSERIAL        PRIMARY KEY,
    station_id        VARCHAR(64)      NOT NULL,
    measurement_time  TIMESTAMP        NOT NULL,   -- Event Time : horodatage de la mesure (dataset)
    pm25_ugm3         DOUBLE PRECISION NOT NULL,
    threshold_ugm3    DOUBLE PRECISION NOT NULL DEFAULT 155.1,
    meta_ingested_at  TIMESTAMP(3)     NOT NULL,   -- Processing Time : posé par NiFi après le ControlRate (UTC)
    inserted_at       TIMESTAMP(3)     NOT NULL DEFAULT (clock_timestamp() AT TIME ZONE 'UTC')
);

-- Index temporels : le dashboard ne lit que les alertes les plus récentes
CREATE INDEX IF NOT EXISTS idx_realtime_alerts_inserted_at ON realtime_alerts (inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_realtime_alerts_station     ON realtime_alerts (station_id, inserted_at DESC);

-- Latence de la voie rapide : de l'horodatage NiFi jusqu'à la ligne disponible en base
CREATE OR REPLACE VIEW v_alert_latency AS
SELECT
    alert_pk,
    station_id,
    measurement_time,
    pm25_ugm3,
    meta_ingested_at,
    inserted_at,
    EXTRACT(EPOCH FROM (inserted_at - meta_ingested_at)) * 1000 AS pipeline_latency_ms
FROM realtime_alerts;
