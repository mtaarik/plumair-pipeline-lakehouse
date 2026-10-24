-- ==============================================================================
-- Catalogue JDBC Apache Iceberg (utilisé par Trino, cf. trino/iceberg.properties)
-- Les tables internes sont créées explicitement : les versions récentes de Trino
-- n'initialisent plus elles-mêmes le schéma du catalogue JDBC.
-- Exécuté une seule fois, à la création du volume plumair-postgres-data.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name               VARCHAR(255) NOT NULL,
    table_namespace            VARCHAR(255) NOT NULL,
    table_name                 VARCHAR(255) NOT NULL,
    metadata_location          VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type               VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name   VARCHAR(255) NOT NULL,
    namespace      VARCHAR(255) NOT NULL,
    property_key   VARCHAR(255) NOT NULL,
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);
