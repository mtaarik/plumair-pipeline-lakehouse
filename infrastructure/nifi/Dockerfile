FROM apache/nifi:1.23.2

# Driver JDBC PostgreSQL : utilisé par le DBCPConnectionPool du processeur PutDatabaseRecord
# (voie rapide : écriture des alertes dans la base OLTP plumair_ops)
ARG PG_JDBC_VERSION=42.7.4
ADD --chown=nifi:nifi \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar \
    /opt/nifi/nifi-current/drivers/postgresql.jar
