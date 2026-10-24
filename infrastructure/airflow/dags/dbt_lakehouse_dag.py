from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'plumair',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# target/ et logs/ restent dans le conteneur : le projet monté peut aussi être compilé depuis l'hôte
DBT = ('cd /opt/airflow/dbt_project && dbt {cmd} --profiles-dir /opt/airflow/.dbt '
       '--target-path /tmp/dbt_target --log-path /tmp/dbt_logs')

# Définition du DAG qui s'exécute toutes les 5 minutes (micro-batch de la voie analytique)
with DAG(
    'dbt_lakehouse_pipeline',
    default_args=default_args,
    description='Exécution des modèles dbt pour la plateforme PlumAir Lakehouse',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Un seul run à la fois : un run Bronze -> Gold ne doit jamais chevaucher le suivant
    max_active_runs=1,
    tags=['dbt', 'lakehouse', 'trino'],
) as dag:

    # Référentiels statiques (catégories AQI, seuils CPCB/OMS, opérateurs)
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=DBT.format(cmd='seed'),
    )

    # Bronze (Kafka -> Iceberg, append) puis Silver (nettoyage, merge)
    dbt_run_bronze_silver = BashOperator(
        task_id='dbt_run_bronze_silver',
        bash_command=DBT.format(cmd='run --select bronze silver'),
    )

    # Historisation SCD2 du référentiel stations (lu depuis silver_station)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=DBT.format(cmd='snapshot'),
    )

    # Modèle en étoile Gold, KPI et vue ML
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=DBT.format(cmd='run --select gold'),
    )

    # Validation des contrats de données sur l'ensemble des couches
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=DBT.format(cmd='test'),
    )

    dbt_seed >> dbt_run_bronze_silver >> dbt_snapshot >> dbt_run_gold >> dbt_test
