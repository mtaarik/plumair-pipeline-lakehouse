"""Calibration du seuil d'alerte de la voie rapide (Fast Track).

Le seuil est le 99e percentile du PM2.5 calculé sur l'ensemble du dataset source (29 villes),
soit 155.1 µg/m³. Appliqué au flux simulé (les 20 villes les plus polluées), il isole environ
1,4 % des mesures, contre près de 6 % avec un seuil fixe à 100 µg/m³ (risque d'Alert Fatigue).

À lancer depuis la racine du dépôt : python ingestion/preparation/compute_alert_threshold.py
"""
import pandas as pd

source = pd.read_csv('data/raw/new_air_quality_dataset.csv', usecols=['City', 'PM2_5_ugm3'])
stream = pd.read_csv('data/raw/station_hour_streaming.csv', usecols=['StationId', 'PM2.5'])

pm25_source = source['PM2_5_ugm3'].dropna()
pm25_stream = stream['PM2.5'].dropna()

threshold = round(pm25_source.quantile(0.99), 1)

print(f"Dataset source : {len(source)} lignes, {source['City'].nunique()} villes")
print(f"Flux simulé    : {len(stream)} lignes, {stream['StationId'].nunique()} villes")
print(f"\nSeuil retenu (P99 du dataset source) : {threshold} µg/m³")

for label, value in [('seuil fixe 100', 100), (f'seuil P99 {threshold}', threshold)]:
    share = (pm25_stream > value).mean() * 100
    count = int((pm25_stream > value).sum())
    print(f"  {label:<18} -> {share:5.2f} % du flux ({count} alertes)")
