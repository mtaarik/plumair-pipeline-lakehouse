"""Préparation du premier dataset (v1 : Air Quality Data in India 2015-2020), conservée pour traçabilité.

Entrée  : data/old_dataset/station_hour.csv (2 589 083 lignes, 110 stations) + stations.csv
Sortie  : data/old_dataset/station_hour_streaming.csv (475 116 lignes, 10 stations)

Ce dataset a ensuite été remplacé par le dataset v2 (variables météo) : voir ../prepare_new_dataset.py
"""
import pandas as pd

print("1. Chargement des données brutes...")
df_hour = pd.read_csv('data/old_dataset/station_hour.csv')
df_stations = pd.read_csv('data/old_dataset/stations.csv')

print("2. Identification des stations les plus complètes...")
# Nombre de relevés PM2.5 non nuls par station : on garde les 10 plus complètes,
# pour ne pas saturer le CPU et la RAM du poste local pendant la simulation
top_stations = df_hour.dropna(subset=['PM2.5']).groupby('StationId').size().nlargest(10).index
print(f"Stations sélectionnées : {list(top_stations)}")

print("3. Filtrage des données horaires et du référentiel...")
df_hour_filtered = df_hour[df_hour['StationId'].isin(top_stations)].copy()
df_stations_filtered = df_stations[df_stations['StationId'].isin(top_stations)].copy()

print("4. Tri chronologique pour la simulation NiFi...")
# Le CSV Kaggle est trié par station : sans ce tri, NiFi rejouerait une station après l'autre
df_hour_filtered['Datetime'] = pd.to_datetime(df_hour_filtered['Datetime'])
df_hour_filtered = df_hour_filtered.sort_values(by=['Datetime', 'StationId'])

print("5. Sauvegarde des fichiers optimisés...")
df_hour_filtered.to_csv('data/old_dataset/station_hour_streaming.csv', index=False)
df_stations_filtered.to_csv('data/old_dataset/stations_filtered.csv', index=False)

print("Terminé ! Le dataset est allégé et prêt pour l'ingestion.")
