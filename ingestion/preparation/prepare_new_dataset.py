"""Préparation du fichier de simulation OT (dataset v2 : Air Quality Dataset - Indian Cities 2022-2025).

Entrée  : data/raw/new_air_quality_dataset.csv  (Kaggle, 842 160 lignes, 71 colonnes, 29 villes)
Sortie  : data/raw/station_hour_streaming.csv   (580 800 lignes, 63 colonnes, 20 villes)

À lancer depuis la racine du dépôt : python ingestion/preparation/prepare_new_dataset.py
"""
import pandas as pd

input_file = 'data/raw/new_air_quality_dataset.csv'
output_file = 'data/raw/station_hour_streaming.csv'

print("1. Chargement des données...")
df = pd.read_csv(input_file)

print("2. Purge des 8 colonnes fantômes...")
# Colonnes vides à 100 % dans la source : elles n'apportent rien et alourdissent le JSON
empty_cols = [
    'Temp_80m_C', 'Temp_120m_C', 'Temp_180m_C',
    'Wind_Speed_80m_kmh', 'Wind_Speed_120m_kmh',
    'UV_Index', 'NH3_ugm3', 'Inversion_Strength_C'
]
df = df.drop(columns=empty_cols, errors='ignore')

print("3. Sélection des 20 villes aux pics les plus extrêmes...")
# Les 20 villes au PM2.5 maximal le plus élevé (cf. explore_dataset.py, section 5)
top_20_cities = [
    'Mumbai', 'Gurugram', 'Delhi', 'Kolkata', 'Agartala',
    'Patna', 'Lucknow', 'Aizawl', 'Imphal', 'Raipur',
    'Chandigarh', 'Bhubaneswar', 'Ranchi', 'Jaipur', 'Guwahati',
    'Bhopal', 'Hyderabad', 'Ahmedabad', 'Kohima', 'Chennai'
]
df_final = df[df['City'].isin(top_20_cities)]

print("4. Rétrocompatibilité avec le pipeline NiFi...")
# Renommage pour correspondre au code existant dans NiFi et dbt
df_final = df_final.rename(columns={
    'City': 'StationId',
    'PM2_5_ugm3': 'PM2.5'
})

print("5. Exportation du fichier de streaming...")
# Tri par date pour simuler un vrai flux chronologique IoT
df_final = df_final.sort_values(by='Datetime')
df_final.to_csv(output_file, index=False)

print(f"\nSuccès ! Fichier généré : {output_file}")
print(f"Dimensions finales : {df_final.shape[0]} lignes, {df_final.shape[1]} colonnes.")
print("--> Seuil d'alerte du processeur QueryRecord : voir compute_alert_threshold.py (155.1)")
