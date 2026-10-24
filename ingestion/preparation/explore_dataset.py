import pandas as pd

file_path = 'data/raw/station_hour_streaming.csv'

print("Chargement des données en cours...")
df = pd.read_csv(file_path)

print("\n==================================================")
print("       RAPPORT D'EXPLORATION DU DATASET")
print("==================================================")

print("\n--- 1. APERÇU TEMPOREL ---")
print(f"Date de début : {df['Datetime'].min()}")
print(f"Date de fin   : {df['Datetime'].max()}")
print(f"Nombre total de lignes : {len(df)}")

print("\n1. Aperçu des colonnes (Météo + Polluants) :")
print(df.info())

print("\n2. Analyse de la Volumétrie (Risque de surcharge) :")
# Utilisation dynamique de 'City' ou 'StationId' selon le dataset
city_col = 'City' if 'City' in df.columns else 'StationId'
pm25_col = 'PM2_5_ugm3' if 'PM2_5_ugm3' in df.columns else 'PM2.5'

total_stations = df[city_col].nunique()
print(f"Nombre total de villes distinctes : {total_stations}")
print(f"Nombre total de lignes : {len(df)}")

print("\n--- 2. ANALYSE DES VALEURS MANQUANTES (Top 15) ---")
# On calcule le pourcentage de données manquantes par colonne
missing_pct = (df.isnull().sum() / len(df)) * 100
missing_filtered = missing_pct[missing_pct > 0].sort_values(ascending=False).head(15)
if not missing_filtered.empty:
    print(missing_filtered.apply(lambda x: f"{x:.2f}%"))
else:
    print("Excellente nouvelle : 0 valeur manquante dans ce dataset !")

print(f"\n--- 3. DISTRIBUTION DE LA CIBLE ({pm25_col}) ---")
# Analyse des percentiles pour trouver le point de bascule des alertes
if pm25_col in df.columns:
    print(df[pm25_col].describe(percentiles=[0.25, 0.50, 0.75, 0.90, 0.95, 0.98, 0.99]))

print("\n--- 4. DISTRIBUTION DES GAZ ET AUTRES POLLUANTS ---")
gas_cols = [c for c in ['CO_ugm3', 'NO2_ugm3', 'SO2_ugm3', 'O3_ugm3', 'PM10_ugm3', 'Dust_ugm3'] if c in df.columns]
if gas_cols:
    gas_summary = df[gas_cols].describe(percentiles=[0.25, 0.50, 0.75, 0.90, 0.95, 0.99]).T
    print(gas_summary.to_string(formatters={
        'mean': '{:.2f}'.format,
        'std': '{:.2f}'.format,
        'min': '{:.2f}'.format,
        '25%': '{:.2f}'.format,
        '50%': '{:.2f}'.format,
        '75%': '{:.2f}'.format,
        '90%': '{:.2f}'.format,
        '95%': '{:.2f}'.format,
        '99%': '{:.2f}'.format,
        'max': '{:.2f}'.format
    }))
else:
    print("Aucune colonne de gaz trouvée.")

print("\n--- 5. ÉQUILIBRE DES VILLES (Top 25) ---")
# On vérifie si certaines villes ont beaucoup plus de relevés que d'autres
# et on regarde leur niveau de pollution maximum pour identifier les zones critiques
city_stats = df.groupby(city_col).agg(
    Nombre_Releves=(city_col, 'count'),
    PM25_Moyen=(pm25_col, 'mean'),
    PM25_Max=(pm25_col, 'max')
).sort_values(by='Nombre_Releves', ascending=False).head(25)

# Formatage pour une lecture propre dans le terminal
print(city_stats.to_string(formatters={
    'PM25_Moyen': '{:.1f}'.format,
    'PM25_Max': '{:.1f}'.format
}))