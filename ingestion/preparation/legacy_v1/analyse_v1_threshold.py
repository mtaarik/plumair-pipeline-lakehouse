"""Analyse du seuil d'alerte sur le dataset v1, conservée pour traçabilité.

Résultat : 27,94 % des mesures dépassent 100 µg/m³ (Alert Fatigue). Le seuil statique retenu
alors, 350 µg/m³, isolait 2,8 % des mesures (environ le 97e percentile).
"""
import pandas as pd

df = pd.read_csv("data/old_dataset/station_hour_streaming.csv")
df = df.dropna(subset=["PM2.5"])

for q in (0.97, 0.98, 0.99):
    print(f"Percentile {int(q * 100)} : {df['PM2.5'].quantile(q):.1f} µg/m³")

for threshold in (100, 350):
    share = (df["PM2.5"] > threshold).mean() * 100
    print(f"Part des mesures PM2.5 > {threshold} µg/m³ : {share:.2f} %")

print("\n--- Part des mesures PM2.5 > 100 par station ---")
pct_by_station = df.groupby("StationId")["PM2.5"].apply(lambda x: (x > 100).mean() * 100)
print(pct_by_station.sort_values(ascending=False).round(2).astype(str).add(" %").to_string())
