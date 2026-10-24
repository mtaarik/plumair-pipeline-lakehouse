# scripts/generate_operators_seed.py
from faker import Faker
import csv

fake = Faker()
Faker.seed(42)  # graine fixe = reproductibilité, traçabilité du jeu de données synthétique

shifts = ["Matin (06h-14h)", "Après-midi (14h-22h)", "Nuit (22h-06h)"]

with open("seeds/gold_dim_operator.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["operator_id", "operator_name", "shift", "years_experience"])
    for i in range(1, 9):  # 8 opérateurs, 3 équipes de 2-3
        writer.writerow([
            f"OP{i:03d}",
            fake.name(),
            shifts[i % 3],
            fake.random_int(min=1, max=15)
        ])