#!/usr/bin/env python3
"""Génère le référentiel statique data/merchants.json (300 commerçants, JSON Lines).

Usage : python producer/generate_merchants.py [--n 300] [--seed 42]
"""
import argparse
import json
import random
from pathlib import Path

# ville -> (poids, latitude, longitude) ; noms ASCII pour des chemins de partition propres
CITIES = {
    "Casablanca": (0.28, 33.5731, -7.5898),
    "Rabat": (0.14, 34.0209, -6.8416),
    "Marrakech": (0.12, 31.6295, -7.9811),
    "Fes": (0.10, 34.0181, -5.0078),
    "Tanger": (0.10, 35.7595, -5.8340),
    "Agadir": (0.08, 30.4278, -9.5981),
    "Meknes": (0.06, 33.8935, -5.5473),
    "Oujda": (0.05, 34.6814, -1.9086),
    "Kenitra": (0.04, 34.2610, -6.5802),
    "Tetouan": (0.03, 35.5889, -5.3626),
}

CATEGORIES = {
    "epicerie": (0.26, ["Epicerie", "Hanout", "Superette", "Mini Marche"]),
    "restauration": (0.16, ["Cafe", "Snack", "Restaurant", "Patisserie"]),
    "carburant": (0.10, ["Station", "Station Service", "Carburant"]),
    "telecom": (0.10, ["Teleboutique", "Recharge", "Phone Center"]),
    "pharmacie": (0.10, ["Pharmacie", "Parapharmacie"]),
    "e-commerce": (0.10, ["Shop", "Store", "Market"]),
    "habillement": (0.10, ["Boutique", "Mode", "Textile"]),
    "transport": (0.08, ["Taxi", "Transport", "Navette"]),
}

SUFFIXES = ["Al Amal", "Atlas", "Annour", "Assalam", "Al Baraka", "Zitoun", "Majorelle",
            "Andalous", "Al Fath", "Rif", "Sahara", "Oasis", "Medina", "Kasbah", "Al Wahda",
            "Ennakhil", "Al Firdaws", "Bab Lhad", "Al Qods", "Yasmine", "Argana", "Tafilalet"]


def weighted(rng, table):
    keys = list(table)
    return rng.choices(keys, weights=[table[k][0] for k in keys], k=1)[0]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "data" / "merchants.json"))
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for i in range(1, args.n + 1):
            city = weighted(rng, CITIES)
            category = weighted(rng, CATEGORIES)
            _, lat, lon = CITIES[city]
            row = {
                "merchant_id": f"M-{i:03d}",
                "name": f"{rng.choice(CATEGORIES[category][1])} {rng.choice(SUFFIXES)}",
                "city": city,
                "category": category,
                "latitude": round(lat + rng.uniform(-0.06, 0.06), 6),
                "longitude": round(lon + rng.uniform(-0.06, 0.06), 6),
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"{args.n} commercants ecrits dans {out}")


if __name__ == "__main__":
    main()
