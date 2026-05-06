#!/usr/bin/env python3
"""
Mapper 2 : Analyse des productions par année et pays
Dataset : Netflix Movies and TV Shows (Kaggle)
Objectif : identifier les tendances de production par pays et par année
"""

import sys
import csv

def mapper():
    """
    Émet : (country|year, 1) pour chaque contenu
    """
    reader = csv.reader(sys.stdin)
    header_skipped = False

    for line in reader:
        if not header_skipped:
            header_skipped = True
            continue

        try:
            if len(line) < 12:
                continue

            country = line[5].strip()    # country
            year    = line[7].strip()    # release_year

            if not country or not year:
                continue

            # Un contenu peut avoir plusieurs pays de production
            for c in country.split(","):
                c = c.strip()
                if c and year.isdigit():
                    print(f"{c}\t{year}\t1")

        except Exception:
            continue

if __name__ == "__main__":
    mapper()
