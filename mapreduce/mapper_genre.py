#!/usr/bin/env python3
"""
Mapper 1 : Compte le nombre de films par genre
Dataset : Netflix Movies and TV Shows (Kaggle)
Colonnes utilisées : type, listed_in (genres), release_year, rating
"""

import sys
import csv
import io

def mapper():
    """
    Lit depuis stdin (HDFS via Hadoop Streaming).
    Émet : (genre, 1) pour chaque film/série
    """
    reader = csv.reader(sys.stdin)
    header_skipped = False

    for line in reader:
        # Sauter l'en-tête CSV
        if not header_skipped:
            header_skipped = True
            continue

        try:
            if len(line) < 12:
                continue

            content_type = line[1].strip()   # "Movie" ou "TV Show"
            title        = line[2].strip()
            genres       = line[10].strip()  # ex: "Documentaries, International Movies"
            year         = line[7].strip()   # release_year

            if not genres or not year:
                continue

            # Émettre (genre, 1) pour chaque genre de ce film
            for genre in genres.split(","):
                genre = genre.strip()
                if genre:
                    # clé = "genre|type"  valeur = 1
                    print(f"{genre}\t{content_type}\t1")

        except Exception:
            continue  # Ignorer les lignes malformées

if __name__ == "__main__":
    mapper()
