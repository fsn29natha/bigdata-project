#!/usr/bin/env python3
"""
Mapper 3 : Durée des films par genre
Objectif : calculer la durée moyenne des films par genre
"""

import sys
import csv
import re

def extract_duration_minutes(duration_str):
    """Extrait la durée en minutes depuis '90 min' ou '2 Seasons'"""
    match = re.match(r'(\d+)\s*min', duration_str, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None

def mapper():
    reader = csv.reader(sys.stdin)
    header_skipped = False

    for line in reader:
        if not header_skipped:
            header_skipped = True
            continue

        try:
            if len(line) < 12:
                continue

            content_type = line[1].strip()
            genres       = line[10].strip()
            duration     = line[9].strip()   # ex: "90 min" ou "3 Seasons"

            # Traiter uniquement les films (Movies)
            if content_type != "Movie":
                continue

            minutes = extract_duration_minutes(duration)
            if minutes is None or not genres:
                continue

            for genre in genres.split(","):
                genre = genre.strip()
                if genre:
                    # Émettre : genre \t minutes \t 1 (pour calculer la moyenne)
                    print(f"{genre}\t{minutes}\t1")

        except Exception:
            continue

if __name__ == "__main__":
    mapper()
