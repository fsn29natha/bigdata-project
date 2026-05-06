#!/usr/bin/env python3
"""
Reducer 1 : Agrège le nombre de films par genre et type
Reçoit des lignes triées : genre\ttype\t1
Émet : genre\ttype\tcount
"""

import sys
from collections import defaultdict

def reducer():
    """
    Reçoit les lignes du mapper triées par clé (genre).
    Agrège et émet le total par (genre, type).
    """
    counts = defaultdict(int)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 3:
            continue

        genre, content_type, count_str = parts
        try:
            counts[(genre, content_type)] += int(count_str)
        except ValueError:
            continue

    # Émettre les résultats triés par genre
    for (genre, content_type), total in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"{genre}\t{content_type}\t{total}")

if __name__ == "__main__":
    reducer()
