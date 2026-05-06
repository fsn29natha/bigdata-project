#!/usr/bin/env python3
"""
Reducer 2 : Agrège le nombre de productions par (pays, année)
"""

import sys
from collections import defaultdict

def reducer():
    counts = defaultdict(int)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 3:
            continue

        country, year, count_str = parts
        try:
            counts[(country, year)] += int(count_str)
        except ValueError:
            continue

    # Top 10 pays : filtrer les pays avec > 50 productions
    for (country, year), total in sorted(counts.items()):
        print(f"{country}\t{year}\t{total}")

if __name__ == "__main__":
    reducer()
