#!/usr/bin/env python3
"""
Reducer 3 : Calcule la durée moyenne des films par genre
Reçoit : genre \t minutes \t 1
Émet   : genre \t count \t total_minutes \t avg_minutes
"""

import sys
from collections import defaultdict

def reducer():
    totals = defaultdict(lambda: [0, 0])  # {genre: [total_minutes, count]}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 3:
            continue

        genre, minutes_str, count_str = parts
        try:
            minutes = int(minutes_str)
            count   = int(count_str)
            totals[genre][0] += minutes
            totals[genre][1] += count
        except ValueError:
            continue

    print("Genre\tNombre_Films\tTotal_Minutes\tMoyenne_Minutes")
    for genre, (total_min, count) in sorted(totals.items(), key=lambda x: -x[1][1]):
        if count > 0:
            avg = round(total_min / count, 2)
            print(f"{genre}\t{count}\t{total_min}\t{avg}")

if __name__ == "__main__":
    reducer()
