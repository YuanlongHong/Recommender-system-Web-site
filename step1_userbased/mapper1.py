#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or not line[0].isdigit():   # skip the header and anormal lines
        continue

    parts = line.split(',')
    if len(parts) < 3:
        continue 

    user = parts[0]
    movie = parts[1]
    rating = parts[2]

    print(f"{movie}\t{user}:{rating}")