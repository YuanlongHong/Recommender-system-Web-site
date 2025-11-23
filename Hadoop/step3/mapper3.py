#!/usr/bin/env python3
import sys

# --- Mapper 3: Normally seperate and pass the Count ---
# Using in User-Based and Item-Based
# input：ID1,ID2 \t Similarity \t Count
# output：ID1 \t ID2:Similarity:Count (and vice versa)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        parts = line.split("\t")

        # safety check
        if len(parts) < 3:
            continue

        pair_str = parts[0]
        sim = parts[1]
        count = parts[2] # Keep Count for Reducer filtering

        # Split Key "u1,u2" or "m1,m2"
        if "," in pair_str:
            id1, id2 = pair_str.split(",")

            # Symmetric output: A and B are similar, so A is in B's neighbors and B is in A's neighbors

            # Output to ID1
            print(f"{id1}\t{id2}:{sim}:{count}")

            # Output to ID2
            print(f"{id2}\t{id1}:{sim}:{count}")

    except ValueError:
        continue