#!/usr/bin/env python3
import sys
import math

current_pair = None
sum_xy = 0.0
sum_x2 = 0.0
sum_y2 = 0.0
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        pair, ratings = line.split("\t")
        r1, r2 = ratings.split(",")
        r1 = float(r1)
        r2 = float(r2)
    except ValueError:
        continue
    
    if current_pair is None:
        current_pair = pair

    # When key changes, compute similarity
    if pair != current_pair:
        if count > 0:
            denom = math.sqrt(sum_x2) * math.sqrt(sum_y2)
            sim = sum_xy / denom if denom != 0 else 0
            print(f"{current_pair}\t{sim}\t{count}")
        
        # reset accumulator
        current_pair = pair
        sum_xy = 0.0
        sum_x2 = 0.0
        sum_y2 = 0.0
        count = 0

    # accumulate
    sum_xy += r1 * r2
    sum_x2 += r1 * r1
    sum_y2 += r2 * r2
    count += 1

# flush last
if current_pair is not None and count > 0:
    denom = math.sqrt(sum_x2) * math.sqrt(sum_y2)
    sim = sum_xy / denom if denom != 0 else 0
    print(f"{current_pair}\t{sim}\t{count}")