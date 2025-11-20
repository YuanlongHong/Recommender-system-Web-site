#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or not line[0].isdigit():   # 跳过表头和异常行
        continue

    parts = line.split(',')
    if len(parts) < 3:
        continue  # 防止 CSV 行不完整

    user = parts[0]
    movie = parts[1]
    rating = parts[2]

    print(f"{movie}\t{user}:{rating}")