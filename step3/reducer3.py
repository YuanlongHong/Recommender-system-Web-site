#!/usr/bin/env python3
import sys

# --- Reducer 3: dynamic threshold and combine with Top-K ---
# Using in User-Based and Item-Based
# input：MainID \t NeighborID:Sim:Count
# output：MainID \t Neighbor1:Sim,Neighbor2:Sim...

current_id = None
neighbors = []

# --- config ---
K = 50
# Dynamic threshold : keep the more count first.
# Logic: find Count>=5 first and then >=4...
THRESHOLDS = [5, 4, 3, 2, 1]

def emit_result(main_id, neighbor_list):
    """
    excute filter and output Top-K
    """
    # 1. Seperate data
    parsed_neighbors = []
    for n in neighbor_list:
        try:
            # n format: "id:sim:count"
            parts = n.split(':')
            nid = parts[0]
            sim = float(parts[1])
            cnt = int(parts[2])
            parsed_neighbors.append({'id': nid, 'sim': sim, 'count': cnt})
        except (ValueError, IndexError):
            continue

    selected_candidates = None

    # 2. Dynamic threshold filtering
    for t in THRESHOLDS:
        # filtrate Count >= t nerighbors
        filtered = [x for x in parsed_neighbors if x['count'] >= t]

        # if enough candidates, break
        if len(filtered) >= K:
            selected_candidates = filtered
            break

    # 3. Fallback logic
    # If after the loop there are still not enough K neighbors (or there are very few neighbors), use the last filtered result or all data
    if selected_candidates is None:
        selected_candidates = parsed_neighbors

    # 4. Sort by similarity in descending order
    selected_candidates.sort(key=lambda x: x['sim'], reverse=True)

    # 5. Take Top K
    top_k = selected_candidates[:K]

    # 6. Format output (remove Count, keep only ID:Sim for Python prediction script)
    out_list = [f"{x['id']}:{x['sim']}" for x in top_k]
    out_str = ",".join(out_list)

    # Final output: ID \t n1:0.9,n2:0.8...
    print(f"{main_id}\t{out_str}")

# --- Main loop ---
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        # Input format: MainID \t NeighborInfo
        id_val, neighbor_info = line.split("\t", 1)
    except ValueError:
        continue

    if current_id is None:
        current_id = id_val

    if id_val != current_id:
        emit_result(current_id, neighbors)
        current_id = id_val
        neighbors = []

    neighbors.append(neighbor_info)

# Process the last group
if current_id is not None:
    emit_result(current_id, neighbors)