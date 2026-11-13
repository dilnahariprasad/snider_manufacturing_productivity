"""Generate synthetic Snider production data (stdlib only, deterministic)."""
import csv, os, random
from datetime import datetime, timedelta

random.seed(42)
OUT = os.path.join(os.path.dirname(__file__), "..", "data")
PLANTS = {"Detroit": ["L1", "L2"], "Dallas": ["L1", "L2"], "Columbus": ["L1"]}
PRODUCTS = {"Gear-A": 30, "Housing-B": 45, "Shaft-C": 20, "Bracket-D": 15}  # ideal cycle sec
REASONS = [("Mechanical", "Bearing failure"), ("Mechanical", "Tool wear"), ("Changeover", "Product changeover"),
           ("Material", "Material shortage"), ("Electrical", "Sensor fault"), ("Operator", "Break overrun"),
           ("Quality", "Inspection hold")]
WEIGHTS = [5, 8, 12, 4, 3, 2, 3]
SHIFTS = [("Day", 6), ("Swing", 14), ("Night", 22)]

os.makedirs(os.path.join(OUT, "production_runs"), exist_ok=True)
os.makedirs(os.path.join(OUT, "downtime_events"), exist_ok=True)
runs, downs = [], []
day0 = datetime(2025, 1, 1)
rid = eid = 0
for plant, lines in PLANTS.items():
    for line in lines:
        for m in range(1, 4):
            machine = f"{plant[:3].upper()}-{line}-M{m}"
            handicap = random.uniform(0.0, 0.25)  # some machines are chronically worse
            for d in range(60):
                for shift, hour in SHIFTS:
                    rid += 1
                    start = day0 + timedelta(days=d, hours=hour)
                    product = random.choice(list(PRODUCTS))
                    cycle = PRODUCTS[product]
                    planned = 480
                    down_total = 0.0
                    for _ in range(random.choices([0, 1, 2, 3], [3, 4, 2, 1])[0]):
                        eid += 1
                        cat, reason = random.choices(REASONS, WEIGHTS)[0]
                        mins = round(random.uniform(5, 60) * (1 + handicap), 1)
                        down_total += mins
                        downs.append([f"E{eid:06d}", machine, plant, line,
                                      start + timedelta(minutes=random.randint(0, 400)), mins, cat, reason])
                    run = max(planned - down_total, 0)
                    units = int(run * 60 / cycle * random.uniform(0.7, 0.98 - handicap / 2))
                    scrap = int(units * random.uniform(0.005, 0.03 + handicap / 4))
                    runs.append([f"R{rid:06d}", plant, line, machine, product, shift, start, planned,
                                 round(run, 1), units, scrap, cycle])

# Inject a bad row (scrap > produced) and a duplicate to exercise data-quality rules
runs.append(["R999998", "Detroit", "L1", "DET-L1-M1", "Gear-A", "Day", day0, 480, 400, 100, 500, 30])
runs.append(runs[0])

with open(os.path.join(OUT, "production_runs", "production_runs.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["run_id", "plant", "line", "machine_id", "product", "shift", "start_ts", "planned_minutes",
                "run_minutes", "units_produced", "units_scrapped", "ideal_cycle_sec"])
    w.writerows(runs)
with open(os.path.join(OUT, "downtime_events", "downtime_events.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["event_id", "machine_id", "plant", "line", "start_ts", "minutes", "category", "reason"])
    w.writerows(downs)
print(f"runs={len(runs)} downtime_events={len(downs)}")
