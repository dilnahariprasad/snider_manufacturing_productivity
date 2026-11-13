import sys, os
sys.path.append(os.path.dirname(globals().get("__file__", os.path.join(os.getcwd(), "x"))))
from common import parse_args, get_spark

a = parse_args()
spark = get_spark()
c, p = a.catalog, a.prefix
S, G = f"{c}.{p}_silver", f"{c}.{p}_gold"

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.oee_daily AS
WITH agg AS (
  SELECT plant, line, machine_id, production_date,
         SUM(planned_minutes) planned_min, SUM(run_minutes) run_min,
         SUM(units_produced) units, SUM(units_scrapped) scrap, SUM(units_good) good,
         SUM(ideal_cycle_sec * units_produced) ideal_sec
  FROM {S}.production_runs GROUP BY plant, line, machine_id, production_date)
SELECT *,
  LEAST(run_min / planned_min, 1.0) AS availability,
  LEAST(ideal_sec / NULLIF(run_min * 60, 0), 1.0) AS performance,
  good / NULLIF(units, 0) AS quality,
  LEAST(run_min / planned_min, 1.0) * LEAST(ideal_sec / NULLIF(run_min * 60, 0), 1.0) * good / NULLIF(units, 0) AS oee
FROM agg
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.machine_scorecard AS
SELECT plant, line, machine_id,
       ROUND(AVG(oee), 3) avg_oee, ROUND(AVG(availability), 3) avg_availability,
       ROUND(AVG(performance), 3) avg_performance, ROUND(AVG(quality), 3) avg_quality,
       SUM(units) units, SUM(scrap) scrap,
       RANK() OVER (ORDER BY AVG(oee) ASC) AS worst_rank
FROM {G}.oee_daily GROUP BY plant, line, machine_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.downtime_pareto AS
WITH r AS (
  SELECT plant, category, reason, COUNT(*) events, ROUND(SUM(minutes), 1) lost_minutes
  FROM {S}.downtime_events GROUP BY plant, category, reason)
SELECT *, ROUND(100 * SUM(lost_minutes) OVER (PARTITION BY plant ORDER BY lost_minutes DESC)
                / SUM(lost_minutes) OVER (PARTITION BY plant), 1) cumulative_pct
FROM r
""")

# Capacity unlocked if every machine reached the 85% world-class OEE benchmark
spark.sql(f"""
CREATE OR REPLACE TABLE {G}.productivity_opportunity AS
SELECT plant, line, machine_id, avg_oee, units,
       ROUND(units * (0.85 / avg_oee - 1)) AS extra_units_at_world_class
FROM {G}.machine_scorecard WHERE avg_oee < 0.85 AND avg_oee > 0
ORDER BY extra_units_at_world_class DESC
""")
print("Gold layer built")
