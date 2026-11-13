# Snider Manufacturing Productivity Lakehouse

Goal: maximize Snider's productivity by measuring **OEE** (Availability × Performance × Quality),
finding the worst machines, and ranking downtime causes (Pareto) on Databricks.

Flow: CSV landing volume → bronze → silver (dedupe, validation, rejects) → gold KPIs.

| Gold table | Purpose |
|---|---|
| `oee_daily` | OEE + A/P/Q per machine/day |
| `machine_scorecard` | Average OEE, worst-machine rank |
| `downtime_pareto` | Lost minutes by reason with cumulative % |
| `productivity_opportunity` | Extra units if machines hit 85% OEE |

## Run
```powershell
python scripts\generate_data.py
databricks bundle deploy -p northstar
databricks bundle run productivity_pipeline -p northstar   # first run fails at bronze until data is uploaded
databricks fs cp data\production_runs dbfs:/Volumes/workspace/snider_landing/raw/production_runs --recursive -p northstar
databricks fs cp data\downtime_events dbfs:/Volumes/workspace/snider_landing/raw/downtime_events --recursive -p northstar
databricks bundle run productivity_pipeline -p northstar
python -m pytest tests
```
Queries: `sql\kpi_queries.sql`.
