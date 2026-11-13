-- Defaults: catalog workspace, prefix snider

-- 1. Plant OEE
SELECT plant, ROUND(AVG(oee),3) oee, ROUND(AVG(availability),3) availability,
       ROUND(AVG(performance),3) performance, ROUND(AVG(quality),3) quality
FROM workspace.snider_gold.oee_daily GROUP BY plant ORDER BY oee;

-- 2. Worst 5 machines
SELECT * FROM workspace.snider_gold.machine_scorecard ORDER BY worst_rank LIMIT 5;

-- 3. Top downtime causes
SELECT * FROM workspace.snider_gold.downtime_pareto ORDER BY lost_minutes DESC LIMIT 10;

-- 4. Biggest productivity upside
SELECT * FROM workspace.snider_gold.productivity_opportunity LIMIT 10;
