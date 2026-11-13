import sys, os
sys.path.append(os.path.dirname(globals().get("__file__", os.path.join(os.getcwd(), "x"))))
from common import parse_args, get_spark
from pyspark.sql import functions as F

a = parse_args()
spark = get_spark()
c, p = a.catalog, a.prefix

runs = (spark.table(f"{c}.{p}_bronze.production_runs")
        .dropDuplicates(["run_id"])
        .withColumn("start_ts", F.to_timestamp("start_ts"))
        .withColumn("production_date", F.to_date("start_ts")))

valid = (F.col("planned_minutes") > 0) & (F.col("run_minutes") >= 0) & \
        (F.col("units_produced") >= 0) & (F.col("units_scrapped") <= F.col("units_produced"))

runs.filter(valid).withColumn("units_good", F.col("units_produced") - F.col("units_scrapped")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{c}.{p}_silver.production_runs")
runs.filter(~valid).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{c}.{p}_silver.production_runs_rejected")

(spark.table(f"{c}.{p}_bronze.downtime_events")
 .dropDuplicates(["event_id"])
 .withColumn("start_ts", F.to_timestamp("start_ts"))
 .withColumn("production_date", F.to_date("start_ts"))
 .filter(F.col("minutes") > 0)
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{c}.{p}_silver.downtime_events"))
