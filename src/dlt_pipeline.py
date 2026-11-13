"""Declarative version of the Snider pipeline (Lakeflow / DLT) with data-quality expectations."""
import dlt
from pyspark.sql import functions as F

landing = spark.conf.get("landing_path")


@dlt.table(comment="Raw production runs via Auto Loader")
def production_runs_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{landing}/production_runs/")
            .withColumn("_ingested_at", F.current_timestamp()))


@dlt.table(comment="Raw downtime events via Auto Loader")
def downtime_events_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv").option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{landing}/downtime_events/")
            .withColumn("_ingested_at", F.current_timestamp()))


@dlt.table(comment="Validated production runs")
@dlt.expect_or_drop("valid_run_id", "run_id IS NOT NULL")
@dlt.expect_or_drop("planned_positive", "planned_minutes > 0")
@dlt.expect_or_drop("scrap_not_above_output", "units_scrapped <= units_produced")
@dlt.expect_or_drop("non_negative_output", "units_produced >= 0 AND run_minutes >= 0")
def production_runs_silver():
    return (dlt.read_stream("production_runs_bronze")
            .dropDuplicates(["run_id"])
            .withColumn("start_ts", F.to_timestamp("start_ts"))
            .withColumn("production_date", F.to_date("start_ts"))
            .withColumn("units_good", F.col("units_produced") - F.col("units_scrapped")))


@dlt.table(comment="Validated downtime events")
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("positive_minutes", "minutes > 0")
def downtime_events_silver():
    return (dlt.read_stream("downtime_events_bronze")
            .dropDuplicates(["event_id"])
            .withColumn("start_ts", F.to_timestamp("start_ts")))


@dlt.table(comment="OEE per machine per day")
def oee_daily_gold():
    a = (dlt.read("production_runs_silver")
         .groupBy("plant", "line", "machine_id", "production_date")
         .agg(F.sum("planned_minutes").alias("planned_min"), F.sum("run_minutes").alias("run_min"),
              F.sum("units_produced").alias("units"), F.sum("units_good").alias("good"),
              F.sum(F.col("ideal_cycle_sec") * F.col("units_produced")).alias("ideal_sec")))
    availability = F.least(F.col("run_min") / F.col("planned_min"), F.lit(1.0))
    performance = F.least(F.col("ideal_sec") / (F.col("run_min") * 60), F.lit(1.0))
    quality = F.col("good") / F.col("units")
    return (a.withColumn("availability", availability).withColumn("performance", performance)
            .withColumn("quality", quality).withColumn("oee", availability * performance * quality))


@dlt.table(comment="Downtime lost minutes by plant and reason")
def downtime_pareto_gold():
    return (dlt.read("downtime_events_silver").groupBy("plant", "category", "reason")
            .agg(F.count("*").alias("events"), F.round(F.sum("minutes"), 1).alias("lost_minutes")))
