import sys, os
sys.path.append(os.path.dirname(globals().get("__file__", os.path.join(os.getcwd(), "x"))))
from common import parse_args, get_spark
from pyspark.sql import functions as F

a = parse_args()
spark = get_spark()
c, p = a.catalog, a.prefix
raw = f"/Volumes/{c}/{p}_landing/raw"

for name in ["production_runs", "downtime_events"]:
    df = (spark.read.option("header", True).option("inferSchema", True).csv(f"{raw}/{name}/")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{c}.{p}_bronze.{name}")
    print(name, df.count())
