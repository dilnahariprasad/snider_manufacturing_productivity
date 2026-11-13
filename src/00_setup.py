import sys, os
sys.path.append(os.path.dirname(globals().get("__file__", os.path.join(os.getcwd(), "x"))))
from common import parse_args, get_spark

a = parse_args()
spark = get_spark()
c, p = a.catalog, a.prefix

for layer in ["landing", "bronze", "silver", "gold", "dlt"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{p}_{layer}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {c}.{p}_landing.raw")
print("Setup complete:", c, p)
