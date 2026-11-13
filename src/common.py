import argparse


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--prefix", default="snider")
    a, _ = p.parse_known_args()
    return a


def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def oee(planned_min, run_min, units, scrap, ideal_cycle_sec):
    """OEE = availability * performance * quality (availability/performance capped at 1)."""
    if not planned_min or not run_min or not units:
        return 0.0
    availability = min(run_min / planned_min, 1.0)
    performance = min(ideal_cycle_sec * units / (run_min * 60.0), 1.0)
    quality = max(units - scrap, 0) / units
    return availability * performance * quality
