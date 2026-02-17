import argparse
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

PRIORITY = ["partsro", "hyunki_store", "hyunki_market"]


def read_clean(spark: SparkSession, paths: List[str], fmt: str):
    if fmt == "parquet":
        return spark.read.parquet(*paths)
    return spark.read.option("header", "true").csv(paths)


def build_canonical_map(df):
    # For each part_no + car_type + source, pick the most frequent name in that source.
    counts = (
        df.groupBy("part_no", "car_type", "source", "name")
        .count()
        .where(F.col("name").isNotNull() & (F.length(F.col("name")) > 0))
    )

    w = (
        F.row_number()
        .over(
            Window.partitionBy("part_no", "car_type", "source")
            .orderBy(F.col("count").desc(), F.col("name").asc())
        )
    )

    top_per_source = counts.withColumn("rn", w).where(F.col("rn") == 1).drop("rn")

    # Priority: partsro -> hyunki_store -> hyunki_market
    priority_expr = (
        F.when(F.col("source") == "partsro", F.lit(1))
        .when(F.col("source") == "hyunki_store", F.lit(2))
        .when(F.col("source") == "hyunki_market", F.lit(3))
        .otherwise(F.lit(99))
    )

    w2 = (
        F.row_number()
        .over(
            Window.partitionBy("part_no", "car_type")
            .orderBy(priority_expr.asc(), F.col("count").desc(), F.col("name").asc())
        )
    )

    canonical = (
        top_per_source.withColumn("rn", w2)
        .where(F.col("rn") == 1)
        .select("part_no", "car_type", F.col("name").alias("canonical_name"))
    )

    return canonical


def main() -> None:
    parser = argparse.ArgumentParser(description="Build name price summary in Spark")
    parser.add_argument(
        "--input",
        required=True,
        help="Input path(s), comma-separated",
    )
    parser.add_argument("--output", required=True, help="Output path for summary")
    parser.add_argument(
        "--dt",
        required=True,
        help="Transform date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--canonical-output",
        default=None,
        help="Optional output path for part_no -> canonical_name mapping",
    )
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Input format",
    )
    parser.add_argument(
        "--output-format",
        default="csv",
        choices=["parquet", "csv"],
        help="Output format",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("build_name_price_summary").getOrCreate()

    input_paths = [p.strip() for p in args.input.split(",") if p.strip()]

    df = read_clean(spark, input_paths, args.format)
    if "car_type" not in df.columns:
        raise ValueError("Input data must include car_type (normalized value)")

    # price should be numeric
    df = df.withColumn("price", F.col("price").cast("int"))

    canonical = build_canonical_map(df)

    joined = df.join(canonical, on=["part_no", "car_type"], how="inner")

    summary = (
        joined.groupBy("canonical_name", "car_type")
        .agg(
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
        )
        .withColumnRenamed("canonical_name", "name")
        .withColumnRenamed("car_type", "차종")
    )

    summary = summary.withColumn("dt", F.lit(args.dt))
    canonical = canonical.withColumn("dt", F.lit(args.dt))

    if args.output_format == "parquet":
        summary.write.mode("overwrite").parquet(args.output)
        if args.canonical_output:
            canonical.write.mode("overwrite").parquet(args.canonical_output)
    else:
        summary.write.mode("overwrite").option("header", "true").csv(args.output)
        if args.canonical_output:
            canonical.write.mode("overwrite").option("header", "true").csv(
                args.canonical_output
            )

    spark.stop()


if __name__ == "__main__":
    main()
