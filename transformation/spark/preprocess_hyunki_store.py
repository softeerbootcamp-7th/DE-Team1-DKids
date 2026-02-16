import argparse
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_df(spark: SparkSession, input_path: str, dt: Optional[str]):
    df = (
        spark.read.option("header", "true")
        .option("encoding", "utf-8")
        .csv(input_path)
    )

    official_name = F.trim(F.col("official_name"))
    extracted_part = F.regexp_extract(official_name, r"\(([^()]+)\)\s*$", 1)

    name = F.when(
        F.length(extracted_part) > 0,
        F.regexp_replace(official_name, r"\s*\([^()]*\)\s*$", ""),
    ).otherwise(official_name)

    part_no = F.when(F.length(extracted_part) > 0, extracted_part).otherwise(
        F.trim(F.col("part_no"))
    )

    price_digits = F.regexp_replace(F.col("price"), r"[^0-9]", "")
    price = F.when(F.length(price_digits) > 0, price_digits.cast("int"))

    out = (
        df.select(part_no.alias("part_no"), name.alias("name"), price.alias("price"))
        .where(official_name.isNotNull() & (F.length(official_name) > 0))
        .where(F.col("name").isNotNull() & (F.length(F.col("name")) > 0))
        .where(F.col("part_no").isNotNull() & (F.length(F.col("part_no")) > 0))
    )

    if dt:
        out = out.withColumn("dt", F.lit(dt))

    out = out.withColumn("source", F.lit("hyunki_store"))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess hyunki_store in Spark")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--dt", default=None, help="Partition date (YYYY-MM-DD)")
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Output format",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("preprocess_hyunki_store").getOrCreate()

    out = build_df(spark, args.input, args.dt)

    if args.format == "parquet":
        (out.write.mode("overwrite").parquet(args.output))
    else:
        (out.write.mode("overwrite").option("header", "true").csv(args.output))

    spark.stop()


if __name__ == "__main__":
    main()
