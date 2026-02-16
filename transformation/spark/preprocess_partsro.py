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
    name = F.regexp_replace(official_name, r"\s*\([^()]*\)\s*$", "")

    raw_part_no = F.trim(F.col("part_no"))

    # xN pattern, e.g. 2741025000x4
    x_part = F.regexp_extract(raw_part_no, r"^([A-Za-z0-9]+)[xX](\d+)$", 1)
    x_qty = F.regexp_extract(raw_part_no, r"^([A-Za-z0-9]+)[xX](\d+)$", 2)

    # Parentheses pattern
    paren_part = F.regexp_extract(raw_part_no, r"\(([A-Za-z0-9]+)\)", 1)

    # Discontinued replacement pattern
    alt_part = F.regexp_extract(raw_part_no, r"단종되어\s*([A-Za-z0-9]+)\s*로", 1)

    # Full part number
    full_part = F.when(raw_part_no.rlike(r"^[A-Za-z0-9]+$"), raw_part_no)

    # Fallback: first alnum sequence length >=5
    fallback_part = F.regexp_extract(raw_part_no, r"([A-Za-z0-9]{5,})", 1)

    part_no = F.coalesce(
        F.when(F.length(x_part) > 0, x_part),
        F.when(F.length(paren_part) > 0, paren_part),
        F.when(F.length(alt_part) > 0, alt_part),
        full_part,
        F.when(F.length(fallback_part) > 0, fallback_part),
    )

    qty = F.when(F.length(x_qty) > 0, x_qty.cast("int"))

    price_digits = F.regexp_replace(F.col("price"), r"[^0-9]", "")
    base_price = F.when(F.length(price_digits) > 0, price_digits.cast("int"))

    price = F.when(
        (base_price.isNotNull()) & (qty.isNotNull()) & (qty > 0),
        F.round(base_price / qty, 0).cast("int"),
    ).otherwise(base_price)

    out = (
        df.select(part_no.alias("part_no"), name.alias("name"), price.alias("price"))
        .where(official_name.isNotNull() & (F.length(official_name) > 0))
        .where(F.col("name").isNotNull() & (F.length(F.col("name")) > 0))
        .where(F.col("part_no").isNotNull() & (F.length(F.col("part_no")) > 0))
    )

    if dt:
        out = out.withColumn("dt", F.lit(dt))

    out = out.withColumn("source", F.lit("partsro"))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess partsro in Spark")
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

    spark = SparkSession.builder.appName("preprocess_partsro").getOrCreate()

    out = build_df(spark, args.input, args.dt)

    if args.format == "parquet":
        (out.write.mode("overwrite").parquet(args.output))
    else:
        (out.write.mode("overwrite").option("header", "true").csv(args.output))

    spark.stop()


if __name__ == "__main__":
    main()
