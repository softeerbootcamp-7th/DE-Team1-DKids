import argparse
import re
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


SPECCODE_RE = re.compile(r"^\d+X\d+$")
SPECCODE_WITH_PREFIX_RE = re.compile(r"^(.*\D)\s*(\d+X\d+)$")
NON_CAR_TOKEN_RE = re.compile(
    r"(?:ASSY|BRACKET|BOLT|NUT|SCREW|GASKET|SEAL|HOSE|DUCT|COVER|REINF|CHECKER|CLIP|GROMMET|LATCH|GUARD)",
    re.IGNORECASE,
)


def _protect_year_commas(text: str) -> str:
    # Keep year notation such as "16,19년식" as a single token.
    return re.sub(r"(\d{2,4})\s*,\s*(\d{2,4}\s*년식)", r"\1<YCOMMA>\2", text)


def _normalize_token(token: str) -> str:
    token = re.sub(r"\s+", " ", token).strip(" -")
    return token.strip()


def _is_valid_car_type(token: str) -> bool:
    if not token:
        return False
    if NON_CAR_TOKEN_RE.search(token):
        return False
    if "_" in token:
        return False
    if len(token) <= 1:
        return False
    return True


def parse_car_types(raw: Optional[str]) -> List[str]:
    text = (raw or "").strip()
    if not text:
        return ["All"]

    text = text.replace("，", ",")
    text = _protect_year_commas(text)
    text = re.sub(r"\s+", " ", text).strip()

    parts = [p.strip() for p in text.split(",") if p.strip()]
    parts = [p.replace("<YCOMMA>", ",") for p in parts]

    tokens: List[str] = []
    for part in parts:
        if "/" not in part:
            token = _normalize_token(part)
            if _is_valid_car_type(token):
                tokens.append(token)
            continue

        split_parts = [p.strip() for p in re.split(r"\s*/\s*", part) if p.strip()]
        if not split_parts:
            continue

        first = split_parts[0]
        m = SPECCODE_WITH_PREFIX_RE.match(first)
        spec_prefix = m.group(1).strip() if m else ""

        for idx, sub in enumerate(split_parts):
            candidate = sub
            if idx > 0 and spec_prefix and SPECCODE_RE.fullmatch(sub):
                candidate = f"{spec_prefix} {sub}"

            token = _normalize_token(candidate)
            if _is_valid_car_type(token):
                tokens.append(token)

    deduped: List[str] = []
    seen = set()
    for token in tokens:
        if token not in seen:
            seen.add(token)
            deduped.append(token)

    return deduped if deduped else ["All"]


def build_df(spark: SparkSession, input_path: str, dt: Optional[str]):
    parse_car_types_udf = F.udf(parse_car_types, T.ArrayType(T.StringType()))

    df = (
        spark.read.option("header", "true")
        .option("encoding", "utf-8")
        .option("recursiveFileLookup", "true")
        .csv(input_path)
    )
    if dt and "extracted_at" in df.columns:
        df = df.where(F.col("extracted_at").startswith(dt))

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
        df.select(
            part_no.alias("part_no"),
            name.alias("name"),
            price.alias("price"),
            parse_car_types_udf(F.col("applicable")).alias("car_types"),
        )
        .where(official_name.isNotNull() & (F.length(official_name) > 0))
        .where(F.col("name").isNotNull() & (F.length(F.col("name")) > 0))
        .where(F.col("part_no").isNotNull() & (F.length(F.col("part_no")) > 0))
        .withColumn("car_type", F.explode(F.col("car_types")))
        .drop("car_types")
    )

    if dt:
        out = out.withColumn("dt", F.lit(dt))

    out = out.withColumn("source", F.lit("hyunki_store"))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess hyunki_store in Spark (v2)")
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

    spark = SparkSession.builder.appName("preprocess_hyunki_store_2").getOrCreate()

    out = build_df(spark, args.input, args.dt)

    if args.format == "parquet":
        out.write.mode("overwrite").parquet(args.output)
    else:
        out.write.mode("overwrite").option("header", "true").csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
