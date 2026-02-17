import argparse
import re
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


YEAR_INFO_PAREN_RE = re.compile(
    r"\s*\((?=[^)]*(?:\d|년식|연식|이후|이전|~|-))[^)]*\)\s*"
)
YEAR_INFO_INLINE_RE = re.compile(
    r"(?:\b(?:19|20)\d{2}(?:[./-]\d{1,2})?\b|(?:\b\d{4}\s*년식\b)|(?:~\s*(?:19|20)\d{2}))"
)
DATE_STYLE_RE = re.compile(r"\b\d{2}\s*:\s*-?[A-Z]{3}\.?\d{4}\b", re.IGNORECASE)
SPECCODE_RE = re.compile(r"^\d+X\d+$")
SPECCODE_WITH_PREFIX_RE = re.compile(r"^(.*\D)\s*(\d+X\d+)$")
NON_CAR_TOKEN_RE = re.compile(
    r"(?:ASSY|BRACKET|BOLT|NUT|SCREW|GASKET|SEAL|HOSE|DUCT|COVER|REINF|CHECKER|CLIP|GROMMET|LATCH|GUARD)",
    re.IGNORECASE,
)


def _normalize_token(token: str) -> str:
    token = token.replace("&amp;", "&")
    # User requested plain car name: remove all parenthesized info.
    token = re.sub(r"\s*\([^)]*\)\s*", " ", token)
    token = YEAR_INFO_INLINE_RE.sub(" ", token)
    token = DATE_STYLE_RE.sub(" ", token)
    token = re.sub(r"\s+", " ", token).strip(" -").strip()
    token = token.strip(".,:;~").strip()
    return token


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
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+/\s+", ",", text)
    parts = [p.strip() for p in text.split(",") if p.strip()]

    deduped: List[str] = []
    seen = set()
    for token in parts:
        if "/" in token:
            split_parts = [p.strip() for p in re.split(r"\s*/\s*", token) if p.strip()]
            if split_parts:
                first = split_parts[0]
                m = SPECCODE_WITH_PREFIX_RE.match(first)
                spec_prefix = m.group(1).strip() if m else ""
                for idx, sub in enumerate(split_parts):
                    candidate = sub
                    if idx > 0 and spec_prefix and SPECCODE_RE.fullmatch(sub):
                        candidate = f"{spec_prefix} {sub}"
                    normalized = _normalize_token(candidate)
                    if _is_valid_car_type(normalized) and normalized not in seen:
                        seen.add(normalized)
                        deduped.append(normalized)
            continue

        normalized = _normalize_token(token)
        if _is_valid_car_type(normalized) and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)

    return deduped if deduped else ["All"]


def build_df(spark: SparkSession, input_path: str, dt: Optional[str]):
    parse_car_types_udf = F.udf(parse_car_types, T.ArrayType(T.StringType()))

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

    out = out.withColumn("source", F.lit("partsro"))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess partsro in Spark (v2)")
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

    spark = SparkSession.builder.appName("preprocess_partsro_2").getOrCreate()

    out = build_df(spark, args.input, args.dt)

    if args.format == "parquet":
        out.write.mode("overwrite").parquet(args.output)
    else:
        out.write.mode("overwrite").option("header", "true").csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
