import argparse
import re
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


CAR_TYPE_PREFIX_RE = re.compile(r"^\[차종\s*및\s*연식\]\s*")
ALL_ONLY_RE = re.compile(r"^전\s*차종$")
SPECCODE_RE = re.compile(r"^\d+X\d+$")
SPECCODE_WITH_PREFIX_RE = re.compile(r"^(.*\D)\s*(\d+X\d+)$")
YEAR_PAREN_RE = re.compile(r"\s*\((?:[^)]*~[^)]*|\d{4}[^)]*)\)\s*")
NON_CAR_TOKEN_RE = re.compile(
    r"(?:ASSY|BRACKET|BOLT|NUT|SCREW|GASKET|SEAL|HOSE|DUCT|COVER|REINF|CHECKER|CLIP|GROMMET|LATCH|GUARD)",
    re.IGNORECASE,
)


def _normalize_token(token: str) -> str:
    token = token.strip()
    token = re.sub(r"^차종\s*[:：]?\s*", "", token)
    token = YEAR_PAREN_RE.sub(" ", token)
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
    if raw is None:
        return ["All"]

    text = raw.strip()
    if not text:
        return ["All"]

    text = CAR_TYPE_PREFIX_RE.sub("", text).strip()
    if not text:
        return ["All"]

    # "전 차종" is treated as All. If more content exists, keep parsing it too.
    all_included = False
    if text.startswith("전차종"):
        all_included = True
        text = text[len("전차종") :].strip()
    elif text.startswith("전 차종"):
        all_included = True
        text = text[len("전 차종") :].strip()
    elif ALL_ONLY_RE.fullmatch(text):
        return ["All"]

    # Common normalization before splitting.
    text = text.replace("，", ",")
    text = re.sub(r"\)\s+(?=[A-Za-z0-9가-힣])", "), ", text)
    text = re.sub(r"\s+", " ", text).strip()

    parts = [p.strip() for p in text.split(",") if p.strip()]

    tokens: List[str] = []
    if all_included:
        tokens.append("All")

    for part in parts:
        if ALL_ONLY_RE.fullmatch(part) or part in {"전차종", "전 차종"}:
            tokens.append("All")
            continue

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

    # Deduplicate while preserving order.
    deduped: List[str] = []
    seen = set()
    for token in tokens:
        if token not in seen:
            seen.add(token)
            deduped.append(token)

    return deduped if deduped else ["All"]


def _parse_input_paths(raw_input: str) -> List[str]:
    paths = [p.strip() for p in raw_input.split(",") if p.strip()]
    if not paths:
        raise ValueError("At least one input path is required")
    return paths


def build_df(spark: SparkSession, input_path: str, dt: Optional[str]):
    parse_car_types_udf = F.udf(parse_car_types, T.ArrayType(T.StringType()))
    input_paths = _parse_input_paths(input_path)

    df = (
        spark.read.option("header", "true")
        .option("encoding", "utf-8")
        .option("recursiveFileLookup", "true")
        .csv(input_paths)
    )

    name = F.regexp_replace(F.col("name"), r"\s*\([^()]*\)\s*$", "")
    part_no = F.trim(F.col("part_no"))
    price_digits = F.regexp_replace(F.col("price"), r"[^0-9]", "")
    price = F.when(F.length(price_digits) > 0, price_digits.cast("int"))

    out = (
        df.select(
            part_no.alias("part_no"),
            name.alias("name"),
            price.alias("price"),
            parse_car_types_udf(F.col("car_type")).alias("car_types"),
        )
        .where(F.col("name").isNotNull() & (F.length(F.col("name")) > 0))
        .where(F.col("part_no").isNotNull() & (F.length(F.col("part_no")) > 0))
        .withColumn("car_type", F.explode(F.col("car_types")))
        .drop("car_types")
    )

    if dt:
        out = out.withColumn("dt", F.lit(dt))

    out = out.withColumn("source", F.lit("hyunki_market"))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess hyunki_market in Spark (v2)")
    parser.add_argument("--input", required=True, help="Input CSV path(s), comma-separated")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--dt", default=None, help="Partition date (YYYY-MM-DD)")
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Output format",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("preprocess_hyunki_market_2").getOrCreate()

    out = build_df(spark, args.input, args.dt)

    if args.format == "parquet":
        out.write.mode("overwrite").parquet(args.output)
    else:
        out.write.mode("overwrite").option("header", "true").csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
