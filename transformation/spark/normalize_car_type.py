import argparse
import difflib
import re
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


CANONICAL_CAR_TYPES = [
    "EQ900(HI)",
    "Electrified G80(RG3 EV)",
    "Electrified GV70(JK1 EV)",
    "G70(IK)",
    "G80(DH)",
    "G80(RG3)",
    "G90(HI)",
    "G90(RS4)",
    "GV60(JW1 EV)",
    "GV70(JK1)",
    "GV80(JX1)",
    "ST1(A01)",
    "i30(FD)",
    "i30(GD)",
    "i30(PD)",
    "i40(VF)",
    "갤로퍼(M1)",
    "그랜드 스타렉스(TQ)",
    "그랜저 XG(XG)",
    "그랜저 하이브리드(GN7 HEV)",
    "그랜저 하이브리드(HG HEV)",
    "그랜저 하이브리드(IG HEV)",
    "그랜저(GN7)",
    "그랜저(HG)",
    "그랜저(IG)",
    "그랜저(TG)",
    "라비타(FC)",
    "리베로(SR)",
    "마르샤(H1)",
    "맥스크루즈(NC)",
    "베뉴(QX)",
    "베라크루즈(EN)",
    "베르나 하이브리드(MC HEV)",
    "베르나(LC)",
    "베르나(MC)",
    "벨로스터 N(JSN)",
    "벨로스터(FS)",
    "벨로스터(JS)",
    "블루온(EAEV)",
    "스타렉스(A1)",
    "스타리아 하이브리드(US4 HEV)",
    "스타리아(US4)",
    "싼타모(M2)",
    "싼타페 하이브리드(MX5 HEV)",
    "싼타페 하이브리드(TM HEV)",
    "싼타페(CM)",
    "싼타페(DM)",
    "싼타페(MX5)",
    "싼타페(SM)",
    "싼타페(TM)",
    "쏘나타 택시(DN8C)",
    "쏘나타 플러그인 하이브리드(LF PHEV)",
    "쏘나타 하이브리드(DN8 HEV)",
    "쏘나타 하이브리드(LF HEV)",
    "쏘나타 하이브리드(YF HEV)",
    "쏘나타(DN8)",
    "쏘나타(EF)",
    "쏘나타(LF)",
    "쏘나타(NF)",
    "쏘나타(Y2)",
    "쏘나타(Y3)",
    "쏘나타(YF)",
    "쏠라티(EU)",
    "아반떼 N(CN7 N)",
    "아반떼 XD(XD)",
    "아반떼 쿠페(JK)",
    "아반떼 하이브리드(CN7 HEV)",
    "아반떼 하이브리드(HD HEV)",
    "아반떼(AD)",
    "아반떼(CN7)",
    "아반떼(HD)",
    "아반떼(MD)",
    "아반떼(RD)",
    "아슬란(AG)",
    "아이오닉 5 N(NE EV N)",
    "아이오닉 5(NE EV)",
    "아이오닉 6 N(CE EV N)",
    "아이오닉 6(CE EV)",
    "아이오닉 9(ME EV)",
    "아이오닉 일렉트릭(AE EV)",
    "아이오닉 플러그인 하이브리드(AE PHEV)",
    "아이오닉 하이브리드(AE HEV)",
    "아토스(MX)",
    "에쿠스(LZ)",
    "에쿠스(VI)",
    "엑센트(RB)",
    "엑센트(X3)",
    "엘란트라(J1)",
    "제네시스 쿠페(BK)",
    "제네시스(BH)",
    "제네시스(DH)",
    "캐스퍼 일렉트릭(AX1 EV)",
    "캐스퍼(AX1)",
    "코나 N(OS N)",
    "코나 일렉트릭(OS EV)",
    "코나 일렉트릭(SX2 EV)",
    "코나 하이브리드(OS HEV)",
    "코나 하이브리드(SX2 HEV)",
    "코나(OS)",
    "코나(SX2)",
    "클릭(TB)",
    "테라칸(HP)",
    "투스카니(GK)",
    "투싼 하이브리드(NX4 HEV)",
    "투싼(JM)",
    "투싼(LM)",
    "투싼(NX4)",
    "투싼(TL)",
    "트라제 XG(FO)",
    "티뷰론(RC)",
    "팰리세이드 하이브리드(LX3 HEV)",
    "팰리세이드(LX2)",
    "팰리세이드(LX3)",
    "포터Ⅱ 일렉트릭(HR EV)",
    "포터Ⅱ(HR)",
]


def read_clean(spark: SparkSession, paths: List[str], fmt: str):
    if fmt == "parquet":
        return spark.read.parquet(*paths)
    return spark.read.option("header", "true").csv(paths)


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    value = value.strip().lower()
    return "".join(ch for ch in value if ch.isalnum() or ("가" <= ch <= "힣"))


def _tokenize(value: str) -> set:
    if not value:
        return set()
    buf = []
    token = []
    for ch in value.lower():
        if ch.isalnum() or ("가" <= ch <= "힣"):
            token.append(ch)
        else:
            if token:
                buf.append("".join(token))
                token = []
    if token:
        buf.append("".join(token))
    return {x for x in buf if x}


def _base_name(canonical_name: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", canonical_name).strip()


def _derive_keywords(base_name: str) -> set:
    keywords = set()
    if not base_name:
        return keywords

    base = base_name.strip()
    base_l = base.lower()
    tokens = [t for t in re.split(r"\s+", base) if t]

    keywords.add(base)
    keywords.add(base.replace(" ", ""))

    # Model code-style token (e.g. g80, gv70, i30, dn8) helps match noisy source values.
    for tok in tokens:
        if re.fullmatch(r"[A-Za-z]{1,4}\d{1,4}[A-Za-z]?", tok):
            keywords.add(tok)

    has_variant_marker = (
        ("하이브리드" in base)
        or ("일렉트릭" in base)
        or ("플러그인" in base)
        or ("electrified" in base_l)
        or ("hybrid" in base_l)
        or ("electric" in base_l)
        or ("택시" in base)
        or ("쿠페" in base)
        or base_l.endswith(" n")
    )

    # For base model families, allow a broader keyword such as "투싼", "제네시스".
    if (not has_variant_marker) and len(tokens) >= 2:
        keywords.add(tokens[0])

    return {k for k in keywords if k}


def _build_matcher():
    canonical = []
    for item in CANONICAL_CAR_TYPES:
        base = _base_name(item)
        keywords = _derive_keywords(base)
        canonical.append(
            {
                "name": item,
                "base": base,
                "norm": _normalize_text(item),
                "base_norm": _normalize_text(base),
                "tokens": _tokenize(item),
                "keywords_norm": {_normalize_text(k) for k in keywords if _normalize_text(k)},
            }
        )

    def _match_car_type(raw: str) -> str:
        if raw is None:
            return "all"

        text = raw.strip()
        if not text:
            return "all"
        if text.lower() == "all":
            return "all"

        norm = _normalize_text(text)
        if not norm:
            return "unknown"

        best_name = None
        best_score = -1.0
        raw_tokens = _tokenize(text)

        for item in canonical:
            ratio = difflib.SequenceMatcher(None, norm, item["norm"]).ratio()
            jaccard = 0.0
            if raw_tokens or item["tokens"]:
                inter = len(raw_tokens & item["tokens"])
                union = len(raw_tokens | item["tokens"])
                jaccard = (inter / union) if union else 0.0
            contains = 1.0 if (norm in item["norm"] or item["norm"] in norm) else 0.0
            score = (ratio * 0.7) + (jaccard * 0.2) + (contains * 0.1)
            if score > best_score:
                best_score = score
                best_name = item["name"]

        if best_score < 0.55:
            # 2nd pass: keyword mapping for unknowns only.
            # Example: contains "투싼" -> map to a 투싼 canonical item.
            keyword_candidates = []
            for item in canonical:
                matched_len = 0
                for kw in item["keywords_norm"]:
                    if kw and kw in norm:
                        matched_len = max(matched_len, len(kw))
                if matched_len == 0:
                    continue

                base_ratio = difflib.SequenceMatcher(None, norm, item["base_norm"]).ratio()
                keyword_candidates.append((matched_len, base_ratio, item["name"]))

            if keyword_candidates:
                keyword_candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
                return keyword_candidates[0][2]

            return "unknown"
        return best_name if best_name else "unknown"

    return F.udf(_match_car_type, T.StringType())


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize car_type across clean datasets")
    parser.add_argument(
        "--input",
        required=True,
        help="Input path(s), comma-separated",
    )
    parser.add_argument("--output", required=True, help="Output path for normalized records")
    parser.add_argument(
        "--dt",
        required=True,
        help="Transform date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Input format",
    )
    parser.add_argument(
        "--output-format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Output format",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("normalize_car_type").getOrCreate()

    input_paths = [p.strip() for p in args.input.split(",") if p.strip()]
    df = read_clean(spark, input_paths, args.format)

    if "car_type" not in df.columns:
        df = df.withColumn("car_type", F.lit("all"))
    else:
        df = df.withColumn(
            "car_type",
            F.when(F.col("car_type").isNull() | (F.length(F.trim(F.col("car_type"))) == 0), F.lit("all"))
            .otherwise(F.trim(F.col("car_type"))),
        )

    key_col = F.lower(F.trim(F.col("car_type")))
    key_col = F.regexp_replace(key_col, r"[^0-9a-z가-힣]", "")

    df_keyed = df.withColumn("car_type_key", key_col)

    matcher_udf = _build_matcher()
    mapping = (
        df_keyed.select("car_type_key", "car_type")
        .distinct()
        .withColumn("car_type_canonical", matcher_udf(F.col("car_type")))
        .select("car_type_key", "car_type_canonical")
    )

    normalized = (
        df_keyed.join(mapping, on="car_type_key", how="left")
        .withColumn(
            "car_type",
            F.coalesce(F.col("car_type_canonical"), F.lit("unknown")),
        )
        .drop("car_type_canonical", "car_type_key")
    )

    # Keep all original columns as-is, only replacing car_type with normalized value.
    normalized = normalized.select(*df.columns)

    if args.output_format == "parquet":
        normalized.write.mode("overwrite").parquet(args.output)
    else:
        normalized.write.mode("overwrite").option("header", "true").csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
