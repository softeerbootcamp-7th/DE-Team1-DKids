<div align="center">
  <h1>Transformation Pipeline</h1>
  <p><b>EMR Spark 기반 부품 데이터 정제 및 요약 마트 생성 단계</b></p>
</div>

---

### 1. 개요
`transformation` 단계는 크롤링 결과(raw CSV)를 그대로 쓰지 않고,  
EMR(Spark)에서 소스별 전처리와 차종 정규화를 거쳐 서비스 적재용 요약 데이터를 만드는 파이프라인입니다.

Transform 단계에서는 **S3에 저장된 ETL 코드와 Raw Zone 데이터가 Amazon EMR로 전달**되어 Spark 기반 변환 작업이 수행됩니다.
EMR 클러스터는 S3 ETL Code Bucket에 저장된 스파크 애플리케이션을 실행하고, S3 Data Lake의 Raw Zone 데이터를 입력으로 받아 정제·가공·집계 처리를 수행합니다.
작업 수행 중 생성되는 실행 로그는 S3 EMR Log Bucket에 저장되어 모니터링과 디버깅에 활용되며, 변환 완료 데이터는 다시 S3의 Clean/Mart 영역에 적재됩니다.

Airflow DAG의 EMR step 구성(`build_emr_steps`) 기준으로 순차 실행됩니다.

---

### 2. EMR Step 기준 전체 흐름

EMR에서는 아래 5개 Spark step이 순서대로 실행됩니다.

1. `partsro_clean`
2. `hyunki_store_clean`
3. `hyunki_market_clean`
4. `normalize_car_type`
5. `name_price_summary`

즉, **소스별 전처리(3개) → 통합 정규화(1개) → 요약 마트 생성(1개)** 구조입니다.

---

### 3. 단계별 설명

#### 3.1 소스별 전처리 (`preprocess_*.py`)
- 대상 소스:
  - `partsro`
  - `hyunki_store`
  - `hyunki_market`
- 역할:
  - 원본 CSV 스키마를 **공통 컬럼 구조**로 통일
  - 문자열 정제 (공백/특수문자/불필요 괄호 정보 제거)
  - 차종 텍스트 파싱/분해 후 다중 차종을 행 단위로 펼침(`explode`)
  - Spark 처리에 적합한 clean 데이터셋 생성
- 정규화 관점에서의 핵심 처리:
  - 원본 쇼핑몰 데이터의 차종 컬럼은 한 셀에 여러 차종이 함께 들어있는 경우가 많아
    **1정규형(1NF, 컬럼 원자값 원칙)** 이 깨진 상태였습니다.
  - 전처리 단계에서 차종 문자열을 분해하고, `explode`로 행을 분리하여
    **`1행 = 1부품번호 × 1차종`** 형태로 정규화했습니다.
- 소스 통합 공통 스키마(핵심):
  - `part_no`: 부품번호
  - `name`: 부품명
  - `price`: 가격(int)
  - `car_type`: 적용 차종(전처리 단계의 원본/파싱 결과)
  - `source`: 소스 식별값 (`partsro`, `hyunki_store`, `hyunki_market`)
  - `dt`: 처리 일자(파티션용)


- 출력:
  - `clean/<source>/dt=YYYY-MM-DD/` (주로 parquet)

#### 3.2 차종 정규화 (`normalize_car_type.py`)
- 3개 소스의 clean 데이터를 합쳐서 처리합니다.
- 역할:
  - 소스마다 다르게 표기된 차종명을 canonical 형태로 정규화
  - 서비스/분석에서 일관된 차종 기준을 사용할 수 있도록 통일
- 매핑 방식(요약):
  - 사전에 정의한 `CANONICAL_CAR_TYPES` 목록을 기준으로 매칭
  - 1차: 정규화 문자열 기반 유사도 매칭
    - `SequenceMatcher` ratio
    - 토큰 Jaccard
    - 포함관계 점수(contains)
  - 2차(저신뢰 결과 보완): 키워드 기반 fallback
    - 예: `g80`, `투싼`, `i30` 같은 모델 토큰/차종 키워드 포함 여부로 재매핑
  - 예외 처리:
    - 빈 값/누락 -> `all`
    - 매칭 실패 -> `unknown`

- 출력:
  - `clean/normalized/dt=YYYY-MM-DD/` (parquet)

#### 3.3 요약 마트 생성 (`build_name_price_summary.py`)
- 정규화된 데이터를 기준으로 최종 요약 테이블을 만듭니다.
- 역할:
  - 부품 번호/차종 기준으로 대표 부품명(canonical name) 결정
  - 가격 요약(최소/최대 등) 집계용 데이터 생성
  - 서비스 적재에 사용할 마트 산출물 생성
- 통합/집계 방식(핵심):
  - 입력: `clean/normalized` 데이터(3개 소스가 이미 합쳐진 상태)
  - `part_no + car_type + source` 단위 대표명 기준 `min_price`, `max_price` 집

- 최종 요약 마트 컬럼:
  - `part_official_name`
  - `extracted_at`
  - `min_price`
  - `max_price`
  - `car_type`
- 출력:
  - `mart/name_price_summary/dt=YYYY-MM-DD/` (csv)
  - `mart/part_no_canonical_name/dt=YYYY-MM-DD/` (canonical mapping)

---

### 4. 요약
- 크롤링 raw 데이터를 바로 적재하지 않고, EMR Spark에서 정제/정규화 후 사용
- 소스별 전처리와 통합 정규화를 분리해 품질 관리와 유지보수성을 확보
- 최종적으로 서비스에서 쓰기 쉬운 가격 요약 마트 데이터를 생성
