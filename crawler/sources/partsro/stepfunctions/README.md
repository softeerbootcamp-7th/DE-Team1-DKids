# Step Functions 실행 스키마

**상태 머신 파일**: `sources/partsro/stepfunctions/partsro_state_machine.json`

**흐름**
1. Extractor Lambda가 URL 리스트를 S3에 저장
2. Map State가 URL 리스트를 읽어 Worker Lambda 병렬 실행
3. Reducer Lambda가 결과 CSV를 하나로 합침

## 입력 스키마 (State Machine 실행 입력)
아래 값은 모두 실행 입력(JSON)에 넣는다.

- `bucket` (string, 필수)
- `urls_prefix` (string, 선택, 기본값: `maintenance_parts/partsro/urls`)
- `result_prefix` (string, 선택, 기본값: `maintenance_parts/partsro/results`)
- `skip_prefix` (string, 선택, 기본값: `maintenance_parts/partsro/skipped`)
- `final_prefix` (string, 선택, 기본값: `maintenance_parts/partsro/final`)
- `run_id` (string, 선택, 기본값: Extractor가 생성한 타임스탬프)
- `list_url` (string, 선택, 기본값: `https://m.partsro.com/product/list_thumb.html?cate_no=177`)
- `max_pages` (number|null, 선택)
- `count` (number, 선택, 기본값: 500)
- `supplier_code` (string, 선택, 기본값: `S0000000`)

**예시**
```json
{
  "bucket": "YOUR_BUCKET",
  "urls_prefix": "maintenance_parts/partsro/urls",
  "result_prefix": "maintenance_parts/partsro/results",
  "skip_prefix": "maintenance_parts/partsro/skipped",
  "final_prefix": "maintenance_parts/partsro/final",
  "run_id": "20260211_120000",
  "list_url": "https://m.partsro.com/product/list_thumb.html?cate_no=177",
  "max_pages": null,
  "count": 500,
  "supplier_code": "S0000000"
}
```

## Extractor Lambda 출력
Extractor는 다음과 같은 Payload를 반환한다.

- `status` (string)
- `count` (number)
- `s3_bucket` (string)
- `urls_key` (string)
- `run_id` (string)

**예시**
```json
{
  "status": "ok",
  "count": 50000,
  "s3_bucket": "YOUR_BUCKET",
  "urls_key": "maintenance_parts/partsro/urls/20260211_120000/urls.json",
  "run_id": "20260211_120000"
}
```

## Map State 입력 (Worker Lambda)
Map State는 배치 단위로 아래 페이로드를 만든다.

- `urls` (string[])
- `batch_index` (number)
- `run_id` (string)
- `bucket` (string)
- `key_prefix` (string)
- `skip_prefix` (string, 선택)
- `extracted_at` (string, ISO 8601)

**예시**
```json
{
  "urls": [
    "https://m.partsro.com/product/.../657545/category/177/display/1/",
    "https://m.partsro.com/product/.../635228/category/177/display/1/"
  ],
  "batch_index": 123,
  "run_id": "20260211_120000",
  "bucket": "YOUR_BUCKET",
  "key_prefix": "maintenance_parts/partsro/results",
  "skip_prefix": "maintenance_parts/partsro/skipped",
  "extracted_at": "2026-02-11T12:00:00Z"
}
```

## Reducer Lambda 입력
Reducer는 아래 값을 받는다.

- `run_id` (string, 필수)
- `bucket` (string, 필수)
- `result_prefix` (string, 선택)
- `final_prefix` (string, 선택)

**예시**
```json
{
  "run_id": "20260211_120000",
  "bucket": "YOUR_BUCKET",
  "result_prefix": "maintenance_parts/partsro/results",
  "final_prefix": "maintenance_parts/partsro/final"
}
```

## Reducer Lambda 출력
Reducer는 최종 CSV의 위치를 반환한다.

- `status` (string)
- `run_id` (string)
- `s3_bucket` (string)
- `s3_key` (string)
- `parts` (number)

**예시**
```json
{
  "status": "ok",
  "run_id": "20260211_120000",
  "s3_bucket": "YOUR_BUCKET",
  "s3_key": "maintenance_parts/partsro/final/20260211_120000/final.csv",
  "parts": 50000
}
```

## URL 리스트 파일 형식
Step Functions `ItemReader`는 **최상위 JSON 배열** 형식만 허용한다.

```json
[
  "https://m.partsro.com/product/.../657545/category/177/display/1/",
  "https://m.partsro.com/product/.../635228/category/177/display/1/"
]
```
