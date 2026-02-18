# Crawler 파이프라인

자동차 부품 3개 소스 (`partsro`, `hyunki_store`, `hyunki_market`)를  
AWS Step Functions 기반으로 수집하는 영역입니다.

---

## 디렉터리 구조

| 경로 | 역할 |
|------|------|
| `sources/<source>/extractor` | 목록 페이지에서 상세 URL 수집 |
| `sources/<source>/worker` | 상세 페이지 파싱, CSV part 저장, skip 기록 |
| `sources/<source>/reducer` | part CSV 병합 후 `final.csv` 생성 |
| `sources/<source>/stepfunctions` | 소스별 상태머신 정의 |
| `aws/stepfunctions/master_state_machine.json` | 3개 소스 병렬 실행 |

---

## 실행 로직

### 소스별 상태머신 (공통)

1. `CheckOverrideUrls`
2. `ExtractUrls` (또는 `UseOverrideUrls`)
3. `MapUrls` (Distributed Map, worker 병렬 실행)
4. `ReduceResults`

### 마스터 상태머신

1. `partsro`, `hyunki_store`, `hyunki_market` 병렬 실행
2. 브랜치 단위 실패 격리 (`Catch`)

---

## 저장 위치 (S3)

`<source>` 는 `partsro | hyunki_store | hyunki_market` 중 하나입니다.

| 유형 | 경로 |
|------|------|
| URL 목록 | `s3://<bucket>/raw/<source>/urls[/dt=<YYYY-MM-DD>]/<run_id>/urls.json` |
| Worker 결과 | `s3://<bucket>/raw/<source>/parts[/dt=<YYYY-MM-DD>]/<run_id>/part-<batch>.csv` |
| Skip 로그 | `s3://<bucket>/raw/<source>/skipped[/dt=<YYYY-MM-DD>]/<run_id>/skip-<batch>.json` |
| 최종 결과 | `s3://<bucket>/raw/<source>/final[/dt=<YYYY-MM-DD>]/<run_id>/final.csv` |
| 재시도 URL (옵션) | `s3://<bucket>/raw/<source>/retry/dt=<YYYY-MM-DD>/run_id=<retry_run_id>/urls.json` |

**참고**
- `dt=...` 경로는 Airflow 실행 시 입력 prefix에 포함될 때 생성됩니다.
- `run_id`는 실행마다 새 값 사용을 권장합니다.

---

## 🗄️ DynamoDB (옵션)

Worker가 URL 단위 상태를 기록합니다.

| 항목 | 값 |
|------|------|
| 테이블 | `parts_crawl_status` |
| PK | `pk=<source>#dt=<YYYY-MM-DD>` |
| SK | `sk=sha1(url)` |
| 주요 값 | `status(SUCCESS/FAILED)`, `reason`, `http_status`, `attempt`, `run_id`, `ttl` |

---

## 아키텍처 다이어그램

_(다이어그램 추가 위치)_

---

## 입력 파일 사용

| 실행 유형 | 파일 |
|----------|------|
| 단일 소스 실행 | `runs/partsro.input.json` |
|  | `runs/hyunki_store.input.json` |
|  | `runs/hyunki_market.input.json` |
| 전체 병렬 실행 | `runs/master.input.json` |

실행 시 `run_id`를 `null`로 두면 extractor가 현재 시각 기반으로 자동 생성합니다.
