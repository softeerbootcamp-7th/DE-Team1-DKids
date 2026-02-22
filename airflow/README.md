<div align="center">
  <h1>Automotive Parts Data Pipeline</h1>
  <p><b>Airflow를 활용한 자동차 부품 가격 데이터 수집 및 정제 시스템</b></p>
</div>

---

### 1. 프로젝트 개요
본 파이프라인의 목표는 **자동차 부품 가격 데이터를 매일 안정적으로 수집, 정제, 적재**하여 서비스 내에서 신뢰할 수 있는 최신 가격 범위를 제공하는 것입니다. 단순한 데이터 수집을 넘어, 운영 리스크를 최소화하고 데이터의 무결성을 보장하는 데 초점을 맞추었습니다.

---

### 2. 파이프라인 설계 및 실행 흐름
전체 프로세스는 단계별 상태를 추적하며, 실패 시 해당 지점부터 재개할 수 있는 구조로 설계되었습니다.

<div align="center">
  <img src="../images/airflow_graph.png" width="100%" alt="Airflow DAG Workflow">
</div>

1.  **Ingestion Preparation**: 소스별 크롤링에 필요한 입력 파라미터 생성
2.  **Master Step Functions**: AWS Step Functions를 호출하여 분산 크롤링 수행 및 완료 대기
3.  **Slack Notification**: 크롤링 결과 요약(성공/실패 수량 등) 전송
4.  **Self-Healing**: 실패 URL을 DynamoDB에서 추출하여 재시도 라운드 자동 실행
5.  **EMR Spark Transformation**: 분산 처리 환경에서 데이터 정제 및 비즈니스 로직 적용
6.  **Data Validation & Cleanup**: 빈 산출물(Empty CSV) 검증 및 클린업 수행
7.  **RDS Load (Upsert)**: 최종 결과물을 RDS(PostgreSQL)에 중복 없이 적재
8.  **Final Report**: 파이프라인 전체 완료 상태 알림

---

### 3. 핵심 운영 및 안정성 설계

#### ▪️ AWS 인증 보안 (STS/MFA)
보안 가이드라인에 따라 MFA 기반 임시 자격증명(STS 토큰)을 사용합니다. 
매번 수동으로 인증하는 번거로움을 해결하기 위해 `start_with_mfa.sh` 스크립트를 작성하여 아래 과정을 자동화했습니다.
- MFA 코드 입력 및 STS 토큰 획득
- Airflow 환경 변수(`AWS_SESSION_TOKEN` 등) 주입
- 스케줄러 및 웹 서버 컨테이너 자동 재기동

#### ▪️ 스케줄링 정책: `catchup=False`
- **최신성 유지**: 부품 가격은 고정 주기보다 이벤트성 변동이 잦습니다. 주간 배치보다 일간 수집이 정확성 면에서 유리합니다.
- **리스크 관리**: 과거 미실행 구간을 한꺼번에 실행(Backfill)할 경우, 외부 사이트 부하 및 운영 리스크가 급격히 증가합니다. 
- **통제 범위**: "오늘 기준 최신 데이터" 확보를 최우선으로 하며, 과거 데이터는 필요 시 수동 실행으로 통제합니다.

#### ▪️ 멱등성(Idempotency) 보장
동일한 작업을 여러 번 수행해도 결과가 항상 일정하도록 설계했습니다.
- **Spark**: `mode("overwrite")`를 사용하여 특정 파티션 재실행 시 파일 중복 방지
- **RDS**: `ON CONFLICT (part_official_name, car_type, extracted_at) DO UPDATE` 연산을 통해 중복 삽입 방지
- **Step Functions**: 실행명에 `{{ ts_nodash }}`와 `{{ ti.try_number }}`를 조합하여 재시도 시 ID 충돌 방지

---

### 4. 장애 대응 및 모니터링

| 단계 | 발생 가능 리스크 | 대응 전략 |
| :--- | :--- | :--- |
| **크롤링** | 외부 사이트 지연 및 Lambda Throttle | Step Functions 내 지수 백오프 적용 및 DynamoDB 기반 재시도 라운드 운영 |
| **Spark 변환** | 비정상 완료(빈 파일 생성) | `cleanup_empty_mart_outputs` 태스크를 통해 데이터 유무 검증 후 적재 스킵 |
| **데이터 적재** | DB 커넥션 및 키 충돌 | Python 레벨 중복 제거 로직 선행 및 DB Upsert 적용 |
| **공통** | 태스크 실패 | `on_failure_callback`으로 Slack에 로그 URL 전송 (최대 2회 자동 재시도) |

<br>

<div align="center">
  <img src="../images/slackalarm.png" width="80%" alt="Monitoring Screenshot">
</div>

---

### 5. 인프라 설정 관리 (Admin)

<details>
<summary><b>관리 항목 상세 (Connections / Variables)</b></summary>
<br>

**Connections**
- `aws_default`: AWS 리소스 접근 (환경변수 기반)
- `rds_default`: RDS(PostgreSQL) 연결 정보
- `slack_webhook_default`: 알림 채널 웹훅

**Variables**
- **Storage**: `DATA_BUCKET`, `RAW_S3_PREFIX`, `MART_S3_PREFIX` 등 경로 설정
- **Compute**: `EMR_CLUSTER_ID`, `MASTER_SFN_ARN` 등 인프라 ARN
- **Crawler**: `SUPPLIER_CODE`, `MAX_PAGES`, `CRAWL_COUNT` 등 수집 파라미터
- **Database**: `RDS_TABLE`, `DDB_TABLE` 정보
</details>

---

### 6. 실행 환경
- **Executor**: `LocalExecutor` (Docker-compose 기반)
- **Metadata DB**: PostgreSQL
- **선택 배경**: 상시 클라우드 상주 비용을 절감하고, 로컬 환경에서 빠른 디버깅과 병렬 태스크 실행이 가능한 최적의 구조를 채택했습니다.

