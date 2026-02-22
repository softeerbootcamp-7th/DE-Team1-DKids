-- =====================================
-- 1. Schema 생성
-- =====================================
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

SET search_path TO public;

-- =====================================
-- 2. Master Tables (Reference Data)
-- =====================================

CREATE TABLE parts_master (
    part_official_name VARCHAR NOT NULL,
    car_type VARCHAR NOT NULL,
    extracted_at DATE NOT NULL,
    min_price INT,
    max_price INT,
    PRIMARY KEY (part_official_name, car_type, extracted_at)
);

CREATE TABLE labor_master (
    repair_content VARCHAR NOT NULL,
    car_type VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    standard_repair_time DOUBLE PRECISION,
    hour_labor_rate INT,
    change_cycle INT,
    PRIMARY KEY (repair_content, car_type, start_date)
);

-- =====================================
-- 3. Customer
-- =====================================

CREATE TABLE customer (
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    password VARCHAR
);

-- =====================================
-- 4. Estimates (Header)
-- =====================================

CREATE TABLE estimates (
    id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    image_url VARCHAR,
    car_type VARCHAR,
    car_mileage INT,
    service_finish_at DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (customer_id)
        REFERENCES customer(id)
);

-- =====================================
-- 5. Detail Tables
-- =====================================

CREATE TABLE parts (
    estimate_id VARCHAR NOT NULL,
    no INT NOT NULL,
    part_official_name VARCHAR,
    unit_price INT,
    PRIMARY KEY (estimate_id, no),
    FOREIGN KEY (estimate_id)
        REFERENCES estimates(id)
);

CREATE TABLE labor (
    estimate_id VARCHAR NOT NULL,
    no INT NOT NULL,
    repair_content VARCHAR,
    tech_fee INT,
    PRIMARY KEY (estimate_id, no),
    FOREIGN KEY (estimate_id)
        REFERENCES estimates(id)
);

-- =====================================
-- 6. RAG Document Chunks (test 스키마로 이동)
-- =====================================

CREATE TABLE repair_doc (
    id BIGSERIAL PRIMARY KEY,
    document_source TEXT NOT NULL,
    vehicle_model TEXT NOT NULL,
    symptom_text TEXT NOT NULL,
    system_category TEXT NOT NULL,
    repair_parts TEXT NOT NULL,
    pre_replace_check_rule TEXT NOT NULL,
    evidence_text TEXT NOT NULL,
    symptom_embedding VECTOR(1024),
    UNIQUE(vehicle_model, symptom_text, repair_parts, evidence_text)
);

CREATE INDEX repair_doc_model_idx ON repair_doc(vehicle_model);

-- =====================================
-- 7. Sample Data Insert
-- =====================================

-- Master 데이터
INSERT INTO parts_master VALUES
('필터 에어 클리너', '엑센트(RB)', '2026-02-16', 5810, 7000),
('서비스 키트-오일 필터', '엑센트(RB)', '2026-02-16', 6820, 8750);

INSERT INTO labor_master VALUES
('계기판 교환', '엑센트(RB)', '2026-01-01', '9999-12-31', 0.9, 132000, 40000);

-- 고객
INSERT INTO customer VALUES
('test@example.com', '홍길동', '1234');

-- 견적서
INSERT INTO estimates (
    id, customer_id, image_url, car_type, car_mileage, service_finish_at
) VALUES
(
    'EST_20260216_001',
    'test@example.com',
    's3://bucket/estimate1.png',
    '엑센트(RB)',
    85000,
    '2026-02-16'
),
(
    'EST_20260216_002',
    'test@example.com',
    's3://bucket/estimate2.png',
    '엑센트(RB)',
    50000,
    '2026-01-01'
);

-- 부품 상세
INSERT INTO parts VALUES
('EST_20260216_001', 1, '필터 에어 클리너', 6000),
('EST_20260216_001', 2, '서비스 키트-오일 필터', 10000);

-- 공임 상세
INSERT INTO labor VALUES
('EST_20260216_001', 1, '계기판 교환', 150000),
('EST_20260216_002', 1, '계기판 교환', 120000);

-- RAG 근거 문서 샘플
INSERT INTO repair_doc (
    document_source, vehicle_model, symptom_text, system_category,
    repair_parts, pre_replace_check_rule, evidence_text
) VALUES
(
    'hyundai_model_pdf',
    '엑센트(RB)',
    '차량이 한쪽으로 쏠린다',
    '조향현가',
    '드라이브샤프트(볼조인트), 휠베어링, 서스펜션/스티어링 부품',
    '타이어 편마모·공기압 불균형 또는 휠 얼라인먼트 문제만으로 단정하지 말 것.',
    '차량이 한쪽으로 쏠린다 . 드라이브샤프트 볼 조인트 긁힘 교 환 휠 베어링의 마모 , 소음 혹은 소착 교 환 프런트 서스펜션과 스티어링의 결함 조정 혹은 교환'
),
(
    'common_guideline',
    'common',
    '시동 지연 및 배터리 전압 저하',
    '전기충전',
    '배터리, 알터네이터',
    '배터리 성능 테스트와 충전 전압 측정 후 교체 판단',
    '충전 전압이 기준 미달이면 발전기 계통을 먼저 점검한다.'
);