-- =====================================
-- 1. Schema 생성
-- =====================================
CREATE SCHEMA IF NOT EXISTS test;
SET search_path TO test;

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
-- 6. Sample Data Insert
-- =====================================

-- Master 데이터
INSERT INTO parts_master VALUES
('필터 에어 클리너', '제네시스', '2026-02-16', 5810, 7000),
('서비스 키트-오일 필터', '제네시스', '2026-02-16', 6820, 8750);

INSERT INTO labor_master VALUES
('계기판 교환', '제네시스', '2026-01-01', '9999-12-31', 0.9, 132000, 40000);

-- 고객
INSERT INTO customer VALUES
('test@example.com', '홍길동', '1234');

-- 견적서
INSERT INTO estimates (
    id,
    customer_id,
    image_url,
    car_type,
    car_mileage,
    service_finish_at
) VALUES 
(
    'EST_20260216_001',
    'test@example.com',
    's3://bucket/estimate1.png',
    '제네시스',
    85000,
    '2026-02-16'
),
(   'EST_20260216_002',
    'test@example.com',
    's3://bucket/estimate2.png',
    '제네시스',
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


