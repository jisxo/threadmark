# Threadmark

CDC 기반 실시간 스트리밍 결함 데이터(Dirty Data) 정제 파이프라인

## 1. 프로젝트 개요
Threadmark는 이커머스 주문 데이터에서 발생할 수 있는 결함 데이터를 의도적으로 생성하고,
`PostgreSQL -> Debezium -> Kafka -> Spark Structured Streaming` 파이프라인으로 실시간 정제/격리를 수행하는 프로젝트입니다.

핵심 목표는 다음 2가지입니다.
- 정상 데이터는 Silver 레이어로 안정적으로 적재
- 결함 데이터는 Quarantine 레이어로 분리하여 운영 리스크 최소화

## 2. 아키텍처
```text
[Faker Generator]
      |
      v
[PostgreSQL] --(WAL/CDC)--> [Debezium Connect] --> [Kafka: cdc.public.orders]
                                                         |
                                                         v
                                              [Spark Structured Streaming]
                                               |                        |
                                               v                        v
                            [Silver: orders_cleansed (Parquet)] [Quarantine: orders_failed (Parquet)]
                                               |
                                               v
                                           [dbt Models/Tests]
```

## 3. 기술 스택
- Data Generator: Python (`faker`, `psycopg2`)
- Source DB: PostgreSQL
- CDC: Debezium
- Broker: Apache Kafka
- Stream Processing: Apache Spark (PySpark 3.5)
- Object Storage: MinIO (S3-compatible)
- Modeling/Tests: dbt
- Infra: Docker Compose

## 4. Dirty Data 시나리오 (8종)
`faker_gen.py`에서 정상/비정상 트랜잭션을 확률적으로 생성합니다.

1. `LATE ARRIVAL`: 과거 시각 데이터 지연 도착
2. `DUPLICATE`: 동일 주문 중복 삽입
3. `OUT-OF-ORDER`: 상태 역전 (`DELIVERED -> SHIPPED`)
4. `BURST`: 짧은 시간 다량 주문
5. `ZOMBIE ORDER`: 주문은 있으나 결제 없음
6. `OVERSELLING`: 재고 음수 유도
7. `PRICE HIJACK`: 주문 총액과 결제 금액 불일치
8. `SWAPPED SHOP`: 잘못된 shop 매핑 (`shop_id=999` 주입)

## 5. 데이터베이스 핵심 스키마
`db/init_v2.sql` 기준 핵심 테이블

- `orders(order_id, customer_id, shop_id, status, created_at, updated_at)`
- `order_items(order_id, product_id, quantity, price)`
- `payments(order_id, amount, status, created_at)`
- `inventory(product_id, stock_quantity, updated_at)`

CDC 캡처 정확도를 위해 주요 테이블에 `REPLICA IDENTITY FULL`이 적용되어 있습니다.

## 6. Spark 정제/격리 로직
`spark_apps/processor.py` 기준 처리 흐름

1. Kafka JSON 파싱 및 스키마 적용
2. `created_at` -> `event_time` 변환
3. 상태 점수화 (`PENDING=1`, `SHIPPED=2`, `DELIVERED=3`)
4. 워터마크(10분) + 집계
5. `rejection_reason` 기준 Silver/Quarantine 분기

현재 코드 기준 주요 분기 조건
- `shop_id == 999` -> `INVALID_SHOP_ID`
- 지나치게 늦은 이벤트 -> `LATE_ARRIVAL`

저장 경로
- Silver: `s3a://silver-layer/orders_cleansed`
- Quarantine: `s3a://quarantine-layer/orders_failed`
- Checkpoint: `s3a://silver-layer/checkpoints/orders_v9`

## 7. dbt 모델/테스트
`dbt/`에서 Silver/Quarantine 모델과 품질 테스트를 관리합니다.

- 모델: `order_silver.sql`, `order_quarantine.sql`
- 스키마 테스트: `models/schema.yml`
- 커스텀 테스트: 지연 도착, 초과 판매, 가격 하이재킹 검증 SQL

## 8. 빠른 실행 방법
### 1) 인프라 실행
```bash
docker compose up -d
```

### 2) MinIO 버킷 초기화
```bash
make init-minio
```

### 3) Spark 스트리밍 실행
```bash
make run
```

### 4) (선택) dbt 실행
```bash
cd dbt
dbt run
dbt test
```

## 9. 디렉터리 구조
```text
threadmark/
├─ db/                   # PostgreSQL init SQL
├─ spark_apps/           # Spark streaming application
├─ dbt/                  # dbt models/tests
├─ faker_gen.py          # dirty-data generator
├─ connector_config.json # Debezium connector config
├─ docker-compose.yml    # local infra
└─ docs/                 # additional documents
```

## 10. 문서 가이드
- 이 `README.md`: 프로젝트 개요 + 실행 중심 통합 문서
- `docs/project_overview.md`: 구조/운영 관점 상세 설명 문서
