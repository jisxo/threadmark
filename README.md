📋 [프로젝트 명세서 / 시스템 프롬프트]
프로젝트명: CDC 기반 실시간 스트리밍 결함 데이터(Dirty Data) 정제 파이프라인

1. Project Overview (프로젝트 개요)
이 프로젝트는 실제 이커머스 환경에서 발생할 수 있는 8가지 데이터 장애(Dirty Data & Race Condition) 상황을 의도적으로 발생시키고, 이를 Apache Spark Structured Streaming을 활용해 실시간으로 감지 및 정제(Cleansing)하는 데이터 엔지니어링 파이프라인입니다.

2. Architecture & Tech Stack (아키텍처 및 기술 스택)
Data Generator: Python (Faker, psycopg2) - 8대 장애 시나리오에 기반한 트랜잭션 발생

Source Database: PostgreSQL (V2 Schema)

CDC (Change Data Capture): Debezium - Postgres의 변경분(WAL)을 감지하여 카프카로 전송

Message Broker: Apache Kafka - cdc.public.orders 토픽으로 메시지 스트리밍

Stream Processing: Apache Spark (PySpark) 3.5.0 - 장애 데이터 실시간 정제 및 집계

Infrastructure: Docker Compose (M1 Mac 환경)

3. Database Schema (V2 기준 핵심 구조)
categories, products, inventory, customers (기준 정보)

orders: order_id (PK), customer_id (FK), shop_id (할당 매장), status, created_at

order_items: order_id (FK), product_id (FK), quantity, price

payments: order_id (FK), amount, status

설정: 모든 테이블에 REPLICA IDENTITY FULL 적용 완료.

4. The 8 Dirty Data Scenarios (8대 데이터 결함 시나리오)
Python Faker 스크립트는 무한 루프를 돌며 아래 8가지 장애와 1가지 정상 데이터를 확률적으로 발생시킵니다.

[LATE ARRIVAL] 지연 도착: 네트워크 지연을 모사하여 created_at이 현재 시간보다 10분 전인 과거 데이터를 삽입합니다.

[DUPLICATE] 중복 발생: 프론트엔드 더블 클릭 버그를 모사하여, 완전히 동일한 트랜잭션을 2번 연속 삽입합니다.

[OUT-OF-ORDER] 상태 역전: 비동기 처리 오류를 모사하여, DELIVERED(배송완료) 상태를 먼저 업데이트하고, 1초 뒤에 과거 상태인 SHIPPED(배송중)로 업데이트합니다.

[BURST] 트래픽 폭주: 타임 세일 상황을 모사하여, 짧은 시간 내에 15건의 주문 트랜잭션을 한 번에 발생시킵니다.

[ZOMBIE ORDER] 결제 누락: 시스템 장애를 모사하여 orders에는 주문이 들어갔으나 payments 테이블에는 결제 내역이 삽입되지 않은 고아(Orphan) 데이터를 만듭니다.

[OVERSELLING] 초과 판매: 재고 차감 로직 누락을 모사하여, 단일 상품(Product 1)을 50개 강제 주문 처리하고 재고를 마이너스로 만듭니다.

[PRICE HIJACK] 결제 금액 불일치: 악의적인 API 변조를 모사하여, 총 주문 금액(Total Price)과 실제 결제 테이블(payments)에 찍힌 금액(예: $1.00)이 다르게 삽입됩니다.

[SWAPPED SHOP] 매장 정보 교차 (Race Condition): 동시에 2명의 고객(A, B)이 주문할 때, 동시성 제어 실패로 인해 고객 A의 주문에 매장 B가, 고객 B의 주문에 매장 A의 shop_id가 엇갈려(Cross) 저장됩니다.

5. Spark Streaming Defense Logic (스파크 실시간 정제 로직)
Spark processor.py는 카프카에서 데이터를 읽어 들여 아래의 로직으로 결함을 방어합니다.

스키마 명시 (Schema Enforcement): Debezium JSON 포맷에 맞춰 order_id, customer_id, shop_id, status 등을 강타입(Integer, String)으로 파싱합니다.

중복 제거 (Duplicate Drop): .withWatermark("event_time", "10 minutes")를 적용하여 메모리 오버헤드를 막고 10분 내의 중복 데이터를 제거합니다.

상태 역전 방지 (Out-of-Order Handling): PENDING(1) < SHIPPED(2) < DELIVERED(3)로 상태에 점수를 매기고, 윈도우(Window) 그룹핑 후 max(status_score)를 추출하여 항상 최신 상태만 통과시킵니다.

매장 꼬임 감지 (Swapped Shop Detection): 스파크 내부에 고객-정상매장 매핑 마스터 데이터(Static DataFrame)를 구성한 뒤, 카프카 스트림 데이터와 Stream-Static Join을 수행합니다. 실제 들어온 shop_id와 마스터의 expected_shop_id가 다를 경우 is_swapped = True 플래그를 생성하여 오류 데이터를 색출합니다.