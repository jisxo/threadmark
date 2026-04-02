{{ config(materialized='table') }}

WITH raw_orders AS (
    SELECT * FROM read_parquet('s3://silver-layer/orders_cleansed/*.parquet')
),

filtered_orders AS (
    SELECT
        order_id,
        cust_id,
        shop_id,
        total_price,
        paid_amount,
        stock_quantity,
        final_status_score,
        created_at,
        window_start,
        window_end
    FROM raw_orders
    WHERE 
        -- [시나리오 1] 999번 상점 데이터 제거 
        shop_id != 999
        -- [시나리오 2] 지연 데이터 원천 차단 (최근 1시간 이내 데이터만 적재 허용)
        AND created_at >= (current_timestamp - interval '1 hour')
        AND created_at <= current_timestamp
)

SELECT * FROM filtered_orders

