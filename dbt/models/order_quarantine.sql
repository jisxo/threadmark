{{ config(materialized='table') }}

WITH raw_orders AS (
    SELECT * FROM read_parquet('s3://silver-layer/orders_cleansed/*.parquet')
)

SELECT 
    *,
    -- 왜 격리되었는지 사유를 적어줍니다 (이게 A2의 핵심!)
    CASE 
        WHEN shop_id = 999 THEN 'INVALID_SHOP_ID'
        WHEN created_at < (current_timestamp - interval '1 hour') THEN 'LATE_ARRIVAL'
        ELSE 'OTHER_ERROR'
    END AS rejection_reason
FROM raw_orders
WHERE 
    -- 실버 레이어에서 걸러졌던 '불량 조건'들만 여기서 모읍니다.
    shop_id = 999 
    OR created_at < (current_timestamp - interval '1 hour')