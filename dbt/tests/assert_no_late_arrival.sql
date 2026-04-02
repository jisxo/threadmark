-- Scen 1: 10분 지연된 데이터를 잡아내기 위한 5분 임계치 설정
SELECT *
FROM {{ ref('order_silver') }}
WHERE created_at < (CURRENT_TIMESTAMP - INTERVAL 5 MINUTE)