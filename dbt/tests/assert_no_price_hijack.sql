-- 제너레이터의 price_hijack 로직(1.00 설정)을 잡아내는 쿼리
SELECT *
FROM {{ ref('order_silver') }}
WHERE total_price > 1.00 AND paid_amount = 1.00 -- 가격 하이재킹 의심 데이터