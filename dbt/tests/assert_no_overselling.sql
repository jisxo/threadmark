-- Overselling 발생 시 재고가 마이너스로 업데이트된 데이터를 탐지
SELECT *
FROM {{ ref('order_silver') }}
WHERE stock_quantity < 0