WITH purchase_cost_calc AS(
SELECT 
    date_date,
    orders_id,
    products_id,
    CONCAT(orders_id,"_",products_id) AS order_pdt_id,
    revenue,
    quantity,
    purchase_price,
    quantity*purchase_price AS purchase_cost,
FROM {{ ref('stg_raw__sales') }}
LEFT JOIN {{ ref('stg_raw__product') }}
USING (products_id))

SELECT *,
ROUND(revenue-purchase_cost,2) AS margin
FROM purchase_cost_calc