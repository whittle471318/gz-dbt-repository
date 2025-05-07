WITH margin_ship_join AS (
SELECT 
    orders_id,
    date_date,
    margin,
    shipping_fee,
    logcost,
    ship_cost, 
    revenue,
    quantity,
    purchase_cost
FROM {{ ref('int_orders_margin') }}
JOIN {{ ref('stg_raw__ship') }}
USING (orders_id)
)


SELECT 
    orders_id,
    date_date,
    ROUND(margin+shipping_fee-logcost-ship_cost,2) AS operational_margin,
    margin,
    quantity,
    revenue,
    purchase_cost,
    shipping_fee,
    logcost,
    ship_cost
FROM margin_ship_join
ORDER BY orders_id desc