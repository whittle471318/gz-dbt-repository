WITH daily_agg AS(
SELECT
    date_date,
    COUNT(orders_id) AS nb_transactions,
    SUM(revenue) AS revenue,
    SUM(margin) AS margin,
    SUM(operational_margin) AS operational_margin,
    SUM(purchase_cost) AS purchase_cost,
    SUM(shipping_fee) AS shipping_fee,
    SUM(logcost) AS logcost,
    SUM(quantity) AS quantity,
    SUM(ship_cost) AS ship_cost
FROM {{ ref('int_orders_operational') }}
GROUP BY date_date
ORDER BY date_date desc)

SELECT
date_date,
nb_transactions,
ROUND(revenue,2) AS revenue,
ROUND(revenue/nb_transactions,2) AS average_basket,
ROUND(margin,2) AS margin,
ROUND(operational_margin,2) AS operational_margin,
ROUND(purchase_cost,2) AS purchase_cost,
ROUND(shipping_fee,2) AS shipping_fee,
ROUND(logcost,2) AS logcost,
ROUND(ship_cost,2) AS ship_cost,
quantity
FROM daily_agg