WITH day_join AS (
SELECT *
FROM {{ ref('finance_days') }}
LEFT JOIN {{ ref('int_campaigns_day') }}
USING (date_date))

SELECT
date_date,
ROUND((operational_margin-ads_cost),2) AS ads_margin,
average_basket,
operational_margin,
ads_cost,
ads_impression,
ads_click,
quantity,
revenue,
purchase_cost,
margin,
shipping_fee,
logcost,
ship_cost
FROM day_join
ORDER BY date_date desc
