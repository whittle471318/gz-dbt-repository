WITH date_trunc AS (SELECT
{{ date_trunc("month", "date_date") }} AS datemonth,
SUM(ads_margin) AS ads_margin,
SUM(average_basket) AS average_basket,
SUM(operational_margin) AS operational_margin,
SUM(ads_cost) AS ads_cost,
SUM(ads_impression) AS ads_impression,
SUM(ads_click) AS ads_click,
SUM(quantity) AS quantity,
SUM(revenue) AS revenue,
SUM(purchase_cost) AS purchase_cost,
SUM(margin) AS margin,
SUM(shipping_fee) AS shipping_fee,
SUM(logcost) AS logcost,
SUM(ship_cost) AS ship_cost
FROM {{ ref('finance_campaigns_day') }}
GROUP BY datemonth
ORDER BY datemonth desc)

SELECT
CAST(datemonth AS DATE) AS datemonth,
ROUND(ads_margin,2) AS ads_margin,
ROUND(average_basket,2) AS average_basket,
ROUND(operational_margin,2) AS operational_margin,
ads_cost,
ads_impression,
ads_click,
quantity,
ROUND(revenue,2) AS revenue,
ROUND(purchase_cost,2) AS purchase_cost,
ROUND(margin,2) AS margin,
ROUND(shipping_fee,2) AS shipping_fee,
ROUND(logcost,2) AS logcost,
ship_cost
FROM date_trunc
ORDER BY datemonth desc