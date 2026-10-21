-- Daily revenue, orders and AOV across all categories
SELECT
    order_date,
    SUM(net_revenue)                                   AS net_revenue,
    SUM(gross_profit)                                  AS gross_profit,
    ROUND(SUM(gross_profit) / NULLIF(SUM(net_revenue), 0), 4) AS gross_margin_pct,
    SUM(refunded_amount)                               AS refunded_amount
FROM gold.agg_daily_sales
GROUP BY order_date
ORDER BY order_date;

-- Order-level AOV must come from the fact table (an order can span categories)
SELECT
    order_date,
    COUNT(DISTINCT order_id)                                        AS orders,
    SUM(recognized_revenue)                                         AS net_revenue,
    ROUND(SUM(recognized_revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_order_value
FROM gold.fact_sales
WHERE is_revenue
GROUP BY order_date
ORDER BY order_date;
