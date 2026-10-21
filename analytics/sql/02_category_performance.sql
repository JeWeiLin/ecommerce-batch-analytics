-- Category performance over the whole period
SELECT
    category,
    SUM(units_sold)    AS units_sold,
    SUM(net_revenue)   AS net_revenue,
    SUM(gross_profit)  AS gross_profit,
    ROUND(SUM(gross_profit) / NULLIF(SUM(net_revenue), 0), 4) AS gross_margin_pct
FROM gold.agg_daily_sales
GROUP BY category
ORDER BY net_revenue DESC;

-- Top 10 products by revenue
SELECT f.product_id, p.product_name, f.category,
       SUM(f.quantity) FILTER (WHERE f.is_revenue) AS units_sold,
       SUM(f.recognized_revenue) AS net_revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON p.product_id = f.product_id AND p.is_current
GROUP BY 1, 2, 3
ORDER BY net_revenue DESC
LIMIT 10;
