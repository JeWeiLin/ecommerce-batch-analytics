-- Repeat purchase rate and time to second order
SELECT
    COUNT(*) FILTER (WHERE order_count >= 1)                        AS buying_customers,
    COUNT(*) FILTER (WHERE is_repeat_customer)                      AS repeat_customers,
    ROUND(1.0 * COUNT(*) FILTER (WHERE is_repeat_customer)
          / NULLIF(COUNT(*) FILTER (WHERE order_count >= 1), 0), 4) AS repeat_rate,
    AVG(days_to_second_order)                                       AS avg_days_to_second_order
FROM gold.agg_customer_orders;

-- Repeat rate by *current* customer segment
SELECT d.segment,
       COUNT(*) AS customers,
       ROUND(AVG(CASE WHEN a.is_repeat_customer THEN 1.0 ELSE 0 END), 4) AS repeat_rate,
       SUM(a.lifetime_revenue) AS lifetime_revenue
FROM gold.agg_customer_orders a
JOIN gold.dim_customer d ON d.customer_id = a.customer_id AND d.is_current
WHERE a.order_count >= 1
GROUP BY d.segment
ORDER BY lifetime_revenue DESC;
