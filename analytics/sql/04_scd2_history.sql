-- Revenue by the segment the customer had AT ORDER TIME (point-in-time, via SCD2)
-- vs. the segment they have TODAY. The difference is exactly why SCD2 matters.
SELECT f.customer_segment AS segment_at_order_time,
       d.segment          AS segment_today,
       SUM(f.recognized_revenue) AS net_revenue
FROM gold.fact_sales f
JOIN gold.dim_customer d ON d.customer_id = f.customer_id AND d.is_current
GROUP BY 1, 2
ORDER BY net_revenue DESC;

-- Full history of one customer / one product price
SELECT customer_id, city, segment, effective_from, effective_to, is_current
FROM gold.dim_customer
WHERE customer_id IN (SELECT customer_id FROM gold.dim_customer WHERE NOT is_current LIMIT 1)
ORDER BY effective_from;

SELECT product_id, list_price, unit_cost, effective_from, effective_to, is_current
FROM gold.dim_product
WHERE product_id IN (SELECT product_id FROM gold.dim_product WHERE NOT is_current LIMIT 1)
ORDER BY effective_from;
