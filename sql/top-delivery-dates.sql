SELECT d.date AS delivery_date, COUNT(*) AS open_orders
FROM fact_order f LEFT JOIN dim_date d ON f.delivery_date_key = d.date_key
WHERE f.status IN ('PENDING', 'PROCESSING', 'REPROCESSING')
GROUP BY d.date
ORDER BY open_orders DESC, delivery_date ASC NULLS LAST
LIMIT 3
