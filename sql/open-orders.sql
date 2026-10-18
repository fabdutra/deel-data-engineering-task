SELECT d.date AS delivery_date, f.status, COUNT(*) AS open_orders
FROM fact_order f LEFT JOIN dim_date d ON f.delivery_date_key = d.date_key
WHERE f.status IN ('PENDING', 'PROCESSING', 'REPROCESSING')
GROUP BY d.date, f.status
ORDER BY delivery_date NULLS LAST, f.status
