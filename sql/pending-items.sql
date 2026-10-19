SELECT product_key AS product_id, COUNT(*) AS pending_item_lines,
       SUM(quantity) AS pending_units
FROM fact_order_item
WHERE status IN ('PENDING', 'PROCESSING', 'REPROCESSING')
GROUP BY product_key
ORDER BY product_id
