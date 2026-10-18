WITH counts AS (
  SELECT customer_key, COUNT(*) AS pending_orders
  FROM fact_order
  WHERE status IN ('PENDING', 'PROCESSING', 'REPROCESSING')
  GROUP BY customer_key
)
SELECT c.customer_key AS customer_id, d.customer_name, c.pending_orders
FROM counts c LEFT JOIN dim_customer d ON c.customer_key = d.customer_key
ORDER BY pending_orders DESC, customer_id ASC
LIMIT 3
