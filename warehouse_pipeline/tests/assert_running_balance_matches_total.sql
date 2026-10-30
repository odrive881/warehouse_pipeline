-- The latest running_balance per warehouse and SKU must equal the sum of all its movements.
-- A mismatch means the incremental prior_balance offset in stg_inventory_movements went wrong.
-- Returns failing rows

WITH totals AS (
	SELECT
		warehouse_id,
		sku,
		SUM(quantity_impact) AS summed_balance
	FROM {{ ref('stg_inventory_movements') }}
	GROUP BY warehouse_id, sku
)

SELECT
	sl.warehouse_id,
	sl.sku,
	sl.total_amount_in_stock,
	t.summed_balance
FROM {{ ref('mart_stock_level') }} sl
JOIN totals t ON sl.warehouse_id = t.warehouse_id AND sl.sku = t.sku
WHERE sl.total_amount_in_stock IS DISTINCT FROM t.summed_balance
