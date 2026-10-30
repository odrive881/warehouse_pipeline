{{ config(materialized='table') }}

-- Current stock per warehouse and SKU is the running_balance of its most recent event,
-- which staging already computes, so the full history doesn't need to be re-summed.
WITH latest_balance AS (
	SELECT DISTINCT ON (warehouse_id, sku)
		warehouse_id,
		sku,
		running_balance
	FROM {{ ref('stg_inventory_movements') }}
	ORDER BY warehouse_id, sku, event_timestamp DESC, event_id DESC
)

SELECT
	lb.warehouse_id,
	lb.sku,
	pm.product_name,
	pm.category,
	pm.unit_cost,
	pm.safety_stock_threshold,
	lb.running_balance AS total_amount_in_stock,
	ROUND(lb.running_balance * pm.unit_cost::NUMERIC, 2) AS total_in_stock_value
FROM latest_balance lb
LEFT JOIN {{ ref('product_master') }} pm ON lb.sku = pm.sku

