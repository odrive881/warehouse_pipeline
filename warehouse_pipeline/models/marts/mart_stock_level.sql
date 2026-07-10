
--NOTE:
--This mart is very short and simple, but might not be the most efficient way to calculate total amount in stock

{{ config(materialized='table') }}

SELECT			
	stg.sku,
	pm.product_name,
	pm.category,
	pm.unit_cost,
	pm.safety_stock_threshold,
	SUM(stg.quantity_impact) as total_amount_in_stock,
	ROUND(SUM(stg.quantity_impact) * pm.unit_cost::NUMERIC, 2) AS total_in_stock_value
FROM {{ ref('stg_inventory_movements') }} stg
LEFT JOIN {{ ref('product_master') }} pm ON stg.sku = pm.sku
GROUP BY 1, 2, 3, 4, 5

