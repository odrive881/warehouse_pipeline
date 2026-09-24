
{{ config(materialized='table') }}


WITH shipped_by_month AS (
SELECT 
	DATE_TRUNC('month', event_timestamp) AS period,
	sku, 
	warehouse_id,
	SUM(quantity) AS units_shipped
FROM {{ ref('stg_inventory_movements') }}
WHERE event_type IN ('pick', 'shipment')
GROUP BY 1, 2, 3	
),



stock_by_month AS (
SELECT 
	DATE_TRUNC('month', event_timestamp) AS period,
	sku,
	warehouse_id,
	AVG(running_balance) as current_stock
FROM {{ ref('stg_inventory_movements') }}
GROUP BY 1, 2, 3
)


SELECT 
	st.period,
	st.sku,
	pm.product_name,
	pm.category,
	st.warehouse_id,
	COALESCE(sh.units_shipped, 0) AS units_shipped,
	ROUND(st.current_stock, 2) as current_stock,
	ROUND(COALESCE(sh.units_shipped, 0) / NULLIF(st.current_stock,0), 2) as turnover_ratio
FROM stock_by_month st
LEFT JOIN shipped_by_month sh ON st.sku = sh.sku AND st.warehouse_id = sh.warehouse_id AND st.period = sh.period
LEFT JOIN {{ ref('product_master') }} pm ON st.sku = pm.sku 
ORDER BY period ASC

