{{ config(materialized='table') }}

-- Alerts are raised per warehouse: product_master's safety_stock_threshold applies
-- to each warehouse's own stock, since one site's surplus can't fill another's picks.
SELECT
	st.warehouse_id,
	st.sku,
	st.product_name,
	st.category,
	st.unit_cost,
	st.safety_stock_threshold,
	st.safety_stock_threshold - st.total_amount_in_stock AS units_below_threshold
FROM {{ ref('mart_stock_level') }} st
WHERE st.safety_stock_threshold > st.total_amount_in_stock
