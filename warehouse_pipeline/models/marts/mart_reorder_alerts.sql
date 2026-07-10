{{ config(materialized='table') }}

SELECT
	st.sku,
	st.product_name,
	st.category,
	st.unit_cost,
	st.safety_stock_threshold,
	st.safety_stock_threshold - total_amount_in_stock AS units_below_threshold
FROM {{ ref('mart_stock_level') }} st
LEFT JOIN {{ ref('product_master') }} pm USING (sku)
WHERE st.safety_stock_threshold > st.total_amount_in_stock
