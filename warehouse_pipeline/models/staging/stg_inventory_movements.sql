{{
	config(
		materialized='incremental',
		unique_key='event_id'
	)
}}

SELECT
	event_id,
	sku,
	warehouse_id,
	event_type,
	quantity,
	event_timestamp,
	CASE
		WHEN event_type IN ('receipt', 'return') THEN QUANTITY
		WHEN event_type IN ('pick', 'damage', 'shipment') THEN - QUANTITY
	END AS quantity_impact,
	SUM(
		CASE
				WHEN event_type IN ('receipt', 'return') THEN QUANTITY
				WHEN event_type IN ('pick', 'damage', 'shipment') THEN - QUANTITY
			END 
		) 
		OVER(
		PARTITION BY sku
		ORDER BY event_timestamp
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_balance
FROM {{ source('warehouse_db', 'movements') }} 

{% if is_incremental() %}
WHERE event_timestamp > (SELECT COALESCE( MAX(event_timestamp), '1970-01-01'::timestamp) FROM {{ this }})
{% endif %}