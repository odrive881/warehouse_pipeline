{{
	config(
		materialized='incremental',
		unique_key='event_id'
	)
}}

WITH new_events AS (
	SELECT
		event_id,
		sku,
		warehouse_id,
		event_type,
		quantity,
		event_timestamp,
		CASE
			WHEN event_type IN ('receipt', 'return') THEN quantity
			WHEN event_type IN ('pick', 'damage', 'shipment') THEN - quantity
		END AS quantity_impact
	FROM {{ source('warehouse_db', 'movements') }}

	{% if is_incremental() %}
	WHERE event_timestamp > (SELECT COALESCE( MAX(event_timestamp), '1970-01-01'::timestamp) FROM {{ this }})
	{% endif %}
)

{% if is_incremental() %}
-- Closing balance per SKU from the already-built table. The window function below only
-- sees the new events, so without this offset the balance would restart from zero
-- at the beginning of every incremental batch.
, prior_balance AS (
	SELECT DISTINCT ON (sku)
		sku,
		running_balance
	FROM {{ this }}
	ORDER BY sku, event_timestamp DESC, event_id DESC
)
{% endif %}

SELECT
	ne.event_id,
	ne.sku,
	ne.warehouse_id,
	ne.event_type,
	ne.quantity,
	ne.event_timestamp,
	ne.quantity_impact,
	SUM(ne.quantity_impact) OVER (
		PARTITION BY ne.sku
		ORDER BY ne.event_timestamp, ne.event_id
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	)
	{% if is_incremental() %}
	+ COALESCE(pb.running_balance, 0)
	{% endif %}
	AS running_balance
FROM new_events ne
{% if is_incremental() %}
LEFT JOIN prior_balance pb ON ne.sku = pb.sku
{% endif %}
