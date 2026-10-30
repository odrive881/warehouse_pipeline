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
	-- Filtering on event_id rather than event_timestamp means an event sharing the latest
	-- loaded timestamp is not skipped. loader.py only accepts batches whose ids and timestamps
	-- both come after everything already loaded, so the new events never predate the prior balance.
	WHERE event_id > (SELECT COALESCE(MAX(event_id), 0) FROM {{ this }})
	{% endif %}
)

{% if is_incremental() %}
-- Closing balance per warehouse and SKU from the already-built table. The window function below
-- only sees the new events, so without this offset the balance would restart from zero
-- at the beginning of every incremental batch.
, prior_balance AS (
	SELECT DISTINCT ON (warehouse_id, sku)
		warehouse_id,
		sku,
		running_balance
	FROM {{ this }}
	ORDER BY warehouse_id, sku, event_timestamp DESC, event_id DESC
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
		PARTITION BY ne.warehouse_id, ne.sku
		ORDER BY ne.event_timestamp, ne.event_id
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	)
	{% if is_incremental() %}
	+ COALESCE(pb.running_balance, 0)
	{% endif %}
	AS running_balance
FROM new_events ne
{% if is_incremental() %}
LEFT JOIN prior_balance pb ON ne.warehouse_id = pb.warehouse_id AND ne.sku = pb.sku
{% endif %}
