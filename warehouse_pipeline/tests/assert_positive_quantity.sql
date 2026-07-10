SELECT *
FROM {{ ref('stg_inventory_movements') }}
WHERE QUANTITY <= 0