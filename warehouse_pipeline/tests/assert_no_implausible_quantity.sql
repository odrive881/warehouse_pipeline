SELECT * 
FROM {{ ref('stg_inventory_movements') }}
WHERE quantity > 1000