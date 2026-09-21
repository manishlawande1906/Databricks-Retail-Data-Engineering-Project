-- Step 1: Offline sales — resolve raw ids to business keys
-- offline_orders references customer_id / product_id directly (not the
-- unified customer_key / sku_id), so both need a dimension lookup.
-- offline_orders only has a date (no time-of-day), so we treat it as midnight for the point-in-time comparison.
CREATE OR REFRESH STREAMING TABLE stg_offline_sales AS
SELECT
    o.order_id,
    CAST(o.order_date as timestamp) as order_ts,
    'offline' as channel,
    o.payment_mode,
    c.customer_key,
    p.product_sk,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price as total_amount,
    Cast(o.customer_id as string) as _source_cust_ref,
    Cast(o.product_id as string) as _source_prod_ref
FROM STREAM(db_lakehouse_retail.bronze.offline_orders) o
LEFT JOIN db_lakehouse_retail.silver.customer_dim c
    ON o.customer_id = c.offline_customer_id
LEFT JOIN db_lakehouse_retail.silver.product_dim p
    ON o.product_id = p.product_id
    AND p.__END_AT is NULL;

---- online sales -- explode items, resolve customers, point-in time product
CREATE OR REFRESH STREAMING TABLE stg_online_sales AS
WITH exploded AS (
    SELECT 
        s.order_id,
        s.order_timestamp,
        s.payment_mode,
        s.customer.customer_id AS online_customer_id,
        trim(item.sku_id) AS item_sku_id,
        item.quantity AS item_quantity,
        item.amount AS item_amount
    FROM STREAM(db_lakehouse_retail.bronze.online_orders_raw) s
    LATERAL VIEW explode(s.items) item_tbl AS item
)
SELECT 
    e.order_id AS order_id,
    e.order_timestamp AS order_ts,
    'online' AS channel,
    e.payment_mode AS payment_mode,
    c.customer_key AS customer_key,
    p.product_sk AS product_sk,
    e.item_quantity AS quantity,
    CASE WHEN e.item_quantity > 0 
         THEN CAST(e.item_amount / e.item_quantity AS decimal(10,2))
         ELSE NULL END as unit_price,
    CAST(e.item_amount as decimal(10,2)) AS total_amount,
    CAST(e.online_customer_id AS string) AS _source_cust_ref,
    trim(e.item_sku_id) AS _source_prod_ref
FROM exploded AS e
LEFT JOIN db_lakehouse_retail.silver.customer_dim c
    ON e.online_customer_id = c.online_cusotmer_id
LEFT JOIN db_lakehouse_retail.silver.product_dim p
    ON e.item_sku_id = p.sku_id
    AND p.__END_AT is NULL;


---step 3: Union both tables-------
create or refresh streaming table stg_sales_updates as
select * from stream(stg_offline_sales)
union all 
select * from stream(stg_online_sales);


------step 4: split resolved & unresolved (quarantine)
-- A null customer_key or sku_id means the dimension lookup failed
--eg. an order for a customer/product not yet present in Silver or a product outside any track __start_at/__end_at window.
CREATE OR REFRESH STREAMING TABLE sales_fact AS
SELECT * EXCEPT(_source_cust_ref, _source_prod_ref)
FROM STREAM(stg_sales_updates)
WHERE customer_key IS NOT NULL AND product_sk IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE sales_fact_quanrantine AS
SELECT *,
    CASE 
        WHEN customer_key IS NULL AND product_sk IS NULL THEN 'unmatched_product_and_customer'
        WHEN customer_key IS NULL THEN 'unmatched_customer'
        Else 'unmatched_product'
    END AS _reject_reason,
    current_timestamp() as _quarantined_at
FROM Stream(stg_sales_updates)
Where customer_key IS NULL OR product_sk IS NULL;









