---- Standardize offline products --
CREATE OR REFRESH STREAMING TABLE stg_offline_products AS
SELECT 
    upper(trim(sku_id)) as sku_id,
    cast (product_id as int) as product_id,
    initcap(trim(lower(product_name))) as product_name,
    initcap(trim(lower(category))) as category,
    upper(trim(brand)) as brand,
    Cast(last_update as timestamp) as _source_ts,
    1 as source_priority
FROM STREAM(db_lakehouse_retail.bronze.offline_products);

------- Standardize online products --
CREATE OR REFRESH STREAMING TABLE stg_online_products AS
SELECT 
    upper(trim(item.sku_id)) as sku_id,
    Cast(null as int) as product_id,
    initcap(trim(lower(item.product_name))) as product_name,
    initcap(trim(lower(item.category))) as category,
    upper(trim(item.brand)) as brand,
    Cast(order_timestamp as timestamp) as _source_ts,
    2 as source_priority
FROM STREAM(db_lakehouse_retail.bronze.online_orders_raw)
LATERAL VIEW explode(items) item_tbl as item;


---- Union + Composite sequence key -------
-- IMPORTANT: unlike customer_dim's string-based _seq_key, this one must stay a real TIMESTAMP. 
--STORED AS SCD TYPE 2 populates _START_AT/_END_ with this same type - and the Sales fact needs to range-join real order timestamps against _START_AT/_END_ for point-in-time product lookups. Priority is encoded as a microsecond-scale suffix of a string prefix: negligible for ordering vs. real timestamps, but still guarantees online (priority 2) sorts after offline (priority 1) on an exact tie.

CREATE OR REFRESH STREAMING TABLE stg_products_updates AS
SELECT 
    *,
    _source_ts + (source_priority * interval 1 microsecond) as _seq_key
FROM (
    SELECT * FROM STREAM(stg_offline_products)
    UNION ALL
    SELECT * FROM STREAM(stg_online_products)
);


---- -- Step 4: Split valid vs invalid (quarantine), and compute the surrogate key
-- Delta IDENTITY (auto-increment) columns are NOT supported on AUTO CDC INTO
-- target tables, so we can't let the engine auto-assign a surrogate key.
-- Instead, product_sk is a deterministic hash (sku_id, _seq_key) -- unique
-- per version, stable across runs (same inputs always get the same),
-- and computed here in the staging table so it flows straight through as a
-- normal column via AUTO CDC INTO.

create or refresh streaming table stg_products_valid as
Select 
    *,
    sha2(concat_ws('|', sku_id, Cast(_seq_key as string)), 256) as product_sk
from stream(stg_products_updates)
where sku_id is not null and length(sku_id) >0;

create or refresh streaming table stg_products_quarantine as 
select
    *,
    'missing_sku_id' as _reject_reason,
    current_timestamp() as _quarantine_at
from stream(stg_products_updates)
where sku_id is null or length(sku_id) = 0;

--------- Step 5: Auto CDC INTO the final SCD2 product dimension-------
-- STORED AS SCD TYPE 2 gives you _START_AT / _END_AT automatically, using
-- the same type as _seq_key (TIMESTAMP here). A new version row is inserted
-- whenever a tracked column changes; TRACK HISTORY ON * (the default) means
-- any attribute change - product_name, category, or brand - creates history.

CREATE OR REFRESH STREAMING TABLE product_dim;

CREATE FLOW product_dim_flow AS AUTO CDC INTO product_dim
FROM STREAM(stg_products_valid)
KEYS (product_sk)
IGNORE NULL UPDATES
SEQUENCE BY _seq_key
COLUMNS * EXCEPT(_seq_key, source_priority, _source_ts)
STORED AS SCD TYPE 2
TRACK HISTORY ON product_name, category, brand;












