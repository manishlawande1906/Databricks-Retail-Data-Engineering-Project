---- Standardize offline_customer data -----
CREATE OR REFRESH STREAMING TABLE stg_offline_customers AS 
SELECT 
    CASE
        when length(regexp_replace(phone_number, '[^0-9]', '')) >= 10
            Then substring(regexp_replace(phone_number, '[^0-9]', ''), -10, 10)
        Else regexp_replace(phone_number, '[^0-9]', '')
    END as customer_key,
    cast(customer_id as INT) as offline_customer_id,
    cast(null as string) as online_cusotmer_id,
    trim(customer_name) as  customer_name,
    lower(trim(email)) as email,
    trim(city) as city,
    cast(last_update as timestamp) as _source_ts,
    1 as source_priority
    FROM STREAM(db_lakehouse_retail.bronze.offline_customers) WITH (SKIPCHANGECOMMITS);

    ---- Standardize online_customer data -----
    CREATE OR REFRESH STREAMING TABLE stg_online_customers AS 
SELECT 
    CASE
        when length(regexp_replace(customer.phone, '[^0-9]', '')) >= 10
            Then substring(regexp_replace(customer.phone, '[^0-9]', ''), -10, 10)
        Else regexp_replace(customer.phone, '[^0-9]', '')
    End as customer_key,
    cast(customer.customer_id as INT) as offline_customer_id,
    customer.customer_id as online_cusotmer_id,
    trim(customer.name) as  customer_name,
    lower(trim(customer.email)) as email,
    cast(null as string) as city,
    cast(order_timestamp as timestamp) as _source_ts,
    2 as source_priority
    FROM STREAM(db_lakehouse_retail.bronze.online_orders_raw);

    -------- Union + composite sequence key ------
CREATE OR REFRESH STREAMING TABLE stg_customer_updates AS 
SELECT 
    *,
    concat(lpad(source_priority, 2, '0'), date_format(_source_ts, 'yyyyMMddHHmmssSSS')) as _seq_key
FROM 
    (SELECT 
        customer_key,
        offline_customer_id,
        online_cusotmer_id,
        customer_name,
        email,
        city,
        _source_ts,
        source_priority
     FROM STREAM(stg_offline_customers)
    UNION ALL
    SELECT 
        customer_key,
        offline_customer_id,
        online_cusotmer_id,
        customer_name,
        email,
        city,
        _source_ts,
        source_priority
     FROM STREAM(stg_online_customers));

---- Split valid vs invalid (quarantine)-------
CREATE or REFRESH STREAMING TABLE stg_customer_valid As
Select * from stream(stg_customer_updates)
where len(customer_key) = 10;

CREATE or REFRESH STREAMING TABLE customer_quarantine As    
Select *,
'invalid_phone_number' AS _reject_reason,
current_timestamp() as _quanratined_at
from stream(stg_customer_updates)
where len(customer_key) != 10;


-----------Auto CDC into the final SCD1 customer dim ----
-- Ignore null updates - an incoming row's NULLS never overwrite an existing row's values
CREATE or REFRESH STREAMING TABLE customer_dim;

CREATE FLOW customer_dim_flow as AUTO CDC INTO customer_dim
FROM STREAM(stg_customer_valid)
KEYS (customer_key)
IGNORE NULL UPDATES
SEQUENCE BY _seq_key
COLUMNS * EXCEPT (_seq_key, source_priority, _source_ts)
STORED AS SCD TYPE 1;


















