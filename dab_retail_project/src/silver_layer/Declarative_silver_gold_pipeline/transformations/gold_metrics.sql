-- Gold layer - reporting view
-- Lakeflow Declarative pipeline (SQL)

--Q1. What is total revenue by channel?
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_revenue_by_channel AS
SELECT 
    channel,
    sum(total_amount) as total_revenue,
    count(distinct order_id) as total_orders,
    sum(quantity) as total_quantity
FROM silver.sales_fact
GROUP BY channel;

--Q2. Which product generate highest revenue?
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_product_performance AS
SELECT 
    pd.sku_id,
    pd.product_id,
    pd.product_name,
    sum(fs.total_amount) as total_revenue,
    sum(fs.quantity) as total_quantity_sold
FROM silver.sales_fact fs
JOIN silver.product_dim pd 
ON fs.product_sk= pd.product_sk
GROUP BY pd.sku_id, pd.product_id, pd.product_name
ORDER BY total_revenue DESC LIMIT 1;

--Q3. Who are the highest value customer?
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_customer_value AS
SELECT 
    cd.customer_key,
    cd.customer_name,
    cd.email,
    sum(fs.total_amount) as total_revenue,
    count(distinct fs.order_id) as total_orders
From db_lakehouse_retail.silver.customer_dim cd
LEFT JOIN db_lakehouse_retail.silver.sales_fact fs
ON cd.customer_key = fs.customer_key
GROUP BY cd.customer_key, cd.customer_name, cd.email
ORDER BY total_revenue DESC LIMIT 1;

    
