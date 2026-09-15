-- ============================================================
-- Superstore Retail Sales Dashboard Queries
-- Databricks SQL
-- ============================================================

-- 1. KPI Metrics
SELECT
    total_customers,
    total_orders,
    total_sales,
    total_profit
FROM mini_project_1.project1_schema.gold_kpis;


-- 2. Sales by Country
SELECT
    country,
    total_sales
FROM mini_project_1.project1_schema.gold_sales_by_country
ORDER BY total_sales DESC;


-- 3. Profit by Region and Country
SELECT
    country,
    region,
    total_profit
FROM mini_project_1.project1_schema.gold_profit_by_region_country
ORDER BY total_profit DESC;


-- 4. Sales by Category
SELECT
    category,
    total_sales
FROM mini_project_1.project1_schema.gold_sales_by_category
ORDER BY total_sales DESC;


-- 5. Top 10 Sub-Categories by Sales
SELECT
    sub_category,
    total_sales
FROM mini_project_1.project1_schema.gold_top_10_subcategories
ORDER BY total_sales DESC;


-- 6. Top Products by Ordered Quantity
SELECT
    product_name,
    total_quantity
FROM mini_project_1.project1_schema.gold_top_products_quantity
ORDER BY total_quantity DESC;


-- 7. Top Customers by Sales and City
SELECT
    customer_name,
    city,
    total_sales
FROM mini_project_1.project1_schema.gold_top_10_customers
ORDER BY total_sales DESC;
