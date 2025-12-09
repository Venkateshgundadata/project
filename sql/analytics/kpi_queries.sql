-- E-Commerce Analytics Queries and KPIs
-- Sample queries for business intelligence and reporting

-- ============================================================================
-- SALES ANALYTICS
-- ============================================================================

-- 1. Daily Sales Trend (Last 30 Days)
SELECT 
    sales_date,
    SUM(total_revenue) as daily_revenue,
    SUM(total_orders) as daily_orders,
    AVG(avg_order_value) as avg_order_value
FROM fact_daily_sales
WHERE sales_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY sales_date
ORDER BY sales_date DESC;

-- 2. Top 10 Product Categories by Revenue
SELECT 
    category,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    SUM(total_items_sold) as items_sold,
    ROUND(AVG(avg_order_value), 2) as avg_order_value
FROM fact_daily_sales
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Monthly Revenue Trend
SELECT 
    DATE_TRUNC('month', sales_date) as month,
    SUM(total_revenue) as monthly_revenue,
    SUM(total_orders) as monthly_orders,
    SUM(unique_customers) as unique_customers
FROM fact_daily_sales
GROUP BY DATE_TRUNC('month', sales_date)
ORDER BY month DESC;

-- ============================================================================
-- CUSTOMER ANALYTICS
-- ============================================================================

-- 4. Customer Segmentation Analysis
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dim_customers
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- 5. Top 20 Customers by Total Spend
SELECT 
    dc.customer_id,
    dc.full_name,
    dc.customer_segment,
    dc.loyalty_tier,
    COUNT(fco.order_id) as total_orders,
    SUM(fco.total_amount) as total_spent,
    ROUND(AVG(fco.total_amount), 2) as avg_order_value
FROM dim_customers dc
JOIN fact_customer_orders fco ON dc.customer_key = fco.customer_key
GROUP BY dc.customer_id, dc.full_name, dc.customer_segment, dc.loyalty_tier
ORDER BY total_spent DESC
LIMIT 20;

-- 6. Customer Retention Rate (Repeat Customers)
WITH customer_orders AS (
    SELECT 
        customer_key,
        COUNT(*) as order_count
    FROM fact_customer_orders
    GROUP BY customer_key
)
SELECT 
    CASE 
        WHEN order_count = 1 THEN 'One-time'
        WHEN order_count BETWEEN 2 AND 5 THEN 'Occasional'
        WHEN order_count > 5 THEN 'Loyal'
    END as customer_type,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM customer_orders
GROUP BY customer_type;

-- ============================================================================
-- PRODUCT ANALYTICS
-- ============================================================================

-- 7. Top 20 Products by Revenue
SELECT 
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    dp.current_price,
    COUNT(*) as times_ordered,
    SUM(quantity) as total_quantity_sold
FROM dim_products dp
-- Note: This would need to join with order_items from source
-- Simplified version showing product catalog
ORDER BY dp.product_id
LIMIT 20;

-- 8. Product Performance by Category
SELECT 
    category,
    COUNT(*) as product_count,
    ROUND(AVG(current_price), 2) as avg_price,
    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_products,
    COUNT(CASE WHEN is_active = FALSE THEN 1 END) as inactive_products
FROM dim_products
GROUP BY category
ORDER BY product_count DESC;

-- ============================================================================
-- INVENTORY ANALYTICS
-- ============================================================================

-- 9. Low Stock Products
SELECT 
    product_id,
    product_name,
    category,
    current_stock,
    reorder_level,
    stock_status
FROM agg_inventory_status
WHERE stock_status IN ('Low Stock', 'Out of Stock')
ORDER BY current_stock ASC
LIMIT 50;

-- 10. Inventory Turnover by Category
SELECT 
    category,
    COUNT(*) as product_count,
    SUM(current_stock) as total_stock,
    SUM(total_out) as total_sold,
    ROUND(SUM(total_out) * 1.0 / NULLIF(SUM(current_stock), 0), 2) as turnover_ratio
FROM agg_inventory_status
GROUP BY category
ORDER BY turnover_ratio DESC;

-- ============================================================================
-- MARKETING ANALYTICS
-- ============================================================================

-- 11. Campaign Performance by Channel
SELECT 
    channel,
    COUNT(*) as campaign_count,
    SUM(total_impressions) as total_impressions,
    SUM(total_clicks) as total_clicks,
    SUM(total_conversions) as total_conversions,
    SUM(total_revenue) as total_revenue,
    SUM(budget) as total_budget,
    ROUND(AVG(ctr), 2) as avg_ctr,
    ROUND(AVG(conversion_rate), 2) as avg_conversion_rate,
    ROUND(AVG(roi_percentage), 2) as avg_roi
FROM agg_campaign_performance
GROUP BY channel
ORDER BY total_revenue DESC;

-- 12. Top 10 Campaigns by ROI
SELECT 
    campaign_name,
    channel,
    budget,
    total_revenue,
    roi_percentage,
    total_conversions,
    cost_per_conversion
FROM agg_campaign_performance
WHERE roi_percentage IS NOT NULL
ORDER BY roi_percentage DESC
LIMIT 10;

-- 13. Campaign Effectiveness Over Time
SELECT 
    DATE_TRUNC('month', start_date) as month,
    COUNT(*) as campaigns_launched,
    SUM(budget) as total_budget,
    SUM(total_revenue) as total_revenue,
    ROUND(AVG(roi_percentage), 2) as avg_roi,
    ROUND(AVG(conversion_rate), 2) as avg_conversion_rate
FROM agg_campaign_performance
GROUP BY DATE_TRUNC('month', start_date)
ORDER BY month DESC;

-- ============================================================================
-- ORDER ANALYTICS
-- ============================================================================

-- 14. Order Status Distribution
SELECT 
    order_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    SUM(total_amount) as total_value
FROM fact_customer_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- 15. Average Order Fulfillment Time
SELECT 
    order_status,
    COUNT(*) as order_count,
    ROUND(AVG(days_to_fulfill), 1) as avg_days_to_fulfill,
    MIN(days_to_fulfill) as min_days,
    MAX(days_to_fulfill) as max_days
FROM fact_customer_orders
WHERE days_to_fulfill IS NOT NULL
GROUP BY order_status
ORDER BY avg_days_to_fulfill;

-- 16. Payment Method Analysis
SELECT 
    payment_method,
    COUNT(*) as order_count,
    SUM(total_amount) as total_value,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    COUNT(CASE WHEN payment_status = 'Paid' THEN 1 END) as successful_payments,
    COUNT(CASE WHEN payment_status = 'Failed' THEN 1 END) as failed_payments
FROM fact_customer_orders
GROUP BY payment_method
ORDER BY total_value DESC;

-- ============================================================================
-- KEY PERFORMANCE INDICATORS (KPIs)
-- ============================================================================

-- 17. Overall Business KPIs
SELECT 
    COUNT(DISTINCT customer_key) as total_customers,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    SUM(total_items) as total_items_sold,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_key), 2) as revenue_per_customer
FROM fact_customer_orders;

-- 18. Month-over-Month Growth
WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', sales_date) as month,
        SUM(total_revenue) as revenue
    FROM fact_daily_sales
    GROUP BY DATE_TRUNC('month', sales_date)
)
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) / 
          NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100, 2) as growth_percentage
FROM monthly_stats
ORDER BY month DESC
LIMIT 12;

-- 19. Customer Lifetime Value (Simplified)
SELECT 
    dc.customer_segment,
    COUNT(DISTINCT dc.customer_key) as customer_count,
    ROUND(AVG(customer_stats.total_spent), 2) as avg_lifetime_value,
    ROUND(AVG(customer_stats.order_count), 1) as avg_orders_per_customer
FROM dim_customers dc
JOIN (
    SELECT 
        customer_key,
        SUM(total_amount) as total_spent,
        COUNT(*) as order_count
    FROM fact_customer_orders
    GROUP BY customer_key
) customer_stats ON dc.customer_key = customer_stats.customer_key
GROUP BY dc.customer_segment
ORDER BY avg_lifetime_value DESC;

-- 20. Conversion Funnel Analysis
SELECT 
    'Total Customers' as stage,
    COUNT(*) as count
FROM dim_customers
UNION ALL
SELECT 
    'Customers with Orders' as stage,
    COUNT(DISTINCT customer_key) as count
FROM fact_customer_orders
UNION ALL
SELECT 
    'Customers with Multiple Orders' as stage,
    COUNT(*) as count
FROM (
    SELECT customer_key
    FROM fact_customer_orders
    GROUP BY customer_key
    HAVING COUNT(*) > 1
) repeat_customers;
