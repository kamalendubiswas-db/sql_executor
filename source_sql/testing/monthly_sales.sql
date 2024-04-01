/*
Monthly sales
*/
WITH monthly_sales AS (
    SELECT
        t.time_month,
        p.category_id,
        p.product_id,
        c.customer_id,
        SUM(f.sales_amount) AS total_sales,
        COUNT(f.transaction_id) AS transaction_count
    FROM fact_sales f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY t.time_month, p.category_id, p.product_id, c.customer_id
),
ranked_products AS (
    SELECT
        time_month,
        category_id,
        product_id,
        total_sales,
        RANK() OVER (
            PARTITION BY time_month, category_id
            ORDER BY total_sales DESC
        ) AS sales_rank
    FROM monthly_sales
),
average_category_sales AS (
    SELECT
        category_id,
        AVG(total_sales) AS avg_monthly_sales
    FROM monthly_sales
    GROUP BY category_id
),
previous_month_top_products AS (
    SELECT
        category_id,
        product_id,
        total_sales
    FROM ranked_products
    WHERE
        sales_rank = 1 AND
        time_month = (SELECT MAX(time_month) FROM ranked_products WHERE time_month < CURRENT_DATE)
)
SELECT
    cat.category_name,
    prod.product_name,
    cust.customer_name,
    ms.time_month,
    ms.total_sales,
    ms.transaction_count,
    rp.sales_rank,
    acs.avg_monthly_sales,
    CASE
        WHEN pmt.product_id IS NOT NULL THEN 'Top Seller Previous Month'
        ELSE NULL
    END AS previous_month_top_seller
FROM monthly_sales ms
JOIN ranked_products rp ON ms.time_month = rp.time_month AND ms.category_id = rp.category_id AND ms.product_id = rp.product_id
JOIN average_category_sales acs ON ms.category_id = acs.category_id
JOIN dim_category cat ON ms.category_id = cat.category_id
JOIN dim_product prod ON ms.product_id = prod.product_id
JOIN dim_customer cust ON ms.customer_id = cust.customer_id
LEFT JOIN previous_month_top_products pmt ON ms.category_id = pmt.category_id AND ms.product_id = pmt.product_id
ORDER BY ms.time_month DESC, cat.category_name, rp.sales_rank;
