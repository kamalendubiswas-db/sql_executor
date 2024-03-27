WITH order_line_summary AS (
  SELECT
    ol.order_id,
    ol.product_id,
    ol.quantity,
    ol.revenue,
    ol.order_date,
    d.region,
    d.country,
    d.city
  FROM
    fact_order_line ol
    JOIN dim_date d ON ol.order_date = d.date
    JOIN dim_product p ON ol.product_id = p.product_id
    JOIN dim_customer c ON ol.customer_id = c.customer_id
    JOIN dim_location l ON c.location_id = l.location_id
  WHERE
    ol.quantity > 10
),
total_revenue_per_product AS (
  SELECT
    product_id,
    SUM(revenue) AS total_revenue
  FROM
    order_line_summary
  GROUP BY
    product_id
),
ranked_products AS (
  SELECT
    product_id,
    total_revenue,
    RANK() OVER (
      ORDER BY
        total_revenue DESC
    ) AS rank
  FROM
    total_revenue_per_product
)
SELECT
  s.order_id,
  s.product_id,
  s.quantity,
  s.revenue,
  s.order_date,
  s.region,
  s.country,
  s.city,
  r.total_revenue,
  r.rank
FROM
  order_line_summary s
  JOIN ranked_products r ON s.product_id = r.product_id
ORDER BY
  r.rank;