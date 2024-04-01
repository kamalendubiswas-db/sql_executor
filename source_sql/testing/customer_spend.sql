-- This table identifies the customer spend for the month of december

/*
comment Customer spend analysis
*/
WITH customer_spend_as_of_december AS (
  SELECT
    c.customer_id,
    c.customer_name,
    d.department_name,
    SUM(f.amount) AS total_spend
  FROM
    facts.purchases f
  JOIN
    dim_customers c ON f.customer_id = c.customer_id
  JOIN
    dim_products p ON f.product_id = p.product_id
  JOIN
    dim_departments d ON p.department_id = d.department_id
  WHERE
    EXTRACT(MONTH FROM f.purchase_date) = 12 AND
    EXTRACT(YEAR FROM f.purchase_date) = 2023
  GROUP BY
    c.customer_id, c.customer_name, d.department_name
),
customer_ranked_by_spend AS (
  SELECT
    customer_id,
    customer_name,
    department_name,
    total_spend,
    RANK() OVER (ORDER BY total_spend DESC) AS spend_rank
  FROM
    customer_spend_as_of_december
)
SELECT
  customer_id,
  customer_name,
  department_name,
  total_spend,
  spend_rank
FROM
  customer_ranked_by_spend
WHERE
  spend_rank <= 10
ORDER BY
  spend_rank;