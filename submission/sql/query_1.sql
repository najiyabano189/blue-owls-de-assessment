-- Query 1: Revenue Trend Analysis with Ranking
-- Strategy: CTEs to build monthly revenue per category, then window
-- functions for rank, MoM growth, and 3-month rolling average.
-- Only top 5 categories by overall revenue.
-- Only months with at least 10 transactions.

WITH monthly AS (
    SELECT
        p.product_category_name,
        YEAR(f.order_date)              AS year,
        MONTH(f.order_date)             AS month,
        SUM(f.price + f.freight_value)  AS monthly_revenue,
        COUNT(*)                        AS transaction_count
    FROM fact_order_items f
    JOIN dim_products p ON f.product_key = p.product_key
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 10
),
ranked AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY year, month
            ORDER BY monthly_revenue DESC
        ) AS monthly_rank
    FROM monthly
),
top5 AS (
    SELECT product_category_name
    FROM monthly
    GROUP BY product_category_name
    ORDER BY SUM(monthly_revenue) DESC
    LIMIT 5
),
with_growth AS (
    SELECT r.*,
        LAG(r.monthly_revenue) OVER (
            PARTITION BY r.product_category_name
            ORDER BY r.year, r.month
        ) AS prev_month_revenue,
        AVG(r.monthly_revenue) OVER (
            PARTITION BY r.product_category_name
            ORDER BY r.year, r.month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_avg_revenue
    FROM ranked r
    INNER JOIN top5 t ON r.product_category_name = t.product_category_name
)
SELECT
    product_category_name,
    year,
    month,
    ROUND(monthly_revenue, 2)                                AS monthly_revenue,
    monthly_rank,
    ROUND(
        (monthly_revenue - prev_month_revenue)
        / NULLIF(prev_month_revenue, 0) * 100
    , 2)                                                     AS mom_growth_pct,
    ROUND(rolling_3m_avg_revenue, 2)                         AS rolling_3m_avg_revenue
FROM with_growth
ORDER BY product_category_name, year, month
