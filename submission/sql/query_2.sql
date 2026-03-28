-- Query 2: Seller Performance Scorecard
-- Strategy: Calculate raw metrics per seller, then use PERCENT_RANK()
-- window function for each metric. Invert ranking for late_delivery_rate
-- and avg_days_vs_estimate so lower = better becomes higher percentile.
-- Composite score = weighted average of three percentiles.
-- Only sellers with at least 20 orders included.

WITH seller_metrics AS (
    SELECT
        s.seller_id,
        s.seller_state,
        COUNT(*)                                    AS total_orders,
        ROUND(SUM(f.price + f.freight_value), 2)   AS total_revenue,
        ROUND(
            SUM(CASE WHEN f.is_late_delivery = true THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*), 2
        )                                           AS late_delivery_rate,
        ROUND(AVG(f.days_delivery_vs_estimate), 2) AS avg_days_vs_estimate
    FROM fact_order_items f
    JOIN dim_sellers s ON f.seller_key = s.seller_key
    WHERE f.days_delivery_vs_estimate IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) >= 20
),
percentiles AS (
    SELECT *,
        ROUND(PERCENT_RANK() OVER (
            ORDER BY late_delivery_rate ASC
        ) * 100, 2)                                AS on_time_pctl,
        ROUND(PERCENT_RANK() OVER (
            ORDER BY avg_days_vs_estimate ASC
        ) * 100, 2)                                AS speed_pctl,
        ROUND(PERCENT_RANK() OVER (
            ORDER BY total_revenue DESC
        ) * 100, 2)                                AS revenue_pctl
    FROM seller_metrics
)
SELECT
    seller_id,
    seller_state,
    total_orders,
    total_revenue,
    late_delivery_rate,
    avg_days_vs_estimate,
    on_time_pctl,
    speed_pctl,
    revenue_pctl,
    ROUND(
        (on_time_pctl * 0.40) +
        (speed_pctl   * 0.30) +
        (revenue_pctl * 0.30)
    , 2)                                           AS composite_score,
    RANK() OVER (
        ORDER BY
            (on_time_pctl * 0.40) +
            (speed_pctl   * 0.30) +
            (revenue_pctl * 0.30) DESC
    )                                              AS overall_rank
FROM percentiles
ORDER BY overall_rank
