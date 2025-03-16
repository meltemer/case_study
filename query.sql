SELECT 
    customer_name,
    EXTRACT('MONTH' from s.sales_date) AS month,
    ((s.price_sold_per_device-s.cost_per_device)*quantity) AS total_revenue,
    SUM(s.units_sold) AS total_units_sold,
    CASE WHEN SUM(s.units_sold) IS NULL THEN 0
    ELSE (total_revenue / SUM(s.units_sold)::float) END AS revenue_per_unit
FROM sales s
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY customer_name, month
ORDER BY month, total_revenue DESC;
