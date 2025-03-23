----bringing fx rates into the sales table 

drop table if exists public.tmp_sales_w_rates ;
create table public.tmp_sales_w_rates as
SELECT 
 DISTINCT  s.*, 
 concat(sold_currency,'USD') as sold_cur,
 concat(cost_currency,'USD') as cost_cur,
	 CASE 
		WHEN lower(concat(sold_currency,'USD'))= lower('usdusd') THEN 1
        WHEN lower(concat(sold_currency,'USD'))= lower('gbpusd') THEN first_value(fr.gbpusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(sold_currency,'USD'))= lower('eurusd') THEN first_value(fr.eurusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(sold_currency,'USD'))= lower('audusd') THEN first_value(fr.audusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(sold_currency,'USD'))= lower('jpyusd') THEN first_value(fr.jpyusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(sold_currency,'USD'))= lower('krwusd') THEN first_value(fr.krwusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(sold_currency,'USD'))= lower('hkdusd') THEN first_value(fr.hkdusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(sold_currency,'USD'))= lower('twdusd') THEN first_value(fr.twdusd) over (partition by s.sales_order_id order by sales_date-"date" )
 ELSE NULL
    END AS sold_fx,
    
    	 CASE 
		WHEN lower(concat(cost_currency,'USD'))= lower('usdusd') THEN 1
        WHEN lower(concat(cost_currency,'USD'))= lower('gbpusd') THEN first_value(fr.gbpusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(cost_currency,'USD'))= lower('eurusd') THEN first_value(fr.eurusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(cost_currency,'USD'))= lower('audusd') THEN first_value(fr.audusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(cost_currency,'USD'))= lower('jpyusd') THEN first_value(fr.jpyusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(cost_currency,'USD'))= lower('krwusd') THEN first_value(fr.krwusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(cost_currency,'USD'))= lower('hkdusd') THEN first_value(fr.hkdusd) over (partition by s.sales_order_id order by sales_date-"date" )
        WHEN lower(concat(cost_currency,'USD'))= lower('twdusd') THEN first_value(fr.twdusd) over (partition by s.sales_order_id order by sales_date-"date" )

        ELSE NULL
    END AS cost_fx
    
FROM  public.sales s 
left join public.fx_rates fr on s.sales_date >= fr."date" ;


------- revenue analysis

with cte as (SELECT 
   -- EXTRACT('MONTH' from s.sales_date) AS month,
    to_char(s.sales_date, 'YYYY-MM')  as month,                                                            
    s.customer_id,
    c.customer_name,
    s.quantity,
    ((s.price_sold_per_device*sold_fx)*quantity) AS total_revenue
FROM  public.tmp_sales_w_rates s 
left join public.customers c ON c.id = s.customer_id)
select 
month, 
customer_name, 
sum(total_revenue)/ sum(quantity) as total_revenue_per_item
from cte 
group by 1,2 order by 1,2; 
