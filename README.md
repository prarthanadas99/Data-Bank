# Data-Bank

1. How many unique nodes are there on the Data Bank system?
```sql
select distinct node_id from customer_nodes;
```
2. What is the number of nodes per region?
```sql
select region_id, count( node_id) as cnt_of_node_per_region from customer_nodes
group by region_id;
```
3. How many customers are allocated to each region?
```sql
select cn.region_id, r.region_name, count(cn.customer_id) customer_cnt
from customer_nodes cn
inner join regions r on cn.region_id = r.region_id
group by cn.region_id, r.region_name
order by customer_cnt desc;
```
4. How many days on average are customers reallocated to a different node?
```sql
select cn.customer_id, cn.node_id, cn.start_date, cn.end_date, datediff(cn.end_date, cn.start_date ) dif
from customer_nodes cn
inner join regions r on cn.region_id = r.region_id
WHERE end_date != '9999-12-31'
group by cn.customer_id, cn.node_id, cn.start_date, cn.end_date;
```
5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
```sql
WITH date_diff AS (
	SELECT cn.customer_id, cn.region_id, r.region_name, DATEDIFF(end_date, start_date) AS reallocation_days
	FROM customer_nodes cn
	INNER JOIN regions r
	ON cn.region_id = r.region_id
	WHERE end_date != '9999-12-31'
)
SELECT DISTINCT region_id, region_name,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY reallocation_days) OVER(PARTITION BY region_name) AS median,
PERCENTILE_CONT(0.8) WITHIN GROUP(ORDER BY reallocation_days) OVER(PARTITION BY region_name) AS percentile_80,
PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY reallocation_days) OVER(PARTITION BY region_name) AS percentile_95
FROM date_diff
ORDER BY region_name;
```
6. What is the unique count and total amount for each transaction type?
```sql
select txn_type, sum(txn_amount) as total_amount, count(*) cnt_of_deposits from customer_transactions
group by txn_type;
```
7. What is the average total historical deposit counts and amounts for all customers?
```sql
with deposits as (
select customer_id, txn_type, count(txn_type) as cnt_of_deposits,
sum(txn_amount) as total_amount from customer_transactions
where txn_type = "deposit"
group by customer_id
)
select customer_id, txn_type, round(avg(cnt_of_deposits)) as avg_of_deposits,
round(avg(total_amount)) as avg_total_amount from deposits
group by customer_id, txn_type;
```
8. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
```sql
with txn as(
select customer_id, extract(month from txn_date) as mn, extract(year from txn_date) as yr,
count(CASE WHEN txn_type = 'deposit' THEN 1 END) AS deposit_cnt,
count(CASE WHEN txn_type = 'purchase' THEN 1 END) AS purchase_cnt,
count(CASE WHEN txn_type = 'withdrawal' THEN 1 END) AS withdrawal_cnt from customer_transactions
group by customer_id, extract(month from txn_date), extract(year from txn_date)
)
select mn, count(customer_id) as cust_cnt from txn 
where deposit_cnt >1 and (purchase_cnt > 0 or withdrawal_cnt > 0)
group by mn;
```
9. What is the closing balance for each customer at the end of the month?
```sql
WITH cte AS (
	SELECT customer_id,
	DATEADD(MONTH, DATEDIFF(MONTH, 0, txn_date), 0) AS month_start,
	SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -1 * txn_amount END) AS total_amount
	FROM customer_transactions
	GROUP BY customer_id, DATEADD(MONTH, DATEDIFF(MONTH, 0, txn_date), 0)
)
SELECT cte.customer_id, DATEPART(MONTH, cte.month_start) AS month, DATENAME(MONTH, cte.month_start) AS month_name,
SUM(cte.total_amount) OVER(PARTITION BY cte.customer_id ORDER BY cte.month_start) AS closing_balance
FROM cte;
```
10. running customer balance column that includes the impact each transaction
```sql
SELECT customer_id, txn_date,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -1 * txn_amount END) AS total_amount
FROM customer_transactions
GROUP BY customer_id, txn_date
order by customer_id;
```
11.customer balance at the end of each month
```sql
with tn as(
SELECT customer_id, txn_date,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -1 * txn_amount END) AS total_amount
FROM customer_transactions
GROUP BY customer_id, txn_date
order by customer_id
),
 tn1 as (SELECT distinct customer_id, total_amount,
max(txn_date) over (partition by customer_id) as end_of_month,
count(txn_date) over (partition by customer_id) as cnt,
row_number() over (partition by customer_id) as rw
from tn
)
select customer_id, extract(month from end_of_month) as month_end, total_amount from tn1
where cnt = rw
order by total_amount desc;
```
12. minimum, average and maximum values of the running balance for each customer
```sql
with tn as(
SELECT customer_id, txn_date,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -1 * txn_amount END) AS total_amount
FROM customer_transactions
GROUP BY customer_id, txn_date
order by customer_id
)
SELECT distinct customer_id, 
max(total_amount) over (partition by customer_id) as max_amount,
min(total_amount) over (partition by customer_id) as min_amount,
avg(total_amount) over (partition by customer_id) as avg_amount
from tn;
```
