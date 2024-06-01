/* Find out nth Order/Salary from the tables. */
SELECT * FROM (
  SELECT emp_name, emp_salary, DENSE_RANK() OVER (ORDER BY emp_salary DESC) AS r
  FROM emp
) AS subquery
WHERE r = 3;

/* Find the no of output records in each join from given Table 1 & Table 2 */
-- Left Join , Inner Join, Outer Join , Right Join

/*YOY,MOM Growth related questions. */

WITH monthly_metrics AS (
SELECT EXTRACT(year from day) as year,
	 EXTRACT(month from day) as month,
       SUM(revenue) as revenue
  FROM daily_metrics 
  GROUP BY 1,2
)
SELECT year AS current_year, 
       month AS current_month, 
       revenue AS revenue_current_month, 
       LAG(year,12) OVER ( ORDER BY year, month) AS previous_year, 
       LAG(month,12) OVER ( ORDER BY year, month) AS month_comparing_with,
       LAG(revenue,12) OVER ( ORDER BY year, month) AS revenue_12_months_ago,
       revenue - LAG(revenue,12) OVER (ORDER BY year, month) AS month_to_month_difference
FROM monthly_metrics
ORDER BY 1,2;
  
/* 4- Find out Employee ,Manager Hierarchy (Self join related question) or Employees who are earning more than managers. */

SELECT *
FROM employees w,
     employees m
WHERE w.manager_id = m.emp_id
  AND w.salary> m.salary;

Spark loads data into memory in the form of partitions. In an ideal world, this distribution of data among the partitions is uniform. 
But in real world, when one or more partitions have significantly more data compared to the others, is called data skewness
/*
6- Some row level scanning medium to complex questions using CTE or recursive CTE, like (Missing no /Missing Item from the list etc.)
7- No of matches played by every team or Source to Destination flight combination using CROSS JOIN.
8-Use window functions to perform advanced analytical tasks, such as calculating moving averages or detecting outliers.
9- Implement logic to handle hierarchical data, such as finding all descendants of a given node in a tree structure.
10-Identify and remove duplicate records from a table. */
