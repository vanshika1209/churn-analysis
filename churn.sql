CREATE TABLE customer_churn (
    customer_id VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(10),
    age INT,
    married VARCHAR(5),
    number_of_dependents INT,
    city VARCHAR(100),
    zip_code VARCHAR(20),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    number_of_referrals INT,
    tenure_in_months INT,
    offer VARCHAR(20),

    phone_service VARCHAR(5),
    avg_monthly_long_distance_charges DECIMAL(10,2),
    multiple_lines VARCHAR(5),

    internet_service VARCHAR(5),
    internet_type VARCHAR(20),
    avg_monthly_gb_download DECIMAL(10,2),

    online_security VARCHAR(5),
    online_backup VARCHAR(5),
    device_protection_plan VARCHAR(5),
    premium_tech_support VARCHAR(5),

    streaming_tv VARCHAR(5),
    streaming_movies VARCHAR(5),
    streaming_music VARCHAR(5),
    unlimited_data VARCHAR(5),

    contract VARCHAR(20),
    paperless_billing VARCHAR(5),
    payment_method VARCHAR(30),

    monthly_charge DECIMAL(10,2),
    total_charges DECIMAL(12,2),
    total_refunds DECIMAL(12,2),
    total_extra_data_charges DECIMAL(12,2),
    total_long_distance_charges DECIMAL(12,2),
    total_revenue DECIMAL(12,2),

    customer_status VARCHAR(20),
    churn_category VARCHAR(50),
    churn_reason TEXT
);

CREATE TABLE zip_code_population (
    zip_code varchar(20) PRIMARY KEY,
    population INT
);

CREATE TABLE churn_clean AS
SELECT *
FROM customer_churn;

SELECT COUNT(*) FROM churn_clean;--data size
SELECT * FROM churn_clean LIMIT 10; --column structure


--check missing value
SELECT 
    COUNT(*) FILTER (WHERE "total_charges" IS NULL) AS total_charges_nulls,
    COUNT(*) FILTER (WHERE "monthly_charge" IS NULL) AS monthly_charge_nulls
FROM churn_clean;


--remove duplicates if exists
SELECT customer_id, COUNT(*)
FROM churn_clean
GROUP BY customer_id
HAVING COUNT(*) > 1;


--create churn flag
ALTER TABLE churn_clean
ADD COLUMN churn_flag INT;

UPDATE churn_clean
SET churn_flag = CASE 
    WHEN "customer_status" = 'Churned' THEN 1
    ELSE 0
END;


--create tenure group
ALTER TABLE churn_clean
ADD COLUMN tenure_group TEXT;

UPDATE churn_clean
SET tenure_group = CASE
    WHEN "tenure_in_months" <= 6 THEN '0-6 months'
    WHEN "tenure_in_months" <= 12 THEN '6-12 months'
    WHEN "tenure_in_months" <= 24 THEN '1-2 years'
    ELSE '2+ years'
END;


--monthly revenue approximation
ALTER TABLE churn_clean
ADD COLUMN avg_revenue_per_month NUMERIC;

UPDATE churn_clean
SET avg_revenue_per_month = 
    "total_revenue" / NULLIF("tenure_in_months", 0);


--population table
CREATE TABLE churn_final AS
SELECT c.*, z.Population
FROM churn_clean c
LEFT JOIN zip_code_population z
ON c."zip_code" = z."zip_code";

--churn_flag = 1 → Churned  
--churn_flag = 0 → Stayed / Joined


--overall churn rate
SELECT 
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_clean;


--churn by contract type
SELECT 
    contract,
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (WHERE churn_flag = 1) AS churned_customers,
    ROUND(
        COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*), 
        2
    ) AS churn_rate
FROM churn_clean
GROUP BY contract
ORDER BY churn_rate DESC;


--churn rate by payment method
SELECT 
    payment_method,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_clean
GROUP BY payment_method
ORDER BY churn_rate DESC;


--churn rate by tenure group
SELECT 
    tenure_group,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_clean
GROUP BY tenure_group
ORDER BY churn_rate DESC;


--churn rate by internet type
SELECT 
    CASE 
        WHEN internet_service = 'No' THEN 'No Internet'
        WHEN internet_type IS NULL THEN 'Unknown'
        ELSE internet_type
    END AS internet_category,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_clean
GROUP BY internet_category
ORDER BY churn_rate DESC;


--behavioral analysis of churn
SELECT 
    streaming_tv,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_clean
GROUP BY streaming_tv;


--revenue loss due to churn
SELECT 
    SUM(total_revenue) AS total_revenue,
    SUM(CASE WHEN churn_flag = 1 THEN total_revenue ELSE 0 END) AS churned_revenue
FROM churn_clean;


--churn reasons
SELECT 
    churn_category,
    COUNT(*) AS total_churned
FROM churn_clean
WHERE churn_flag = 1
GROUP BY churn_category
ORDER BY total_churned DESC;

SELECT 
    churn_reason,
    COUNT(*) AS total
FROM churn_clean
WHERE churn_flag = 1
GROUP BY churn_reason
ORDER BY total DESC
LIMIT 10;


--population based churn
SELECT 
    CASE 
        WHEN population > 100000 THEN 'High Density'
        WHEN population > 50000 THEN 'Medium Density'
        ELSE 'Low Density'
    END AS population_segment,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_final
GROUP BY population_segment;


--rank customers by revenue
WITH ranked_customers AS (
    SELECT 
        customer_id,
        total_revenue,
        churn_flag,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM churn_clean
)
SELECT *
FROM ranked_customers
WHERE revenue_rank <= 10;


--churn rate by revenue percentile
WITH customer_segments AS (
    SELECT 
        customer_id,
        churn_flag,
        NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile
    FROM churn_clean
)
SELECT 
    revenue_quartile,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM customer_segments
GROUP BY revenue_quartile
ORDER BY revenue_quartile;


--multifactor analysis
SELECT 
    contract,
    internet_type,
    premium_tech_support,
    COUNT(*) FILTER (WHERE churn_flag = 1) * 100.0 / COUNT(*) AS churn_rate
FROM churn_clean
GROUP BY contract, internet_type, premium_tech_support
ORDER BY churn_rate DESC
LIMIT 10;

