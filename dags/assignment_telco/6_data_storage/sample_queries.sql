-- 1. Check first 10 records with engineered features
SELECT customerID, tenure, MonthlyCharges, TotalCharges, 
       AvgChargesPerMonth, ExtraCharges, LifetimeValue, Tenure_Charges_Interaction
FROM processed_data
LIMIT 10;


-- 2. Average lifetime value of churned vs non-churned customers
SELECT Churn, 
       ROUND(AVG(LifetimeValue), 2) AS avg_lifetime_value
FROM processed_data
GROUP BY Churn;


-- 3. Top 10 customers with highest extra charges
SELECT customerID, tenure, MonthlyCharges, TotalCharges, ExtraCharges
FROM processed_data
ORDER BY ExtraCharges DESC
LIMIT 10;


-- 4. Relationship between tenure groups and average charges
SELECT 
    CASE 
        WHEN tenure BETWEEN 0 AND 12 THEN '0-12 months'
        WHEN tenure BETWEEN 13 AND 24 THEN '13-24 months'
        WHEN tenure BETWEEN 25 AND 48 THEN '25-48 months'
        ELSE '49+ months'
    END AS tenure_group,
    ROUND(AVG(AvgChargesPerMonth), 2) AS avg_monthly_spend
FROM processed_data
GROUP BY tenure_group
ORDER BY tenure_group;


-- 5. Customers with highest predicted lifetime value
SELECT customerID, tenure, MonthlyCharges, LifetimeValue
FROM processed_data
ORDER BY LifetimeValue DESC
LIMIT 10;
