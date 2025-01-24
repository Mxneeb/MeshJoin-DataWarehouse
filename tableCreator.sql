drop database SalesDB;
CREATE DATABASE SalesDB;
USE SalesDB;

-- DROP TABLE IF EXISTS customers;
-- DROP TABLE IF EXISTS products;
-- Create PRODUCTS table
CREATE TABLE PRODUCTS (
    PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME VARCHAR(100),
    PRODUCT_PRICE DECIMAL(10, 2),
    SUPPLIER_ID INT,
    SUPPLIER_NAME VARCHAR(100),
    STORE_ID INT,
    STORE_NAME VARCHAR(100)
);

-- Create CUSTOMERS table
CREATE TABLE CUSTOMERS (
    CUSTOMER_ID INT PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(100),
    GENDER CHAR(1)  -- Use 'M' for Male, 'F' for Female, etc.
);

-- Drop the Fact Table first
-- DROP TABLE IF EXISTS Sales_Fact;

-- Then drop the Dimension Tables
-- DROP TABLE IF EXISTS Product_Dimension;
-- DROP TABLE IF EXISTS Customer_Dimension;
-- DROP TABLE IF EXISTS Supplier_Dimension;
-- DROP TABLE IF EXISTS Store_Dimension;
-- DROP TABLE IF EXISTS Date_Dimension;


-- Creating Dimension Tables
-- Product Dimension
CREATE TABLE Product_Dimension (
    Product_ID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductPrice DECIMAL(10, 2)
);

-- Customer Dimension
CREATE TABLE Customer_Dimension (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(255),
    Gender CHAR(1)
);

-- Supplier Dimension
CREATE TABLE Supplier_Dimension (
    Supplier_ID INT PRIMARY KEY,
    SupplierName VARCHAR(255)
);

-- Store Dimension
CREATE TABLE Store_Dimension (
    Store_ID INT PRIMARY KEY,
    StoreName VARCHAR(255)
);

-- Optional Date Dimension for advanced analysis
CREATE TABLE Date_Dimension (
    Date_ID INT PRIMARY KEY,
    Order_Date DATE,
    Day_Of_Week VARCHAR(10),
    Month VARCHAR(10),
    Year INT,
    Quarter INT,
    Weekday_Weekend INT
);

-- Creating the Fact Table with Supplier_ID added
CREATE TABLE Sales_Fact (
    Order_ID INT PRIMARY KEY,
    Product_ID INT,
    Customer_ID INT,
    Store_ID INT,
    Supplier_ID INT, 
    Quantity_Ordered INT,
    Total_Sale DECIMAL(10, 2),
    Order_Date DATE,
    FOREIGN KEY (Product_ID) REFERENCES Product_Dimension(Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customer_Dimension(Customer_ID),
    FOREIGN KEY (Store_ID) REFERENCES Store_Dimension(Store_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier_Dimension(Supplier_ID)  -- Ensuring reference integrity
);
-- Q1
SELECT
    DATE_FORMAT(s.Order_Date, '%Y-%m') AS Month,
    CASE
        WHEN DAYOFWEEK(s.Order_Date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS Day_Type,
    p.ProductName,
    SUM(s.Total_Sale) AS Total_Revenue
FROM
    Sales_Fact s
JOIN Product_Dimension p ON s.Product_ID = p.Product_ID
JOIN Date_Dimension d ON s.Order_Date = d.Order_Date
WHERE
    YEAR(s.Order_Date) = 2019
GROUP BY
    DATE_FORMAT(s.Order_Date, '%Y-%m'), Day_Type, p.ProductName
ORDER BY
    Month, Day_Type, Total_Revenue DESC
LIMIT 5;


-- Q2
SELECT 
    store.Store_ID,
    store.StoreName,
    QUARTER(s.Order_Date) AS Quarter,
    SUM(s.Total_Sale) AS Total_Sales,
    (SUM(s.Total_Sale) - LAG(SUM(s.Total_Sale)) OVER (PARTITION BY store.Store_ID ORDER BY QUARTER(s.Order_Date))) / 
    LAG(SUM(s.Total_Sale)) OVER (PARTITION BY store.Store_ID ORDER BY QUARTER(s.Order_Date)) * 100 AS Growth_Rate
FROM 
    Sales_Fact s
JOIN Store_Dimension store ON s.Store_ID = store.Store_ID
WHERE 
    YEAR(s.Order_Date) = 2019
GROUP BY 
    store.Store_ID, store.StoreName, QUARTER(s.Order_Date)
ORDER BY 
    store.Store_ID, Quarter;
    
-- Q3
SELECT 
    st.StoreName,
    su.SupplierName,
    p.ProductName,
    SUM(s.Total_Sale) AS Total_Sales
FROM 
    Sales_Fact s
JOIN Store_Dimension st ON s.Store_ID = st.Store_ID
JOIN Supplier_Dimension su ON s.Supplier_ID = su.Supplier_ID
JOIN Product_Dimension p ON s.Product_ID = p.Product_ID
GROUP BY 
    st.StoreName, su.SupplierName, p.ProductName
ORDER BY 
    st.StoreName, su.SupplierName, p.ProductName;
    
-- Q4
SELECT 
    p.ProductName,
    CASE
        WHEN MONTH(s.Order_Date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(s.Order_Date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(s.Order_Date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS Season,
    SUM(s.Total_Sale) AS Total_Sales
FROM 
    Sales_Fact s
JOIN Product_Dimension p ON s.Product_ID = p.Product_ID
GROUP BY 
    p.ProductName, Season
ORDER BY 
    p.ProductName, Season;
    
-- Q5
WITH Monthly_Revenue AS (
    -- Step 1: Calculate total revenue for each store and supplier pair by month
    SELECT
        sf.Store_ID,
        sf.Supplier_ID,
        EXTRACT(YEAR FROM sf.Order_Date) AS Year,
        EXTRACT(MONTH FROM sf.Order_Date) AS Month,
        SUM(sf.Total_Sale) AS Total_Revenue
    FROM
        Sales_Fact sf
    GROUP BY
        sf.Store_ID,
        sf.Supplier_ID,
        EXTRACT(YEAR FROM sf.Order_Date),
        EXTRACT(MONTH FROM sf.Order_Date)
),
Revenue_Change AS (
    -- Step 2: Calculate the month-to-month percentage change in revenue
    SELECT
        sr.Store_ID,
        sr.Supplier_ID,
        sr.Year,
        sr.Month,
        sr.Total_Revenue,
        LAG(sr.Total_Revenue) OVER (PARTITION BY sr.Store_ID, sr.Supplier_ID ORDER BY sr.Year, sr.Month) AS Prev_Month_Revenue
    FROM
        Monthly_Revenue sr
)
SELECT
    rc.Store_ID,
    rc.Supplier_ID,
    rc.Year,
    rc.Month,
    rc.Total_Revenue,
    rc.Prev_Month_Revenue,
    CASE
        WHEN rc.Prev_Month_Revenue IS NULL THEN NULL
        ELSE ROUND((rc.Total_Revenue - rc.Prev_Month_Revenue) / rc.Prev_Month_Revenue * 100, 2)
    END AS Revenue_Volatility_Percentage
FROM
    Revenue_Change rc
ORDER BY
    rc.Store_ID, rc.Supplier_ID, rc.Year, rc.Month;

-- Q6
SELECT 
    p1.ProductName AS Product_1,
    p2.ProductName AS Product_2,
    COUNT(*) AS Purchase_Count
FROM 
    Sales_Fact s1
JOIN Sales_Fact s2 ON s1.Order_ID = s2.Order_ID AND s1.Product_ID != s2.Product_ID
JOIN Product_Dimension p1 ON s1.Product_ID = p1.Product_ID
JOIN Product_Dimension p2 ON s2.Product_ID = p2.Product_ID
GROUP BY 
    p1.ProductName, p2.ProductName
ORDER BY 
    Purchase_Count DESC
LIMIT 5;

-- Q7
SELECT 
    st.StoreName,
    su.SupplierName,
    p.ProductName,
    SUM(s.Total_Sale) AS Total_Sales
FROM 
    Sales_Fact s
JOIN Store_Dimension st ON s.Store_ID = st.Store_ID
JOIN Supplier_Dimension su ON s.Supplier_ID = su.Supplier_ID
JOIN Product_Dimension p ON s.Product_ID = p.Product_ID
GROUP BY 
    st.StoreName, su.SupplierName, p.ProductName
WITH ROLLUP
ORDER BY 
    st.StoreName, su.SupplierName, p.ProductName;
    
-- Q8
SELECT 
    p.ProductName,
    SUM(CASE WHEN MONTH(s.Order_Date) <= 6 THEN s.Total_Sale ELSE 0 END) AS H1_Revenue,
    SUM(CASE WHEN MONTH(s.Order_Date) > 6 THEN s.Total_Sale ELSE 0 END) AS H2_Revenue,
    SUM(CASE WHEN MONTH(s.Order_Date) <= 6 THEN s.Quantity_Ordered ELSE 0 END) AS H1_Quantity,
    SUM(CASE WHEN MONTH(s.Order_Date) > 6 THEN s.Quantity_Ordered ELSE 0 END) AS H2_Quantity,
    SUM(s.Total_Sale) AS Yearly_Revenue,
    SUM(s.Quantity_Ordered) AS Yearly_Quantity
FROM 
    Sales_Fact s
JOIN Product_Dimension p ON s.Product_ID = p.Product_ID
GROUP BY 
    p.ProductName
ORDER BY 
    p.ProductName;
    
-- Q9
WITH Daily_Product_Sales AS (
    -- Step 1: Calculate daily total sales for each product
    SELECT
        sf.Product_ID,
        sf.Order_Date,
        SUM(sf.Total_Sale) AS Daily_Sales
    FROM
        Sales_Fact sf
    GROUP BY
        sf.Product_ID,
        sf.Order_Date
),
Product_Averages AS (
    -- Step 2: Calculate the daily average sales for each product
    SELECT
        Product_ID,
        AVG(Daily_Sales) AS Avg_Daily_Sales
    FROM
        Daily_Product_Sales
    GROUP BY
        Product_ID
)
SELECT
    dps.Product_ID,
    dps.Order_Date,
    dps.Daily_Sales,
    pa.Avg_Daily_Sales,
    CASE 
        WHEN dps.Daily_Sales > 2 * pa.Avg_Daily_Sales THEN 'Spike'
        ELSE 'Normal'
    END AS Sales_Flag
FROM
    Daily_Product_Sales dps
JOIN
    Product_Averages pa
    ON dps.Product_ID = pa.Product_ID
ORDER BY
    dps.Product_ID, dps.Order_Date;

-- Q10
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    st.StoreName,
    QUARTER(s.Order_Date) AS Quarter,
    SUM(s.Total_Sale) AS Total_Sales
FROM 
    Sales_Fact s
JOIN Store_Dimension st ON s.Store_ID = st.Store_ID
GROUP BY 
    st.StoreName, QUARTER(s.Order_Date)
ORDER BY 
    st.StoreName, Quarter;
