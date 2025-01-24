# **Data Warehouse for Sales Transactions with Mesh Join Algorithm**

## **1. Project Overview**
The project is to develop a Data Warehouse (DW) that will hold sales transactions about products, customers, and suppliers. Its objective is to leverage the Mesh Join algorithm so that huge datasets can be joined and processed properly for further analysis purposes.

### **Major Tasks**
- Designing schema for the Data Warehouse.
- Implementing the Mesh Join algorithm to load data into the DW.
- Processing data in chunks to optimize memory usage.
- Inserting the data into relevant dimension tables (Product, Customer, Store, Supplier, and Date) and the fact table (Sales_Fact).

The objective of the project is to focus on the efficient processing of transaction data comprising details of orders, products, customers, and suppliers, and then load it into the relational database using the Mesh Join algorithm.

---

## **2. Schema for Data Warehouse**
The schema consists of several dimension tables and a fact table:

### **Dimension Tables**
- **Product_Dimension**: `Product_ID, ProductName, ProductPrice`
- **Customer_Dimension**: `Customer_ID, Customer_Name, Gender`
- **Supplier_Dimension**: `Supplier_ID, SupplierName`
- **Store_Dimension**: `Store_ID, StoreName`
- **Date_Dimension**: `Date_ID, Order_Date, Day_Of_Week, Month, Year, Quarter, Weekday_Weekend`

### **Fact Table**
- **Sales_Fact**: `Order_ID, Product_ID, Customer_ID, Store_ID, Supplier_ID, Quantity_Ordered, Total_Sale, Order_Date`

---

## **3. Mesh Join Algorithm**
The Mesh Join algorithm efficiently processes and joins large datasets by handling them in smaller chunks and loading them into the Data Warehouse.

### **How It Works**
1. **Producer Thread**: Reads data from transaction CSV files in chunks, processes them, and puts the data into a blocking queue.
2. **Consumer Thread**: Receives transaction data from the queue and joins it with metadata (e.g., products, customers).
3. **Insert Data**: For every matched transaction, data is inserted into the appropriate dimension and fact tables.
4. **Concurrency**: Multiple chunks of metadata are processed in parallel, optimizing data loading.

The Mesh Join algorithm utilizes multi-threading along with blocking queues to optimize memory usage during the ETL (Extract, Transform, Load) process.

---

## **4. Shortcomings of Mesh Join**
While the Mesh Join algorithm is efficient, it has some limitations:

### **1. Memory Constraints**
- Processes large datasets in memory, requiring careful optimization of chunk sizes to avoid memory overflow or performance degradation.

### **2. Complexity in Handling Data Integrity**
- Foreign key relationships require careful management. Missing data (e.g., in product or customer files) can cause integrity issues and loading errors.

### **3. Scalability**
- Performance decreases with very large datasets due to parallel joins and limitations in database or application scalability.

---

## **5. What I Learned from the Project**
This project provided valuable insights into handling large datasets efficiently in a Data Warehouse and understanding how it differs from a traditional database.

### **Key Learnings**
- **Efficient Data Processing**: Learned how Mesh Join breaks data into chunks to improve performance and reduce memory usage.
- **Concurrency and Multi-Threading**: Gained hands-on experience with Java's multi-threading to optimize data processing through producer and consumer threads.
- **Database Design and Data Warehousing**: Learned to design a Star Schema for efficient querying and analysis in a Data Warehouse.
- **Optimizing ETL Processes**: Built an ETL pipeline to load data efficiently while handling errors during the data-loading process.

---

## **Conclusion**
This project enhanced my understanding of managing transactional data in a Data Warehouse environment, optimizing ETL workflows, and ensuring data integrity. It also provided in-depth experience with the Mesh Join algorithm to process large datasets efficiently using chunking and parallel processing.
