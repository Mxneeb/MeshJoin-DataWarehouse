# A Near-Real-Time Data Warehouse Prototype for METRO Shopping Store with MeshJoin Implimentation
1. Project Overview:
The project is to develop a DW that will be holding sales transactions about products, customers, and suppliers. Its objective is to leverage the Mesh Join algorithm so that huge datasets can be joined and processed properly for further analysis purposes. Major tasks in the project are:
•	Designing schema for Data Warehouse.
•	Implementing Mesh Join algorithm for load data into the DW.
•	Process data in chunks to optimize memory usage.
•	Inserting the data into relevant dimension tables (Product, Customer, Store, Supplier, and Date) and the fact table (Sales_Fact).
The objective of the project is to focus on the efficient processing of transaction data comprising details of orders, products, customers, and suppliers, and then load it into the relational database using the Mesh Join algorithm.
2. Schema for Data Warehouse:
The schema consists of several dimension tables and a fact table:
•	Dimension Tables:
o	Product_Dimension
o	Customer_Dimension
o	Supplier_Dimension
o	Store_Dimension
o	Date_Dimension
•	Fact Table:
o	Sales_Fact
Schema Diagram:
•	Product_Dimension (Product_ID, ProductName, ProductPrice)
•	Customer_Dimension (Customer_ID, Customer_Name, Gender)
•	Supplier_Dimension (Supplier_ID, SupplierName)
•	Store_Dimension (Store_ID, StoreName)
•	Date_Dimension (Date_ID, Order_Date, Day_Of_Week, Month, Year, Quarter, Weekday_Weekend)
•	Sales_Fact (Order_ID, Product_ID, Customer_ID, Store_ID, Supplier_ID, Quantity_Ordered, Total_Sale, Order_Date)
3. Mesh Join Algorithm:
1.	Mesh Join is used to efficiently join large datasets by processing them in smaller chunks and loading them into a Data Warehouse. The algorithm applies multiple queues for parallel processing of transaction data and metadata-product and customer details. Here's how the Mesh Join is applied:
2.	Producer Thread: Reads data from transaction CSV in chunks, processes them, and puts the data into a blocking queue.
3.	Consumer Thread: Receives transaction data from queue and joins it with metadata (products, customers).
4.	Insert Data: For every matched transaction, data gets inserted into appropriate dimension and fact tables.
5.	Concurrency: Many chunks of metadata get processed in parallel which optimizes data loading.
The Mesh Join uses multi-threading along with blocking queues to load data into a MySQL database in such a way that memory usage during the ETL (Extract, Transform, Load) is optimized.
4. Three Shortcomings in Mesh Join:
While the Mesh Join provides an efficient way to process and join data, it does have some cons which are discussed below:
1.	Memory Constraints:
o	Mesh Join processes large datasets in main memory which means that if the available memory is insufficient for the chunks, it could lead to performance degradation or worse, memory overflow. To be on the safe side, chunk sizes need to be optimized rather carefully.
2.	Complexity in Handling Data Integrity:
o	Handling foreign key relationships is very sensitive with the Mesh Join algorithm. Whilst using the algorithm for joining several tables, the foreign keys will need careful management. Loss of any data, such as missing data in a product or customer file could lead to data integrity inconsistency and eventually cause errors in loading.
3.	Scalability:
o	While Mesh Join is efficient for medium-sized datasets, it does get slower with size of data. The algorithm has to process several parallel joins, and poor scaling of the database server or the application could be a potential performance bottleneck.
5. What Did I Learn from the Project?
Throughout the implementation of this project, I gained valuable insights into how to handle data efficiently in a warehouse, moreover I learned the importance of a data warehouse that how it is different from a traditional database and its advantages.
Along with that I learned a lot about the meshjoin algorithm, how it manages to join large amounts of data efficiently using a technique called chunking. In the Journey I also got hands on experience with JAVA, which I had no prior experience with, but learned it along the way.



Some Key points that I learned are mentioned below:
•	Efficient Data Processing:
o	Mesh Join provided an efficient way to join and process large data by breaking down the data into smaller chunks. This approach improved performance and reduced memory usage.
•	Concurrency and Multi-Threading:
o	Implementing multi-threading and parallel processing in Java helped me understand java even further, moreover I learned the importance of concurrency in optimizing data processing. By dividing the workload between producer and consumer threads, I was able to efficiently handle large amounts of data.
•	Database Design and Data Warehousing:
o	I gained immense knowledge of designing a Data Warehouse schema with fact and dimension tables, using a particular type of schema (Star Schema). I learned the significance of properly structuring the schema to support efficient querying and analysis in a data warehouse.
•	Importance of Real-Time Data Insertion:
•	Optimizing ETL Processes:
o	The project provided hands-on experience with (ETL) process. I learned how to create the ETL pipeline to efficiently load the data into the Data Warehouse and handle errors during the data loading process.
Conclusion: This project helped me understand managing large amounts of transactional data, optimizing workflows throughout the project, and ensuring data integrity in a Data Warehouse environment. It also deepened my understanding of using the Mesh Join algorithm to optimize applications that rely heavily on large data sizes.

