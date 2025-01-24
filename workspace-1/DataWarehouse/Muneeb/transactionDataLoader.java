package Muneeb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

class Transaction {
    int orderId;
    String orderDate; 
    int productId;
    int quantityOrdered;
    int customerId;
    int timeId;

    Transaction(int orderId, String orderDate, int productId, int quantityOrdered, int customerId, int timeId) {
        this.orderId = orderId;
        this.orderDate = orderDate.split(" ")[0];
        this.productId = productId;
        this.quantityOrdered = quantityOrdered;
        this.customerId = customerId;
        this.timeId = timeId;
    }
}

public class transactionDataLoader {

    private static final BlockingQueue<List<Transaction>> transactionQueue = new LinkedBlockingQueue<>();
    private static final Queue<List<Map<String, Object>>> productMetadataQueue = new LinkedList<>();
    private static final Queue<List<Map<String, Object>>> customerMetadataQueue = new LinkedList<>();
    private static final Map<Integer, String[]> hashTable = new HashMap<>();

    public static void main(String[] args) {
    	Thread producerThread = new Thread(() -> {
    	    try (BufferedReader reader = new BufferedReader(new FileReader("D:\\workspace-1\\DataWarehouse\\Muneeb\\transactions_data.csv"))) {
    	        String line;
    	        boolean isFirstLine = true;
    	        List<Transaction> chunk = new ArrayList<>();
    	        Map<String, Integer> dateToTimeId = new HashMap<>();
    	        AtomicInteger timeIdCounter = new AtomicInteger(1);
    	        int timeId = timeIdCounter.getAndIncrement(); // Safely increment

    	        while ((line = reader.readLine()) != null) {
    	            if (isFirstLine) {
    	                isFirstLine = false;
    	                continue; // Skip the header
    	            }

    	            try {
    	                String[] values = line.split(",");
    	                if (values.length < 5) {
    	                    System.err.printf("Skipping malformed row (missing columns): %s%n", line);
    	                    continue; // Skip rows with fewer than 5 essential columns
    	                }

    	                // Extract and sanitize the date field
    	                String orderDate = values[1].split(" ")[0].trim(); // Retain only the date part
    	                int orderId = Integer.parseInt(values[0].trim());
    	                int productId = Integer.parseInt(values[2].trim());
    	                int quantityOrdered = Integer.parseInt(values[3].trim());
    	                int customerId = Integer.parseInt(values[4].trim());

    	                // Assign a `time_id` based on `orderDate`
    	                
    	                if (values.length > 5) {
    	                    timeId = Integer.parseInt(values[5].trim());
    	                } else {
    	                    timeId = dateToTimeId.computeIfAbsent(orderDate, k ->timeIdCounter.getAndIncrement());
    	                }

    	                // Create a transaction object and add to the chunk
    	                Transaction transaction = new Transaction(orderId, orderDate, productId, quantityOrdered, customerId, timeId);
    	                chunk.add(transaction);

    	                if (chunk.size() == 21) { // Process in chunks of 21 rows
    	                    transactionQueue.put(new ArrayList<>(chunk));
    	                    chunk.clear();
    	                }
    	            } catch (Exception e) {
    	                System.err.printf("Skipping malformed row (parsing error): %s%n", line);
    	                e.printStackTrace();
    	            }
    	        }

    	        // Add the last chunk if any
    	        if (!chunk.isEmpty()) {
    	            transactionQueue.put(new ArrayList<>(chunk));
    	        }

    	        // End of stream marker
    	        transactionQueue.put(Collections.emptyList());
    	    } catch (Exception e) {
    	        e.printStackTrace();
    	    }
    	});
    	Thread consumerThread = new Thread(() -> {
    	    try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/SalesDB", "root", "12345678")) {
    	        conn.setAutoCommit(false); // Use transaction control for batch inserts
    	        loadMetadataBuffers(); // Ensure metadata buffers are preloaded

    	        while (true) {
    	            List<Transaction> transactionChunk = transactionQueue.take();
    	            if (transactionChunk.isEmpty()) break; // End of stream

    	            for (int i = 0; i < 10; i++) { // Rotate through metadata chunks
    	                List<Map<String, Object>> productChunk = productMetadataQueue.poll();
    	                List<Map<String, Object>> customerChunk = customerMetadataQueue.poll();

    	                if (productChunk == null || customerChunk == null) {
    	                    System.err.println("Error: Metadata chunk is null. Skipping iteration.");
    	                    continue;
    	                }

    	                for (Transaction transaction : transactionChunk) {
    	                    // Insert into Date Dimension
    	                    try (PreparedStatement dateDimStmt = conn.prepareStatement(
    	                            "INSERT IGNORE INTO Date_Dimension (Date_ID, Order_Date, Day_Of_Week, Month, Year, Quarter, Weekday_Weekend) " +
    	                            "VALUES (?, ?, ?, ?, ?, ?, ?)")) {

    	                        // Decompose the orderDate into components
    	                        LocalDate date = LocalDate.parse(transaction.orderDate);
    	                        int year = date.getYear();
    	                        int month = date.getMonthValue();
    	                        int dayOfWeek = date.getDayOfWeek().getValue(); // Monday = 1, Sunday = 7
    	                        int quarter = (month - 1) / 3 + 1; // Calculate the quarter
    	                        int weekdayWeekend = (dayOfWeek == 6 || dayOfWeek == 7) ? 0 : 1; // Weekend = 0, Weekday = 1
    	                        String dayOfWeekName = date.getDayOfWeek().toString();
    	                        String monthName = date.getMonth().toString();

    	                        // Populate the Date Dimension statement
    	                        dateDimStmt.setInt(1, date.hashCode()); // Use hashCode of date as unique Date_ID
    	                        dateDimStmt.setString(2, transaction.orderDate);
    	                        dateDimStmt.setString(3, dayOfWeekName);
    	                        dateDimStmt.setString(4, monthName);
    	                        dateDimStmt.setInt(5, year);
    	                        dateDimStmt.setInt(6, quarter);
    	                        dateDimStmt.setInt(7, weekdayWeekend);

    	                        dateDimStmt.execute(); // Execute the insert
    	                    }

    	                    for (Map<String, Object> product : productChunk) {
    	                        for (Map<String, Object> customer : customerChunk) {
    	                            if (transaction.productId == (int) product.get("Product_ID") &&
    	                                    transaction.customerId == (int) customer.get("Customer_ID")) {

    	                                BigDecimal productPrice = new BigDecimal(product.get("ProductPrice").toString());
    	                                BigDecimal totalSale = productPrice.multiply(BigDecimal.valueOf(transaction.quantityOrdered));

    	                                // Insert data into the Data Warehouse tables
    	                                try (PreparedStatement productDimStmt = conn.prepareStatement(
    	                                        "INSERT IGNORE INTO Product_Dimension (Product_ID, ProductName, ProductPrice) VALUES (?, ?, ?)");
    	                                     PreparedStatement customerDimStmt = conn.prepareStatement(
    	                                        "INSERT IGNORE INTO Customer_Dimension (Customer_ID, Customer_Name, Gender) VALUES (?, ?, ?)");
    	                                     PreparedStatement storeDimStmt = conn.prepareStatement(
    	                                        "INSERT IGNORE INTO Store_Dimension (Store_ID, StoreName) VALUES (?, ?)");
    	                                     PreparedStatement supplierDimStmt = conn.prepareStatement(
    	                                        "INSERT IGNORE INTO Supplier_Dimension (Supplier_ID, SupplierName) VALUES (?, ?)");
    	                                     PreparedStatement salesFactStmt = conn.prepareStatement(
    	                                            "INSERT IGNORE INTO Sales_Fact (Order_ID, Product_ID, Customer_ID, Store_ID, Supplier_ID, Quantity_Ordered, Total_Sale, Order_Date) " +
    	                                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {

    	                                    // Populate Product Dimension
    	                                    productDimStmt.setInt(1, (int) product.get("Product_ID"));
    	                                    productDimStmt.setString(2, product.get("ProductName").toString());
    	                                    productDimStmt.setBigDecimal(3, productPrice);
    	                                    productDimStmt.execute();

    	                                    // Populate Customer Dimension
    	                                    customerDimStmt.setInt(1, (int) customer.get("Customer_ID"));
    	                                    customerDimStmt.setString(2, customer.get("Customer_Name").toString());
    	                                    customerDimStmt.setString(3, customer.get("Gender").toString());
    	                                    customerDimStmt.execute();

    	                                    // Populate Store Dimension
    	                                    storeDimStmt.setInt(1, (int) product.get("Store_ID"));
    	                                    storeDimStmt.setString(2, product.get("Store_Name").toString());
    	                                    storeDimStmt.execute();

    	                                    // Populate Supplier Dimension
    	                                    supplierDimStmt.setInt(1, (int) product.get("Supplier_ID"));
    	                                    supplierDimStmt.setString(2, product.get("Supplier_Name").toString());
    	                                    supplierDimStmt.execute();

    	                                    // Populate Sales Fact
    	                                    salesFactStmt.setInt(1, transaction.orderId);
    	                                    salesFactStmt.setInt(2, transaction.productId);
    	                                    salesFactStmt.setInt(3, transaction.customerId);
    	                                    salesFactStmt.setInt(4, (int) product.get("Store_ID"));
    	                                    salesFactStmt.setInt(5, (int) product.get("Supplier_ID"));
    	                                    salesFactStmt.setInt(6, transaction.quantityOrdered);
    	                                    salesFactStmt.setBigDecimal(7, totalSale);
    	                                    salesFactStmt.setString(8, transaction.orderDate);
    	                                    salesFactStmt.execute();
    	                                }

    	                                // printing on terminal to check data
    	                                String output = String.format(
    	                                        "Processed Transaction: Order ID = %d | Order Date = %s | Product ID = %d | Quantity Ordered = %d | Customer ID = %d | Total Sale = %.2f%n",
    	                                        transaction.orderId, transaction.orderDate, transaction.productId, transaction.quantityOrdered, transaction.customerId, totalSale);
    	                                System.out.println(output);

    	                                // Save transaction info to the hash table for possible later use
    	                                hashTable.put(transaction.orderId, new String[]{
    	                                        customer.get("Customer_Name").toString(),
    	                                        product.get("ProductName").toString(),
    	                                        productPrice.toString(),
    	                                        totalSale.toString()
    	                                });
    	                            }
    	                        }
    	                    }
    	                }

    	                // Rotate metadata buffers back
    	                productMetadataQueue.add(productChunk);
    	                customerMetadataQueue.add(customerChunk);
    	            }

    	            conn.commit(); // Commit transaction batch
    	        }
    	    } catch (Exception e) {
    	        e.printStackTrace();
    	    }
    	});

        producerThread.start();
        consumerThread.start();

        try {
            producerThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void loadMetadataBuffers() {
        try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/SalesDB", "root", "12345678")) {
            Statement stmt = conn.createStatement();

            ResultSet rsProduct = stmt.executeQuery("SELECT * FROM PRODUCTS");
            List<Map<String, Object>> productChunk = new ArrayList<>();
            int productCount = 0;

            while (rsProduct.next()) {
                Map<String, Object> product = new HashMap<>();
                product.put("Product_ID", rsProduct.getInt("PRODUCT_ID"));
                product.put("ProductName", rsProduct.getString("PRODUCT_NAME"));
                product.put("ProductPrice", rsProduct.getBigDecimal("PRODUCT_PRICE"));
                product.put("Supplier_ID", rsProduct.getInt("SUPPLIER_ID"));
                product.put("Supplier_Name", rsProduct.getString("SUPPLIER_NAME"));
                product.put("Store_ID", rsProduct.getInt("STORE_ID"));
                product.put("Store_Name", rsProduct.getString("STORE_NAME"));
                productChunk.add(product);

                if (++productCount == 21) {
                    productMetadataQueue.add(new ArrayList<>(productChunk));
                    productChunk.clear();
                    productCount = 0;
                }
            }
            if (!productChunk.isEmpty()) {
                productMetadataQueue.add(productChunk);
            }

            ResultSet rsCustomer = stmt.executeQuery("SELECT * FROM CUSTOMERS");
            List<Map<String, Object>> customerChunk = new ArrayList<>();
            int customerCount = 0;

            while (rsCustomer.next()) {
                Map<String, Object> customer = new HashMap<>();
                customer.put("Customer_ID", rsCustomer.getInt("CUSTOMER_ID"));
                customer.put("Customer_Name", rsCustomer.getString("CUSTOMER_NAME"));
                customer.put("Gender", rsCustomer.getString("GENDER"));
                customerChunk.add(customer);

                if (++customerCount == 21) {
                    customerMetadataQueue.add(new ArrayList<>(customerChunk));
                    customerChunk.clear();
                    customerCount = 0;
                }
            }
            if (!customerChunk.isEmpty()) {
                customerMetadataQueue.add(customerChunk);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}