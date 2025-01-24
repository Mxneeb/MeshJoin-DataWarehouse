package Muneeb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class dataExport {

    public static void main(String[] args) {
        // Database connection details
        String url = "jdbc:mysql://localhost:3306/salesDB";
        String username = "root";
        String password = "12345678";

        // File paths
        String productsFile = "D:/workspace-1/DataWarehouse/Muneeb/products_data.csv";
        String customersFile = "D:/workspace-1/DataWarehouse/Muneeb/customers_data.csv";

        Connection connection = null;

        try {
            // Establish database connection
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to the database successfully!");

            // Insert data into PRODUCTS table
            insertProductsData(connection, productsFile);

            // Insert data into CUSTOMERS table
            insertCustomersData(connection, customersFile);

        } catch (SQLException e) {
            System.out.println("Database connection error:");
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                    System.out.println("Connection closed.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

 // Method to insert data into PRODUCTS table
    public static void insertProductsData(Connection connection, String filePath) {
        String query = "INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = connection.prepareStatement(query)) {

            String line;
            boolean isFirstLine = true; // Flag to skip the header

            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue; // Skip the header line
                }

                System.out.println("Processing line: " + line); // Debug print

                String[] values = line.split(",");
                if (values.length < 7) {
                    System.out.println("Skipping malformed line: " + line);
                    continue; // Skip lines with fewer columns than expected
                }

                try {
                    pstmt.setInt(1, Integer.parseInt(values[0])); // PRODUCT_ID
                    pstmt.setString(2, values[1]);               // PRODUCT_NAME

                    // Clean the price string by removing "$" or other non-numeric characters
                    String cleanPrice = values[2].replace("$", "").trim();
                    pstmt.setBigDecimal(3, new java.math.BigDecimal(cleanPrice)); // PRODUCT_PRICE

                    pstmt.setInt(4, Integer.parseInt(values[3])); // SUPPLIER_ID
                    pstmt.setString(5, values[4]);               // SUPPLIER_NAME
                    pstmt.setInt(6, Integer.parseInt(values[5])); // STORE_ID
                    pstmt.setString(7, values[6]);               // STORE_NAME

                    pstmt.addBatch();
                } catch (NumberFormatException e) {
                    System.out.println("Skipping line due to number format error: " + line);
                    e.printStackTrace();
                }
            }

            pstmt.executeBatch();
            System.out.println("Data inserted into PRODUCTS table successfully!");

        } catch (IOException | SQLException e) {
            System.out.println("Error inserting data into PRODUCTS table:");
            e.printStackTrace();
        }
    }
 // Method to insert data into CUSTOMERS table
    public static void insertCustomersData(Connection connection, String filePath) {
        String query = "INSERT IGNORE INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, GENDER) VALUES (?, ?, ?)";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = connection.prepareStatement(query)) {

            String line;
            boolean isFirstLine = true; // Flag to skip the header

            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue; // Skip the header line
                }

                System.out.println("Processing line: " + line); // Debug print

                String[] values = line.split(",");
                if (values.length < 3) {
                    System.out.println("Skipping malformed line: " + line);
                    continue; // Skip lines with fewer columns than expected
                }

                try {
                    pstmt.setInt(1, Integer.parseInt(values[0])); // CUSTOMER_ID
                    pstmt.setString(2, values[1]);               // CUSTOMER_NAME

                    // Process GENDER to ensure it is a single character
                    String gender = values[2].trim();
                    if (gender.length() > 1) {
                        gender = String.valueOf(gender.charAt(0)).toUpperCase(); // Take first character
                    }
                    pstmt.setString(3, gender);

                    pstmt.addBatch();
                } catch (NumberFormatException e) {
                    System.out.println("Skipping line due to number format error: " + line);
                    e.printStackTrace();
                }
            }

            pstmt.executeBatch();
            System.out.println("Data inserted into CUSTOMERS table successfully!");

        } catch (IOException | SQLException e) {
            System.out.println("Error inserting data into CUSTOMERS table:");
            e.printStackTrace();
        }
    }
}
