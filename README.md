# Product Catalogue Using Hadoop Ecosystem 

# Project Overview 

This project involved building a robust and scalable data pipeline to collect, store, process, and analyze e-commerce product data from multiple online sources. The primary goal was to establish a resilient infrastructure for large-scale data handling and to transform raw, semi-structured data into a clean, structured format suitable for business intelligence and analysis. 

# Technologies Used 
The following technologies were integral to the project's implementation:

Operating System: Ubuntu Linux (for VMs) 

Virtualization/Containerization: Docker, Docker Swarm 

Distributed File System: Sparl Cluster HDFS 

Relational Database: MySQL 

Programming Language: Python 

Python Libraries: pandas, mysql-connector-python, hdfs 

Database Management Tool: MySQL Workbench 

Networking: Tailscale (for secure network communication between VMs and local machine) 

# Phase 1: Data Acquisition & Distributed Storage (HDFS) 

This phase focused on collecting the raw product data and setting up a fault-tolerant distributed storage system. 

# 1.1 Data Acquisition 

Raw product data was sourced from three major e-commerce platforms: Amazon, eBay, and Newegg. This data, typically including product titles/descriptions, URLs, prices, and image URLs, was initially obtained in CSV format. 

# 1.2 Hadoop Cluster Setup 

A multi-node Spark cluster was deployed to provide a scalable and fault-tolerant distributed file system (HDFS). The cluster architecture comprised: 

NameNode: The master node responsible for managing the file system namespace and regulating client access to files. 

DataNodes: Worker nodes that store the actual data blocks. 

Deployment Strategy: The Hadoop cluster file cluster.yml were deployed as Docker containers on multiple Ubuntu Virtual Machines (VMs). Docker Swarm was utilized for container orchestration, offering significant benefits: 

 

Deployment Strategy: The Hadoop cluster file cluster.yml were deployed on Docker swarm to  handles creating all the necessary services (e.g., NameNode, DataNodes, Spark Master, Spark Workers) on the various VMs in your cluster and providing significant benfits: 

 

Portability & Consistency: Ensured uniform execution environments across all VMs. 

Isolation: Prevented conflicts between services by running them in isolated containers. 

Simplified Deployment & Management: Automated the deployment, scaling, and management of Hadoop services across the cluster. 

High Availability & Self-Healing: Docker Swarm automatically monitored container health, restarting failed containers and rescheduling them on healthy nodes in case of a VM failure. 

# 1.3 HDFS Configuration & Replication 

The core HDFS behavior was configured via hdfs-site.xml. A critical modification involved setting the dfs.replication factor. 

Parameter Modified: dfs.replication 

Value Set: 4 (increased from the default, typically 3) 

Purpose: To enhance data redundancy and fault tolerance. By storing 4 copies of each data block across different DataNodes, the system's ability to withstand multiple node failures without data loss was significantly improved. 

Implementation: This configuration change was performed directly within the Hadoop container's file system, requiring precise navigation and modification of the XML configuration file. 

# 1.4 HDFS File Management & Redundancy Testing 

After configuring the replication factor, the collected CSV files were uploaded to HDFS. 

File Upload: The CSV files were uploaded to specific HDFS paths  

Redundancy Test: A test was conducted to validate the increased replication factor and HDFS's fault tolerance: 

Replication Verification: Confirmed that data blocks for the uploaded files were indeed replicated 4 times across distinct DataNodes using HDFS command-line tools. 

Simulated Failure: A worker DataNode VM was intentionally taken down by draining one of the nodes present in the cluster. 

Availability Test: A read operation was performed from the other available online nodes, which implies that data is replicated successfully. As for the write operation, a file was uploaded while a node was still down/drained. As soon as the node was up again, the uploaded file was automatically replicated to the earlier drained node. 

 

This phase successfully validated the resilience and high availability of the HDFS cluster, confirming its suitability for storing critical product data. 

# Phase 2: MySQL Database Design & Setup 

This phase involved setting up a relational database and designing a structured schema for the ingested product data. 

# 2.1 MySQL Database Environment 

A MySQL server was deployed as a Docker container on a dedicated Linux VM. This setup provided an isolated and easily manageable database environment. Connectivity from the local development machine (Windows) was established securely using Tailscale, and MySQL Workbench was used for database administration and querying. 

# 2.2 Database Schema Design 

A normalized relational database schema, named product_catalog, was designed to store the e-commerce data efficiently. The schema consists of five interconnected tables: 

brands: A lookup table storing unique brand names (brand_id as PRIMARY KEY, brand_name as UNIQUE). 

categories: A lookup table for product categories, allowing for hierarchical structures (category_id as PRIMARY KEY, category_name as UNIQUE). 

products: The core table for product information (product_id as PRIMARY KEY). 

Includes title, product_url (UNIQUE), brand_id (FOREIGN KEY to brands), category_id (FOREIGN KEY to categories), source_name (to identify the e-commerce platform), and last_updated_at. 

prices: Stores historical and current price data (price_id as PRIMARY KEY). 

Includes product_id (FOREIGN KEY to products), price_amount, currency, scrape_date, and is_current (flag for the latest price). 

images: Stores URLs for product images (image_id as PRIMARY KEY). 

Includes product_id (FOREIGN KEY to products), image_url (UNIQUE), and is_thumbnail. 

Data Refinement: We found problems while building the database, so we had to make some important changes to its design: 

 

product_url (in products) and image_url (in images) VARCHAR lengths were reduced from 1024 to 767 to comply with MySQL's InnoDB index length limits for utf8mb4 character sets. 

price_amount (in prices) data type was adjusted from DECIMAL(5, 2) to DECIMAL(10, 2) to accommodate larger price values without "out of range" errors. 

# Phase 3: Data Ingestion Pipeline (Python) 

A Python script was developed to automate the end-to-end data ingestion process, moving data from HDFS into the structured MySQL database. 

# 3.1 Script Functionality 

The Python script performs the following sequence of operations for each e-commerce data source: 

HDFS Download: Connects to the HDFS NameNode  and downloads the raw CSV file to a temporary local file. 

CSV Parsing & Column Mapping: Reads the downloaded CSV into a pandas DataFrame. It then applies a dynamic column_mapping specific to each source (Amazon, eBay, Newegg) to standardize varying column headers (e.g., Product_description to title, Item_URL to link) to match the MySQL schema. 

Data Cleaning & Transformation: 

Missing Value Handling: Converts NaN (Not a Number) values from pandas to Python None, which are then correctly inserted as NULL into MySQL, preventing "Unknown column 'nan'" errors. 

Price Parsing: Implements a robust regular expression-based logic to extract numerical price values from complex strings. This handles various formats, including currency symbols (₹, $), thousands separators (,, .), decimal separators (,, . for European formats), and price ranges (e.g., 17,50bis 17,73), ensuring only a clean float value is obtained or the price is skipped if unparseable. 

Brand Extraction: Infers brand names from product titles using keyword matching (e.g., "Acer", "HP", "Dell"). 

Category Extraction: Infers more granular categories (e.g., "Gaming Laptop", "Ultrabook", "2-in-1 Laptop") from product titles using keyword matching, defaulting to "Laptop" if no specific subcategory is found. 

Database Insertion Logic: 

Brands & Categories: Checks if a brand or category already exists in their respective lookup tables; if so, it uses the existing ID; otherwise, it inserts the new brand/category. 

Products: Uses product_url as a unique identifier. If a product with the same URL already exists, its title, brand_id, category_id, source_name, and last_updated_at are updated (an "upsert" like behavior). If new, the product is inserted. 

Prices: For each product, all previous prices are marked as is_current = FALSE, and the newly ingested price is inserted with is_current = TRUE, maintaining a historical price record. 

Images: Checks if an image_url already exists in the images table due to its UNIQUE constraint. If it exists, the insertion is skipped; otherwise, the new image URL is inserted. 

Error Handling & Cleanup: Includes try-except blocks for robust error management (e.g., HDFS download failures, MySQL connection errors, data parsing issues) and ensures temporary local CSV files are removed after processing. 

# Phase 4: Data Analysis (Business Queries) 

With the data successfully ingested into the normalized MySQL database, it is now ready for analytical querying. Several business-oriented SQL queries were developed to extract key insights: 

Source Performance: Total number of products ingested from each e-commerce source. 

Brand Pricing: Average current price for products associated with each brand. 

High-Value Products: Identification of the top 5 most expensive products currently available, along with their brand and source. 

Category Distribution: Count of products within each specific category (e.g., Gaming Laptop, Ultrabook). 

Price Outliers: Products with current prices exceeding the overall average product price. 

Brand Diversification: Brands offering products across multiple distinct categories. 

Image Coverage: Products with multiple associated images and their corresponding current prices. 

# Conclusion 

This project successfully delivered a functional and resilient data pipeline for e-commerce product data. My direct involvement in establishing the fault-tolerant Hadoop HDFS cluster, including advanced configuration and rigorous redundancy testing, formed the backbone of the system. Furthermore, my contributions to the MySQL database design and the development of the intelligent Python ingestion script were crucial for transforming raw, multi-source data into a clean, structured, and query-ready format. The project provided invaluable practical experience in distributed systems, data modeling, ETL processes, and robust error handling in a real-world data engineering context. 

 
