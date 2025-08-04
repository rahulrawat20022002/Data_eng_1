# import pandas as pd
# import mysql.connector
# import os
# import re # For parsing specifications
# from datetime import date

# # --- MySQL Database Configuration ---
# MYSQL_CONFIG = {
#     'host': 'master-node', # e.g., '100.X.Y.Z' or 'ubuntu1.tailca76e7.ts.net'
#     'user': 'lenovo_mysql', # Ensure this matches the user you created in MySQL Workbench
#     'password': '123789456', # Ensure this matches the password for that user
#     'database': 'product_catalog'
# }

# # Define paths for the local CSV files and their corresponding retailer names
# # Ensure these CSV files are in the same directory as this script, or provide their full paths.
# CSV_FILES_TO_PROCESS = {
#     # "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_ebay\\ebay_extracted_products_data.csv": "eBay",
#     "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_ebay\\ebay_extracted_products_data.csv": "Amazon",
#     # "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_newegg\\newegg_detailed_products.csv": "Newegg"
# }

# def get_or_create_retailer(cursor, retailer_name, base_url=None):
#     """Gets retailer_id or creates a new retailer entry."""
#     cursor.execute("SELECT retailer_id FROM Retailers WHERE retailer_name = %s", (retailer_name,))
#     retailer_result = cursor.fetchone()
#     if retailer_result:
#         return retailer_result[0]
#     else:
#         cursor.execute("INSERT INTO Retailers (retailer_name, base_url) VALUES (%s, %s)", (retailer_name, base_url))
#         # No conn.commit() here, will be committed by main ingestion loop or specific commit points
#         return cursor.lastrowid

# def get_or_create_product(cursor, conn, product_name, brand, category, description=None):
#     """
#     Checks if a product exists based on name, brand, and category.
#     If yes, returns product_id. If no, inserts and returns new product_id.
#     """
#     # Attempt to find an existing product based on key attributes
#     cursor.execute("""
#         SELECT product_id FROM Products
#         WHERE product_name = %s AND brand = %s AND category = %s
#     """, (product_name, brand, category))
#     product_result = cursor.fetchone()

#     if product_result:
#         return product_result[0]
#     else:
#         # Product does not exist, insert it
#         cursor.execute("""
#             INSERT INTO Products (product_name, brand, category, description)
#             VALUES (%s, %s, %s, %s)
#         """, (product_name, brand, category, description))
#         # No conn.commit() here
#         return cursor.lastrowid

# def parse_price(price_str):
#     """Safely parses a price string into a float, handling various currency symbols and formats."""
#     if pd.isna(price_str) or price_str is None:
#         return None
#     price_str = str(price_str).replace('₹', '').replace('$', '').replace('€', '').replace('£', '').replace(',', '').strip()
#     if not price_str or price_str.lower() in ['n/a', 'nan', 'none', 'free']: # Added 'free'
#         return None
#     try:
#         return float(price_str)
#     except ValueError:
#         return None

# def extract_brand_from_title(title):
#     """Extracts common brands from product title."""
#     if pd.isna(title) or title is None:
#         return "Unknown"
#     # Extended list of common brands
#     common_brands = ["Acer", "HP", "Dell", "Lenovo", "ASUS", "MSI", "Apple", "Samsung", "Microsoft", 
#                      "Razer", "Gigabyte", "Alienware", "Chromebook", "LG", "Huawei", "Google", "Microsoft Surface", "Vaio"]
#     for brand in common_brands:
#         # Use word boundaries (\b) to match whole words and escape special characters in brand names
#         if re.search(r'\b' + re.escape(brand) + r'\b', title, re.IGNORECASE):
#             return brand
#     return "Unknown"

# def parse_specifications(specs_str):
#     """
#     Parses a string of specifications/key features into a list of (name, value) tuples.
#     Handles various formats like "Key:Value", "Key - Value", "Key (Value)", and bullet points.
#     """
#     if pd.isna(specs_str) or not specs_str:
#         return []

#     specs_list = []
#     # Replace common separators with a consistent one (e.g., "|") for easier splitting
#     # Prioritize specific patterns before generic splitting
#     cleaned_specs_str = specs_str.replace('\n', '|').replace(';', '|').replace(', ', '|').strip()

#     # Handle "Key: Value" or "Key - Value" patterns
#     # Split by '|' first, then process each segment
#     segments = re.split(r'\|', cleaned_specs_str)

#     for segment in segments:
#         segment = segment.strip()
#         if not segment:
#             continue

#         # Try to split by common key-value delimiters
#         match = re.match(r'^(.*?)\s*[:\-]\s*(.*)$', segment)
#         if match:
#             name = match.group(1).strip()
#             value = match.group(2).strip()
#         else:
#             # If no clear key-value delimiter, treat the whole segment as a feature/spec name, value can be None or same as name
#             name = segment
#             value = None # Or you could set value = segment and handle specific cases in name if needed

#         if name:
#             specs_list.append((name, value))
#     return specs_list


# def ingest_dataframe_to_mysql(df, retailer_name):
#     """Ingests data from a pandas DataFrame into the MySQL database tables."""
#     conn = None
#     cursor = None

#     try:
#         conn = mysql.connector.connect(**MYSQL_CONFIG)
#         cursor = conn.cursor()
#         print(f"Successfully connected to MySQL database for {retailer_name} data.")

#         # Get or create retailer_id once for this DataFrame
#         retailer_base_url = {
#             "eBay": "https://www.ebay.com/",
#             "Amazon": "https://www.amazon.com/",
#             "Newegg": "https://www.newegg.com/"
#         }.get(retailer_name, None)
#         retailer_id = get_or_create_retailer(cursor, retailer_name, retailer_base_url)
#         conn.commit() # Commit retailer insertion immediately

#         processed_count = 0
#         for index, row in df.iterrows():
#             # Extract common columns and handle NaNs from various possible column names
#             # Prioritize 'title' then 'name'
#             product_name = row.get('title') if pd.notna(row.get('title')) else \
#                            row.get('name') if pd.notna(row.get('name')) else None
            
#             # Skip row if product_name is critical and missing
#             if product_name is None:
#                 print(f"Skipping row {index} from {retailer_name} due to missing product name.")
#                 continue

#             # Extract or derive brand and category
#             brand = extract_brand_from_title(product_name)
#             # Use 'category' column if available, else default to 'Laptop'
#             category = row.get('category') if pd.notna(row.get('category')) else "Laptop"
            
#             description = row.get('description') if pd.notna(row.get('description')) else \
#                           row.get('Description') if pd.notna(row.get('Description')) else None

#             # --- 1. Get or Create Product ---
#             product_id = get_or_create_product(cursor, conn, product_name, brand, category, description)
#             # Product_id is now available
            
#             # --- 2. Insert into Product_Listings ---
#             # Handle various URL column names
#             listing_url = row.get('link') if pd.notna(row.get('link')) else \
#                           row.get('url') if pd.notna(row.get('url')) else \
#                           row.get('Product URL') if pd.notna(row.get('Product URL')) else \
#                           row.get('Item URL') if pd.notna(row.get('Item URL')) else None
            
#             # Use specific price columns based on retailer if known, otherwise generic 'price'
#             current_price = parse_price(row.get('price'))
#             original_price = parse_price(row.get('Original Price')) if 'Original Price' in row else None
            
#             if retailer_name == "Newegg":
#                 current_price = parse_price(row.get('Price')) # Newegg uses 'Price'
#                 original_price = parse_price(row.get('MSRP')) if 'MSRP' in row else original_price # Newegg uses 'MSRP'

#             currency = 'USD' # Default currency, update if specific currency data is available in CSVs
#             # If Amazon data uses '₹', then update logic specific to Amazon
#             if retailer_name == 'Amazon':
#                 # Check for currency symbol in price string if a dedicated currency column doesn't exist
#                 if 'price' in row and isinstance(row['price'], str) and '₹' in row['price']:
#                     currency = 'INR' 
#             elif retailer_name == 'eBay':
#                  # eBay prices can be in USD, GBP, EUR etc. For now, assuming USD
#                  if 'Price' in row and isinstance(row['Price'], str) and '$' in row['Price']:
#                      currency = 'USD'
#                  elif 'Price' in row and isinstance(row['Price'], str) and '£' in row['Price']:
#                      currency = 'GBP'

#             # Handle availability columns
#             availability = row.get('availability') if pd.notna(row.get('availability')) else \
#                            row.get('Stock') if pd.notna(row.get('Stock')) else None
            
#             condition = row.get('condition') if pd.notna(row.get('condition')) else \
#                         row.get('Condition') if pd.notna(row.get('Condition')) else None # For eBay

#             # Shipping cost
#             shipping_cost = parse_price(row.get('shipping_cost')) if 'shipping_cost' in row else \
#                             parse_price(row.get('Shipping')) if 'Shipping' in row else None

#             delivery_information = row.get('delivery_time') if pd.notna(row.get('delivery_time')) else None
            
#             # Seller info
#             seller_name = row.get('seller') if pd.notna(row.get('seller')) else \
#                           row.get('Seller Name') if pd.notna(row.get('Seller Name')) else None
            
#             seller_rating = row.get('seller_rating') if pd.notna(row.get('seller_rating')) else \
#                             row.get('Seller Rating') if pd.notna(row.get('Seller Rating')) else None
            
#             # Product identifier (e.g., ASIN, Item ID)
#             product_identifier = row.get('asin') if pd.notna(row.get('asin')) else \
#                                  row.get('item_id') if pd.notna(row.get('item_id')) else \
#                                  row.get('Product ID') if pd.notna(row.get('Product ID')) else None

#             # Image URL
#             image_url = row.get('image_url') if pd.notna(row.get('image_url')) else \
#                         row.get('Image URL') if pd.notna(row.get('Image URL')) else \
#                         row.get('Image') if pd.notna(row.get('Image')) else None

#             # Insert into Product_Listings
#             try:
#                 cursor.execute("""
#                     INSERT INTO Product_Listings (
#                         product_id, retailer_id, listing_url, current_price, original_price, currency,
#                         availability, `condition`, shipping_cost, delivery_information,
#                         seller_name, seller_rating, product_identifier, image_url
#                     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                     ON DUPLICATE KEY UPDATE -- Update if listing_url (UNIQUE) already exists
#                         current_price = VALUES(current_price),
#                         original_price = VALUES(original_price),
#                         availability = VALUES(availability),
#                         `condition` = VALUES(`condition`),
#                         shipping_cost = VALUES(shipping_cost),
#                         delivery_information = VALUES(delivery_information),
#                         seller_name = VALUES(seller_name),
#                         seller_rating = VALUES(seller_rating),
#                         image_url = VALUES(image_url),
#                         listing_created_at = listing_created_at -- Don't update creation timestamp
#                 """, (
#                     product_id, retailer_id, listing_url, current_price, original_price, currency,
#                     availability, condition, shipping_cost, delivery_information,
#                     seller_name, seller_rating, product_identifier, image_url
#                 ))
#                 listing_id = cursor.lastrowid
#                 # If an update occurred, lastrowid might be 0 or not what's expected for ID
#                 # Need to re-query for listing_id if ON DUPLICATE KEY UPDATE was triggered
#                 if cursor.rowcount == 0: # rowcount is 0 for updates that didn't change data, 1 for insert, 2 for update+insert
#                     cursor.execute("SELECT listing_id FROM Product_Listings WHERE listing_url = %s", (listing_url,))
#                     existing_listing = cursor.fetchone()
#                     if existing_listing:
#                         listing_id = existing_listing[0]
#                     else:
#                         print(f"Warning: Could not get listing_id for URL {listing_url}. Skipping related inserts.")
#                         conn.rollback() # Rollback if listing_id couldn't be determined
#                         continue
#             except mysql.connector.Error as e:
#                 print(f"Error inserting/updating Product_Listing for {product_name} ({listing_url}): {e}. Rolling back row.")
#                 conn.rollback()
#                 continue # Skip to next row

#             # --- 3. Insert into Product_Specifications ---
#             # Columns that might contain specifications/key features
#             specs_columns = [
#                 'Specifications', 'Key Features', 'Details', 'Spec', 'Technical Details', 'Product Dimensions',
#                 'Item Weight', 'Manufacturer', 'ASIN', 'Customer Reviews', 'Best Sellers Rank', 'Date First Available'
#             ]
#             for col in specs_columns:
#                 if col in row and pd.notna(row[col]):
#                     specs_data = parse_specifications(row[col])
#                     for spec_name, spec_value in specs_data:
#                         if spec_name: # Ensure spec_name is not empty
#                             # Insert or Update specification
#                             cursor.execute("""
#                                 INSERT INTO Product_Specifications (product_id, spec_name, spec_value, source_retailer_id)
#                                 VALUES (%s, %s, %s, %s)
#                                 ON DUPLICATE KEY UPDATE spec_value = VALUES(spec_value)
#                             """, (product_id, spec_name, spec_value, retailer_id))

#             # --- 4. Insert into Reviews_and_Ratings ---
#             rating_value = row.get('Rating') if pd.notna(row.get('Rating')) else \
#                            row.get('rating') if pd.notna(row.get('rating')) else None
            
#             total_reviews_listing = row.get('Number of reviews') if pd.notna(row.get('Number of reviews')) else \
#                                     row.get('reviews_count') if pd.notna(row.get('reviews_count')) else None
            
#             # Convert review count to integer safely
#             if isinstance(total_reviews_listing, str):
#                 total_reviews_listing = int(re.sub(r'[^\d]', '', total_reviews_listing)) if re.sub(r'[^\d]', '', total_reviews_listing) else 0
#             else:
#                 total_reviews_listing = int(total_reviews_listing) if pd.notna(total_reviews_listing) else 0

#             if rating_value is not None:
#                 # Insert a placeholder review entry for the aggregate rating from the listing
#                 # You might want to prevent duplicate review entries if running multiple times on same data
#                 # For simplicity, we are inserting a record to represent the snapshot of rating from the listing
#                 cursor.execute("""
#                     INSERT INTO Reviews_and_Ratings (listing_id, rating_value, review_date)
#                     VALUES (%s, %s, %s)
#                 """, (listing_id, rating_value, date.today())) # review_date is current ingestion date

#                 # Update average_rating and total_reviews in Products table
#                 # This is a simplified aggregate logic. For more precision, recalculate from Reviews_and_Ratings table
#                 # after all data is loaded or implement a more sophisticated real-time aggregate.
                
#                 # Fetch current aggregates from Products table (could be NULL if product is new)
#                 cursor.execute("SELECT average_rating, total_reviews FROM Products WHERE product_id = %s", (product_id,))
#                 current_product_agg = cursor.fetchone()
                
#                 new_average_rating = rating_value # Default to current listing rating
#                 new_total_reviews = total_reviews_listing # Default to current listing reviews count

#                 if current_product_agg and current_product_agg[0] is not None and current_product_agg[1] is not None:
#                     # Calculate new weighted average only if there were existing reviews
#                     existing_avg = current_product_agg[0]
#                     existing_total = current_product_agg[1]

#                     if existing_total > 0 or total_reviews_listing > 0: # Avoid division by zero
#                         new_total_reviews = existing_total + total_reviews_listing
#                         if new_total_reviews > 0:
#                             new_average_rating = ((existing_avg * existing_total) + (rating_value * total_reviews_listing)) / new_total_reviews
#                         else:
#                             new_average_rating = None # No reviews at all
                    
#                 cursor.execute("""
#                     UPDATE Products SET average_rating = %s, total_reviews = %s
#                     WHERE product_id = %s
#                 """, (new_average_rating, new_total_reviews, product_id))
            
#             conn.commit() # Commit after each product's full data is processed
#             processed_count += 1

#         print(f"Data ingestion for {retailer_name} completed. Processed {processed_count} rows.")

#     except mysql.connector.Error as err:
#         print(f"MySQL Error during {retailer_name} ingestion: {err}")
#         if conn:
#             conn.rollback()
#     except Exception as e:
#         print(f"An unexpected error occurred during {retailer_name} ingestion: {e}")
#         if conn:
#             conn.rollback()
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# # Main execution logic
# if __name__ == "__main__":
#     for csv_file_name, retailer_name in CSV_FILES_TO_PROCESS.items():
#         local_csv_path = csv_file_name # Assuming CSVs are in the same directory as script

#         if not os.path.exists(local_csv_path):
#             print(f"Error: Local CSV file '{local_csv_path}' not found. Please ensure it's in the same directory as the script or provide the full path.")
#             continue # Skip to next file

#         try:
#             print(f"\n--- Starting ingestion for {retailer_name} from {local_csv_path} ---")
#             df = pd.read_csv(local_csv_path, encoding='utf-8')
#             ingest_dataframe_to_mysql(df, retailer_name)
#         except pd.errors.EmptyDataError:
#             print(f"Warning: {local_csv_path} is empty. Skipping.")
#         except pd.errors.ParserError as e:
#             print(f"Error parsing CSV file {local_csv_path}: {e}. Skipping.")
#         except Exception as e:
#             print(f"An error occurred while reading or processing {local_csv_path}: {e}")





# --------------------->

# import pandas as pd
# import mysql.connector
# import os
# from hdfs import InsecureClient
# from datetime import date
# import math # Import math to check for NaN

# # --- HDFS Configuration ---
# # Replace with the Tailscale IP of your HDFS NameNode VM or its resolvable hostname
# HDFS_NAMENODE_URL = 'http://master-node:9870' # Ensure 'master-node' resolves to its Tailscale IP on Windows

# # Define HDFS paths and corresponding local temporary file paths for each source
# # IMPORTANT: Ensure these HDFS paths match exactly where your files are located.
# HDFS_FILES_TO_INGEST = {
#     "amazon": {
#         "hdfs_path": "/amz_data/data_amz.csv",
#         "local_path": "temp_amazon_data.csv",
#         "column_mapping": {
#             "title": "Product_description", # Adjust this to your Amazon CSV's actual title column name
#             "link": "Product_url",          # Adjust this to your Amazon CSV's actual link column name
#             "price": "Price",               # Adjust this to your Amazon CSV's actual price column name
#             "image_url": "Image_url"        # Adjust this to your Amazon CSV's actual image URL column name
#         }
#     },
#     "ebay": {
#         "hdfs_path": "/ebay_data/data_ebay.csv",
#         "local_path": "temp_ebay_data.csv",
#         "column_mapping": {
#             "title": "product_description", # Adjust this to your eBay CSV's actual title column name
#             "link": "product_url",             # Adjust this to your eBay CSV's actual link column name
#             "price": "price",               # Adjust this to your eBay CSV's actual price column name
#             "image_url": "image_url"        # Adjust this to your eBay CSV's actual image URL column name
#         }
#     },
#     "newegg": {
#         "hdfs_path": "/newegg_data/data/data_newegg.csv", # FIX: Changed backslash to forward slash for HDFS path
#         "local_path": "temp_newegg_data.csv",
#         "column_mapping": {
#             "title": "Product Name", # Adjust this to your Newegg CSV's actual title column name
#             "link": "Product URL",   # Adjust this to your Newegg CSV's actual link column name
#             "price": "Price",        # Adjust this to your Newegg CSV's actual price column name
#             "image_url": "Image URL" # Adjust this to your Newegg CSV's actual image URL column name
#         }
#     }
# }

# # --- MySQL Database Configuration ---
# MYSQL_CONFIG = {
#     'host': 'master-node', # Ensure this is the Tailscale IP or resolvable hostname of your MySQL VM
#     'user': 'lenovo_mysql',
#     'password': '123789456',
#     'database': 'product_catalog'
# }

# # Initialize HDFS client (outside functions as it's used by download_from_hdfs)
# try:
#     hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user='rahul')
#     print(f"Connected to HDFS NameNode at {HDFS_NAMENODE_URL} as user 'rahul'")
# except Exception as e:
#     print(f"Error connecting to HDFS: {e}")
#     print("Please ensure your HDFS NameNode is running and the URL/port are correct.")
#     exit()

# def download_from_hdfs(hdfs_path, local_path):
#     """Downloads a single CSV file from HDFS to a local temporary file."""
#     try:
#         print(f"Downloading '{hdfs_path}' from HDFS to '{local_path}'...")
#         with hdfs_client.read(hdfs_path) as reader:
#             with open(local_path, 'wb') as writer:
#                 for chunk in reader:
#                     writer.write(chunk)
#         print(f"Download of '{hdfs_path}' complete.")
#         return True
#     except Exception as e:
#         print(f"Error downloading '{hdfs_path}' from HDFS: {e}")
#         print("Please ensure HDFS NameNode is running, URL/port are correct, and file exists.")
#         return False

# def ingest_to_mysql():
#     """Ingests data from all local CSVs into the MySQL database."""
#     conn = None 
#     cursor = None

#     try:
#         conn = mysql.connector.connect(**MYSQL_CONFIG)
#         cursor = conn.cursor()
#         print("Successfully connected to MySQL database.")

#         for source_name, config in HDFS_FILES_TO_INGEST.items():
#             hdfs_path = config["hdfs_path"]
#             local_path = config["local_path"]
#             column_mapping = config["column_mapping"]

#             # Step 1: Download the file from HDFS
#             if not download_from_hdfs(hdfs_path, local_path):
#                 print(f"Skipping ingestion for {source_name} due to HDFS download failure.")
#                 continue

#             if not os.path.exists(local_path):
#                 print(f"Error: Local CSV file '{local_path}' not found after download. Cannot ingest.")
#                 continue

#             print(f"\n--- Ingesting data from {source_name} (File: {local_path}) ---")
#             df = pd.read_csv(local_path, encoding='utf-8')

#             # --- DEBUGGING: Print original columns ---
#             print(f"  Original columns in '{local_path}': {df.columns.tolist()}")

#             # --- IMPORTANT: Validate if source columns exist before renaming ---
#             missing_source_cols = []
#             for target_col, source_col in column_mapping.items():
#                 if source_col not in df.columns:
#                     missing_source_cols.append(source_col)
            
#             if missing_source_cols:
#                 print(f"ERROR: For source '{source_name}', the following columns are missing in the CSV file based on your 'column_mapping': {missing_source_cols}")
#                 print("Please check your CSV file headers and update the 'column_mapping' in the script accordingly.")
#                 os.remove(local_path) # Clean up downloaded file
#                 continue # Skip to next file

#             # Rename columns based on the mapping for the current source
#             df_renamed = df.rename(columns={
#                 column_mapping["title"]: "title",
#                 column_mapping["link"]: "link",
#                 column_mapping["price"]: "price",
#                 column_mapping["image_url"]: "image_url"
#             })

#             # --- DEBUGGING: Print renamed columns ---
#             print(f"  Renamed columns for processing: {df_renamed.columns.tolist()}")


#             # Iterate through DataFrame rows and insert into tables
#             for index, row in df_renamed.iterrows():
#                 # Safely get values, converting NaN to None for database compatibility
#                 title = row['title'] if 'title' in row and pd.notna(row['title']) else None
#                 product_url = row['link'] if 'link' in row and pd.notna(row['link']) else None
#                 image_url = row['image_url'] if 'image_url' in row and pd.notna(row['image_url']) else None

#                 # Handle price parsing
#                 price_amount = None
#                 if 'price' in row and pd.notna(row['price']):
#                     price_str = str(row['price']).replace('₹', '').replace('$', '').replace(',', '').strip()
#                     if price_str:
#                         try:
#                             price_amount = float(price_str)
#                         except ValueError:
#                             print(f"Warning: Could not parse price '{row['price']}' for title '{title}'. Skipping price.")
                
#                 # Skip row if essential data is missing
#                 if title is None or product_url is None:
#                     print(f"Skipping row {index} from {source_name} due to missing title or product_url.")
#                     continue

#                 # --- 1. Handle Brands ---
#                 brand_name = "Unknown"
#                 if title is not None:
#                     common_brands = ["Acer", "HP", "Dell", "Lenovo", "ASUS", "MSI", "Apple", "Samsung", "Microsoft", "Razer"]
#                     for brand in common_brands:
#                         if brand.lower() in title.lower():
#                             brand_name = brand
#                             break

#                 cursor.execute("SELECT brand_id FROM brands WHERE brand_name = %s", (brand_name,))
#                 brand_result = cursor.fetchone()
#                 if brand_result:
#                     brand_id = brand_result[0]
#                 else:
#                     cursor.execute("INSERT INTO brands (brand_name) VALUES (%s)", (brand_name,))
#                     conn.commit()
#                     brand_id = cursor.lastrowid

#                 # --- 2. Handle Categories ---
#                 # Enhanced category extraction logic based on keywords in title
#                 category_name = "Laptop" # Default category
#                 if title is not None:
#                     title_lower = title.lower()
#                     if "gaming" in title_lower:
#                         category_name = "Gaming Laptop"
#                     elif "ultrabook" in title_lower or "thin and light" in title_lower:
#                         category_name = "Ultrabook"
#                     elif "2-in-1" in title_lower or "convertible" in title_lower:
#                         category_name = "2-in-1 Laptop"
#                     elif "chromebook" in title_lower:
#                         category_name = "Chromebook"
#                     elif "macbook" in title_lower or "apple" in title_lower:
#                         category_name = "MacBook"
#                     # Add more specific categories based on your data if needed
#                     # else: category_name remains "Laptop"

#                 cursor.execute("SELECT category_id FROM categories WHERE category_name = %s", (category_name,))
#                 category_result = cursor.fetchone()
#                 if category_result:
#                     category_id = category_result[0]
#                 else:
#                     cursor.execute("INSERT INTO categories (category_name) VALUES (%s)", (category_name,))
#                     conn.commit()
#                     category_id = cursor.lastrowid

#                 # --- 3. Insert into Products ---
#                 cursor.execute("SELECT product_id FROM products WHERE product_url = %s", (product_url,))
#                 product_result = cursor.fetchone()
#                 if product_result:
#                     product_id = product_result[0]
#                     cursor.execute("UPDATE products SET title=%s, brand_id=%s, category_id=%s, source_name=%s, last_updated_at=NOW() WHERE product_id=%s",
#                                    (title, brand_id, category_id, source_name, product_id))
#                 else:
#                     cursor.execute("INSERT INTO products (title, product_url, brand_id, category_id, source_name) VALUES (%s, %s, %s, %s, %s)",
#                                    (title, product_url, brand_id, category_id, source_name))
#                     conn.commit()
#                     product_id = cursor.lastrowid

#                 # --- 4. Insert into Prices ---
#                 if price_amount is not None:
#                     cursor.execute("UPDATE prices SET is_current = FALSE WHERE product_id = %s", (product_id,))
#                     cursor.execute("INSERT INTO prices (product_id, price_amount, currency, scrape_date, is_current) VALUES (%s, %s, %s, %s, %s)",
#                                    (product_id, price_amount, 'INR', date.today(), True))

#                 # --- 5. Handle Images ---
#                 if image_url is not None:
#                     cursor.execute("SELECT image_id FROM images WHERE image_url = %s", (image_url,))
#                     image_result = cursor.fetchone()
#                     if not image_result:
#                         cursor.execute("INSERT INTO images (product_id, image_url, is_thumbnail) VALUES (%s, %s, %s)",
#                                        (product_id, image_url, True))
                
#                 conn.commit()

#         print("\nAll data ingestion completed successfully!")

#     except mysql.connector.Error as err:
#         print(f"MySQL Error: {err}")
#         if conn:
#             conn.rollback()
#     except Exception as e:
#         print(f"An unexpected error occurred during ingestion: {e}")
#         if conn:
#             conn.rollback()
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#         for source_name, config in HDFS_FILES_TO_INGEST.items():
#             local_path = config["local_path"]
#             if os.path.exists(local_path):
#                 os.remove(local_path)
#                 print(f"Removed temporary local CSV: {local_path}")

# if __name__ == "__main__":
#     ingest_to_mysql()



# ---------------------


# import pandas as pd
# import mysql.connector
# import os
# from hdfs import InsecureClient
# from datetime import date
# import math # Import math to check for NaN
# import re   # Import re for regular expressions

# # --- HDFS Configuration ---
# # Replace with the Tailscale IP of your HDFS NameNode VM or its resolvable hostname
# HDFS_NAMENODE_URL = 'http://master-node:9870' # Ensure 'master-node' resolves to its Tailscale IP on Windows

# # Define HDFS paths and corresponding local temporary file paths for each source
# # IMPORTANT: Ensure these HDFS paths match exactly where your files are located.
# HDFS_FILES_TO_INGEST = {
#     "amazon": {
#         "hdfs_path": "/amz_data/data_amz.csv",
#         "local_path": "temp_amazon_data.csv",
#         "column_mapping": {
#             "title": "Product_description", # Adjust this to your Amazon CSV's actual title column name
#             "link": "Product_url",          # Adjust this to your Amazon CSV's actual link column name
#             "price": "Price",               # Adjust this to your Amazon CSV's actual price column name
#             "image_url": "Image_url"        # Adjust this to your Amazon CSV's actual image URL column name
#         }
#     },
#     "ebay": {
#         "hdfs_path": "/ebay_data/data_ebay.csv",
#         "local_path": "temp_ebay_data.csv",
#         "column_mapping": {
#             "title": "product_description", # Adjust this to your eBay CSV's actual title column name
#             "link": "product_url",             # Adjust this to your eBay CSV's actual link column name
#             "price": "price",               # Adjust this to your eBay CSV's actual price column name
#             "image_url": "image_url"        # Adjust this to your eBay CSV's actual image URL column name
#         }
#     },
#     "newegg": {
#         "hdfs_path": "/newegg_data/data/data_newegg.csv", # FIX: Changed backslash to forward slash for HDFS path
#         "local_path": "temp_newegg_data.csv",
#         "column_mapping": {
#             "title": "Product Name", # Adjust this to your Newegg CSV's actual title column name
#             "link": "Product URL",   # Adjust this to your Newegg CSV's actual link column name
#             "price": "Price",        # Adjust this to your Newegg CSV's actual price column name
#             "image_url": "Image URL" # Adjust this to your Newegg CSV's actual image URL column name
#         }
#     }
# }

# # --- MySQL Database Configuration ---
# MYSQL_CONFIG = {
#     'host': 'master-node', # Ensure this is the Tailscale IP or resolvable hostname of your MySQL VM
#     'user': 'lenovo_mysql',
#     'password': '123789456',
#     'database': 'product_catalog'
# }

# # Initialize HDFS client (outside functions as it's used by download_from_hdfs)
# try:
#     hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user='rahul')
#     print(f"Connected to HDFS NameNode at {HDFS_NAMENODE_URL} as user 'rahul'")
# except Exception as e:
#     print(f"Error connecting to HDFS: {e}")
#     print("Please ensure your HDFS NameNode is running and the URL/port are correct.")
#     exit()

# def download_from_hdfs(hdfs_path, local_path):
#     """Downloads a single CSV file from HDFS to a local temporary file."""
#     try:
#         print(f"Downloading '{hdfs_path}' from HDFS to '{local_path}'...")
#         with hdfs_client.read(hdfs_path) as reader:
#             with open(local_path, 'wb') as writer:
#                 for chunk in reader:
#                     writer.write(chunk)
#         print(f"Download of '{hdfs_path}' complete.")
#         return True
#     except Exception as e:
#         print(f"Error downloading '{hdfs_path}' from HDFS: {e}")
#         print("Please ensure HDFS NameNode is running, URL/port are correct, and file exists.")
#         return False

# def ingest_to_mysql():
#     """Ingests data from all local CSVs into the MySQL database."""
#     conn = None 
#     cursor = None

#     try:
#         conn = mysql.connector.connect(**MYSQL_CONFIG)
#         cursor = conn.cursor()
#         print("Successfully connected to MySQL database.")

#         for source_name, config in HDFS_FILES_TO_INGEST.items():
#             hdfs_path = config["hdfs_path"]
#             local_path = config["local_path"]
#             column_mapping = config["column_mapping"]

#             # Step 1: Download the file from HDFS
#             if not download_from_hdfs(hdfs_path, local_path):
#                 print(f"Skipping ingestion for {source_name} due to HDFS download failure.")
#                 continue

#             if not os.path.exists(local_path):
#                 print(f"Error: Local CSV file '{local_path}' not found after download. Cannot ingest.")
#                 continue

#             print(f"\n--- Ingesting data from {source_name} (File: {local_path}) ---")
#             df = pd.read_csv(local_path, encoding='utf-8')

#             # --- DEBUGGING: Print original columns ---
#             print(f"  Original columns in '{local_path}': {df.columns.tolist()}")

#             # --- IMPORTANT: Validate if source columns exist before renaming ---
#             missing_source_cols = []
#             for target_col, source_col in column_mapping.items():
#                 if source_col not in df.columns:
#                     missing_source_cols.append(source_col)
            
#             if missing_source_cols:
#                 print(f"ERROR: For source '{source_name}', the following columns are missing in the CSV file based on your 'column_mapping': {missing_source_cols}")
#                 print("Please check your CSV file headers and update the 'column_mapping' in the script accordingly.")
#                 os.remove(local_path) # Clean up downloaded file
#                 continue # Skip to next file

#             # Rename columns based on the mapping for the current source
#             df_renamed = df.rename(columns={
#                 column_mapping["title"]: "title",
#                 column_mapping["link"]: "link",
#                 column_mapping["price"]: "price",
#                 column_mapping["image_url"]: "image_url"
#             })

#             # --- DEBUGGING: Print renamed columns ---
#             print(f"  Renamed columns for processing: {df_renamed.columns.tolist()}")


#             # Iterate through DataFrame rows and insert into tables
#             for index, row in df_renamed.iterrows():
#                 # Safely get values, converting NaN to None for database compatibility
#                 title = row['title'] if 'title' in row and pd.notna(row['title']) else None
#                 product_url = row['link'] if 'link' in row and pd.notna(row['link']) else None
#                 image_url = row['image_url'] if 'image_url' in row and pd.notna(row['image_url']) else None

#                 # --- Handle price parsing (Enhanced Logic) ---
#                 price_amount = None
#                 if 'price' in row and pd.notna(row['price']):
#                     raw_price_str = str(row['price']).strip()
                    
#                     # Regex to find the first number, allowing for commas/dots as decimal/thousands separators
#                     # It tries to capture a sequence of digits, optional thousands separators (comma/dot),
#                     # and an optional decimal part (comma/dot followed by digits).
#                     # This is more robust for various price formats.
#                     price_match = re.search(r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)', raw_price_str)
                    
#                     if price_match:
#                         extracted_price_str = price_match.group(1).strip()
                        
#                         # Normalize the decimal separator to a dot and remove thousands separators
#                         # First, remove all non-digit/non-decimal characters, keeping only one decimal point
#                         # This assumes European format (comma decimal) or US format (dot decimal)
#                         if ',' in extracted_price_str and '.' in extracted_price_str:
#                             # If both are present, assume European thousands separator (dot) and comma decimal
#                             cleaned_price_str = extracted_price_str.replace('.', '').replace(',', '.')
#                         elif ',' in extracted_price_str:
#                             # Only comma present, assume it's the decimal separator
#                             cleaned_price_str = extracted_price_str.replace(',', '.')
#                         else:
#                             # Only dot present or no decimal, assume standard US format or no decimal
#                             cleaned_price_str = extracted_price_str
                        
#                         # Remove any remaining non-digit/non-dot characters (like spaces or stray symbols)
#                         cleaned_price_str = re.sub(r'[^\d.]', '', cleaned_price_str)

#                         try:
#                             price_amount = float(cleaned_price_str)
#                         except ValueError:
#                             print(f"Warning: Could not parse final cleaned price '{cleaned_price_str}' from original '{raw_price_str}' for title '{title}'. Skipping price.")
#                     else:
#                         print(f"Warning: No valid number found in price string '{raw_price_str}' for title '{title}'. Skipping price.")
                
#                 # Skip row if essential data is missing (title or product_url)
#                 if title is None or product_url is None:
#                     print(f"Skipping row {index} from {source_name} due to missing title or product_url.")
#                     continue

#                 # --- 1. Handle Brands ---
#                 brand_name = "Unknown" # Default
#                 if title is not None: # Use the cleaned title
#                     common_brands = ["Acer", "HP", "Dell", "Lenovo", "ASUS", "MSI", "Apple", "Samsung", "Microsoft", "Razer"]
#                     for brand in common_brands:
#                         if brand.lower() in title.lower():
#                             brand_name = brand
#                             break

#                 cursor.execute("SELECT brand_id FROM brands WHERE brand_name = %s", (brand_name,))
#                 brand_result = cursor.fetchone()
#                 if brand_result:
#                     brand_id = brand_result[0]
#                 else:
#                     cursor.execute("INSERT INTO brands (brand_name) VALUES (%s)", (brand_name,))
#                     conn.commit() # Commit immediately to get lastrowid
#                     brand_id = cursor.lastrowid

#                 # --- 2. Handle Categories ---
#                 # Enhanced category extraction logic based on keywords in title
#                 category_name = "Laptop" # Default category
#                 if title is not None:
#                     title_lower = title.lower()
#                     if "gaming" in title_lower:
#                         category_name = "Gaming Laptop"
#                     elif "ultrabook" in title_lower or "thin and light" in title_lower:
#                         category_name = "Ultrabook"
#                     elif "2-in-1" in title_lower or "convertible" in title_lower:
#                         category_name = "2-in-1 Laptop"
#                     elif "chromebook" in title_lower:
#                         category_name = "Chromebook"
#                     elif "macbook" in title_lower or "apple" in title_lower:
#                         category_name = "MacBook"
#                     # Add more specific categories based on your data if needed
#                     # else: category_name remains "Laptop"

#                 cursor.execute("SELECT category_id FROM categories WHERE category_name = %s", (category_name,))
#                 category_result = cursor.fetchone()
#                 if category_result:
#                     category_id = category_result[0]
#                 else:
#                     cursor.execute("INSERT INTO categories (category_name) VALUES (%s)", (category_name,))
#                     conn.commit()
#                     category_id = cursor.lastrowid

#                 # --- 3. Insert into Products ---
#                 cursor.execute("SELECT product_id FROM products WHERE product_url = %s", (product_url,))
#                 product_result = cursor.fetchone()
#                 if product_result:
#                     product_id = product_result[0]
#                     cursor.execute("UPDATE products SET title=%s, brand_id=%s, category_id=%s, source_name=%s, last_updated_at=NOW() WHERE product_id=%s",
#                                    (title, brand_id, category_id, source_name, product_id))
#                 else:
#                     cursor.execute("INSERT INTO products (title, product_url, brand_id, category_id, source_name) VALUES (%s, %s, %s, %s, %s)",
#                                    (title, product_url, brand_id, category_id, source_name))
#                     conn.commit()
#                     product_id = cursor.lastrowid

#                 # --- 4. Insert into Prices ---
#                 if price_amount is not None:
#                     cursor.execute("UPDATE prices SET is_current = FALSE WHERE product_id = %s", (product_id,))
#                     cursor.execute("INSERT INTO prices (product_id, price_amount, currency, scrape_date, is_current) VALUES (%s, %s, %s, %s, %s)",
#                                    (product_id, price_amount, 'INR', date.today(), True))

#                 # --- 5. Handle Images ---
#                 if image_url is not None:
#                     cursor.execute("SELECT image_id FROM images WHERE image_url = %s", (image_url,))
#                     image_result = cursor.fetchone()
#                     if not image_result:
#                         cursor.execute("INSERT INTO images (product_id, image_url, is_thumbnail) VALUES (%s, %s, %s)",
#                                        (product_id, image_url, True))
                
#                 conn.commit()

#         print("\nAll data ingestion completed successfully!")

#     except mysql.connector.Error as err:
#         print(f"MySQL Error: {err}")
#         if conn:
#             conn.rollback()
#     except Exception as e:
#         print(f"An unexpected error occurred during ingestion: {e}")
#         if conn:
#             conn.rollback()
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
#         for source_name, config in HDFS_FILES_TO_INGEST.items():
#             local_path = config["local_path"]
#             if os.path.exists(local_path):
#                 os.remove(local_path)
#                 print(f"Removed temporary local CSV: {local_path}")

# if __name__ == "__main__":
#     ingest_to_mysql()



# ------------------------->>>>>>>>>>>>>>>>>>>>>video






















import pandas as pd
import mysql.connector
import os
from hdfs import InsecureClient
from datetime import date
import math
import re

HDFS_NAMENODE_URL = 'http://master-node:9870'

HDFS_FILES_TO_INGEST = {
    "amazon": {
        "hdfs_path": "/amz_data/data_amz.csv",
        "local_path": "temp_amazon_data.csv",
        "column_mapping": {
            "title": "Product_description",
            "link": "Product_url",
            "price": "Price",
            "image_url": "Image_url"
        }
    },
    "ebay": {
        "hdfs_path": "/ebay_data/data_ebay.csv",
        "local_path": "temp_ebay_data.csv",
        "column_mapping": {
            "title": "product_description",
            "link": "product_url",
            "price": "price",
            "image_url": "image_url"
        }
    },
    "newegg": {
        "hdfs_path": "/newegg_data/data/data_newegg.csv",
        "local_path": "temp_newegg_data.csv",
        "column_mapping": {
            "title": "Product Name",
            "link": "Product URL",
            "price": "Price",
            "image_url": "Image URL"
        }
    }
}

MYSQL_CONFIG = {
    'host': 'master-node',
    'user': 'lenovo_mysql',
    'password': '123789456',
    'database': 'product_listing'
}

try:
    hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user='rahul')
    print(f"Connected to HDFS NameNode at {HDFS_NAMENODE_URL} as user 'rahul'")
except Exception as e:
    print(f"Error connecting to HDFS: {e}")
    print("Please ensure your HDFS NameNode is running and the URL/port are correct.")
    exit()

def download_from_hdfs(hdfs_path, local_path):
    try:
        print(f"Downloading '{hdfs_path}' from HDFS to '{local_path}'...")
        with hdfs_client.read(hdfs_path) as reader:
            with open(local_path, 'wb') as writer:
                for chunk in reader:
                    writer.write(chunk)
        print(f"Download of '{hdfs_path}' complete.")
        return True
    except Exception as e:
        print(f"Error downloading '{hdfs_path}' from HDFS: {e}")
        print("Please ensure HDFS NameNode is running, URL/port are correct, and file exists.")
        return False

def ingest_to_mysql():
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        print("Successfully connected to MySQL database.")

        for source_name, config in HDFS_FILES_TO_INGEST.items():
            hdfs_path = config["hdfs_path"]
            local_path = config["local_path"]
            column_mapping = config["column_mapping"]

            if not download_from_hdfs(hdfs_path, local_path):
                print(f"Skipping ingestion for {source_name} due to HDFS download failure.")
                continue

            if not os.path.exists(local_path):
                print(f"Error: Local CSV file '{local_path}' not found after download. Cannot ingest.")
                continue

            print(f"\n--- Ingesting data from {source_name} (File: {local_path}) ---")
            df = pd.read_csv(local_path, encoding='utf-8')

            print(f"  Original columns in '{local_path}': {df.columns.tolist()}")

            missing_source_cols = []
            for target_col, source_col in column_mapping.items():
                if source_col not in df.columns:
                    missing_source_cols.append(source_col)

            if missing_source_cols:
                print(f"ERROR: For source '{source_name}', the following columns are missing in the CSV file based on your 'column_mapping': {missing_source_cols}")
                print("Please check your CSV file headers and update the 'column_mapping' in the script accordingly.")
                os.remove(local_path)
                continue

            df_renamed = df.rename(columns={
                column_mapping["title"]: "title",
                column_mapping["link"]: "link",
                column_mapping["price"]: "price",
                column_mapping["image_url"]: "image_url"
            })

            print(f"  Renamed columns for processing: {df_renamed.columns.tolist()}")

            for index, row in df_renamed.iterrows():
                title = row['title'] if 'title' in row and pd.notna(row['title']) else None
                product_url = row['link'] if 'link' in row and pd.notna(row['link']) else None
                image_url = row['image_url'] if 'image_url' in row and pd.notna(row['image_url']) else None

                price_amount = None
                if 'price' in row and pd.notna(row['price']):
                    raw_price_str = str(row['price']).strip()

                    price_match = re.search(r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)', raw_price_str)

                    if price_match:
                        extracted_price_str = price_match.group(1).strip()

                        if ',' in extracted_price_str and '.' in extracted_price_str:
                            cleaned_price_str = extracted_price_str.replace('.', '').replace(',', '.')
                        elif ',' in extracted_price_str:
                            cleaned_price_str = extracted_price_str.replace(',', '.')
                        else:
                            cleaned_price_str = extracted_price_str

                        cleaned_price_str = re.sub(r'[^\d.]', '', cleaned_price_str)

                        try:
                            price_amount = float(cleaned_price_str)
                        except ValueError:
                            print(f"Warning: Could not parse final cleaned price '{cleaned_price_str}' from original '{raw_price_str}' for title '{title}'. Skipping price.")
                    else:
                        print(f"Warning: No valid number found in price string '{raw_price_str}' for title '{title}'. Skipping price.")

                if title is None or product_url is None:
                    print(f"Skipping row {index} from {source_name} due to missing title or product_url.")
                    continue

                brand_name = "Unknown"
                if title is not None:
                    common_brands = ["Acer", "HP", "Dell", "Lenovo", "ASUS", "MSI", "Apple", "Samsung", "Microsoft", "Razer"]
                    for brand in common_brands:
                        if brand.lower() in title.lower():
                            brand_name = brand
                            break

                cursor.execute("SELECT brand_id FROM brands WHERE brand_name = %s", (brand_name,))
                brand_result = cursor.fetchone()
                if brand_result:
                    brand_id = brand_result[0]
                else:
                    cursor.execute("INSERT INTO brands (brand_name) VALUES (%s)", (brand_name,))
                    conn.commit()
                    brand_id = cursor.lastrowid

                category_name = "Laptop"
                if title is not None:
                    title_lower = title.lower()
                    if "gaming" in title_lower:
                        category_name = "Gaming Laptop"
                    elif "ultrabook" in title_lower or "thin and light" in title_lower:
                        category_name = "Ultrabook"
                    elif "2-in-1" in title_lower or "convertible" in title_lower:
                        category_name = "2-in-1 Laptop"
                    elif "chromebook" in title_lower:
                        category_name = "Chromebook"
                    elif "macbook" in title_lower or "apple" in title_lower:
                        category_name = "MacBook"

                cursor.execute("SELECT category_id FROM categories WHERE category_name = %s", (category_name,))
                category_result = cursor.fetchone()
                if category_result:
                    category_id = category_result[0]
                else:
                    cursor.execute("INSERT INTO categories (category_name) VALUES (%s)", (category_name,))
                    conn.commit()
                    category_id = cursor.lastrowid

                cursor.execute("SELECT product_id FROM products WHERE product_url = %s", (product_url,))
                product_result = cursor.fetchone()
                if product_result:
                    product_id = product_result[0]
                    cursor.execute("UPDATE products SET title=%s, brand_id=%s, category_id=%s, source_name=%s, last_updated_at=NOW() WHERE product_id=%s",
                                   (title, brand_id, category_id, source_name, product_id))
                else:
                    cursor.execute("INSERT INTO products (title, product_url, brand_id, category_id, source_name) VALUES (%s, %s, %s, %s, %s)",
                                   (title, product_url, brand_id, category_id, source_name))
                    conn.commit()
                    product_id = cursor.lastrowid

                if price_amount is not None:
                    cursor.execute("UPDATE prices SET is_current = FALSE WHERE product_id = %s", (product_id,))
                    cursor.execute("INSERT INTO prices (product_id, price_amount, currency, scrape_date, is_current) VALUES (%s, %s, %s, %s, %s)",
                                   (product_id, price_amount, 'INR', date.today(), True))

                if image_url is not None:
                    cursor.execute("SELECT image_id FROM images WHERE image_url = %s", (image_url,))
                    image_result = cursor.fetchone()
                    if not image_result:
                        cursor.execute("INSERT INTO images (product_id, image_url, is_thumbnail) VALUES (%s, %s, %s)",
                                       (product_id, image_url, True))

                conn.commit()

        print("\nAll data ingestion completed successfully!")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during ingestion: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        for source_name, config in HDFS_FILES_TO_INGEST.items():
            local_path = config["local_path"]
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"Removed temporary local CSV: {local_path}")

if __name__ == "__main__":
    ingest_to_mysql()










