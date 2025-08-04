# from bs4 import BeautifulSoup
# import os
# import pandas as pd
# import urllib.parse
# import html
# from hdfs import InsecureClient # Import the HDFS client library

# # --- 1. Define the full path to your data folder ---
# DATA_FOLDER_PATH = "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_ebay"

# # --- 2. HDFS Configuration ---
# # Replace 'your_namenode_ip_or_hostname' with the actual IP address or hostname of your HDFS NameNode.
# # The default WebHDFS port is usually 9870 or 50070.
# # HDFS_NAMENODE_URL = 'http://master-node:9870' # Example: 'http://192.168.1.100:9870'
# # HDFS_OUTPUT_DIR = '/crawled_data/ebay_data' # Path in HDFS where data will be stored
# # HDFS_OUTPUT_FILENAME = 'ebay_data.csv' # Name of the CSV file in HDFS

# # Initialize HDFS client
# # try:
# #     client = InsecureClient(HDFS_NAMENODE_URL)
# #     print(f"Connected to HDFS NameNode at {HDFS_NAMENODE_URL}")
# # except Exception as e:
# #     print(f"Error connecting to HDFS: {e}")
# #     print("Please ensure your HDFS NameNode is running and the URL/port are correct.")
# #     # Exit or handle the error appropriately if HDFS connection is critical
# #     exit()

# # --- 3. Initialize Data Storage ---
# extracted_data = {'product_url': [], 'image_url': [], 'price': [], 'product_description': [],'delivery_time': [],'pickup_availability': [],'cancellation_policy': [],'product_status': [],'product_views': []}

# # --- 4. Process Each HTML File ---
# if not os.path.exists(DATA_FOLDER_PATH):
#     print(f"Error: The folder '{DATA_FOLDER_PATH}' was not found.")
#     print("Please ensure this path is correct and the folder exists.")
# else:
#     for filename in os.listdir(DATA_FOLDER_PATH):
#         if filename.endswith(".html"):
#             file_full_path = os.path.join(DATA_FOLDER_PATH, filename)
#             print(f"Processing file: {file_full_path}")

#             try:
#                 with open(file_full_path, 'r', encoding='utf-8') as f:
#                     html_doc = f.read()

#                 soup = BeautifulSoup(html_doc, 'html.parser')

#                 t = soup.find("h2")
#                 title = t.get_text(strip=True) if t else "N/A"
#                 print(f"  Title: {title}")

#                 product_link_tag = soup.find("a", class_="a-link-normal s-line-clamp-2 s-line-clamp-3-for-col-12 s-link-style a-text-normal")
                
#                 link = "N/A"
#                 if product_link_tag and 'href' in product_link_tag.attrs:
#                     raw_href = product_link_tag['href']
#                     unescaped_href = html.unescape(raw_href)
                    
#                     if "/sspa/click" in unescaped_href or "/gp/redirect.html" in unescaped_href:
#                         parsed_url_components = urllib.parse.urlparse(unescaped_href)
#                         query_parameters = urllib.parse.parse_qs(parsed_url_components.query)
                        
#                         if 'url' in query_parameters and query_parameters['url']:
#                             encoded_product_path = query_parameters['url'][0]
#                             decoded_product_path = urllib.parse.unquote(encoded_product_path)
                            
#                             if decoded_product_path.startswith('http://') or decoded_product_path.startswith('https://'):
#                                 link = decoded_product_path
#                             else:
#                                 link = "https://amazon.in" + decoded_product_path
#                         else:
#                             print(f"    Warning: Click-tracking link found without 'url' parameter: {unescaped_href}")
#                             link = "N/A (Click-tracking, URL param missing)"
#                     else:
#                         if unescaped_href.startswith('/'):
#                             link = "https://amazon.in" + unescaped_href
#                         else:
#                             if unescaped_href.startswith('http://') or unescaped_href.startswith('https://'):
#                                 link = unescaped_href
#                             else:
#                                 link = "N/A (Unexpected link format)"
                
#                 print(f"  Link: {link}")

#                 p = soup.find("span", attrs={'class': 'a-price'})
#                 price = p.get_text(strip=True) if p else "N/A"
#                 print(f"  Price: {price}")

#                 image_tag = soup.find("img", class_="s-image")
#                 image_url = image_tag['src'] if image_tag and 'src' in image_tag.attrs else "N/A"
#                 print(f"  Image URL: {image_url}")

#                 extracted_data['title'].append(title)
#                 extracted_data['price'].append(price)
#                 extracted_data['link'].append(link)
#                 extracted_data['image_url'].append(image_url)

#             except Exception as e:
#                 print(f"  Error processing {filename}: {e}")

# # --- 5. Create DataFrame and Store in HDFS ---
# if extracted_data['title']:
#     df = pd.DataFrame(data=extracted_data)
    
#     # Create a temporary local CSV file
#     local_csv_path = "temp_laptops_data.csv"
#     df.to_csv(local_csv_path, index=False, encoding='utf-8')
#     print(f"\nLocal CSV created at: {local_csv_path}")

#     # Ensure the HDFS directory exists
#     try:
#         client.makedirs(HDFS_OUTPUT_DIR, permission=755) # Create directory if it doesn't exist
#         print(f"HDFS directory '{HDFS_OUTPUT_DIR}' ensured.")
#     except Exception as e:
#         print(f"Error creating HDFS directory: {e}")
#         # Continue, as the directory might already exist

#     # Upload the CSV file to HDFS
#     hdfs_file_path = os.path.join(HDFS_OUTPUT_DIR, HDFS_OUTPUT_FILENAME)
#     try:
#         # The 'overwrite=True' argument will replace the file if it already exists.
#         # Use 'overwrite=False' if you want to append or handle existing files differently.
#         client.upload(hdfs_file_path, local_csv_path, overwrite=True)
#         print(f"Successfully uploaded '{local_csv_path}' to HDFS at '{hdfs_file_path}'")
#     except Exception as e:
#         print(f"Error uploading to HDFS: {e}")
#     finally:
#         # Clean up the temporary local CSV file
#         if os.path.exists(local_csv_path):
#             os.remove(local_csv_path)
#             print(f"Removed temporary local CSV: {local_csv_path}")
# else:
#     print("\nNo data was extracted. No CSV to store in HDFS.")







# -------------------------------------------------



from bs4 import BeautifulSoup
import os
import pandas as pd
import urllib.parse
import html # For unescaping HTML entities

# --- 1. Define the full path to your data folder ---
# Make sure this path is correct and contains your HTML files.
DATA_FOLDER_PATH = "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_ebay"

# --- 2. Initialize Data Storage ---
extracted_data = {
    'product_description': [],
    'product_url': [],
    'image_url': [],
    'price': [],
    'delivery_time': [],
    'pickup_availability': [],
    'cancellation_policy': [],
    'product_status': [],
    'product_views': []
}

# --- 3. Process Each HTML File in the Folder ---
if not os.path.exists(DATA_FOLDER_PATH):
    print(f"Error: The folder '{DATA_FOLDER_PATH}' was not found.")
    print("Please ensure this path is correct and the folder exists.")
else:
    for filename in os.listdir(DATA_FOLDER_PATH):
        if filename.endswith(".html"):
            file_full_path = os.path.join(DATA_FOLDER_PATH, filename)
            print(f"Processing file: {file_full_path}")

            try:
                with open(file_full_path, 'r', encoding='utf-8') as f:
                    html_doc = f.read()

                soup = BeautifulSoup(html_doc, 'html.parser')

                # --- Extract Product Description (Title) ---
                # Find the span with role="heading" inside the s-item__title div
                description_tag = soup.find('div', class_='s-item__title')
                product_description = "N/A"
                if description_tag:
                    heading_span = description_tag.find('span', role='heading')
                    product_description = heading_span.get_text(strip=True) if heading_span else "N/A"
                print(f"  Product Description: {product_description}")

                # --- Extract Product URL ---
                # Find the 'href' attribute of the main product link (class="s-item__link")
                product_link_tag = soup.find('a', class_='s-item__link')
                product_url = "N/A"
                if product_link_tag and 'href' in product_link_tag.attrs:
                    raw_href = product_link_tag['href']
                    # Unescape HTML entities in the URL to get the clean link
                    product_url = html.unescape(raw_href)
                print(f"  Product URL: {product_url}")

                # --- Extract Image URL ---
                # Find the 'src' attribute of the img tag within the image wrapper
                image_wrapper = soup.find('div', class_='s-item__image-wrapper')
                image_url = "N/A"
                if image_wrapper:
                    image_tag = image_wrapper.find('img')
                    image_url = image_tag['src'] if image_tag and 'src' in image_tag.attrs else "N/A"
                print(f"  Image URL: {image_url}")

                # --- Extract Price ---
                # Find the span with class="s-item__price"
                price_tag = soup.find('span', class_='s-item__price')
                price = price_tag.get_text(strip=True) if price_tag else "N/A"
                print(f"  Price: {price}")

                # --- Extract Delivery Time ---
                # Find the span with class="s-item__dynamic s-item__freeXDays"
                delivery_time_tag = soup.find('span', class_='s-item__dynamic s-item__freeXDays')
                delivery_time = delivery_time_tag.get_text(strip=True) if delivery_time_tag else "N/A"
                print(f"  Delivery Time: {delivery_time}")

                # --- Extract Pickup Availability ---
                # Find the span with class="s-item__delivery-options s-item__deliveryOptions"
                pickup_tag = soup.find('span', class_='s-item__delivery-options')
                pickup_availability = pickup_tag.get_text(strip=True) if pickup_tag else "N/A"
                print(f"  Pickup Availability: {pickup_availability}")

                # --- Extract Cancellation Policy ---
                # Find the span with class="s-item__free-returns s-item__freeReturnsNoFee"
                cancellation_tag = soup.find('span', class_='s-item__free-returns')
                cancellation_policy = cancellation_tag.get_text(strip=True) if cancellation_tag else "N/A"
                print(f"  Cancellation Policy: {cancellation_policy}")

                # --- Extract Product Status ---
                # Find the span with class "s-item__hotness s-item__authorized-seller" and then the bold text inside it
                status_parent_tag = soup.find('span', class_='s-item__hotness')
                product_status = "N/A"
                if status_parent_tag:
                    status_bold_tag = status_parent_tag.find('span', class_='BOLD')
                    product_status = status_bold_tag.get_text(strip=True) if status_bold_tag else "N/A"
                print(f"  Product Status: {product_status}")

                # --- Extract Product Views ---
                # Find the span with class "s-item__dynamic s-item__watchCountTotal" and then the bold text inside it
                views_parent_tag = soup.find('span', class_='s-item__dynamic s-item__watchCountTotal')
                product_views = "N/A"
                if views_parent_tag:
                    views_bold_tag = views_parent_tag.find('span', class_='BOLD')
                    product_views = views_bold_tag.get_text(strip=True) if views_bold_tag else "N/A"
                print(f"  Product Views: {product_views}")


                # Append extracted data
                extracted_data['product_description'].append(product_description)
                extracted_data['product_url'].append(product_url)
                extracted_data['image_url'].append(image_url)
                extracted_data['price'].append(price)
                extracted_data['delivery_time'].append(delivery_time)
                extracted_data['pickup_availability'].append(pickup_availability)
                extracted_data['cancellation_policy'].append(cancellation_policy)
                extracted_data['product_status'].append(product_status)
                extracted_data['product_views'].append(product_views)

            except Exception as e:
                print(f"  Error processing {filename}: {e}")

# --- 4. Create DataFrame and Store Locally as CSV ---
if extracted_data['product_description']: # Check if any data was extracted
    df = pd.DataFrame(data=extracted_data)

    # Define the path for the local CSV file
    local_csv_path = "ebay_extracted_products_data.csv"
    df.to_csv(local_csv_path, index=False, encoding='utf-8')
    print(f"\nSuccessfully created local CSV file at: {local_csv_path}")
else:
    print("\nNo data was extracted from any HTML files. No CSV file created.")