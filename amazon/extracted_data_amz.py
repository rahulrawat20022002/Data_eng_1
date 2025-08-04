# from bs4 import BeautifulSoup
# import os
# import pandas as pd
# import urllib.parse
# import html # For unescaping HTML entities

# # --- 1. Define the full path to your data folder ---
# # Make sure this path is correct and contains your HTML files.
# DATA_FOLDER_PATH = "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_amazon"

# # --- 2. Initialize Data Storage with requested columns ---
# extracted_data = {
#     'Product_description': [],
#     'Product_url': [],
#     'Image_url': [],
#     'Price': [],
#     'Delivery_time': [],
#     'Product_Rating': [],
#     'previous_bought':[],
#     'Discount':[],
#     'delivery_type':[],
#     'fastest_delivery':[]
# }

# # --- 3. Process Each HTML File in the Folder ---
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

#                 # --- Extract Product Description (Title) ---
#                 description_h2 = soup.find('h2', class_='a-size-medium a-spacing-none a-color-base a-text-normal')
#                 product_description = description_h2.get_text(strip=True) if description_h2 else "N/A"
#                 print(f"    Product Description: {product_description}")

#                 # --- Extract Product URL ---
#                 product_link_tag = soup.find('a', class_='a-link-normal s-line-clamp-2')
#                 product_url = "N/A"
#                 if product_link_tag and 'href' in product_link_tag.attrs:
#                     raw_href = product_link_tag['href']
#                     if raw_href.startswith('/'):
#                         product_url = "https://amazon.in" + html.unescape(raw_href)
#                     else:
#                         product_url = html.unescape(raw_href)
#                 print(f"    Product URL: {product_url}")

#                 # --- Extract Image URL ---
#                 image_tag = soup.find('img', class_='s-image')
#                 image_url = image_tag['src'] if image_tag and 'src' in image_tag.attrs else "N/A"
#                 print(f"    Image URL: {image_url}")

#                 # --- Extract Price ---
#                 price_offscreen_span = soup.find('span', class_='a-price')
#                 price = price_offscreen_span.find('span', class_='a-offscreen').get_text(strip=True) if price_offscreen_span and price_offscreen_span.find('span', class_='a-offscreen') else "N/A"
#                 print(f"    Price: {price}")
                
#                 # --- Extract Product Rating ---
#                 product_rating = "N/A"
#                 stars_tag = soup.find('i', class_='a-icon-star-small')
#                 if stars_tag:
#                     stars_text_span = stars_tag.find('span', class_='a-icon-alt')
#                     if stars_text_span:
#                         product_rating = stars_text_span.get_text(strip=True)
#                 print(f"    Product Rating: {product_rating}")

#                 # --- Extract 'previous_bought' (e.g., "10K+ bought in past month") ---
#                 previous_bought = "N/A"
#                 bought_in_past_month_tag = soup.find('div', class_='a-row a-size-base')
#                 if bought_in_past_month_tag:
#                     bought_text_span = bought_in_past_month_tag.find('span', class_='a-size-base a-color-secondary')
#                     if bought_text_span and 'bought in past month' in bought_text_span.get_text():
#                         previous_bought = bought_text_span.get_text(strip=True)
#                 print(f"    Previous Bought: {previous_bought}")

#                 # --- Extract Discount ---
#                 discount = "N/A"
#                 # The discount percentage is often in a <span> directly following the price link's <a> tag
#                 # specifically, after a <span class="a-letter-space">
#                 product_price_link = soup.find('a', class_='a-link-normal s-no-hover s-underline-text s-underline-link-text s-link-style a-text-normal')
#                 if product_price_link:
#                     # Find the immediate next sibling which is a span (a-letter-space), then its next sibling
#                     next_sibling_after_price_link = product_price_link.find_next_sibling('span', class_='a-letter-space')
#                     if next_sibling_after_price_link:
#                         actual_discount_span = next_sibling_after_price_link.find_next_sibling('span')
#                         if actual_discount_span and '%' in actual_discount_span.get_text():
#                             discount = actual_discount_span.get_text(strip=True)
#                 print(f"    Discount: {discount}")

#                 # --- Extract Delivery_time, delivery_type, fastest_delivery ---
#                 general_delivery_time = "N/A"
#                 delivery_type_val = "N/A" # Renamed to avoid conflict with function
#                 fastest_delivery_info = "N/A"

#                 # General Delivery Time (e.g., "FREE delivery Wed, 9 Jul")
#                 main_delivery_span = soup.find('span', attrs={'aria-label': lambda x: x and 'FREE delivery' in x})
#                 if main_delivery_span:
#                     general_delivery_time = main_delivery_span['aria-label'].strip()
#                     delivery_type_val = "FREE Delivery"
                
#                 # Check for Prime badge to refine delivery_type
#                 prime_icon = soup.find('i', class_='a-icon-prime')
#                 if prime_icon:
#                     if delivery_type_val == "FREE Delivery":
#                         delivery_type_val = "FREE Prime Delivery"
#                     elif delivery_type_val == "N/A": # If not already free, just Prime
#                         delivery_type_val = "Prime Delivery"

#                 # Fastest Delivery (e.g., "Or fastest delivery Tomorrow, 7 Jul")
#                 fastest_delivery_span = soup.find('span', attrs={'aria-label': lambda x: x and 'fastest delivery' in x})
#                 if fastest_delivery_span:
#                     fastest_delivery_info = fastest_delivery_span['aria-label'].strip()

#                 print(f"    Delivery Time (General): {general_delivery_time}")
#                 print(f"    Delivery Type: {delivery_type_val}")
#                 print(f"    Fastest Delivery Info: {fastest_delivery_info}")


#                 # Append extracted data to the dictionary
#                 extracted_data['Product_description'].append(product_description)
#                 extracted_data['Product_url'].append(product_url)
#                 extracted_data['Image_url'].append(image_url)
#                 extracted_data['Price'].append(price)
#                 extracted_data['Delivery_time'].append(general_delivery_time)
#                 extracted_data['Product_Rating'].append(product_rating)
#                 extracted_data['previous_bought'].append(previous_bought)
#                 extracted_data['Discount'].append(discount)
#                 extracted_data['delivery_type'].append(delivery_type_val)
#                 extracted_data['fastest_delivery'].append(fastest_delivery_info)

#             except Exception as e:
#                 print(f"    Error processing {filename}: {e}")

# # --- 4. Create DataFrame and Store Locally as CSV ---
# if extracted_data['Product_description']: # Check if any data was extracted
#     df = pd.DataFrame(data=extracted_data)

#     # Define the path for the local CSV file
#     local_csv_path = "amazon_extracted_products_data.csv"
#     df.to_csv(local_csv_path, index=False, encoding='utf-8')
#     print(f"\nSuccessfully created local CSV file at: {local_csv_path}")
# else:
#     print("\nNo data was extracted from any HTML files. No CSV file created.")



from bs4 import BeautifulSoup
import os
import pandas as pd
import urllib.parse
import html # For unescaping HTML entities

# --- 1. Define the full path to your data folder ---
# Make sure this path is correct and contains your HTML files.
DATA_FOLDER_PATH = "C:\\Users\\rahul\\OneDrive\\Desktop\\Data engineering 1\\data_amazon"

# --- 2. Initialize Data Storage with requested columns ---
extracted_data = {
    'Product_description': [],
    'Product_url': [],
    'Image_url': [],
    'Price': [],
    'Delivery_time': [],
    'Product_Rating': [],
    'previous_bought':[],
    'Discount':[],
    'delivery_type':[],
    'fastest_delivery':[]
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
                description_h2 = soup.find('h2', class_='a-size-medium a-spacing-none a-color-base a-text-normal')
                product_description = description_h2.get_text(strip=True) if description_h2 else "N/A"
                print(f"    Product Description: {product_description}")

                # --- Extract Product URL (Updated based on your suggestion) ---
                product_url = "N/A"
                # Find the main product card container
                main_product_card = soup.find('div', class_='puis-card-container')
                
                if main_product_card:
                    # Find the anchor tag within this product card
                    link_tag = main_product_card.find('a', class_='a-link-normal')
                    if link_tag and 'href' in link_tag.attrs:
                        raw_href = link_tag['href']
                        if raw_href.startswith('/'):
                            # Ensure absolute URL for Amazon India (adjust domain if scraping other regions)
                            product_url = "https://amazon.in" + html.unescape(raw_href)
                        else:
                            product_url = html.unescape(raw_href)
                print(f"    Product URL: {product_url}")

                # --- Extract Image URL ---
                image_tag = soup.find('img', class_='s-image')
                image_url = image_tag['src'] if image_tag and 'src' in image_tag.attrs else "N/A"
                print(f"    Image URL: {image_url}")

                # --- Extract Price ---
                price_offscreen_span = soup.find('span', class_='a-price')
                price = price_offscreen_span.find('span', class_='a-offscreen').get_text(strip=True) if price_offscreen_span and price_offscreen_span.find('span', class_='a-offscreen') else "N/A"
                print(f"    Price: {price}")
                
                # --- Extract Product Rating ---
                product_rating = "N/A"
                stars_tag = soup.find('i', class_='a-icon-star-small')
                if stars_tag:
                    stars_text_span = stars_tag.find('span', class_='a-icon-alt')
                    if stars_text_span:
                        product_rating = stars_text_span.get_text(strip=True)
                print(f"    Product Rating: {product_rating}")

                # --- Extract 'previous_bought' (e.g., "10K+ bought in past month") ---
                previous_bought = "N/A"
                bought_in_past_month_tag = soup.find('div', class_='a-row a-size-base')
                if bought_in_past_month_tag:
                    bought_text_span = bought_in_past_month_tag.find('span', class_='a-size-base a-color-secondary')
                    if bought_text_span and 'bought in past month' in bought_text_span.get_text():
                        previous_bought = bought_text_span.get_text(strip=True)
                print(f"    Previous Bought: {previous_bought}")

                # --- Extract Discount ---
                discount = "N/A"
                product_price_link = soup.find('a', class_='a-link-normal s-no-hover s-underline-text s-underline-link-text s-link-style a-text-normal')
                if product_price_link:
                    next_sibling_after_price_link = product_price_link.find_next_sibling('span', class_='a-letter-space')
                    if next_sibling_after_price_link:
                        actual_discount_span = next_sibling_after_price_link.find_next_sibling('span')
                        if actual_discount_span and '%' in actual_discount_span.get_text():
                            discount = actual_discount_span.get_text(strip=True)
                print(f"    Discount: {discount}")

                # --- Extract Delivery_time, delivery_type, fastest_delivery ---
                general_delivery_time = "N/A"
                delivery_type_val = "N/A"
                fastest_delivery_info = "N/A"

                main_delivery_span = soup.find('span', attrs={'aria-label': lambda x: x and 'FREE delivery' in x})
                if main_delivery_span:
                    general_delivery_time = main_delivery_span['aria-label'].strip()
                    delivery_type_val = "FREE Delivery"
                
                prime_icon = soup.find('i', class_='a-icon-prime')
                if prime_icon:
                    if delivery_type_val == "FREE Delivery":
                        delivery_type_val = "FREE Prime Delivery"
                    elif delivery_type_val == "N/A":
                        delivery_type_val = "Prime Delivery"

                fastest_delivery_span = soup.find('span', attrs={'aria-label': lambda x: x and 'fastest delivery' in x})
                if fastest_delivery_span:
                    fastest_delivery_info = fastest_delivery_span['aria-label'].strip()

                print(f"    Delivery Time (General): {general_delivery_time}")
                print(f"    Delivery Type: {delivery_type_val}")
                print(f"    Fastest Delivery Info: {fastest_delivery_info}")


                # Append extracted data to the dictionary
                extracted_data['Product_description'].append(product_description)
                extracted_data['Product_url'].append(product_url)
                extracted_data['Image_url'].append(image_url)
                extracted_data['Price'].append(price)
                extracted_data['Delivery_time'].append(general_delivery_time)
                extracted_data['Product_Rating'].append(product_rating)
                extracted_data['previous_bought'].append(previous_bought)
                extracted_data['Discount'].append(discount)
                extracted_data['delivery_type'].append(delivery_type_val)
                extracted_data['fastest_delivery'].append(fastest_delivery_info)

            except Exception as e:
                print(f"    Error processing {filename}: {e}")

# --- 4. Create DataFrame and Store Locally as CSV ---
if extracted_data['Product_description']: # Check if any data was extracted
    df = pd.DataFrame(data=extracted_data)

    # Define the path for the local CSV file
    local_csv_path = "amazon_extracted_products_data.csv"
    df.to_csv(local_csv_path, index=False, encoding='utf-8')
    print(f"\nSuccessfully created local CSV file at: {local_csv_path}")
else:
    print("\nNo data was extracted from any HTML files. No CSV file created.")
