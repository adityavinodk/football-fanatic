import pandas as pd
import re


# Function to extract the last numeric value from the price string
def extract_price(price_str):
    # Find all numeric values
    prices = re.findall(r"\$\s*(\d+)", price_str)
    # Return the last numeric value if exists, otherwise return None
    return int(prices[-1]) if prices else None


# Load the CSV file to examine its structure
file_path = "./airbnb_listings.csv"
data = pd.read_csv(file_path)

# Display the first few rows and the column names to understand the structure
data.head(), data.columns

# Reload the CSV, treating the first row as the header
data = pd.read_csv(file_path, header=None)

# Setting appropriate column names
data.columns = [
    "id",
    "listing",
    "desc",
    "price",
    "city",
    "country",
    "info_date",
    "checkin_date",
    "checkout_date",
]

# Display the first few rows to verify the correction
data.head()

# Remove newline characters from the 'Price per Night' column
print(f"Cleaning desc column")
data["desc"] = data["desc"].replace(to_replace=r"\n", value=" ", regex=True)
print(f"Cleaning price column")
data["price"] = data["price"].replace(to_replace=r"\n", value=" ", regex=True)
# data["price"] = data["price"].apply(extract_price)
print(f"Cleaning checkout_date column")
# data["checkout_date"] = data["checkout_date"].str.split().str[0]
# Display the cleaned data to verify the changes
print(data.head())

# Save the cleaned data to a new CSV file
cleaned_file_path = "./airbnb_listings_new.csv"
data.to_csv(cleaned_file_path, index=False)
print(f"File saved!")

# For some reason this is necessary
cleaned_file_path
