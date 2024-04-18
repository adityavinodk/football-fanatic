from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests
import pandas as pd
import os


# Configuration and date setup
# Set start and end dates for the data collection period.
start_date = datetime(2024, 4, 18)
end_date = datetime(2024, 6, 2)
info_date = '2024-04-18'
city = "Manchester"


# Dictionary to store all hotel data for eventual export to DataFrame.
hotel_data = {
    'name': [],
    'address': [],
    'city': [],
    'country': [],
    'avg_review': [],
    'facility': [],
    'property_type': [],
    'size': [],
    'checkin': [],
    'checkout': [],
    'price': [],
    'info_date': []
}


# Iterate over each date between the start and end dates.
current_date = start_date
while current_date <= end_date:
    date = current_date.strftime("%Y-%m-%d")
    date_object = datetime.strptime(date, "%Y-%m-%d")

    # Target weekend dates for scraping (Friday to Sunday).
    if date_object.weekday() >= 4:  # 0 = Monday, 6 = Sunday
        checkout = date
        start = date_object.weekday() - 3
        for j in range(start, date_object.weekday()+1):
            checkin = current_date - timedelta(days=j)
            checkin = checkin.strftime("%Y-%m-%d")
            pageNumber = 0
            while True:
                
                '''
                    Use a cURL-to-Python converter to transform cURL commands into Python requests code. 
                    Insert the Python requests code here.
                '''

                # Send a POST request to retrieve hotel data.
                response = requests.post('https://www.agoda.com/graphql/search', cookies=cookies, headers=headers, json=json_data)
                if response.status_code != 200:
                    print("HTTP request failed with status code: {0}. Unable to retrieve data for date range: {1} to {2}".format(response.status_code, checkin, checkout))
                    break
                properties = response.json().get('data', {}).get('citySearch', {}).get('properties', [])

                # Handle case where no properties were found.
                if len(properties) == 0:
                    print("No properties found for the dates {0} to {1}. This may indicate a data availability issue or an empty search result.".format(checkin, checkout))
                    break
                if pageNumber % 5 == 0:
                    print("Processing page number: {0} for check-in date: {1} and check-out date: {2}.".format(pageNumber, checkin, checkout))
                pageNumber += 1

                # Process each property found.
                for item in properties:
                    # Collecting and storing various pieces of information about each hotel.
                    hotel_data['name'].append(item.get('content', {}).get('informationSummary', {}).get('defaultName', ''))
                    hotel_data['country'].append(item.get('content', {}).get('informationSummary', {}).get('address', {}).get('country', {}).get('name', ''))
                    hotel_data['city'].append(item.get('content', {}).get('informationSummary', {}).get('address', {}).get('city', {}).get('name', ''))
                    hotel_data['address'].append(item.get('content', {}).get('informationSummary', {}).get('address', {}).get('area', {}).get('name', ''))
                    hotel_data['property_type'].append(item.get('content', {}).get('informationSummary', {}).get('propertyType', ''))
                    hotel_data['avg_review'].append(item.get('content', {}).get('informationSummary', {}).get('rating', 0.0))
                    hotel_data['size'].append(item.get('enrichment', {}).get('roomInformation', {}).get('cheapestRoomSizeSqm', 0.0))
                    facilities = item.get('enrichment', {}).get('roomInformation', {}).get('facilities', [])
                    hotel_data['facility'].append([f['propertyFacilityName'] for f in facilities if 'propertyFacilityName' in f])
                    try:
                        price = (item.get('pricing', {})
                                        .get('offers', [{}])[0]
                                        .get('roomOffers', [{}])[0]
                                        .get('room', {})
                                        .get('pricing', [{}])[0]
                                        .get('price', {})
                                        .get('perNight', {})
                                        .get('exclusive', {})
                                        .get('display', 0.0))
                    except IndexError:
                        price = 0.0
                    hotel_data['price'].append(price)
                    hotel_data['checkin'].append(checkin)
                    hotel_data['checkout'].append(checkout)
                    hotel_data['info_date'].append(info_date)
    # Move to the next day
    current_date += timedelta(days=1)


# Create DataFrame from hotel_data dictionary
df = pd.DataFrame(hotel_data)

# Directory and file handling
if not os.path.exists('/Users/leo/Desktop/scrape/{}'.format(city)):
    os.makedirs('/Users/leo/Desktop/scrape/{}'.format(city))

# Save the DataFrame to an Excel file
df.to_excel('/Users/leo/Desktop/scrape/{}/scrape.xlsx'.format(city), index=False)

# Save the DataFrame to a Parquet file
df.to_parquet('/Users/leo/Desktop/scrape/{}/scrape.parquer'.format(city), index=False)
