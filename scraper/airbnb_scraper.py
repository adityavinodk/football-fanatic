import asyncio
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# Global variable to keep track of the unique ID
id_counter = 1


async def scrape_airbnb(city_name, country, checkin_date, checkout_date):
    global id_counter  # Access the global variable
    async with async_playwright() as pw:
        # Launch new browser
        # CHANGE HEADLESS TO SEE BROWSERS
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        # Prepare Airbnb URL for the city
        city_url = f"https://www.airbnb.com/s/{city_name}--{country}/homes?checkin={checkin_date}&checkout={checkout_date}&adults=1"
        try:
            # Go to Airbnb URL
            await page.goto(city_url, timeout=600000)
            # Wait for the listings to load
            await page.wait_for_selector("div.g1qv1ctd.c1v0rf5q.dir.dir-ltr")
            await asyncio.sleep(2)
            # Extract information
            results = []
            listings = await page.query_selector_all(
                "div.g1qv1ctd.c1v0rf5q.dir.dir-ltr"
            )
            for listing in listings:
                result = {}
                # Generate unique ID
                result["id"] = id_counter
                id_counter += 1
                # Property name
                name_element = await listing.query_selector(
                    'div[data-testid="listing-card-title"]'
                )
                if name_element:
                    result["property_name"] = await page.evaluate(
                        "(el) => el.textContent", name_element
                    )
                else:
                    result["property_name"] = "N/A"
                # Location
                location_element = await listing.query_selector(
                    'div[data-testid="listing-card-subtitle"]'
                )
                result["location"] = (
                    await location_element.inner_text() if location_element else "N/A"
                )
                # Price
                price_element = await listing.query_selector("div._1jo4hgw")
                result["price"] = (
                    await price_element.inner_text() if price_element else "N/A"
                )
                results.append(result)
            # Close browser
            await browser.close()
            return results
        except Exception as e:
            print(f"Error occurred while scraping {city_name}, {country}: {e}")
            # Close browser on error
            await browser.close()
            return []


async def main():
    global id_counter  # Reset ID counter for each run
    id_counter = 1

    with open("cities.txt", "r") as city_file, open("dates.txt", "r") as date_file:
        cities = [line.strip() for line in city_file.readlines()]
        dates = [line.strip().split(", ") for line in date_file.readlines()]

    all_results = []
    print("Starting...")
    for city in cities:
        city_name, country = city.split(", ")
        for checkin_date, checkout_date in dates:
            # print("abc")
            checkin = datetime.strptime(checkin_date, "%m.%d")
            checkout = datetime.strptime(checkout_date, "%m.%d")
            checkin = checkin.replace(year=2024)
            checkout = checkout.replace(year=2024)
            # print(checkin)
            # print(checkout)
            checkin_scrape = checkin.strftime("%Y-%m-%d")
            checkout_scrape = checkout.strftime("%Y-%m-%d")
            # Skip if check-in date is before the current date
            today_midnight = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            if checkin < today_midnight:
                # print("Skipped!")
                continue
            # Scrape Airbnb listings for each city
            results = await scrape_airbnb(
                city_name,
                country,
                checkin_scrape,
                checkout_scrape,
            )
            access_date = datetime.now().strftime("%Y-%m-%d")  # Current date
            for result in results:
                result["city"] = city_name
                result["country"] = country
                result["access_date"] = access_date
                result["checkin_date"] = checkin_scrape
                result["checkout_date"] = checkout_scrape
            print(
                f"Scraped {city_name} with checkin {checkin_scrape} and checkout {checkout_scrape}"
            )
            all_results.extend(results)

    df = pd.DataFrame(all_results)
    df.to_csv("airbnb_listings.csv", index=False, mode="a", header=not id_counter > 1)
    print("Finished!")


asyncio.run(main())
