import yfinance as yf
from datetime import datetime, timedelta

def fetch_usd_to_eur_exchange_rates(days=60):
    # Calculate the start and end dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Set the ticker for USD to EUR exchange rates
    ticker = 'USDEUR=X'

    # Fetch historical data from Yahoo Finance
    data = yf.download(ticker, start=start_date, end=end_date)

    # take the average of the open and close prices
    data['avg'] = (data['Open'] + data['Close']) / 2

    # add weekend dates, pair them with the previous Friday's rate
    data = data.resample('D').ffill()



    # drop all unnecessary columns, keeping only the average rate and the date
    data = data.drop(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    # rename to euro
    data = data.rename(columns={'avg': 'EUR'})

    # round to 2 decimal places
    data = data.round(2)

    # Print the data
    print(data)

    # Optionally, save the data to a CSV file
    data.to_csv('data/USD_EUR.csv')
    print("Data has been saved to 'USD_EUR.csv'.")

# Example: fetch data for the past 90 days
fetch_usd_to_eur_exchange_rates(90)
