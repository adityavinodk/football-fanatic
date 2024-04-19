from src.google_flight_analysis.scrape import *

flights = Scrape("MUC", "LAX", "2023-05-28")
print(flights)