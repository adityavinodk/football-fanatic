import requests
from datetime import datetime
import pytz
import json
from tqdm import tqdm
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count


def fetchHTML(url):
    response = requests.get(url)
    return response.text


def fetchEventData(city):
    soup = BeautifulSoup(
        fetchHTML(f"https://www.livefootballtickets.com/city/{city}-tickets.html"),
        "html.parser",
    )
    event_comps = soup.find_all(class_="event-list-item")
    events = []
    if event_comps:
        for event in event_comps:
            # Get date of Event
            date_comp = event.find(class_="event-date")
            if date_comp.find(class_="day"):
                day = date_comp.find(class_="day").text
                month = date_comp.find(class_="month").text
                year = date_comp.find(class_="year").text

                # Get event details
                event_info_comp = event.find(class_="event-info")
                tournament = event_info_comp.find(id="Category").text
                name = event_info_comp.find(id="Name")
                ticket_a = name.find("a")
                tickets_link = ticket_a.get("href")
                teams = [x.strip() for x in ticket_a.text.split("v")]
                info_detail = event_info_comp.find(class_="event-info-detail")
                time = info_detail.find(class_="event-time").text
                stadium = info_detail.find(class_="event-stadium").text
                city, country = [
                    x.strip()
                    for x in info_detail.find(class_="event-city").text.split(",")
                ]
                data = {
                    "sport": "FOOTBALL",
                    "date": f"{day} {month} {year}",
                    "tournament": tournament,
                    "teams": teams,
                    "time": f"{day} {month} {year} {time}",
                    "stadium": stadium,
                    "city": city,
                    "country": country,
                }

                soup2 = BeautifulSoup(fetchHTML(tickets_link), "html.parser")
                ticket_comps = soup2.find("ol", id="ticket-ui").children
                tickets = []
                for ticket in ticket_comps:
                    if not isinstance(ticket, str):
                        category = ticket.find("h5", class_="ticket-heading").text
                        price = (
                            ticket.find("div", class_="ticket-price").find("h4").text
                        )
                        ticket_info = ticket.find(
                            "span", class_="ticket-info-details"
                        ).text.strip()
                        tickets.append(
                            {"category": category, "price": price, "info": ticket_info}
                        )
                data["tickets"] = tickets
                events.append(data)
    return events

def fetchEventDataParallel(city):
    return fetchEventData(city)

if __name__ == "__main__":
    cities = [
        "MANCHESTER",
        "BARCELONA",
        "MUNICH",
        "BIRMINGHAM",
        "MADRID",
        "LONDON",
        "PARIS",
        "AMSTERDAM",
        "BERLIN",
        "FRANKFURT",
        "HAMBURG",
        "DORTMUND",
        "LISBON",
        "MILAN",
        "LIVERPOOL",
        "MARSEILLE",
        "GELSENKIRCHEN",
        "LYON",
        "ROME",
        "EDINBURGH",
        "TORINO",
        "GLASGOW",
        "ISTANBUL",
    ]
    utc_now = datetime.utcnow()
    est_timezone = pytz.timezone("US/Eastern")
    est_now = utc_now.replace(tzinfo=pytz.utc).astimezone(est_timezone)
    current_date_est = est_now.date()

    events = []
    with Pool(cpu_count()) as pool:
        events = list(tqdm(pool.imap(fetchEventDataParallel, cities), total=len(cities)))
    events = [event for sublist in events for event in sublist]

    with open(f"data/{current_date_est}.json", "w") as json_file:
        json.dump(events, json_file)
