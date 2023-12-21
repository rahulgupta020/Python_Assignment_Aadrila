import requests
import pymongo
import time
import argparse
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ['WEATHER_KEY']
db_name = os.environ['DB_NAME']
col_name = os.environ['COL_NAME']

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient[db_name]

def fetch_data(city):
    response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}")
    data = response.json()
    if (response.status_code==200):
        return data
    else:
        return {}

def store_data(data):
    temp_k = data['main']['temp']
    temp_celsius = int(temp_k - 273.15)
    temp_fahrenheit = int((temp_celsius * 9/5) + 32)
    data["temp_celsius"] = temp_celsius
    data["temp_fahrenheit"] = temp_fahrenheit
    data["timestamp"] = time.time()
    mydb[col_name].insert_one(data)

def query_data(city):
    result = mydb[col_name].find_one({"name":city})
    if result:
        print(f"Weather data for {city}:")
        print(result)
    else:
        print(f"No weather data found for {city}")



def main():
    parser = argparse.ArgumentParser(description="Fetch, Store and display weather data from OpenWeatherMap API")
    parser.add_argument("action", choices=["fetch", "query"], help="Action to perform: fetch or query")
    parser.add_argument("--city", help="City name for fetch or query")

    args = parser.parse_args()
    # print("args = ", args)

    if args.action == "fetch":
        if not args.city:
            print("Please provide a city name for fetching.")
            return
        weather_data = fetch_data(args.city)
        store_data(weather_data)
        print(f"Weather data for {args.city} fetched and stored in MongoDB.")
    elif args.action == "query":
        if not args.city:
            print("Please provide a city name for querying.")
            return
        query_data(args.city)
    else:
        print("Invalid action. Use 'fetch' or 'query'.")


if __name__ == "__main__":
    main()
