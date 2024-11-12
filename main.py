import requests
import json
from kafka import KafkaProducer
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"
WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather"

CITIES = [
    {"name": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"name": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"name": "Marrakech", "lat": 31.6295, "lon": -7.9811},
    {"name": "Fès", "lat": 34.0181, "lon": -5.0078},
    {"name": "Tanger", "lat": 35.7595, "lon": -5.8340},
    {"name": "Agadir", "lat": 30.4278, "lon": -9.5981}
]

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], 
                         value_serializer = lambda v: json.dumps(v).encode('utf-8'))

def fetch_data(city):   
    air_quality_params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "appid": API_KEY
    }

    air_quality_response = requests.get(BASE_URL, params=air_quality_params)

    weather_params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "appid": API_KEY,
        "units": "metric"  # Pour obtenir la température en Celsius
    }

    weather_response = requests.get(WEATHER_URL, params=weather_params)

    if air_quality_response.status_code == 200 and weather_response.status_code == 200:
        air_quality_data = air_quality_response.json()
        weather_data = weather_response.json()
        
        combined_data = {
            "city": city["name"],
            "timestamp": int(time.time()),
            "temperature": weather_data["main"]["temp"],
            "air_quality": air_quality_data["list"][0]
        }
        return combined_data
    else:
        print(f"Erreur lors de la récupération des données pour {city['name']}")
        print(f"Air Quality Response: {air_quality_response.text}")
        print(f"Weather Response: {weather_response.text}")
        return None

def main():
    while True:
        for city in CITIES:
            data = fetch_data(city)
            if data:
                print(f"Données récupérées pour {city['name']}:")
                print(json.dumps(data, indent=2))
                producer.send('air_quality_data', value=data)
        time.sleep(120)  # Attendre 30 minutes avant la prochaine extraction

if __name__ == "__main__":
    main()
    
         


