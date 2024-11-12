import json
from kafka import KafkaConsumer
from datetime import datetime
import psycopg2
import pandas as pd
import numpy as np
import signal
import sys

# Configuration du consommateur Kafka
consumer = KafkaConsumer(
    'air_quality_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='air_quality_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="air_quality_db",
    user="ow_project",
    password="passedemot",
    host="localhost"
)
cursor = conn.cursor()

# Création de la table pour les statistiques
cursor.execute('''
CREATE TABLE IF NOT EXISTS air_quality_stats (
    id SERIAL PRIMARY KEY,
    city TEXT,
    timestamp TIMESTAMP,
    temperature_mean FLOAT,
    temperature_min FLOAT,
    temperature_max FLOAT,
    aqi_mean FLOAT,
    aqi_min INTEGER,
    aqi_max INTEGER,
    co_mean FLOAT,
    no2_mean FLOAT,
    o3_mean FLOAT,
    so2_mean FLOAT,
    pm2_5_mean FLOAT,
    pm10_mean FLOAT,
    main_pollutant TEXT,
    temp_aqi_correlation FLOAT,
    high_aqi_hours INTEGER
)
''')
conn.commit()

# Variable pour contrôler l'exécution du programme
running = True

def signal_handler(sig, frame):
    global running
    print("Arrêt du programme en cours...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

def process_and_save_message(message):
    data = message.value
    timestamp = datetime.fromtimestamp(data['timestamp'])
    
    processed_data = {
        'city': data['city'],
        'timestamp': timestamp,
        'temperature': data['temperature'],
        'aqi': data['air_quality']['main']['aqi'],
        'co': data['air_quality']['components']['co'],
        'no2': data['air_quality']['components']['no2'],
        'o3': data['air_quality']['components']['o3'],
        'so2': data['air_quality']['components']['so2'],
        'pm2_5': data['air_quality']['components']['pm2_5'],
        'pm10': data['air_quality']['components']['pm10']
    }
    
    # Calculer et sauvegarder les statistiques immédiatement
    calculate_and_save_statistics([processed_data])

    print(f"Données traitées pour {data['city']} à {timestamp}")

def calculate_and_save_statistics(data_store):
    df = pd.DataFrame(data_store)
    
    for city, city_data in df.groupby('city'):
        stats = {}
        
        # Statistiques générales
        for column in ['temperature', 'aqi', 'co', 'no2', 'o3', 'so2', 'pm2_5', 'pm10']:
            stats[f'{column}_mean'] = float(city_data[column].mean())
            if column in ['temperature', 'aqi']:
                stats[f'{column}_min'] = int(city_data[column].min())
                stats[f'{column}_max'] = int(city_data[column].max())
        
        # Polluant principal
        pollutants = ['co', 'no2', 'o3', 'so2', 'pm2_5', 'pm10']
        stats['main_pollutant'] = pollutants[np.argmax([city_data[p].mean() for p in pollutants])]
        
        # Corrélation température-AQI
        if len(city_data) > 1 and city_data['temperature'].std() != 0 and city_data['aqi'].std() != 0:
            stats['temp_aqi_correlation'] = float(city_data['temperature'].corr(city_data['aqi']))
        else:
            stats['temp_aqi_correlation'] = None
            print(f"Avertissement : Impossible de calculer la corrélation pour {city}. Pas assez de données ou pas de variation.")
        
        # Nombre d'heures avec AQI >= 4 (mauvais)
        stats['high_aqi_hours'] = int((city_data['aqi'] >= 4).sum())
        
        # Insérer les statistiques dans la base de données
        cursor.execute('''
        INSERT INTO air_quality_stats 
        (city, timestamp, temperature_mean, temperature_min, temperature_max, 
        aqi_mean, aqi_min, aqi_max, co_mean, no2_mean, o3_mean, so2_mean, 
        pm2_5_mean, pm10_mean, main_pollutant, temp_aqi_correlation, high_aqi_hours)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            city, datetime.now(), stats['temperature_mean'], stats['temperature_min'], 
            stats['temperature_max'], stats['aqi_mean'], stats['aqi_min'], stats['aqi_max'],
            stats['co_mean'], stats['no2_mean'], stats['o3_mean'], stats['so2_mean'],
            stats['pm2_5_mean'], stats['pm10_mean'], stats['main_pollutant'],
            stats['temp_aqi_correlation'], stats['high_aqi_hours']
        ))
    
    conn.commit()
    print("Statistiques calculées et sauvegardées dans la base de données.")

def main():
    print(f"Démarrage du consommateur à {datetime.now()}")
    print("En attente de messages... (Appuyez sur Ctrl+C pour arrêter)")

    try:
        while running:
            message = next(consumer)
            process_and_save_message(message)
    except KeyboardInterrupt:
        print("Interruption détectée. Arrêt en cours...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        print(f"Arrêt du consommateur à {datetime.now()}")

if __name__ == "__main__":
    main()
