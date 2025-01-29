from dataclasses import dataclass
from typing import List
import os
import logging

@dataclass(frozen=True)
class City:
    name: str
    latitude: float
    longitude: float

CITIES = [
    City("Munich", 48.137154, 11.576124),
    City("Frankfurt", 50.110924, 8.682127),
    City("Berlin", 52.520008, 13.404954),
    City("Praha", 50.075538, 14.437800),
    City("Brno", 49.195061, 16.606836),
    City("Budapest", 47.497913, 19.040236),
    City("Vienna", 48.208174, 16.373819),
    City("Bucharest", 44.426767, 26.102538)
]

class Config:
    DATABASE_URL = os.getenv('DATABASE_URL')
    WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
    BATCH_SIZE = 1000
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) 
