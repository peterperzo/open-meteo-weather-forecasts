import psycopg2
from psycopg2.extras import execute_values
from typing import List
import logging
from L0.models import WeatherForecast
from config import Config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(Config.DATABASE_URL)
        self._create_tables()

    def _create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_forecasts (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT NOT NULL,
                    precipitation FLOAT NOT NULL,
                    windspeed FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(city, timestamp)
                )
            """)
            self.conn.commit()

    def upsert_forecasts(self, forecasts: List[WeatherForecast]) -> int:
        if not forecasts:
            return 0

        with self.conn.cursor() as cur:
            try:
                # Convert forecasts to tuples for batch insert
                values = [(
                    f.city,
                    f.timestamp,
                    f.temperature,
                    f.precipitation,
                    f.windspeed
                ) for f in forecasts]

                # Efficient batch upsert
                execute_values(cur, """
                    INSERT INTO weather_forecasts 
                        (city, timestamp, temperature, precipitation, windspeed)
                    VALUES %s
                    ON CONFLICT (city, timestamp) DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        precipitation = EXCLUDED.precipitation,
                        windspeed = EXCLUDED.windspeed,
                        created_at = CURRENT_TIMESTAMP
                    """, values, page_size=Config.BATCH_SIZE)
                
                self.conn.commit()
                return len(forecasts)

            except Exception as e:
                self.conn.rollback()
                logger.error(f"Database error: {e}")
                raise

    def close(self):
        self.conn.close() 
