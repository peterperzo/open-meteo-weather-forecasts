import psycopg2
import logging
from config import Config
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_rain_forecasts_table():
    """Create table for storing detailed daily rain forecasts"""
    conn = psycopg2.connect(Config.DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_rain_forecasts (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100) NOT NULL,
                    forecast_date DATE NOT NULL,
                    rain_start TIMESTAMP NOT NULL,
                    rain_end TIMESTAMP NOT NULL,
                    total_rain DECIMAL NOT NULL,
                    max_rain_intensity DECIMAL NOT NULL,
                    avg_temperature DECIMAL NOT NULL,
                    avg_wind DECIMAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(city, forecast_date)
                );
            """)
            conn.commit()
            logger.info("Daily rain forecasts table created/verified")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise
    finally:
        conn.close()

def get_rain_forecasts():
    """Get locations and times where rain is expected in next 7 days, organized by day"""
    conn = psycopg2.connect(Config.DATABASE_URL)
    
    try:
        # First ensure table exists
        create_rain_forecasts_table()
        
        with conn.cursor() as cur:
            # Get daily summaries for rainy days
            cur.execute("""
                WITH daily_summary AS (
                    SELECT 
                        city,
                        DATE(timestamp) as date,
                        MIN(timestamp) as rain_start,
                        MAX(timestamp) as rain_end,
                        SUM(precipitation) as total_rain,
                        MAX(precipitation) as max_rain,
                        AVG(temperature) as avg_temp,
                        AVG(windspeed) as avg_wind
                    FROM weather_forecasts
                    WHERE 
                        precipitation > 0
                        AND timestamp > CURRENT_TIMESTAMP
                        AND timestamp < CURRENT_DATE + INTERVAL '7 days'
                    GROUP BY city, DATE(timestamp)
                    HAVING SUM(precipitation) > 0
                )
                SELECT *
                FROM daily_summary
                ORDER BY city, date;
            """)
            
            rain_results = cur.fetchall()
            
            if not rain_results:
                logger.info("No rain expected in any location in next 7 days")
                return

            # Get weather summaries from OpenAI
            cur.execute("""
                SELECT 
                    city,
                    summary_text
                FROM weather_summaries
                WHERE summary_date = CURRENT_DATE
                ORDER BY city;
            """)
            
            summaries = {row[0]: row[1] for row in cur.fetchall()}
            
            logger.info("\nDetailed Weather Forecast:")
            logger.info("---------------------------------")
            
            # Store the daily rain forecasts
            for row in rain_results:
                city, date, rain_start, rain_end, total_rain, max_rain, avg_temp, avg_wind = row
                
                # Insert or update the daily forecast
                cur.execute("""
                    INSERT INTO daily_rain_forecasts 
                        (city, forecast_date, rain_start, rain_end, total_rain, 
                         max_rain_intensity, avg_temperature, avg_wind)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city, forecast_date) 
                    DO UPDATE SET
                        rain_start = EXCLUDED.rain_start,
                        rain_end = EXCLUDED.rain_end,
                        total_rain = EXCLUDED.total_rain,
                        max_rain_intensity = EXCLUDED.max_rain_intensity,
                        avg_temperature = EXCLUDED.avg_temperature,
                        avg_wind = EXCLUDED.avg_wind,
                        created_at = CURRENT_TIMESTAMP;
                """, (
                    city, date, rain_start, rain_end, total_rain, 
                    max_rain, avg_temp, avg_wind
                ))
            
            conn.commit()
            
            # Display the forecast
            current_city = None
            for row in rain_results:
                city, date, rain_start, rain_end, total_rain, max_rain, avg_temp, avg_wind = row
                
                if city != current_city:
                    logger.info(f"\n{city}:")
                    if city in summaries:
                        logger.info(f"{summaries[city]}\n")
                    current_city = city
                
                logger.info(
                    f"{date.strftime('%Y-%m-%d')}: "
                    f"Rain period: {rain_start.strftime('%H:%M')} - {rain_end.strftime('%H:%M')}, "
                    f"Total rain: {total_rain:.1f}mm, "
                    f"Max intensity: {max_rain:.1f}mm, "
                    f"Avg temperature: {avg_temp:.1f}°C, "
                    f"Avg wind: {avg_wind:.1f}km/h"
                )
            
    except Exception as e:
        logger.error(f"Error querying rain forecast: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    get_rain_forecasts()
