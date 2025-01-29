import psycopg2
import openai
import logging
import os
import time
from datetime import datetime
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

api_key = os.getenv('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

client = openai.OpenAI(
    api_key=api_key
)

def create_summary_table():
    """Create table for weather summaries if it doesn't exist"""
    conn = psycopg2.connect(Config.DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_summaries (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(50) NOT NULL,
                    summary_date DATE NOT NULL,
                    summary_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(city, summary_date)
                );
            """)
            conn.commit()
            logger.info("Summary table created/verified")
    finally:
        conn.close()

def get_city_weather_data(city: str):
    """Get 7-day weather data for a city"""
    conn = psycopg2.connect(Config.DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    DATE(timestamp) as date,
                    AVG(temperature) as avg_temp,
                    SUM(precipitation) as total_precip,
                    AVG(windspeed) as avg_wind,
                    MIN(temperature) as min_temp,
                    MAX(temperature) as max_temp
                FROM weather_forecasts
                WHERE city = %s
                AND timestamp BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
                GROUP BY DATE(timestamp)
                ORDER BY DATE(timestamp);
            """, (city,))
            data = cur.fetchall()
            logger.info(f"Retrieved {len(data)} daily weather records for {city}")
            return data
    finally:
        conn.close()

def generate_summary(city: str, weather_data, base_delay=5, max_retries=4):
    """
    Generate weather summary with rate limiting
    
    Delay schedule:
    - 1 second between successful calls
    - On rate limit:
        Attempt 1: 5 seconds  (base_delay * 2^0)
        Attempt 2: 10 seconds (base_delay * 2^1)
        Attempt 3: 20 seconds (base_delay * 2^2)
        Attempt 4: 40 seconds (base_delay * 2^3)
    """
    if not weather_data:
        logger.warning(f"No weather data available for {city}")
        return None
        
    weather_text = "\n".join([
        f"Date: {row[0].strftime('%Y-%m-%d')}, "
        f"Avg Temp: {row[1]:.1f}°C (Min: {row[4]:.1f}°C, Max: {row[5]:.1f}°C), "
        f"Total Precip: {row[2]:.1f}mm, Avg Wind: {row[3]:.1f}km/h"
        for row in weather_data
    ])
    
    for attempt in range(max_retries):
        try:
            # Add delay between attempts (except first attempt)
            if attempt > 0:
                delay = base_delay * (2 ** attempt)
                logger.info(f"Rate limit reached. Waiting {delay} seconds before attempt {attempt + 1}/{max_retries}")
                time.sleep(delay)
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a weather forecaster providing clear, concise 7-day weather summaries."
                    },
                    {
                        "role": "user",
                        "content": f"""Based on this 7-day weather forecast for {city}, create a brief, 
                        natural-sounding summary highlighting the 7-day weather pattern, significant changes, 
                        and notable conditions:

                        {weather_text}

                        Please provide a concise, human-friendly summary in 3-4 sentences."""
                    }
                ],
                max_tokens=200,
                temperature=0.7
            )
            summary = response.choices[0].message.content.strip()
            logger.info(f"Generated summary for {city}")
            
            # Add fixed delay after successful call to prevent rate limits
            time.sleep(1)
            logger.info("Waiting 1 second before next city")
            
            return summary
            
        except openai.RateLimitError as e:
            if attempt == max_retries - 1:
                logger.error(f"Rate limit reached for {city} after {max_retries} attempts")
                return None
            continue
            
        except Exception as e:
            logger.error(f"Error generating summary for {city}: {e}")
            return None

def save_summary(city: str, summary: str):
    """Save the generated summary to database"""
    if not summary:
        return
        
    conn = psycopg2.connect(Config.DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO weather_summaries (city, summary_date, summary_text)
                VALUES (%s, CURRENT_DATE, %s)
                ON CONFLICT (city, summary_date) 
                DO UPDATE SET 
                    summary_text = EXCLUDED.summary_text,
                    created_at = CURRENT_TIMESTAMP;
            """, (city, summary))
            conn.commit()
            logger.info(f"Saved summary for {city}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving summary for {city}: {e}")
    finally:
        conn.close()

def generate_weather_summaries():
    """Main function to generate and save weather summaries"""
    try:
        create_summary_table()
        
        conn = psycopg2.connect(Config.DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT city FROM weather_forecasts;")
                cities = [row[0] for row in cur.fetchall()]
                logger.info(f"Found {len(cities)} cities to process")
        finally:
            conn.close()
        
        for city in cities:
            logger.info(f"Processing {city}")
            
            weather_data = get_city_weather_data(city)
            
            if weather_data:
                summary = generate_summary(city, weather_data)
                if summary:
                    save_summary(city, summary)
            
            logger.info(f"Completed processing {city}")
            
    except Exception as e:
        logger.error(f"Error in generate_weather_summaries: {e}")
        raise

if __name__ == "__main__":
    generate_weather_summaries()
