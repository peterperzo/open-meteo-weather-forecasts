import psycopg2
import logging
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_data_cleaning():
    """Clean weather forecast data in the database"""
    conn = psycopg2.connect(Config.DATABASE_URL)
    
    try:
        with conn.cursor() as cur:
            logger.info("Starting data cleaning...")
            
            # Remove duplicates (keep most recent)
            cur.execute("""
                DELETE FROM weather_forecasts a USING weather_forecasts b
                WHERE a.id < b.id 
                AND a.city = b.city 
                AND a.timestamp = b.timestamp
                RETURNING a.id;
            """)
            duplicates_removed = cur.rowcount
            logger.info(f"Removed {duplicates_removed} duplicate records")
            
            # Remove invalid values
            cur.execute("""
                DELETE FROM weather_forecasts 
                WHERE temperature < -100 OR temperature > 100
                OR precipitation < 0
                OR windspeed < 0
                OR city IS NULL 
                OR timestamp IS NULL
                RETURNING id;
            """)
            invalid_removed = cur.rowcount
            logger.info(f"Removed {invalid_removed} records with invalid values")
            
            # Remove old data (older than 30 days)
            cur.execute("""
                DELETE FROM weather_forecasts 
                WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '30 days'
                RETURNING id;
            """)
            old_removed = cur.rowcount
            logger.info(f"Removed {old_removed} old records")
            
            # Round numerical values
            cur.execute("""
                UPDATE weather_forecasts 
                SET 
                    temperature = ROUND(temperature::numeric, 2),
                    precipitation = ROUND(precipitation::numeric, 2),
                    windspeed = ROUND(windspeed::numeric, 2)
                WHERE 
                    temperature != ROUND(temperature::numeric, 2)
                    OR precipitation != ROUND(precipitation::numeric, 2)
                    OR windspeed != ROUND(windspeed::numeric, 2)
                RETURNING id;
            """)
            rounded = cur.rowcount
            logger.info(f"Rounded values in {rounded} records")
            
            # Get total count for reference
            cur.execute("SELECT COUNT(*) FROM weather_forecasts")
            total_remaining = cur.fetchone()[0]
            
            conn.commit()
            logger.info(f"""
            Cleaning Summary:
            ----------------
            Duplicates removed: {duplicates_removed}
            Invalid records removed: {invalid_removed}
            Old records removed: {old_removed}
            Records rounded: {rounded}
            Total records remaining: {total_remaining}
            """)
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during data cleaning: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run_data_cleaning()
