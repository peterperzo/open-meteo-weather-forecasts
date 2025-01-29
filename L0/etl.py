import asyncio
import logging
from config import CITIES
from L0.weather_client import WeatherClient
from L0.database import Database

logger = logging.getLogger(__name__)

class WeatherETL:
    def __init__(self):
        self.client = WeatherClient()
        self.db = Database()

    async def run(self):
        try:
            logger.info("Starting ETL process")
            
            # Fetch weather data
            forecasts = await self.client.fetch_forecasts(CITIES)
            
            if not forecasts:
                logger.warning("No forecasts retrieved")
                return
            
            # Save to database
            records_updated = self.db.upsert_forecasts(forecasts)
            logger.info(f"Successfully updated {records_updated} forecast records")

        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise

        finally:
            self.db.close()

def run_etl():
    etl = WeatherETL()
    asyncio.run(etl.run()) 
