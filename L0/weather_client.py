import httpx
import asyncio
from datetime import datetime
from typing import List
import logging
from L0.models import WeatherForecast
from config import City, Config

logger = logging.getLogger(__name__)

class WeatherClient:
    async def fetch_forecasts(self, cities: List[City]) -> List[WeatherForecast]:
        async with httpx.AsyncClient() as client:
            tasks = [self._fetch_city_forecast(client, city) for city in cities]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            forecasts = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch forecast: {result}")
                else:
                    forecasts.extend(result)
            
            return forecasts

    async def _fetch_city_forecast(
        self, 
        client: httpx.AsyncClient, 
        city: City
    ) -> List[WeatherForecast]:
        try:
            params = {
                "latitude": city.latitude,
                "longitude": city.longitude,
                "hourly": "temperature_2m,precipitation,windspeed_10m",
                "timezone": "auto"
            }

            response = await client.get(Config.WEATHER_API_URL, params=params)
            response.raise_for_status()
            data = response.json()

            forecasts = []
            hourly = data["hourly"]
            
            for i, time_str in enumerate(hourly["time"]):
                forecast = WeatherForecast(
                    city=city.name,
                    timestamp=datetime.fromisoformat(time_str),
                    temperature=hourly["temperature_2m"][i],
                    precipitation=hourly["precipitation"][i],
                    windspeed=hourly["windspeed_10m"][i]
                )
                forecasts.append(forecast)

            logger.info(f"Successfully fetched forecast for {city.name}")
            return forecasts

        except Exception as e:
            logger.error(f"Error fetching forecast for {city.name}: {e}")
            raise 
