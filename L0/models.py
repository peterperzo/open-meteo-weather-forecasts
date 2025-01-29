from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class WeatherForecast:
    city: str
    timestamp: datetime
    temperature: float
    precipitation: float
    windspeed: float
    id: Optional[int] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        self.temperature = round(self.temperature, 2)
        self.precipitation = round(self.precipitation, 2)
        self.windspeed = round(self.windspeed, 2)
        
        if self.precipitation < 0 or self.windspeed < 0:
            raise ValueError("Precipitation and windspeed cannot be negative")
        if not -100 <= self.temperature <= 100:
            raise ValueError("Temperature out of reasonable range") 