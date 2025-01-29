from L0.etl import run_etl
from L1.data_cleaning import run_data_cleaning
from L1.weather_summary import generate_weather_summaries
from L1.rain_forecast import get_rain_forecasts

if __name__ == "__main__":

    run_etl()
    run_data_cleaning()
    generate_weather_summaries()
    get_rain_forecasts()
