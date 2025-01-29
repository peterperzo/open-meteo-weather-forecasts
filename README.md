# rohlik-case-study-open-meteo

Weather forecast ETL pipeline. Data sourced from [Open-Meteo API](https://open-meteo.com).

## Project Structure

### Main Structure

```
.
├── L0/                  # Raw data layer
├── L1/                  # Processing layer
├── main.py              # Pipeline orchestration
├── requirements.txt     # Project dependencies
├── config.py            # Configuration and constants
└── README.md            # Documentation
```

### Detailed Layer Structure

### L0 (Raw Data Layer)

Layer for handling raw data ingestion and storage.

```
L0/
├── weather_client.py   # Raw data fetching from Open-Meteo API
├── models.py           # Data models for raw weather data
├── database.py         # Raw data storage operations
└── etl.py              # Handles raw data ingestion flow
```

### L1 (Processing Layer)

Layer for data processing and transformations.

```
L1/
├── data_cleaning.py     # Data cleaning operations
├── weather_summary.py   # OpenAI summary generation
└── rain_forecast.py     # Rain forecast processing
```

## Features

- Fetches weather data for selected cities from Open-Meteo API
- Stores raw weather data in PostgreSQL database hosted on Render.com
- Cleans and processes weather data
- Generates natural language summaries using OpenAI
- Creates rain forecasts

## Configuration
```
├── config.py         
│   └── Cities:       
│       ├── Munich     
│       ├── Frankfurt 
│       ├── Berlin     
│       ├── Praha      
│       ├── Brno       
│       ├── Budapest
│       ├── Vienna    
│       └── Bucharest  
```
Repository configured with GitHub Actions for automated weekly runs.
Required secrets stored in GitHub:
- OPENAI_API_KEY       
- DATABASE_URL         
