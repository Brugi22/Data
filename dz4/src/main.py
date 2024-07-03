from fastapi import FastAPI
import os
import pandas as pd
import geohash
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()

# This will enable CORS for all routes, don't remove this configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def hello_world():
    return {"message": "Hello World!"}


# implement new get endpoint on route "/api/aqi", return aggregated json data calculated from aqi_data.csv
# def get_aqi(): ....

@app.get("/api/aqi")
def get_aqi():
    df = pd.read_csv('aqi_data.csv')
    df['Geohash'] = df.apply(lambda row: geohash.encode(row['Latitude'], row['Longitude'], precision=6), axis=1)
    grouped_df = df.groupby('Geohash').agg(
        AqiMin=('AirQualityIndex', 'min'),
        AqiMax=('AirQualityIndex', 'max'),
        AqiAvg=('AirQualityIndex', 'mean')
    ).reset_index()
    return grouped_df.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)
