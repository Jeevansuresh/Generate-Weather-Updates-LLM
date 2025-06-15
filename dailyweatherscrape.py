from meteostat import Point, Hourly
import mysql.connector
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "localhost",
    "user": "jagi",
    "password": "Padjagi75$",
    "database": "weather"
}

chennai = Point(13.0827, 80.2707)

def fetch_yesterday_weather():
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    start = datetime(yesterday.year, yesterday.month, yesterday.day)
    end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59)

    data = Hourly(chennai, start, end)
    df = data.fetch()

    def get_range_data(start_hr, end_hr):
        block = df[(df.index.hour >= start_hr) & (df.index.hour < end_hr)]
        temp = block["temp"].mean() if not block["temp"].isnull().all() else 0
        rain = block["prcp"].sum() if not block["prcp"].isnull().all() else 0
        return round(temp or 0, 2), round(rain or 0, 2)

    return {
        "date": start.date(),
        "morn": get_range_data(6, 9),
        "afternoon": get_range_data(12, 15),
        "evening": get_range_data(18, 21),
        "night": get_range_data(0, 3)
    }

def insert_weather_record(day):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM CHENNAI_WEATHER WHERE date = %s", (day["date"],))
    if cursor.fetchone()[0] > 0:
        print(f"⚠️ Data for {day['date']} already exists. Skipping.")
        conn.close()
        return

    sql = """INSERT INTO CHENNAI_WEATHER 
             (date, morn_temp, morn_rain, an_temp, an_rain, eve_temp, eve_rain, night_temp, night_rain)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    val = (
        day["date"],
        *day["morn"],
        *day["afternoon"],
        *day["evening"],
        *day["night"]
    )
    cursor.execute(sql, val)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted yesterday's weather: {day['date']}")

if __name__ == "__main__":
    yesterday_data = fetch_yesterday_weather()
    insert_weather_record(yesterday_data)
