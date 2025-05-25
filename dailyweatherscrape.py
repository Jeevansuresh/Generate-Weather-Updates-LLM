import google.generativeai as genai
import pandas as pd
from datetime import datetime
import mysql.connector

# --- Gemini API Configuration ---
GEMINI_API_KEY = "AIzaSyCRbnEmx-3tgblNCzwFc5EE5CiQ_IBipYk"
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# --- MySQL Configuration ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "Skyno@1978"
DB_NAME = "weather"

def get_weather_data_gemini(city, current_date_str):
    prompt = f"""Provide the estimated temperature (°C) and rain probability (%) for {city} on {current_date_str} for the following time periods (in IST):

Morning (6:00 AM - 9:00 AM):
Afternoon (12:00 PM - 3:00 PM):
Evening (6:00 PM - 9:00 PM):
Night (12:00 AM - 3:00 AM of the next day):

Present the data as a simple CSV format with the following columns:
Time Period,Temperature (°C),Rain Chance (%)
Morning,TEMP_MORNING,RAIN_MORNING
Afternoon,TEMP_AFTERNOON,RAIN_AFTERNOON
Evening,TEMP_EVENING,RAIN_EVENING
Night,TEMP_NIGHT,RAIN_NIGHT

Only provide the CSV data. Do not include any introductory or explanatory text.
"""

    try:
        response = model.generate_content(prompt)
        if response.parts:
            raw_output = response.parts[0].text.strip()
            raw_output = raw_output.replace("’", "'").replace("°C", "°C")  # normalize
            lines = raw_output.splitlines()

            if len(lines) == 5 and all(h.strip().lower() in ["time period", "temperature (°c)", "rain chance (%)"]
                                       for h in lines[0].split(",")):
                weather_info = {}
                for line in lines[1:]:
                    parts = line.split(',')
                    if len(parts) == 3:
                        period = parts[0].strip().lower()
                        try:
                            temp = int(float(parts[1].strip()))
                            rain = int(parts[2].strip())
                            weather_info[period] = {
                                "temperature_celsius": temp,
                                "rain_probability_percent": rain
                            }
                        except ValueError:
                            print(f"Warning: Parse issue in line '{line}'")
                    else:
                        print(f"Warning: Malformed line: '{line}'")
                return city, weather_info
            else:
                print(f"Warning: Unexpected format for {city}. Raw output:\n{raw_output}")
                return city, None
        else:
            print(f"Warning: No content received from Gemini for {city}")
            return city, None
    except Exception as e:
        print(f"Error querying Gemini for {city}: {e}")
        return city, None

if __name__ == "__main__":
    try:
        df_locations = pd.read_csv("locations.csv", header=None)
        cities = df_locations[0].tolist()
    except FileNotFoundError:
        print("Error: locations.csv not found.")
        exit()
    except Exception as e:
        print(f"Error reading locations.csv: {e}")
        exit()

    current_date_str = datetime.today().strftime("%Y-%m-%d")
    all_weather_data_flat = []
    daily_weather_by_city = {}

    for city in cities:
        city_lower = city.strip().lower()
        city_display = city.strip()

        city_name, weather_info = get_weather_data_gemini(city_display, current_date_str)

        if weather_info:
            daily_weather_by_city[city_lower] = weather_info
            for period, data in weather_info.items():
                all_weather_data_flat.append({
                    "location": city_display,
                    "time_period": period,
                    "temperature_celsius": data["temperature_celsius"],
                    "rain_probability_percent": data["rain_probability_percent"]
                })

    if all_weather_data_flat:
        df_output = pd.DataFrame(all_weather_data_flat)
        df_output.to_csv("weather_report.csv", index=False)
        print("\n✅ Weather data saved to weather_report.csv")
    else:
        print("⚠️ No weather data retrieved.")

    # --- Insert into CHENNAI_WEATHER table ---
    target_db_city = "chennai"
    if target_db_city in daily_weather_by_city:
        try:
            mydb = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            mycursor = mydb.cursor()
            print(f"\n✅ Connected to MySQL database '{DB_NAME}'.")

            weather_today = daily_weather_by_city[target_db_city]
            date_today = datetime.today().date()

            def extract(period): return (
                weather_today.get(period, {}).get('temperature_celsius'),
                weather_today.get(period, {}).get('rain_probability_percent')
            )

            sql = """INSERT INTO CHENNAI_WEATHER (date, morn_temp, morn_rain, an_temp, an_rain, eve_temp, eve_rain, night_temp, night_rain)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            val = (date_today,
                   *extract('morning'),
                   *extract('afternoon'),
                   *extract('evening'),
                   *extract('night'))

            mycursor.execute(sql, val)
            mydb.commit()
            print(f"✅ Inserted weather data for Chennai on {current_date_str} into CHENNAI_WEATHER.")

        except mysql.connector.Error as err:
            print(f"❌ MySQL Error: {err}")
        finally:
            if 'mycursor' in locals(): mycursor.close()
            if 'mydb' in locals() and mydb.is_connected(): mydb.close()
            print("🔒 MySQL connection closed.")
    else:
        print("⚠️ No weather data for 'chennai'. Skipping DB insert.")
