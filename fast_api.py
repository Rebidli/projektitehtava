from fastapi import FastAPI, Response, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from contextlib import contextmanager
import json
import bcrypt
import pymysql


app = FastAPI()


DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'projektitehtava',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

connection = pymysql.connect(**DB_CONFIG)
try:
    with connection.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            password VARCHAR(255) NOT NULL
        );
        """
        cursor.execute(sql)
    connection.commit()
finally:
    connection.close()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

@contextmanager
def get_db_connection():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
    finally:
        connection.close()

def get_password_hash(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(plain_password, hashed_password):
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())

class UserCredentials(BaseModel):
    username: str
    password: str

@app.post("/register/")
def register_user(username: str, password: str):
    hashed_password = get_password_hash(password)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("INSERT INTO users (username, password) VALUES (%s, %s)", (username, hashed_password))
                conn.commit()
            except pymysql.err.IntegrityError:
                raise HTTPException(status_code=400, detail="Username already registered")
    return {"message": "User registered successfully"}

@app.post("/login")
def login(credentials: UserCredentials):
    username = credentials.username
    password = credentials.password
    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"id": user["id"], "username": user["username"]}

def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    if verify_password(password, user['password']):
        return user
    return False

def get_user(username: str):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, username, password FROM users WHERE username = %s", (username,))
            user = cursor.fetchone()
            return user

@app.get('/api/sensor_data/averages')
def get_sensor_data_averages():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.year, dd.month, dd.day,
                       AVG(sm.value) as average_value, MIN(sm.value) as min_value, MAX(sm.value) as max_value
                FROM sensor_measurement sm
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                GROUP BY dd.year, dd.month, dd.day
                ORDER BY dd.year, dd.month, dd.day
            """)
            data = cursor.fetchall()
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor_data/details')
def get_all_sensor_data():
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value, 
                       dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
            """)
            data = cursor.fetchall()
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor_data/{sensor_type}/average')
def get_average_sensor_data_by_type(sensor_type: str):
    type_filter = f"%{sensor_type.capitalize()}%"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT sd.sensor_name, AVG(sm.value) as average_value
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                WHERE sd.sensor_name LIKE %s
                GROUP BY sd.sensor_name
            """, (type_filter,))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No average data found for {sensor_type} sensors", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor_data/{sensor_type}/details')
def get_detailed_sensor_data_by_type(sensor_type: str):
    type_filter = f"%{sensor_type.capitalize()}%"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value,
                       dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                WHERE sd.sensor_name LIKE %s
            """, (type_filter,))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No detailed data found for {sensor_type} sensors", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/sensor_data/device/{device_id}/average')
def get_average_data_by_device(device_id: str):
    return get_sensor_data_by_criteria('sd.device_id', device_id, average=True)

# Endpoint for detailed data by device ID
@app.get('/sensor_data/device/{device_id}/details')
def get_detailed_data_by_device(device_id: str):
    return get_sensor_data_by_criteria('sd.device_id', device_id, average=False)

# Endpoint for average data by sensor ID
@app.get('/sensor_data/sensor/{sensor_id}/average')
def get_average_data_by_sensor(sensor_id: str):
    return get_sensor_data_by_criteria('sd.sensor_id', sensor_id, average=True)

# Endpoint for detailed data by sensor ID
@app.get('/sensor_data/sensor/{sensor_id}/details')
def get_detailed_data_by_sensor(sensor_id: str):
    return get_sensor_data_by_criteria('sd.sensor_id', sensor_id, average=False)

def get_sensor_data_by_criteria(criteria: str, value: str, average: bool):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if average:
                query = f"""
                    SELECT {criteria}, AVG(sm.value) as average_value
                    FROM sensor_measurement sm
                    JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                    WHERE {criteria} = %s
                    GROUP BY {criteria}
                """
            else:
                query = f"""
                    SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value,
                           dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
                    FROM sensor_measurement sm
                    JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                    JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                    WHERE {criteria} = %s
                """
            cursor.execute(query, (value,))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No sensor data found for {criteria} = {value}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")
@app.get('/api/sensor/yearly/average')
def get_yearly_average(year: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.year, 
                       AVG(sm.value) as average_value, 
                       MIN(sm.value) as min_value, 
                       MAX(sm.value) as max_value
                FROM sensor_measurement sm
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s
                GROUP BY dd.year
            """, (year,))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No data found for year {year}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/yearly/details')
def get_yearly_details(year: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.year, sd.sensor_id, sd.sensor_name, sd.device_name, sm.value
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s
                ORDER BY sd.sensor_id
            """, (year,))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No data found for year {year}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/monthly/average')
def get_monthly_average(year: int, month: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.month,
                       AVG(sm.value) as average_value,
                       MIN(sm.value) as min_value,
                       MAX(sm.value) as max_value
                FROM sensor_measurement sm
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s
                GROUP BY dd.month
            """, (year, month))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No data found for year {year} and month {month}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/monthly/details')
def get_monthly_details(year: int, month: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.month, sd.sensor_id, sd.sensor_name, sd.device_name, sm.value
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s
                ORDER BY sd.sensor_id
            """, (year, month))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No data found for year {year} and month {month}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/weekly/average')
def get_weekly_average(year: int, month: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.year, dd.month, WEEK(dd.date) as week,
                       AVG(sm.value) as average_value,
                       MIN(sm.value) as min_value,
                       MAX(sm.value) as max_value
                FROM sensor_measurement sm
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s
                GROUP BY dd.year, dd.month, WEEK(dd.date)
                ORDER BY WEEK(dd.date)
            """, (year, month))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No weekly data found for year {year} and month {month}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/weekly/details')
def get_weekly_details(year: int, month: int):
    detailed_data = {}
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT WEEK(dd.date) as week,
                       sd.sensor_name, sd.sensor_id, sd.device_name, sm.value
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s
                ORDER BY WEEK(dd.date), sd.sensor_name, sd.sensor_id
            """, (year, month))
            data = cursor.fetchall()
            for item in data:
                week = f"Week {item['week']:02d}"
                if week not in detailed_data:
                    detailed_data[week] = []
                detailed_data[week].append(item)
            if not detailed_data:
                return Response(content=f"No weekly details found for year {year} and month {month}", status_code=404)
            json_data = json.dumps(detailed_data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/daily/average')
def get_daily_average(year: int, month: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.year, dd.month, dd.day,
                       AVG(sm.value) as average_value,
                       MIN(sm.value) as min_value,
                       MAX(sm.value) as max_value
                FROM sensor_measurement sm
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s
                GROUP BY dd.year, dd.month, dd.day
                ORDER BY dd.day
            """, (year, month))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No daily average data found for year {year} and month {month}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/daily/details')
def get_daily_details(year: int, month: int):
    detailed_data = {}
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.day, sd.sensor_name, sd.sensor_id, sd.device_name, sm.value
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s
                ORDER BY dd.day, sd.sensor_name, sd.sensor_id
            """, (year, month))
            data = cursor.fetchall()
            for item in data:
                day = f"{item['day']:02d}"
                if day not in detailed_data:
                    detailed_data[day] = []
                detailed_data[day].append(item)
            if not detailed_data:
                return Response(content=f"No daily details found for year {year} and month {month}", status_code=404)
            json_data = json.dumps(detailed_data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")


@app.get('/api/sensor/hourly/average')
def get_hourly_average(year: int, month: int, day: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.hour, 
                       AVG(sm.value) as average_value, 
                       MIN(sm.value) as min_value, 
                       MAX(sm.value) as max_value
                FROM sensor_measurement sm
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s AND dd.day = %s
                GROUP BY dd.hour
            """, (year, month, day))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No data found for {year}-{month}-{day}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")

@app.get('/api/sensor/hourly/details')
def get_hourly_details(year: int, month: int, day: int):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT dd.hour, sd.sensor_id, sd.sensor_name, sd.device_name, sm.value
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE dd.year = %s AND dd.month = %s AND dd.day = %s
                ORDER BY dd.hour, sd.sensor_id
            """, (year, month, day))
            data = cursor.fetchall()
            if not data:
                return Response(content=f"No data found for {year}-{month}-{day}", status_code=404)
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=4)
            return Response(content=json_data, media_type="application/json")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="localhost", port=8001)
