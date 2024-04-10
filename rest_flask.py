import pymysql
import json
from flask import Flask, Response
from contextlib import contextmanager

app = Flask(__name__)


DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'projektitehtava',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


@contextmanager
def get_db_connection():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
    finally:
        connection.close()


@app.route('/sensor_data', methods=['GET'])
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
            json_data = json.dumps(data, default=str, indent=4, sort_keys=False)
            return Response(response=json_data, status=200, mimetype="application/json")


@app.route('/sensor_data/<sensor_type>', methods=['GET'])
def get_sensor_data_by_type(sensor_type):
    sensor_type = sensor_type.lower()
    if sensor_type not in ['temperature', 'pressure', 'humidity']:
        return Response(response="Sensor type not supported.", status=400, mimetype="application/json")

    query_type = {
        'temperature': '%Temperature%',
        'pressure': '%Pressure%',
        'humidity': '%Humidity%'
    }[sensor_type]

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value, 
                       dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE sd.sensor_name LIKE %s
            """, (query_type,))
            data = cursor.fetchall()
            json_data = json.dumps(data, default=str, indent=4, sort_keys=False)
            return Response(response=json_data, status=200, mimetype="application/json")


@app.route('/sensor_data/device/<device_id>', methods=['GET'])
def get_sensor_data_by_device(device_id):
    return get_specific_sensor_data('sd.device_id', device_id)


@app.route('/sensor_data/sensor/<sensor_id>', methods=['GET'])
def get_sensor_data_by_sensor(sensor_id):
    return get_specific_sensor_data('sd.sensor_id', sensor_id)

def get_specific_sensor_data(column, value):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            query = f"""
                SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value, 
                       dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
                FROM sensor_measurement sm
                JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
                JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
                WHERE {column} = %s
            """
            cursor.execute(query, (value,))
            data = cursor.fetchall()
            if not data:
                return Response(response="No sensor data found", status=404, mimetype="application/json")
            json_data = json.dumps(data, default=str, indent=4, sort_keys=False)
            return Response(response=json_data, status=200, mimetype="application/json")

if __name__ == '__main__':
    app.run(debug=True)
