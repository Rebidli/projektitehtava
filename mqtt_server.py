import json
import certifi
import pymysql
import paho.mqtt.client as mqtt
from contextlib import contextmanager
from datetime import datetime


MQTT_BROKER = "mqtt.tequ.fi"
MQTT_PORT = 8883
MQTT_TOPIC = "tequ/type/coolbox/id/503/event/data"
MQTT_USERNAME = "tvt"
MQTT_PASSWORD = "TVT2023!"
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'projektitehtava',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

with open('coolbox_metadata.json', 'r', encoding='UTF-8') as config_file:
    metadata = json.load(config_file)

@contextmanager
def get_db_connection():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
    finally:
        connection.close()



def insert_sensor_dim(device_id, device_name, sensor_id, sensor_name, unit):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO sensor_dim (sensor_id, sensor_name, device_id, unit, device_name)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (sensor_id, sensor_name, device_id, unit, device_name))
        conn.commit()


def insert_date_dim(timestamp):
    date_parts = (timestamp.year, timestamp.month, timestamp.day,
                  timestamp.hour, timestamp.minute, timestamp.second,
                  timestamp.microsecond // 1000)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO date_dim (year, month, week, day, hour, minute, second, millisecond)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, date_parts)
        conn.commit()
        return cursor.lastrowid



def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload)
        ts = payload['ts']
        ts_in_sec = float(str(ts)[:-3] + "." + str(ts)[-3:])
        dt = datetime.fromtimestamp(float(ts_in_sec))

        devices_data = payload.get('d', {})
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for device_id, sensors in devices_data.items():
                    device_metadata = metadata['devices'].get(device_id, {})
                    device_name = device_metadata.get('sd')

                    for sensor_id, sensor_info in sensors.items():
                        sensor_metadata = device_metadata.get('sensors', {}).get(sensor_id, {})
                        unit = sensor_metadata.get('unit')
                        value = sensor_info.get('v')
                        parts = sensor_id.split('_')
                        sensor_name = ''.join(parts[-1])

                        if unit == 'kPa' or 'Humidity' in sensor_name or 'AirTemperature' in sensor_name:
                            cursor.execute("SELECT sensor_id FROM sensor_dim WHERE sensor_id = %s", (sensor_id,))
                            if not cursor.fetchone():
                                insert_sensor_dim(device_id, device_name, sensor_id, sensor_name, unit)

                            date_id = insert_date_dim(dt)

                            sql = "INSERT INTO sensor_measurement (sensor_dim_sensor_id, date_dim_date_id, value) VALUES (%s, %s, %s)"
                            cursor.execute(sql, (sensor_id, date_id, value))
                            print(
                                f"Device: {device_name} (ID: {device_id}), Sensor: {sensor_id}, Value: {value}, Unit: {unit}, TimeStamp: {dt}")
            conn.commit()
    except Exception as e:
        print(e)



mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqttc.connect(MQTT_BROKER, MQTT_PORT, 60)
mqttc.tls_set(certifi.where())


mqttc.loop_forever()
