import json
from flask import Flask, request, redirect, url_for, render_template, flash, Response
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import pymysql
from contextlib import contextmanager
from flask import flash

app = Flask(__name__)
app.secret_key = 'salainen_avain'

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

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
    conn = pymysql.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

class User(UserMixin):
    def __init__(self, user_id, username, password_hash):
        self.id = user_id
        self.username = username
        self.password_hash = password_hash

@login_manager.user_loader
def load_user(user_id):
    user = get_user_by_id(user_id)
    return User(user['id'], user['username'], user['password']) if user else None

def get_user_by_id(user_id):
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        return cursor.fetchone()

def get_user_by_username(username):
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        return cursor.fetchone()

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password_hash = generate_password_hash(request.form['password'])
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO users (username, password) VALUES (%s, %s)", (username, password_hash))
            conn.commit()
        return redirect(url_for('login'))
    return render_template('register.html')



@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = get_user_by_username(username)
        if user and check_password_hash(user['password'], password):
            login_user(User(user['id'], user['username'], user['password']))
            return redirect(url_for('get_all_sensor_data'))
        else:
            flash('Virheellinen käyttäjätunnus tai salasana.')
    return render_template('login.html')



@app.route('/sensor_data', methods=['GET'])
@login_required
def get_all_sensor_data():
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""
            SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value, 
                   dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
            FROM sensor_measurement sm
            JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
            JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
        """)
        data = cursor.fetchall()
        return Response(json.dumps(data, default=str, indent=4), status=200, mimetype="application/json")

@app.route('/sensor_data/<sensor_type>', methods=['GET'])
def get_sensor_data_by_type(sensor_type):
    query_type = {'temperature': '%Temperature%', 'pressure': '%Pressure%', 'humidity': '%Humidity%'}.get(sensor_type.lower())
    if not query_type:
        return Response("Sensor type not supported.", status=400, mimetype="application/json")
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""
            SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value, 
                   dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
            FROM sensor_measurement sm
            JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
            JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
            WHERE sd.sensor_name LIKE %s
        """, (query_type,))
        data = cursor.fetchall()
        return Response(json.dumps(data, default=str, indent=4), status=200, mimetype="application/json")

@app.route('/sensor_data/device/<device_id>', methods=['GET'])
def get_sensor_data_by_device(device_id):
    return get_specific_sensor_data('sd.device_id', device_id)

@app.route('/sensor_data/sensor/<sensor_id>', methods=['GET'])
def get_sensor_data_by_sensor(sensor_id):
    return get_specific_sensor_data('sd.sensor_id', sensor_id)

def get_specific_sensor_data(column, value):
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT sd.sensor_id, sd.sensor_name, sd.device_name, sd.device_id, sd.unit, sm.value, 
                   dd.year, dd.month, dd.day, dd.hour, dd.minute, dd.second, dd.millisecond
            FROM sensor_measurement sm
            JOIN sensor_dim sd ON sm.sensor_dim_sensor_id = sd.sensor_id
            JOIN date_dim dd ON sm.date_dim_date_id = dd.date_id
            WHERE {column} = %s
        """, (value,))
        data = cursor.fetchall()
        return Response(json.dumps(data, default=str, indent=4), status=200, mimetype="application/json")

if __name__ == '__main__':
    app.run(debug=True)
