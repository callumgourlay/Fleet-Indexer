from flask import Flask, render_template
import sqlite3

app = Flask(__name__)

@app.route('/')
def index():
    conn = sqlite3.connect('data/vehicles.db')
    c = conn.cursor()
    c.execute('''SELECT vehicles.id, vehicles.slug, vehicles.fleet_number, vehicles.fleet_code,
                        vehicles.reg, liveries.name, vehicles.branding,
                        operators.name, vehicles.garage, vehicle_types.name,
                        vehicle_types.electric, vehicle_types.double_decker,
                        vehicle_types.coach, vehicles.withdrawn
                 FROM vehicles
                 LEFT JOIN liveries ON vehicles.livery_id = liveries.id
                 LEFT JOIN operators ON vehicles.operator_id = operators.id
                 LEFT JOIN vehicle_types ON vehicles.vehicle_type_id = vehicle_types.id
                 WHERE vehicles.operator_id = 'FGLA'
                 ORDER BY vehicles.fleet_number ASC''')
    fgla_data = c.fetchall()
    conn.close()
    conn = sqlite3.connect('vehicles.db')
    c = conn.cursor()
    c.execute('''SELECT vehicles.id, vehicles.slug, vehicles.fleet_number, vehicles.fleet_code,
                        vehicles.reg, liveries.name, vehicles.branding,
                        operators.name, vehicles.garage, vehicle_types.name,
                        vehicle_types.electric, vehicle_types.double_decker,
                        vehicle_types.coach, vehicles.withdrawn
                 FROM vehicles
                 LEFT JOIN liveries ON vehicles.livery_id = liveries.id
                 LEFT JOIN operators ON vehicles.operator_id = operators.id
                 LEFT JOIN vehicle_types ON vehicles.vehicle_type_id = vehicle_types.id
                 WHERE vehicles.operator_id = 'FABD'
                 ORDER BY vehicles.fleet_number ASC''')
    fabd_data = c.fetchall()
    conn.close()
    return render_template('index.html', fgla_data=fgla_data, fabd_data=fabd_data)

if __name__ == '__main__':
    app.run()