import requests
import sqlite3

def get_data(url):
    response = requests.get(url)
    data = response.json()
    next_url = data.get('next')
    results = data['results']
    return results, next_url

def insert_data(data):
    for vehicle in data:
        livery_id = None
        if vehicle['livery'] is not None:
            livery_id = vehicle['livery']['id']
            c.execute("INSERT OR IGNORE INTO liveries (id, name, left, right) VALUES (?, ?, ?, ?)",
                      (vehicle['livery']['id'], vehicle['livery']['name'],
                       vehicle['livery']['left'], vehicle['livery']['right']))
        
        operator_id = None
        if vehicle['operator'] is not None:
            operator_id = vehicle['operator']['id']
            c.execute("INSERT OR IGNORE INTO operators (id, name, parent) VALUES (?, ?, ?)",
                      (vehicle['operator']['id'], vehicle['operator']['name'],
                       vehicle['operator']['parent']))
        
        vehicle_type_id = None
        if vehicle['vehicle_type'] is not None:
            vehicle_type_id = vehicle['vehicle_type']['id']
            c.execute("INSERT OR IGNORE INTO vehicle_types (id, name, electric, double_decker, coach) VALUES (?, ?, ?, ?, ?)",
                      (vehicle['vehicle_type']['id'], vehicle['vehicle_type']['name'],
                       vehicle['vehicle_type']['electric'], vehicle['vehicle_type']['double_decker'],
                       vehicle['vehicle_type']['coach']))
        
        c.execute("INSERT INTO vehicles (id, slug, fleet_number, fleet_code, reg, livery_id, branding, operator_id, garage, vehicle_type_id, withdrawn) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (vehicle['id'], vehicle['slug'], vehicle['fleet_number'], vehicle['fleet_code'], vehicle['reg'],
                   livery_id, vehicle['branding'], operator_id,
                   vehicle['garage'], vehicle_type_id,  vehicle['withdrawn']))

url = "https://bustimes.org/api/vehicles/?id=&vehicle_type=&livery=&withdrawn=&search=&fleet_code=&reg=&slug=&operator=FABD"

conn = sqlite3.connect('vehicles.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS vehicles
             (id INTEGER PRIMARY KEY,
             slug TEXT,
             fleet_number INTEGER,
             fleet_code TEXT,
             reg TEXT,
             livery_id INTEGER,
             branding TEXT,
             operator_id TEXT,
             garage TEXT,
             vehicle_type_id INTEGER,
             withdrawn BOOLEAN,
             FOREIGN KEY(livery_id) REFERENCES liveries(id),
             FOREIGN KEY(operator_id) REFERENCES operators(id),
             FOREIGN KEY(vehicle_type_id) REFERENCES vehicle_types(id))''')

c.execute('''CREATE TABLE IF NOT EXISTS liveries
             (id INTEGER PRIMARY KEY,
             name TEXT,
             left TEXT,
             right TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS operators
             (id TEXT PRIMARY KEY,
             name TEXT,
             parent TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS vehicle_types
             (id INTEGER PRIMARY KEY,
             name TEXT,
             electric BOOLEAN,
             double_decker BOOLEAN,
             coach BOOLEAN)''')

data, next_url = get_data(url)
insert_data(data)

while next_url:
    data, next_url = get_data(next_url)
    insert_data(data)

conn.commit()
conn.close()
