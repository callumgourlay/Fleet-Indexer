import sqlite3
import pandas as pd
import xmltodict
import re
import os
import shutil
from ftplib import FTP
import zipfile
import config


def import_service_report():
    conn = sqlite3.connect('db1.db')
    c = conn.cursor()
    
    c.execute(f'''DROP TABLE IF EXISTS ServiceReport''')
    
    c.execute('''CREATE TABLE ServiceReport (RowId int, RegionCode text, RegionOperatorCode text, ServiceCode text, LineName text, Description text, StartDate text, NationalOperatorCode text, DataSource text)''')

    # load the data into a Pandas DataFrame
    services = pd.read_csv('servicereport.csv')
    # write the data to a sqlite table
    services.to_sql('ServiceReport', conn, if_exists='append', index = False)
    conn.commit()
    conn.close()
    
def pull_data(filename, operator):
    stops = []
    with open(f'S/{filename}.xml', 'r', encoding='utf-8') as file:
        xmlFile = file.read()
    
    response = xmltodict.parse(xmlFile)
    
    #print(response['TransXChange']['StopPoints']['AnnotatedStopPointRef'])
    worker = filename.split('_')
    
    for each in response['TransXChange']['StopPoints']['AnnotatedStopPointRef']:
        try: 
            stops.append([each['StopPointRef'], each['CommonName'], each['LocalityName'], each['LocalityQualifier'], worker[2], worker[3], filename])
        except KeyError:
            route = str(re.findall(r'\d+', filename)); route = route.replace('[',''); route = route.replace(']',''); route = route.replace("'",'')
            stops.append([each['StopPointRef'], each['CommonName'], 'Locality Unavailable', 'Locality Qualifier Unavailiable', operator, route, filename])
            
    return stops
    
def write_to_db(table, data, tblcreate):
    conn = sqlite3.connect('db1.db')
    c = conn.cursor()
    #c.execute(f'''DROP TABLE IF EXISTS {table}''')
    if tblcreate == True:
        c.execute(f'''CREATE TABLE {table} (StopPointRef INT, CommonName text, LocalityName text, LocalityQualifier text, OperatorCode text, LineName text)''')
    
    c.executemany(f'''INSERT INTO {table} VALUES (?,?,?,?,?,?,?)''', data)
    
    conn.commit()
    conn.close()
    
def write_stops(operator):
    templist = []
    conn = sqlite3.connect("db1.db")
    c = conn.cursor()
    myquery = (f'SELECT ServiceCode FROM ServiceReport WHERE NationalOperatorCode="{operator}";')
    c.execute(myquery)
    rows = c.fetchall()
    for row in rows:
        worker = str(row)
        worker = worker.replace('(','')
        worker = worker.replace(')','')
        worker = worker.replace(',','')
        worker = worker.replace("'",'')
        if operator != 'FGLA' and operator != 'FABD':
            worker = 'SVR' + worker
            templist.append(worker)
        elif worker.endswith('_1') != True:
            worker = worker + '_A'
            templist.append(worker)
        else:
            worker = worker
            templist.append(worker)
    conn.commit()
    conn.close()
        
    
    return templist

def write_stops_to_db(operators):
    for x in operators:
        print(f'Working on {x}')
        stops = write_stops(x)
        for each in stops:
            output = pull_data(each, x)
            write_to_db('Stops', output, False)
            print(f'Finished {each}')
        print(f'Finished {each}')
        
def drop_table(table):
    conn = sqlite3.connect('db1.db')
    c = conn.cursor()
    c.execute(f'''DROP TABLE IF EXISTS {table}''')
    c.execute(f'''CREATE TABLE {table} (StopPointRef INT, CommonName text, LocalityName text, LocalityQualifier text, OperatorCode text, LineName text, ServiceCode text)''')
    conn.commit()
    conn.close()


# FTP server details
server = 'ftp.tnds.basemap.co.uk'
username = config.username
password = config.password

# Directories and file to download
zip_filename = 'S.zip'
zip_filepath = '/TNDSV2.5/'
csv_filename = 'servicereport.csv'
csv_filepath = '/TNDSV2.5/'

# Download the zip file to a specified directory
data_dir = './data'
if not os.path.exists(data_dir):
    os.mkdir(data_dir)
    print('Directory created: ' + data_dir)
os.chdir(data_dir)

ftp = FTP(server)
ftp.login(user=username, passwd=password)
print('Connected to ' + server)
with open(zip_filename, 'wb') as f:
    ftp.retrbinary('RETR ' + zip_filepath + zip_filename, f.write)

print('Downloaded ' + zip_filename)

# Extract the contents of the zip file to a specified directory
zip_extract_dir = 'S'
if not os.path.exists(zip_extract_dir):
    os.mkdir(zip_extract_dir)

with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
    zip_ref.extractall(zip_extract_dir)

# Download the csv file to a specified directory
csv_dir = './'
with open(csv_filename, 'wb') as f:
    ftp.retrbinary('RETR ' + csv_filepath + csv_filename, f.write)

print('Downloaded ' + csv_filename)

print('Finished downloading files')
print('Importing data into database...')
    
import_service_report()
operators = ['FGLA', 'FABD']
drop_table('Stops')
write_stops_to_db(operators)

print('Finished importing data into database')

# Close the FTP connection
ftp.quit()
print('Disconnected from ' + server)

# Remove downloaded files and directory contents
os.remove(os.path.join(csv_dir, csv_filename))
for file in os.listdir(zip_extract_dir):
    file_path = os.path.join(zip_extract_dir, file)
    if os.path.isfile(file_path):
        os.remove(file_path)
os.remove(zip_filename)
print('Removed downloaded files and cleared directory')


print('Beginning Operator Import')

conn = sqlite3.connect('db1.db')
c = conn.cursor()
c.execute(f'''DROP TABLE IF EXISTS operators''')
c.execute(f'''CREATE TABLE Operators (NOCCode TEXT PK, OperatorPublicName text, RefNm TEXT, OpNm TEXT)''')
# load the data into a Pandas DataFrame
operators = pd.read_csv('operators.csv')
operators = operators[['NOCCODE', 'OperatorPublicName', 'RefNm', 'OpNm']]
# write the data to a sqlite table
operators.to_sql('Operators', conn, if_exists='append', index = False)
conn.commit()
conn.close()
print('Finished Operator Import')