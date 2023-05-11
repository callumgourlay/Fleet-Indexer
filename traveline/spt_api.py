import requests
import json
import requests
from datetime import date, datetime
import xmltodict
import sqlite3

def get_the_stop(stop):
    # Define and sort the time
    today = date.today(); thetime = datetime.now(); stop_response = []; timings = []; response = []
    if thetime.hour < 10:
        hour = f'0{thetime.hour}'
    else: hour = thetime.hour
    if thetime.minute < 10:
        minute = f'0{thetime.minute}'
    else: minute = thetime.minute
    if thetime.second < 10:
        second = f'0{thetime.second}'
    else: second = thetime.second
    if today.month < 10:
        month = f'0{today.month}'
    else: month = today.month
    if today.day < 10:
        day = f'0{today.day}'
    else: day = today.day

    timestamp = f'{today.year}-{month}-{day}T{hour}:{minute}:{second}'

    # Define the XML To be posted
    xml = f"""<Siri version="1.3" xmlns="http://www.siri.org.uk/siri">
        <ServiceRequest>
            <RequestTimestamp>{timestamp}</RequestTimestamp>
                <RequestorRef>Traveline_To_Trapeze</RequestorRef>
                    <StopMonitoringRequest version="1.3">
                        <RequestTimestamp>{timestamp}</RequestTimestamp>
                        <MonitoringRef>{stop}</MonitoringRef>
                </StopMonitoringRequest>
        </ServiceRequest>
    </Siri>"""

    headers = {'Content-Type': 'application/xml'} # set what your server accepts
    # Make the request
    r = requests.post('http://spt-rt-http.trapezenovus.co.uk:8080/', data=xml, headers=headers)
    # Decode the response
    r = r.content.decode('utf-8')
    # Convert the response to JSON
    response = json.loads(json.dumps(xmltodict.parse(r)))
    
    print(response)
    
get_the_stop('60903754')