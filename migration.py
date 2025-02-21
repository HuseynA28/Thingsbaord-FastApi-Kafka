import requests
import json
from tqdm import tqdm
import time
from datetime import datetime

import traceback



def main():
    # try:

    # Set credentials to login to different thingsboard instances
    source_url = '***'
    source_username = r'***'
    source_password = r'***'
    dest_url = '**'
    dest_username = r'****'
    dest_password = r'****'

    # source_url = 'https://iot.solexperts.com'
    # source_username = 'mark-andre.muth@it-novum.com'
    # source_password = 'nYfpuv-foxdu5-joqpox'
    # dest_url = 'https://solexperts-dev.bda-itnovum.com'
    # dest_username = 'mark-andre.muth@it-novum.com'
    # dest_password = 'HSbgQt00zMu928M'
    timeline = 7



    # Header for login request
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    # Get JWT-Token for both Instances
    payload = {"username": source_username, "password": source_password}
    source_login = requests.post(f'{source_url}/api/auth/login', json=payload, headers=headers, verify=True,
                            auth=(source_username, source_password)).json()

    source_token = source_login['token']
    print(source_token)
    print(f'\n SourceToken: {source_token}\n\n')
    payload = {"username": dest_username, "password": dest_password}
    dest_login = requests.post(f'{dest_url}/api/auth/login', json=payload, headers=headers, verify=True,
                            auth=(source_username, source_password)).json()
    dest_token = dest_login['token']
    # print(f'\n DestToken: {dest_token}\n\n')



    # Retrieve device IDs from source instance
    string = 'Bearer' + ' ' + f'{source_token}'
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Authorization': string}
    params = {
        "page": 0,
        "pageSize": 999999  # Set the limit to 9999 devices per request
    }
    response = requests.get(f'{source_url}/api/tenant/devices', verify=True, headers=headers, params=params)
    devices_data = json.loads(response.text)

    # print(f"Device_data: {devices_data}")

    #### Filter for certain devices
    '''
    # Path to XLS-file
    xls_path = ''

    # Column name
    spaltenname = 0

    # load data
    data_frame = pd.read_excel(xls_path, header=None)

    # Convert to python list
    spalten_liste = data_frame[spaltenname].tolist()

    # Do the filtering
    filtered_devices = {}
    filtered_devices['data'] = []

    for device in devices_data['data']:
        if device['name'] in spalten_liste:
            filtered_devices['data'].append(device)
        else:
            pass

    devices_data = filtered_devices
    # print(len(devices_data['data']))
    #####
    '''


    # Get devices with same name from target
    string = 'Bearer' + ' ' + f'{dest_token}'
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Authorization': string}
    params = {
        "page": 0,
        "pageSize": 999999  # Set the limit to 9999 devices per request
    }
    response = requests.get(f'{dest_url}/api/tenant/devices', verify=True, headers=headers, params=params)
    target_devices_data = json.loads(response.text)

    # Add target instance id to device
    for device in devices_data['data']:
        print(f"NEW: {device}")
        for target in target_devices_data["data"]:
            if device["name"] == target["name"]:
                device["target_id"] = target["id"]["id"]


    # Set up time range
    # start_ts = str(1706525579000 - (3*7*24*60*60*1000))
    # end_ts = str(1706525579000 - (0*31556952000))
    start_ts = 1735686000000#str(int(datetime.now().timestamp() * 1000) - (3*7*24*60*60*1000))
    end_ts = 1738278000000 #str(int(datetime.now().timestamp() * 1000))
    current_end_ts = start_ts + 86400000
    current_start_ts = start_ts
    # Optional counter for debugging purposes
    count = 0

    try:
        with open("position.json", "r") as file:
            remaining_devices = json.load(file)
    except FileNotFoundError:
        # Initialisiere die Datei mit der gesamten Geräteliste
        with open("position.json", "w") as file:
            json.dump(devices_data["data"], file)
        remaining_devices = devices_data["data"]



    print(f"Remaining: {remaining_devices[0]}")
    print(f'Device_data: {devices_data["data"][0]}')


    # Set up progress bar
    total_devices = len(devices_data['data'])

    total_days = int((end_ts - start_ts) / 86400000)
    with tqdm(total=total_days, desc='Days processed',) as pbar:
        days_processed = int((current_end_ts - start_ts) / 86400000)
        pbar.update(total_days-(total_days-days_processed))

        try:
            # Only one day at a time
            while current_end_ts < end_ts + 86400000:
                print("next day")
                # Configure progress bar to show days
                pbar.set_postfix(days=total_days - days_processed)

                # Iterate through the device IDs and import telemetry data into destination instance
                for device in remaining_devices:
                    print("next device")
                    # print(device)

                    print(f"current end ts: {current_end_ts}")
                    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
                    # Get new Token for every device to aviod errors becuase of an invalid token
                    payload = {"username": source_username, "password": source_password}
                    source_login = requests.post(f'{source_url}/api/auth/login', json=payload, headers=headers,
                                            verify=True, auth=(source_username, source_password)).json()
                    source_token = source_login['token']
                    #print(f'device: {device}')
                    # Get device id and target_id
                    device_id = device['id']['id']
                    print(f'device_id: {device_id}')
                    # try:
                    #     print(f"target_id: {device['target_id']}")
                    target_id = device['target_id']
                    # except:
                    #     continue

                    # Retrieve key data from source instance
                    source_key_endpoint = f'{source_url}/api/plugins/telemetry/DEVICE/{device_id}/keys/timeseries'
                    string = 'Bearer' + ' ' + f'{source_token}'
                    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'X-Authorization': string}
                    response = requests.get(source_key_endpoint, verify=True, headers=headers).json()
                    key_data_json = response
                    key_data = ','.join(response)

                    # Retrieve telemetry data from source instance
                    # Aufteilen der keys in kleinere Gruppen
                    key_list = key_data.split(',')  # keys sind durch Kommas getrennt
                    key_list = set(key_list)
                    key_list = list(key_list)
                    chunk_size = 50  # Maximale Anzahl an Schlüsseln pro Anfrage
                    key_chunks = [','.join(key_list[i:i + chunk_size]) for i in range(0, len(key_list), chunk_size)]

                    # Telemetry-Daten in einer großen Liste sammeln
                    telemetry_list = []  # Liste für alle Telemetriedaten

                    # Daten in kleineren Anfragen abrufen
                    for chunk in key_chunks:
                        source_telemetry_endpoint = f'{source_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?keys={chunk}&startTs={current_start_ts}&endTs={current_end_ts}&limit=100000000'
                        #print(len(source_telemetry_endpoint))  # Debug: Länge der URL anzeigen

                        response = requests.get(source_telemetry_endpoint, verify=True, headers=headers).text

                        if response:  # Sicherstellen, dass die Antwort nicht leer ist
                            telemetry_data = response.split('}]},')  # Aufteilen wie im Originalcode
                            #print(telemetry_data)
                            if telemetry_data == ['{}']:
                                pass
                            else:
                                telemetry_list.extend(telemetry_data)  # Daten zur großen Liste hinzufügen
                                print(telemetry_data)
                        else:
                            print(f"Keine Daten für Schlüsselgruppe: {chunk}")

                    '''
                    source_telemetry_endpoint = f'{source_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?keys={key_data}&startTs={start_ts}&endTs={end_ts}&limit=100000000'
                    print(len(source_telemetry_endpoint))
                    response = requests.get(source_telemetry_endpoint, verify=True, headers=headers).text
                    compare1 = response
                    telemetry_data = response.split('}]},')
                    '''

                    print("Got telemetry data")

                    # Set endpoint for data destination
                    dest_telemetry_endpoint = f'{dest_url}/api/plugins/telemetry/DEVICE/{target_id}/timeseries/ANY?scope=ANY'
                    # Iterate through the telemetry data and import it into the destination instance
                    # List variables to collect data
                    telemetry_data = telemetry_list
                    telemetry_list = []
                    timestamps = []



                    for data in telemetry_data:
                        try:
                            # print(data)
                            parsed_data = json.loads(data)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing data: {e}. Daten: {data}")
                            continue
                        with tqdm(total=len(key_data_json),
                                  desc=f"Verarbeite Telemetrie von {current_start_ts} bis {current_end_ts} für Device {device['id']['id']}") as pbar_tele:
                            for key in set(key_data_json):
                                pbar_tele.set_postfix(key=key)
                                # Check telemetry data contains key

                                if key in parsed_data:

                                    try:
                                        # Iterate through values of key
                                        for v, value in enumerate(json.loads(data)[key]):

                                            # Check if there are multiple values
                                            if type(value) == list:

                                                # Check if there is a previous value for the used timestamp and update values for that timestamp
                                                if value[v]['ts'] in timestamps:

                                                    if value[v]['value'].isdigit():
                                                        value[v]['value'] = float(value['value'])
                                                    else:
                                                        pass

                                                    to_add = {key: value[v]['value']}
                                                    for i, t in enumerate(telemetry_list):
                                                        if value[v]['ts'] != t[i]['ts']:
                                                            pass
                                                        else:
                                                            telemetry_list[i]['values'].update(to_add)

                                                # Else create new entry in telemetry list for that timestamp
                                                else:

                                                    if value[v]['value'].isdigit():
                                                        value[v]['value'] = float(value['value'])
                                                    else:
                                                        pass

                                                    payload = {
                                                        'ts': value[v]['ts'],
                                                        'values': {key: value[v]['value']}

                                                    }
                                                    timestamps.append(value[v]['ts'])
                                                    telemetry_list.append(payload)



                                            # Only a single value for key in telemetry data
                                            else:

                                                # Check if there is a previous value for the used timestamp and update values for that timestamp
                                                if value['ts'] in timestamps:

                                                    if value['value'].isdigit():
                                                        value['value'] = float(value['value'])
                                                    else:
                                                        pass

                                                    to_add = {key: value['value']}
                                                    for i, t in enumerate(telemetry_list):
                                                        if value['ts'] != t['ts']:
                                                            pass
                                                        else:
                                                            telemetry_list[i]['values'].update(to_add)

                                                # Else create new entry in telemetry list for that timestamp
                                                else:

                                                    if value['value'].isdigit():
                                                        value['value'] = float(value['value'])
                                                    else:
                                                        pass

                                                    payload = {
                                                        'ts': value['ts'],
                                                        'values': {key: value['value']}
                                                    }
                                                    timestamps.append(value['ts'])
                                                    telemetry_list.append(payload)
                                        pbar_tele.update(1)
                                    except:
                                        pbar_tele.update(1)
                                        pbar_tele.set_postfix(key=f"error parsing key: {key}")
                                        pass
                                    #print(f'example: {telemetry_list[-1]}')

                                else:
                                    pbar_tele.update(1)
                                    #print(f"Key {key} not present in the data.")
                                    continue


                    # Post data to device
                    payload = {"username": dest_username, "password": dest_password}
                    dest_login = requests.post(f'{dest_url}/api/auth/login', json=payload, headers=headers,
                                            verify=True, auth=(source_username, source_password)).json()
                    dest_token = dest_login['token']

                    # print(telemetry_list)

                    payload = telemetry_list



                    payload = [payload[i:i + 10000] for i in range(0,len(payload),10000)]
                    # print(len(timestamps))
                    dest_string = 'Bearer' + ' ' + f'{dest_token}'
                    dest_headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                                    'X-Authorization': dest_string}


                    for post in payload:
                        with open("test", "w") as file:
                            json.dump(post, file)
                        response = requests.post(dest_telemetry_endpoint, headers=dest_headers, json=post, verify=True)

                        #time.sleep(0.1)
                        source_telemetry_endpoint = f'{dest_url}/api/plugins/telemetry/DEVICE/{target_id}/values/timeseries?keys={key_data}&startTs={current_start_ts}&endTs={current_end_ts}&limit=1000000'
                        response2 = requests.get(source_telemetry_endpoint, verify=True, headers=headers).text
                        compare2 = response2

                        '''
                        if compare1 != compare2:
                            dateiname = f'compare_{device["name"]}_{datetime.now().timestamp()}.json'
                            dateiname2 = '2' + dateiname
                            with open(dateiname, "w") as file:
                                json.dump(compare1, file)
                            with open(dateiname2, "w") as file:
                                json.dump(compare2, file)
                        '''

                current_start_ts = current_end_ts
                current_end_ts = current_end_ts + 86400000
                days_processed = int((current_end_ts - start_ts) / 86400000)
                # print(response.text)

                # Wait a little bit
                time.sleep(1)


                if len(telemetry_list) > 0:
                    count = count + 1
                print(count)

                #remaining_devices.remove(device)

                # Update progress bar
                pbar.update(1)
                with open("position.json", "w") as file:
                    json.dump(remaining_devices, file)



        except Exception as e:
                # Fehler aufgetreten, Gerät bleibt in der Liste
                print(f"Error processing device {device_id}: {e}")


    # except:
    #     print(f"Ein Fehler ist aufgetreten: {e}")
    #     print("Traceback:", traceback.format_exc())
    #     raise


if __name__ == "__main__":

    while True:
        try:
            main()

            print("Skript verlassen")

            with open("position.json", "r") as file:
                remaining_devices = json.load(file)
                remaining_devices.remove(remaining_devices[0])
                if len(remaining_devices) == 0:

                    print("Skript erfolgreich abgeschlossen.")
                    break
                with open("position.json", "w") as file:
                    json.dump(remaining_devices, file)

            time.sleep(1)
            print("Skript wurde ohne auftreten eines Fehlers verlassen ich starte neu") # Schleife verlassen, wenn main erfolgreich ist
        except Exception as e:
            print(f"Neustart aufgrund eines Fehlers: {e}")
            time.sleep(5)  # Warte 5 Sekunden vor dem Neustart
