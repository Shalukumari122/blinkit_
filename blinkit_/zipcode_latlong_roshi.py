import pymysql
from curl_cffi import requests
import pandas as pd
import json
from time import sleep

from blinkit_.items import BlinkitItem


# Database setup
db = pymysql.connect(
    host='localhost',  # e.g., 'localhost'
    user='root',  # e.g., 'root'
    password='actowiz',
    database='zepto_'  # e.g., 'test_db'
)
cursor = db.cursor()

# Create a table if it doesn't exist
plQuery = ''' CREATE TABLE IF NOT EXISTS blinkit_lat_long_roshi(
                                                     id INT AUTO_INCREMENT PRIMARY KEY,
                                                     `pincode` VARCHAR(50) UNIQUE,
                                                     city VARCHAR(50),
                                                     lat VARCHAR(250),
                                                     `long` VARCHAR(250),
                                                     serviceable VARCHAR(250),
                                                     Status VARCHAR(50) DEFAULT 'Pending'
                                                     )
                                                   '''
cursor.execute(plQuery)
db.commit()

df_pincodes = pd.read_excel('C:\Shalu\LiveProjects\blinkit_\input_files\blinkit_roshi.xlsx')
pincodes_list = df_pincodes.values.tolist()
pincodes_list = df_pincodes.apply(lambda row: ','.join(row.values.astype(str)), axis=1).tolist()

for pincode in pincodes_list:
# for pincode in ['122022']:
    item = BlinkitItem()
    # pincodes = list(df_pincodes[city])
    # for pincode in ["110007"]:
        # city = 'Mumbai'

    sleep(3)
    # url1 = f"https://blinkit.com/mapAPI/autosuggest_google?query={city} {pincode} ".replace(' ', '%20')
    url1 = f"https://blinkit.com/mapAPI/autosuggest_google?query={pincode} ".replace(' ', '%20')
    # url1 = f"https://blinkit.com/mapAPI/autosuggest_google?query={city.lower()} {pincode}".replace(' ', '%2C')
    headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
                    'app_client': 'consumer_web',
                    'app_version': '52434332',
                    'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
                    'content-type': 'application/json',
                    'device_id': '87433dca-7b1d-4af9-a4ab-1a7fbd22036e',
                    'priority': 'u=1, i',
                    'referer': 'https://blinkit.com/',
                    'rn_bundle_version': '1009003012',
                    'session_uuid': '4a3de0a6-720e-4575-b480-3168f68fac9c',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                    'web_app_version': '1008010016',
                }
    try:
        data1 = requests.get(url=url1,headers=headers).json()
        try:
            predictions_list = data1['predictions']
        except:
            predictions_list = None

        # for predictions in predictions_list:
        for index, predictions in enumerate(predictions_list):
            try:
                placeid = predictions['place_id']
            except:
                placeid = None

            if placeid:
                # url2 = f'https://blinkit.com/mapAPI/place-detail?placeId={placeid}'
                url2 = f'https://blinkit.com/v1/location_info?place_id={placeid}'
                data2 = requests.get(url2,headers=headers).json()
                try:
                    lat = data2['coordinate']['lat']
                    lng = data2['coordinate']['lon']
                    # lat = data2['result']['geometry']['location']['lat']
                    # lng = data2['result']['geometry']['location']['lng']
                except:
                    lat,lng = None,None

                url3 = f"https://blinkit.com/visibility?latitude={lat}&longitude={lng}"
                h2 = {
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
                        'app_client': 'consumer_web',
                        'app_version': '52434332',
                        'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
                        'content-type': 'application/json',
                        'device_id': '87433dca-7b1d-4af9-a4ab-1a7fbd22036e',
                        'priority': 'u=1, i',
                        'referer': 'https://blinkit.com/',
                        'rn_bundle_version': '1009003012',
                        'session_uuid': '4a3de0a6-720e-4575-b480-3168f68fac9c',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                        'web_app_version': '1008010016',
                    }
                data3 = requests.get(url3, headers=h2).json()
                if data3['serviceable']:
                    item['pincode'] = pincode
                    item['lat'] = str(lat)
                    item['`long`'] = str(lng)
                    item['serviceable'] = True

                    field_list = list(item)
                    column_name = ','.join(field_list)
                    values = list(map(lambda x: '%s', list(item.values())))
                    str_values = ','.join(values)
                    insert_query = "INSERT INTO blinkit_lat_long_roshi(" + column_name + ")" + 'VALUES' + "(" + str_values + ")"
                    cursor.execute(insert_query, tuple(list(item.values())))
                    db.commit()
                    print("item inserted successfully============")
                    print(pincode)
                    break

                else:
                    try:
                        item['pincode'] = pincode
                        item['lat'] = str(lat)
                        item['`long`'] = str(lng)
                        item['serviceable'] = False

                        if index + 1 == len(predictions_list):
                            field_list = list(item)
                            column_name = ','.join(field_list)
                            values = list(map(lambda x: '%s', list(item.values())))
                            str_values = ','.join(values)
                            insert_query = "INSERT INTO blinkit_lat_long_roshi(" + column_name + ")" + 'VALUES' + "(" + str_values + ")"
                            cursor.execute(insert_query, tuple(list(item.values())))
                            db.commit()
                            print("item inserted successfully============")
                            print(pincode)
                            # break
                    except:
                        pass
                # else:
                #     if lat and lng:
                #         with open('pincodes.json', 'r') as f:
                #             data = f.read()
                #         data = json.loads(data)
                #         city_data = data[city]
                #         city_data[pincode] = [lat, lng]
                #         data[city] = city_data
                #         with open('pincodes.json', 'w') as e:
                #             e.write(json.dumps(data, indent=2))
            print(pincode)
    except Exception as e:
        print(e)
        print(f"error in {pincode}")
