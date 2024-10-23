import hashlib
import json
import random
import time
from datetime import datetime, date
import os
import re
from time import sleep

import pandas as pd
import pymysql
import scrapy
from curl_cffi import requests
from scrapy.cmdline import execute

from blinkit_.items import Blinkit_roshi


class BlinkitMultiPinSpider(scrapy.Spider):
    name = "blinkit_roshi"

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }
    def __init__(self,start=0, end=0):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='blinkit_')
            self.cur = self.conn.cursor()
            self.start = start
            self.end = end


        except Exception as e:
            print(e)

    def start_requests(self):
        query = f"select * from  blinkit_links_roshi WHERE status ='pending' and  serviceable=1 and id BETWEEN {self.start} AND {self.end}"
        # query = "select * from  blinkit_links_roshi where `Status` ='pending' and `serviceable`=1 limit 1"
        self.cur.execute(query)
        rows = self.cur.fetchall()
        today_date = str(date.today()).replace('-', '_')
        folder_loc = f'C:/Shalu/PageSave/blinkit_weekly/roshi/{today_date}/'
        if not os.path.exists(folder_loc):
            os.makedirs(folder_loc,exist_ok=True)
        for row in rows:
            zipcode = row[0]
            code=zipcode
            zipcode = zipcode.split(',')
            area = zipcode[1]
            zip = zipcode[0]
            city = zipcode[2]
            lat = row[1]
            long = row[2]
            brand_sku_name = row[4]
            Brand_Url = row[5]
            brand_sku = Brand_Url.split('/')[-1]
            unique_id =hashlib.sha256((code +brand_sku_name ).encode()).hexdigest()

            # update_query = """
            #               UPDATE blinkit_links_roshi
            #               SET unique_id = %s
            #               WHERE pincode = %s AND `Roshi Wellness SKUs - as Named in Blinkit` = %s
            #               """
            # self.cur.execute(update_query, (unique_id, code, brand_sku_name))
            # self.conn.commit()
            # print("Row updated successfully.")

            main_loc = folder_loc + f"{unique_id}.html"

            if not os.path.isfile(main_loc):

                meta = {}
                browsers = [
                    "chrome110",
                    "edge99",
                    "safari15_5"
                ]
                meta["impersonate"] = random.choice(browsers)

                cookies = {
                    '_gcl_au': '1.1.876699495.1716446233',
                    '_fbp': 'fb.1.1716446233229.583294513',
                    'gr_1_deviceId': '0eb6178e-fc0d-443f-92d9-02ceec2f70ba',
                    # 'city': 'Bhavnagar',
                    '_gid': 'GA1.2.2133619137.1722503242',
                    '__cf_bm': 'NnfRDXNcusFYVfqDGRoA_ZHyW.Hj56AMwMvWbH5ktgo-1722505118-1.0.1.1-jXgr6YALzv8QPVtQrTx8kn6wq9ilWJmTFc89PjzQhyb5T9rR89AYDRHQoGySietNjyUtQQ7xRzeGzdecAXUbyA',
                    'gr_1_locality': f'{city}',
                    # 'gr_1_locality': 'Bengaluru',
                    '__cfruid': 'cd9f8f6cabe17d09cd93256786015cedef50ba2b-1722505307',
                    '_cfuvid': 'CMn_jFyxZeYv7mQQRIaL2qdTSkZPsGF.PN9q2veJ_Bg-1722505307925-0.0.1.1-604800000',
                    '_ga': 'GA1.1.672859470.1722409155',
                    '_ga_JSMJG966C7': 'GS1.1.1722505140.3.1.1722505460.60.0.0',
                    '_ga_DDJ0134H6Z': 'GS1.2.1722505155.3.1.1722505463.58.0.0',
                    'gr_1_lat': f'{lat}',
                    'gr_1_lon': f'{long}',

                    'gr_1_landmark': '4th%20Cross%20Rd%2C%20Sector%205%2C%20HSR%20Layout%2C%20Bengaluru%2C%20Karnataka%20560102%2C%20India',
                }

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    # 'cookie': '_gcl_au=1.1.876699495.1716446233; _fbp=fb.1.1716446233229.583294513; gr_1_deviceId=0eb6178e-fc0d-443f-92d9-02ceec2f70ba; city=Bhavnagar; _gid=GA1.2.2133619137.1722503242; __cf_bm=NnfRDXNcusFYVfqDGRoA_ZHyW.Hj56AMwMvWbH5ktgo-1722505118-1.0.1.1-jXgr6YALzv8QPVtQrTx8kn6wq9ilWJmTFc89PjzQhyb5T9rR89AYDRHQoGySietNjyUtQQ7xRzeGzdecAXUbyA; gr_1_locality=Bengaluru; __cfruid=cd9f8f6cabe17d09cd93256786015cedef50ba2b-1722505307; _cfuvid=CMn_jFyxZeYv7mQQRIaL2qdTSkZPsGF.PN9q2veJ_Bg-1722505307925-0.0.1.1-604800000; _ga=GA1.1.672859470.1722409155; _ga_JSMJG966C7=GS1.1.1722505140.3.1.1722505460.60.0.0; _ga_DDJ0134H6Z=GS1.2.1722505155.3.1.1722505463.58.0.0; gr_1_lat=12.911862; gr_1_lon=77.6445923; gr_1_landmark=4th%20Cross%20Rd%2C%20Sector%205%2C%20HSR%20Layout%2C%20Bengaluru%2C%20Karnataka%20560102%2C%20India',
                    'pragma': 'no-cache',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                }
                sleep(0.2)
                response2 = requests.request('GET', url=f'{Brand_Url}',
                                             cookies=cookies,
                                             headers=headers,impersonate=random.choice(browsers)

                                             )
                if response2.status_code == 429:
                    retry_after = int(response2.headers.get('Retry-After', 60))  # default to 60 seconds
                    time.sleep(retry_after)


                if response2.status_code == 200:
                    with open(main_loc, 'wb') as file:
                        file.write(response2.text.encode('utf-8'))



                yield scrapy.Request(url="file://" + main_loc, callback=self.parse, meta={"area": area,

                                                                                          "city": city,
                                                                                          "zip":zip,
                                                                                          "unique_id":unique_id,
                                                                                          "brand_sku_name": brand_sku_name,
                                                                                          "brand_sku":brand_sku,
                                                                                           # "zipcode":zipcode,
                                                                                          "Brand_Url": Brand_Url,
                                                                                          },dont_filter=True)

            else:

                yield scrapy.Request(url="file://" + main_loc, callback=self.parse, meta={"area": area,
                                                                                          "city": city,
                                                                                          "zip": zip,
                                                                                          # "zipcode":zipcode,
                                                                                          "brand_sku_name": brand_sku_name,
                                                                                          "brand_sku": brand_sku,
                                                                                          "unique_id": unique_id,                                                                                              "Brand_Url": Brand_Url,
                                                                                          },
                                     dont_filter=True)


    def parse(self, response):
        item=Blinkit_roshi()
        # zipcode=response.meta.get('zipcode')
        data=response.xpath('//script[contains(text(),"window.grofers.PRELOADED_STATE")]/text()').extract_first()
        data=data.split('window.grofers')
        data=data[2].strip()
        data = data.replace(';', '')
        data = data.replace('.PRELOADED_STATE =', '')
        data=json.loads(data)
        if 'ui' in data.keys():
            if 'pdp' in data['ui'].keys():
                if 'product' in data['ui']['pdp'].keys():
                    if 'details' in data['ui']['pdp']['product'].keys():
                        if 'newPrice' in data['ui']['pdp']['product']['details'].keys():
                            brand_selling_price=data['ui']['pdp']['product']['details']['newPrice']
                        if 'oldPrice' in data['ui']['pdp']['product']['details'].keys():
                            brand_mrp=data['ui']['pdp']['product']['details']['oldPrice']
                        if 'offer' in data['ui']['pdp']['product']['details'].keys():
                            brand_discount=data['ui']['pdp']['product']['details']['offer'].replace('OFF','')
                        if 'unit' in data['ui']['pdp']['product']['details'].keys():
                            unit = data['ui']['pdp']['product']['details']['unit']
                        try:
                            quantity = unit.replace('1 pack (', '').replace('Tea Bags)', "").replace('pieces)',
                                                                                                     '').replace(
                                ' g', "").replace('units', '').strip()
                            brand_unit_price = round(int(brand_selling_price) / int(quantity), 2)
                        except Exception as e:

                            pattern = r'\d+'

                            # Find all numbers in the text
                            numbers = re.findall(pattern, unit)

                            # Convert the found numbers to integers
                            numbers = list(map(int, numbers))

                            # Multiply the numbers together
                            if len(numbers) >= 2:
                                result = numbers[0] * numbers[1]
                                brand_unit_price = round(int(brand_selling_price) / int(result), 2)
                            else:
                                print("Not enough numbers to multiply.")

                        item['platform'] = "Blinkit"
                        item['date'] = datetime.now().strftime('%d-%m-%Y')
                        item['pincode'] = response.meta.get('zip')
                        item['city'] = response.meta.get('city')
                        item['Brand'] = "Roshi"
                        item['brand_sku_name'] = response.meta.get('brand_sku_name')
                        item['brand_sku'] = response.meta.get('brand_sku')
                        item['Brand_Url'] = response.meta.get('Brand_Url')
                        stock = response.xpath(
                            '//span[@class="ProductVariants__OutOfStockText-sc-1unev4j-10 bNzmTN"]/text()').extract_first()
                        if stock:
                            item['instock']=0
                            item['brand_mrp'] = "NA"
                            item['brand_selling_price'] = "NA"
                            item['brand_unit_price'] = "NA"
                            item['brand_discount'] = "NA"
                            item['brand_discount_amount'] = "NA"
                            item['unique_id'] = response.meta.get('unique_id')
                            yield item
                        else:
                            item['instock'] = 1
                            item['brand_mrp'] = brand_mrp
                            item['brand_selling_price'] = brand_selling_price
                            item['brand_unit_price'] = brand_unit_price
                            item['brand_discount'] = brand_discount
                            item['brand_discount_amount'] = brand_mrp - brand_selling_price
                            item['unique_id'] = response.meta.get('unique_id')
                            yield item



if __name__ == '__main__':
    execute('scrapy crawl blinkit_roshi  -a start=0 -a end=2100'.split())
