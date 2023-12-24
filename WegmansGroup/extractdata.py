import csv
import os
import requests
import sqlite3
import json
import pyodbc
import numpy as np
from datetime import datetime
import time
from datetime import date

import pandas as pd
# from unit_uom import uom_uom_pack
# from eprice_pricemult import pricecols
from scrapinghelp import htmlhelper
from multiprocessing.pool import ThreadPool as Pool
import getpass

username = getpass.getuser()

_counter = 0
finaldatalist = []
global upc_dict
upc_dict = {}


def open_db():
    conn = pyodbc.connect("Driver={SQL SERVER};"
                          "Server=rdpdb-scp-1.database.windows.net;"
                          "Database=WebscrapeIntegration;"
                          "uid=rdbatch;pwd=N#mI&7afO5Tp")

    sql_desc = "select  * from Wegmans_ClientUpc"
    try:
        df2 = pd.read_sql(sql_desc, conn)
        df3 = df2.replace(np.nan, '', regex=True)
        upc_dict = df3.set_index('Product_id').T.to_dict('list')
    except:
        time.sleep(2)
        try:
            df2 = pd.read_sql(sql_desc, conn)
            df3 = df2.replace(np.nan, '', regex=True)
            upc_dict = df3.set_index('Product_id').T.to_dict('list')
        except:
            time.sleep(2)
            try:
                df2 = pd.read_sql(sql_desc, conn)
                df3 = df2.replace(np.nan, '', regex=True)
                upc_dict = df3.set_index('Product_id').T.to_dict('list')
            except:
                time.sleep(2)
                df2 = pd.read_sql(sql_desc, conn)
                df3 = df2.replace(np.nan, '', regex=True)
                upc_dict = df3.set_index('Product_id').T.to_dict('list')
                pass
            pass

        pass
    conn.close()
    return upc_dict


open_db()

data_sql = []


class extractdata:
    def clean_cookies(_website):
        os.system("taskkill /im chrome.exe /f")  # close chrome
        conn = sqlite3.connect(
            'C:/Users/' + username + '/AppData/Local/Google/Chrome/User Data/Default/Cookies')  # for make connection

        # point out at the cursor
        c = conn.cursor()

        # create a variable id
        # and assign 0 initially
        id = 0

        # create a variable result
        # initially as True, it will
        # be used to run while loop
        result = True

        # create a while loop and put
        # result as our condition
        while result:

            result = False

            # a list which is empty at first,
            # this is where all the urls will
            # be stored
            ids = []

            # we will go through our database and
            # search for the given keyword
            for rows in c.execute("SELECT creation_utc from cookies"):  # pick all cookies
                # this is just to check all
                # the urls that are being deleted
                # print(rows)

                # we are first selecting the id
                id = rows[0]

                # append in ids which was initially
                # empty with the id of the selected url
                ids.append((id,))

            # execute many command which is delete
            # from urls (this is the table)
            # where id is ids (list having all the urls)
            c.executemany('DELETE from cookies WHERE creation_utc = ?', ids)  # deleet all cookies
            print("Cookies clean succesfully")
            # commit the changes
            conn.commit()

        # close the connection
        conn.close()

    def clean_History(_website: str):

        os.system("taskkill /im chrome.exe /f")
        # establish the connection with
        # history database file which is
        # located at given location
        # you can search in your system
        # for that location and provide
        # the path here
        conn = sqlite3.connect('C:/Users/' + username + '/AppData/Local/Google/Chrome/User Data/Default/History')

        # point out at the cursor
        c = conn.cursor()

        # create a variable id
        # and assign 0 initially
        id = 0

        # create a variable result
        # initially as True, it will
        # be used to run while loop
        result = True

        # create a while loop and put
        # result as our condition
        while result:

            result = False

            # a list which is empty at first,
            # this is where all the urls will
            # be stored
            ids = []

            # we will go through our database and
            # search for the given keyword
            for rows in c.execute("SELECT id,url FROM urls\
            WHERE url LIKE '%" + _website + "%'"):
                # this is just to check all
                # the urls that are being deleted
                # print(rows)

                # we are first selecting the id
                id = rows[0]

                # append in ids which was initially
                # empty with the id of the selected url
                ids.append((id,))

            # execute many command which is delete
            # from urls (this is the table)
            # where id is ids (list having all the urls)
            c.executemany('DELETE from urls WHERE id = ?', ids)
            print("History clean succesfully open chrome and select location")
            # commit the changes
            conn.commit()

        # close the connection
        conn.close()

    def extractdata(_Zip: str, _Cookie: str, _user_context: str, _Url: str, _JobNumber: str, _Store_id: str, _Address: str):
        _website = _Url
        global _counter
        _counter = 0
        global finaldatalist
        finaldatalist = []
        global upc_dict
        upc_dict = {}
        global data_sql
        data_sql = []
        # print("ExtractData Started : " + str(datetime.now()))
        filepath = os.path.dirname(__file__) + "/Psv/"
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        filepath = filepath + str(_JobNumber) + ".PSV"

        if os.path.exists(filepath):
            os.remove(filepath)

        # this line add header in finaldatalist
        finaldatalist.append(htmlhelper.addheader(filepath))
        upc_dict = ""
        if 'wegmans' in _website:
            upc_dict = open_db()
            pass

        # --------------------------------------------------------------------------
        Location_addrss = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/LocationLog.txt"
        if os.path.exists(Location_addrss):
            with open(Location_addrss) as Location_addrss:
                try:
                    Location_addrss = json.load(Location_addrss)
                except Exception as e:
                    print(e)
            # extra1 = Location_addrss['context']['shopping_mode']
            # extra2 = Location_addrss['context']['storeNumber']
            # address = Location_addrss['store']['address']['address1']
            # postal_code = Location_addrss['store']['address']['postal_code']
            # extra3 = str(address) + ", " + str(postal_code)
            # extra1 = Location_addrss['user']['context']['shopping_mode']
            # extra2 = Location_addrss['user']['store']['id']
            # address = Location_addrss['user']['store']['address']['address1']
            # postal_code = Location_addrss['user']['store']['address']['postal_code']
            # extra3 = str(address) + ", " + str(postal_code)
            if 'wegmans' in _website  or 'foodlion' in _website or 'sprout' in _website or 'pricechopper' in _website or 'topsmarkets' in _website:
                extra1 = Location_addrss['user']['context']['shopping_mode']
                extra2 = Location_addrss['user']['store']['id']
                address = Location_addrss['user']['store']['address']['address1']
                postal_code = Location_addrss['user']['store']['address']['postal_code']
                extra3 = str(address) + ", " + str(postal_code)
            else:
                extra1 = Location_addrss['context']['shopping_mode']
                extra2 = Location_addrss['context']['storeNumber']
                address = Location_addrss['store']['address']['address1']
                postal_code = Location_addrss['store']['address']['postal_code']
                extra3 = str(address) + ", " + str(postal_code)
        # --------------------------------------------------------------------------

        filename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/mainlink_" + str(_JobNumber) + ".txt"
        infile = open(filename, 'r')
        mainlink_list = json.load(infile)
        infile.close()

        pool_size = 100
        pool = Pool(pool_size)
        for link in mainlink_list:
            start = 1
            # pool.apply_async(extractdata.readdatafromfile,(link, start, _website, upc_dict, _Zip, _Cookie, _JobNumber, filepath, extra1, extra2, extra3, postal_code, _Store_id, _Address, address))
            extractdata.readdatafromfile(link, start, _website, upc_dict, _Zip, _Cookie, _JobNumber, filepath, extra1, extra2, extra3, postal_code,_Store_id, _Address, address)
        pool.close()
        pool.join()
        f = open(filepath, "w", encoding='utf-8')
        f.writelines(finaldatalist)
        f.close()

        # htmlhelper.insert_data('WegmansGroup_data',data_sql)
        htmlhelper.insert_data('WegmansBrowser_data', data_sql)
        print("ExtractData End")
        try:
            extractdata.clean_cookies(_website)
            extractdata.clean_History()
        except Exception as e:
            print(e)

    def readdatafromfile(link, start: int, _website: str, upc_dict: {}, _Zip: str, _Cookie: str, _JobNumber: str,
                         filepath: str, extra1, extra2,extra3, postal_code, _Store_id, _Address, address):
        id = link["id"]
        category = link["cat1"]
        if category=="Grocery > Beverages > Tea > Black Tea":
            value='Z'
        datafilename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/Data_" + str(id) + "_" + str(
            start) + ".txt"
        loc = "Data_" + str(id) + "_" + str(start) + ".txt"
        if os.path.exists(datafilename):
            with open(datafilename) as datafilename:
                try:
                    datafromfile = json.load(datafilename)
                except Exception as e:
                    print(e)
            _Date = ''
            json_data = datafromfile[1]['items']
            placement_product = []
            try:
                placements = datafromfile[1]["placements"]
                for x in range(len(placements)):
                    try:
                        list = placements[x]["product"]
                        placement_product.append(list)
                    except:
                        continue
                json_data.extend(placement_product)
            except Exception as e:
                print(e)
                pass
            else:
                extractdata.product_data(json_data, _JobNumber, upc_dict, _Date, _Zip, category, _website, filepath,extra1, extra2, extra3, postal_code, _Store_id, _Address, address,loc)
            start = start + 1
            extractdata.readdatafromfile(link, start, _website, upc_dict, _Zip, _Cookie, _JobNumber, filepath,extra1, extra2, extra3, postal_code, _Store_id, _Address, address)

    def ExceptionHandler(content, tag):
        try:
            txt_split = tag.split(",")
            if len(txt_split) == 1:
                value = content[tag]
            if len(txt_split) == 2:
                value = content[txt_split[0]][txt_split[1]]
            if len(txt_split) == 3:
                value = content[txt_split[0]][txt_split[1]][txt_split[2]]
            if len(txt_split) == 4:
                value = content[txt_split[0]][txt_split[1]][txt_split[2]][txt_split[3]]
            if len(txt_split) == 5:
                value = content[txt_split[0]][txt_split[1]][txt_split[2]][txt_split[3]][txt_split[4]]
        except:
            value = ""
        return value

    def product_data(json_data, _JobNumber, upc_dict, _Date, _Zip, category, _website, filepath, extra1, extra2, extra3, postal_code, _Store_id, _Address, address,loc):
        global GETUnnit_UOM
        global rows
        global _counter
        global product_count
        # placement_product = []
        today = datetime.now()
        today_date = today.strftime("%m/%d/%Y")

        _website = _website
        item = ""
        item = json_data
        sum = 0

        for it in item:
            try:
                # it = it["store_product"]
                altindicator = ''
                asin = ''
                valid = ''
                sale_price = ''
                price_mult = ''
                EIndicator = ''
                price = ''
                ealtprice = ''
                altprice = ''
                altprice_mult = ''
                pack = ''
                unit = ''
                uom = ''
                upc = ''
                stock_level=''
                extra5=''
                id = extractdata.ExceptionHandler(it, "id")
                if id == None:
                    id = ""
                try:
                    productid_extra5 = str(it['ext_data']['retailer_reference_code'])
                except:
                    productid_extra5=''
                extra5 = productid_extra5
                brand = extractdata.ExceptionHandler(it, "brand_name")
                if brand == None:
                    brand = ""
                if brand != "":
                    brand = brand.replace('é', 'e').replace('é', 'e').replace('™', '').replace('à', 'a').replace('’',
                                                                                                                 '').replace(
                        'ú', 'u')
                    brand = brand.replace('ñ', 'n').replace('í', 'i').replace('ö', 'o').replace('á', 'a').replace('ó',
                                                                                                                  'o').replace(
                        '®', '')
                aisle = extractdata.ExceptionHandler(it, "aisle")

                # print(aisle)



                if aisle == None:
                    aisle = ""

                size = extractdata.ExceptionHandler(it, 'size_string')
                if size == None:
                    size = ""

                if size != "":
                    size_break = size.split(' ')
                    unit = size_break[0]
                    uom = size_break[1]

                if size != "" and 'x' in size:
                    size_break = size.split(' ')
                    unit = size
                    uom = size_break[len(size_break) - 1]
                    unit = unit.replace(uom, '')
                if 'pk' in uom.lower() or 'pack' in uom.lower() or 'pkg' in uom.lower() or 'pkt' in uom.lower():
                    pack = unit
                    unit=''
                    uom = ''
                image_link = extractdata.ExceptionHandler(it, "images,tile,large")
                description = extractdata.ExceptionHandler(it, "name").replace("|", "-").replace('�', '').replace('®',
                                                                                                                  '').replace(
                    '‘N’', 'N').replace('ñ', 'n').replace('é', 'e').replace('è', '').replace('”', '')
                description = description.replace('™', '').replace('’', '').replace('à', 'a').replace('†', '').replace(
                    'á',
                    'a').replace(
                    'É', 'E').replace('ó', 'o').replace('ö', 'o').replace(' ', ' ')
                description = description.replace(' – ', ' - ').replace('â', 'a').replace('ú', 'u').replace('ç',
                                                                                                            'c').replace(
                    ' — ', ' - ').replace('í', 'i').replace('–', '-').replace('¿', '').replace('Â', 'A')





                if 'wegmans' in _website:
                    try:
                        productid = extractdata.ExceptionHandler(it, "ext_id")
                        orig_upc = upc_dict[productid][0]
                        upc = orig_upc
                        aisle = extractdata.ExceptionHandler(it, "aisle")
                    except Exception as ae:
                        orig_upc = ""
                elif 'foodlion' in _website:
                    try:
                        productid = extractdata.ExceptionHandler(it, "ext_id")
                        orig_upc = upc_dict[productid][0]
                        upc = orig_upc
                        aisle = extractdata.ExceptionHandler(it, "aisle")
                    except Exception as ae:
                        orig_upc = ""
                else:
                    orig_upc = str(extractdata.ExceptionHandler(it, "ext_id"))
                    upc = orig_upc[0:-1]

                if orig_upc == '14345965':
                    v=8


                try:
                    values = GETUnnit_UOM.get_Unit_Uom(size)
                    pack = values[0]
                    if (pack == ""):
                        pack = '1'

                except:
                    pass
                if (pack == ""):
                    pack = '1'

                average_weight = extractdata.ExceptionHandler(it, "average_weight")
                average_uom = extractdata.ExceptionHandler(it, "average_uom")
                if average_uom == None:
                    average_uom = ""
                uomprice = extractdata.ExceptionHandler(it, "uom_price,price")
                if uomprice != None:
                    uomprice = "{:.2f}".format(float(uomprice))
                uomprice_uom = extractdata.ExceptionHandler(it, "uom_price,uom")
                promo_tag = extractdata.ExceptionHandler(it, 'promo_tag')
                display_uom = extractdata.ExceptionHandler(it, 'display_uom')
                if display_uom == None:
                    display_uom = ""
                pageimage = _website + "/product/" + id
                base_quantity = extractdata.ExceptionHandler(it, "base_quantity")

                sale_quantity = extractdata.ExceptionHandler(it, "sale_quantity")
                if sale_quantity == None:
                    sale_quantity = extractdata.ExceptionHandler(it, "loyalty_quantity")

                base_price = extractdata.ExceptionHandler(it, 'base_price')
                if base_price != None:
                    base_price = "{:.2f}".format(float(base_price))

                if base_quantity != None and base_quantity > 1:
                    base_price = str(base_quantity) + " for $" + str(base_price)

                sale_price = extractdata.ExceptionHandler(it, "sale_price")
                if sale_price == None:
                    sale_price = extractdata.ExceptionHandler(it, "loyalty_price")

                if sale_price != None:
                    sale_price = "{:.2f}".format(float(sale_price))

                if sale_price == "0.00":
                    sale_price = None

                if sale_quantity != None and sale_quantity > 1:
                    sale_price = str(sale_quantity) + " for $" + str(sale_price)

                if average_uom.lower().__contains__("lb") and average_uom != "" and average_uom != "":
                    base_price = str(str(uomprice) + "/" + uomprice_uom)
                    if promo_tag != None:
                        price = base_price
                        try:
                            promo_tag = promo_tag.replace('Save $', '').replace('/lb', '').replace('/per lb', '')
                            promo_tag = "{:.2f}".format(float(promo_tag))
                            altprice = float(uomprice) + float(promo_tag)
                            altprice = "{:.2f}".format(float(altprice))
                            altprice = str(altprice) + "/lb"
                        except:
                            pass

                    else:
                        price = base_price
                    description = description + "(Avg. " + str(average_weight) + average_uom + ")"

                if display_uom.lower().__contains__("lb"):
                    if sale_price != None:
                        price = str(sale_price) + "/" + display_uom
                        altprice = str(base_price) + "/" + display_uom
                    else:
                        price = base_price + "/" + display_uom

                if sale_price != None and 'lb' not in price:
                    price = sale_price
                    altprice = base_price

                if price == '' and base_price != None:
                    price = base_price

                if 'for' in str(price):
                    price_break = price.split('for $')
                    eprice = price_break[1]
                    price_mult = price_break[0]
                if 'for' in str(altprice):
                    price_break = altprice.split('for $')
                    ealtprice = price_break[1]
                    altprice_mult = price_break[0]

                if 'for' not in str(price) and str(price) != None and str(price) != '':
                    eprice = str(price).replace('$', '').replace('/lb.', '').replace('/lb', '').replace('/per lb',
                                                                                                        '').replace(
                        'bag',
                        '').replace(
                        '/each', '')
                    price = '$' + str(price)
                    price_mult = "1"
                if 'for' not in str(altprice) and altprice != None and altprice != '':
                    ealtprice = str(altprice).replace('$', '').replace('/lb.', '').replace('/lb', '').replace('/per lb',
                                                                                                              '').replace(
                        'bag', '').replace('/each', '')

                    altprice = '$' + str(altprice)
                    altprice_mult = "1"
                # if "topsmarkets" in _website:
                #     if category != "Tops Pure Cane Granulated Sugar":
                #         category = ''
                #         category_arr = extractdata.ExceptionHandler(it, "categories")
                #         for x in range(len(category_arr)):
                #             if x == 0:
                #                 category = category_arr[x]['name'].replace("|", "-")
                #             else:
                #                 category = category + " > " + category_arr[x]['name'].replace("|", "-")
                #     else:
                #         category = category
                # else:
                #     category = ''
                #     category_arr = extractdata.ExceptionHandler(it, "categories")
                #     for x in range(len(category_arr)):
                #         if x == 0:
                #             category = category_arr[x]['name'].replace("|", "-")
                #         else:
                #             category = category + " > " + category_arr[x]['name'].replace("|", "-")

                saleenddate = extractdata.ExceptionHandler(it, "sale_end_date")
                if saleenddate == None:
                    saleenddate = it["loyalty_end_date"]

                if saleenddate != None:
                    saleenddate_break = saleenddate.split('T')
                    saleenddate = saleenddate_break[0]

                    saleenddate_break = saleenddate.split('-')
                    saleenddate = saleenddate_break[1] + "/" + saleenddate_break[2] + "/" + saleenddate_break[0]

                    try:
                        date_format = "%m/%d/%Y"
                        try:
                            d1 = datetime.strptime(saleenddate, date_format)
                            d2 = datetime.strptime(today_date, date_format)
                        except Exception as e:
                            print(e)
                        delta = d1 - d2

                        if delta.days <= 29:
                            EIndicator = EIndicator + 'A'
                        if delta.days > 400:
                            saleenddate = ""
                    except Exception as e:
                        print(e)

                if 'lb' in price and altprice == '':
                    value = 2
                if (price != None and altprice != ''):
                    if 'A' not in EIndicator:
                        EIndicator = EIndicator + "*"
                if price == None:
                    price = ""
                if 'lb' in str(price).lower():
                    EIndicator = EIndicator + "W"
                if altprice == None:
                    altprice = ""
                if 'lb' in str(altprice).lower():
                    altindicator = "W"

                User_rating = it["product_rating"]["average_rating"]
                User_rating = "{:.1f}".format(float(User_rating))
                if User_rating == "0.0":
                    User_rating = ''
                if 'wegmans' in _website:
                    if saleenddate != None:
                        EIndicator = EIndicator + 'V'
                if 'foodlion' in _website:
                    if saleenddate != None:
                        EIndicator = EIndicator + 'V'
                # product_count = product_count + 1
                note = brand
                try:
                    promo_tag1 = ""
                    promo_tag1 = extractdata.ExceptionHandler(it, "promo_tag")
                    if promo_tag1 == None:
                        promo_tag1 = ""
                    if 'each' in promo_tag1.lower() and 'lb' in price.lower():
                        if 'foodlion' in _website:
                            if 'lb' not in altprice:
                                altindicator = ''
                                altprice = ''
                                ealtprice = ''
                                altprice_mult = ''
                        else:
                            altindicator = ''
                            altprice = ''
                            ealtprice = ''
                            altprice_mult = ''
                except Exception as e:
                    print(e)
                    pass

                if 'foodlion' in _website:
                    orig_upc = productid
                    upc = orig_upc
                    User_rating = extractdata.ExceptionHandler(it, "reco_rating")
                    User_rating = "{:.1f}".format(float(User_rating))
                    note = note.replace('®', '')
                    uom = uom.replace('-', '')
                    unit = unit.strip()
                    uom = uom.strip()
                    if 'fl' in unit:
                        unit = unit.replace('fl', '').strip()
                        uom = 'fl oz'
                    if uom == '15':
                        unit = '15'
                        uom = 'ct'
                    if uom == '0.21' and unit !='':
                        uom = ''
                        unit = ''

                    if unit == '4 x' and uom == '8.4':
                        unit = '4 x 8.4'
                        uom = ''
                    if unit == '4 x' and uom == '12':
                        unit = '4 x 12'
                        uom = ''
                    if size != '':
                        if unit != '' and uom == '':
                            size_brk1 = size.split(' ')
                            unit = size_brk1[0]
                            uom = size_brk1[2]
                        if size == '4 x 8.4 8.4' or size == '4 x 12 12' or size == '8.4 8.4':
                            unit = ''
                            uom = ''
                        if unit.isdigit() and uom.isdigit():
                            uom = ''
                            unit = ''
                    if uom == 'b':
                        uom = 'each'

                if 'wegmans' not in _website:
                    productid = ''
                else:
                    if aisle == None:
                        aisle = ''
                    if aisle != '':
                        aisle.strip()

                if "lowesfoods" in _website:
                    upc = orig_upc
                if 'wegmans' in _website:
                    if 'fl' in unit or 'fl' in uom:
                        uom = 'fl oz'
                        unit = unit.replace('fl', '')
                        unit = unit.strip()
                    if uom == '0.11':
                        unit = '0.11'
                        uom = 'lbs'
                    if uom == '1':
                        uom = ''
                if "Vegan" in unit:
                    unit = unit.replace("Vegan", "")
                    unit = unit.strip()
                    uom = "Vegan " + str(uom)
                elif "Veg" in unit:
                    unit = unit.replace("Veg", "")
                    unit = unit.strip()
                    uom = "Veg " + str(uom)
                elif "Chewable" in unit:
                    unit = unit.replace("Chewable", "")
                    unit = unit.strip()
                    uom = "Chewable " + str(uom)
                _zip = _Zip
                job_number = _JobNumber

                if 'pricechopper' in _website:
                    orig_upc = productid_extra5
                    upc = orig_upc
                    productid = id
                    User_rating = extractdata.ExceptionHandler(it, "reco_rating")
                    User_rating = "{:.1f}".format(float(User_rating))
                    try:
                        if size != None:
                            if ' x ' in size:
                                size_brk = size.split(' x ')
                                pack = size_brk[0]
                                unit = size_brk[1]
                            if ' ' in unit:
                                unit_brk = unit.split(' ')
                                unit = str(unit_brk[0])
                                uom = str(unit_brk[1])
                            elif size != None:
                                size_br = size.split(' ')
                                unit = size_br[0]
                                uom = size_br[1]
                            else:
                                pack = '1'
                            if unit == '8.4' and uom == '8.4':
                                unit = ''
                                uom = ''
                            if unit == '12' and unit == '12':
                                unit = ''
                                uom = ''
                            if uom == '':
                                unit = ''
                            if unit == '4' and uom == '-':
                                unit = ''
                                uom = ''
                    except Exception as e:
                        print(e)
                    if size == None:
                        size = ''
                    if unit == '' and uom == '' and '12 fl oz' in size:
                        unit = '12'
                        uom = 'fl oz'
                    if '12 oz' in size and unit == '' and uom == '':
                        unit = '12'
                        uom = 'oz'

                    if unit == '' and uom == '' and 'x 12' not in size and size != '':
                        sz_brek = size.split(' ')
                        unit = sz_brek[0]
                        uom = sz_brek[1]
                    if '1' in uom or '2' in uom:
                        unit = ''
                        uom = ''
                    if User_rating == '0.0':
                        User_rating = ''
                    if '_' in orig_upc:
                        orig_upc_brk = orig_upc.split('_')
                        orig_upc = orig_upc_brk[1]
                        upc_brk = upc.split('_')
                        upc = upc_brk[1]
                        if promo_tag != None:
                            sale_pric = extractdata.ExceptionHandler(it, "sale_price")
                            if sale_pric == None:
                                sale_pric = extractdata.ExceptionHandler(it, "loyalty_price")


                            if average_uom == '' and sale_pric == None:
                                price = extractdata.ExceptionHandler(it, "base_price")
                                price = float(price)
                                eprice = price
                                promo_tag = promo_tag.replace('Save $', '').replace('/each', '')
                                altprice = float(price) + float(promo_tag)
                                altprice = str(altprice)
                                ealtprice = altprice.replace('$', '').replace('/lb', '')
                                altprice = "{:.2f}".format(float(altprice))
                                price = str(price)
                                price = '$' + str(price)
                                altprice = str(altprice)
                                altprice = '$' + altprice

                    if uom == '2.000' or uom == '8.000':
                        uom = ""
                        unit = ""
                    if ' oz Dr.' in unit and uom == "Wt.":
                        unit = unit.replace(" oz Dr.", "")
                        uom = 'oz'
                    if 'fl' in unit:
                        unit = ''
                    if 'oz' in unit:
                        unit = ''
                    if 'per' in uom:
                        uom = 'pound'
                    if 'sq' in uom:
                        uom = 'sq ft'
                    if 'fl' in uom:
                        uom = 'fl oz'
                    if uom == '' and unit != '':
                        uom = 'oz'
                    if "lb" in price and 'W' in EIndicator:
                        unit = '1'
                        uom = 'lb'
                    if unit == '' and uom == 'oz':
                        uom = 'fl oz'
                        size_br1 = size.split(' ')
                        unit1 = size_br1[0]
                        unit2 = size_br1[2]
                        unit = str(unit1) + " x " + str(unit2)
                    if 'sq' in unit:
                        unit = unit.replace('sq', '')
                        uom = 'sq ft'
                    if ealtprice != "":
                        ealtprice = float(ealtprice)
                        ealtprice = "{:.2f}".format(float(ealtprice))

                    if 'lb' not in price and "W" in EIndicator:
                        price = str(price) + "/lb"
                    if 'lb' not in altprice and "W" in altindicator:
                        altprice = str(altprice) + "/lb"
                    asin = it["ic_product_id"]
                    User_rating = ''
                    if saleenddate == None:
                        saleenddate = ""
                    if 'Catering > ' not in category:
                        if '3' in uom or '1n' in uom or '5' in uom:
                            unit = ""
                            uom = ""
                        uom = uom.replace("0z", 'oz')
                        if ' o' in unit:
                            uom = 'flo oz'
                            unit = unit.replace(' o', '')
                        file = loc
                        _counter = _counter + 1
                        listdata = htmlhelper.createpsv(_counter, _website, str(_zip), str(today_date),
                                                        image_link,
                                                        str(price), price_mult, str(eprice),
                                                        EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                        altindicator,
                                                        str(pack), str(unit).strip(), str(uom).strip(),
                                                        "", productid, str(upc).replace("1_", ""), asin,
                                                        category.replace('é', 'e'),
                                                        description.replace('"', '"'), str(note), User_rating,
                                                        str(valid),
                                                        pageimage, aisle, str(orig_upc).replace("1_", ""), job_number,
                                                        saleenddate, '', extra1, extra2, extra3, '', extra5, '', '', '', '',file)
                        print(listdata)
                        data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date),
                                                                 image_link,
                                                                 str(price), price_mult, str(eprice),
                                                                 EIndicator, altprice, str(altprice_mult),
                                                                 str(ealtprice),
                                                                 altindicator,
                                                                 str(pack), str(unit).strip(), str(uom).strip(),
                                                                 "", productid, str(upc).replace("1_", ""), asin,
                                                                 category.replace('é', 'e'),
                                                                 description.replace('"', '"'), str(note), User_rating,
                                                                 str(valid),
                                                                 pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                                 job_number,
                                                                 saleenddate, '', extra1, extra2, extra3, '', extra5, '', '', '',
                                                                 '',file)
                        finaldatalist.append(listdata)
                        data_sql.append(data_tuple)

                if 'oz' in unit and 'box' in uom:
                    unit = unit.replace('oz', '')
                    uom = uom.replace('box', 'oz')


                if uom == '2.000' or uom == '8.000':
                    uom = ""
                    unit = ""
                if 'topsmarkets' in _website:
                    User_rating = it["reco_rating"]
                    User_rating = "{:.1f}".format(float(User_rating))
                    if User_rating == "0.0":
                        User_rating = ''
                    orig_upc = it["ext_id"]
                    upc = orig_upc
                if aisle != None:
                    aisle = aisle.strip()
                if note != None:
                    note = note.strip()
                if saleenddate == None:
                    saleenddate = ""
            except Exception as e:
                print(e, "advfsggggd")
                pass
            if ' oz Dr.' in unit and uom == "Wt.":
                unit = unit.replace(" oz Dr.", "")
                uom = 'oz'
            if 'fl' in unit:
                unit = ''
            if 'oz' in unit:
                unit = ''
            if 'per' in uom:
                uom = 'pound'
            if 'sq' in uom:
                uom = 'sq ft'
            if 'fl' in uom:
                uom = 'fl oz'
            if uom == '' and unit != '':
                uom = 'oz'
            if "lb" in price and 'W' in EIndicator:
                unit = '1'
                uom = 'lb'

            if 'pricechopper' not in _website and "topsmarkets" not in _website:
                if 'fresh' in _website:
                    #orig_upc = '1234567'
                    if len(orig_upc) > 5:
                        upc = orig_upc[0: -1]
                    else:
                        upc = orig_upc
                asin=''
                try:
                    asin = it["ic_product_id"]
                except Exception as e:
                    asin = ''
                    print(e)

                try:
                    if unit == '' and uom == 'oz':
                        uom = 'fl oz'
                        size_br1 = size.split(' ')
                        unit1 = size_br1[0]
                        unit2 = size_br1[2]
                        unit = str(unit1) + " x " + str(unit2)
                    if 'sq' in unit:
                        unit = unit.replace('sq', '')
                        uom = 'sq ft'
                except Exception as e:
                    print(e)
                if '3' in uom or '1n' in uom or '5' in uom:
                    unit = ""
                    uom = ""
                uom = uom.replace("0z", 'oz')
                if ' o' in unit:
                    uom = 'flo oz'
                    unit = unit.replace(' o', '')
                if 'wegmans' in _website:
                    if 'see store associate' in aisle.lower():
                        EIndicator = EIndicator + "P"

                    # if 'may not be available' in aisle.lower() :

                    availability = extractdata.ExceptionHandler(it, "availability")
                    stock_level = availability["stock_level"]


                    if stock_level==3:
                        EIndicator = EIndicator + "X"

                    if extra2 == _Store_id:
                        try:
                           if unit == "" and 'et' in uom and size != "":
                              sz_brk = size.split(" ")
                              unit = sz_brk[0] + " x " + sz_brk[2]
                              uom = "oz"
                           if unit == "" and uom == "fl oz"  and size != "":
                              sz_brk = size.split(" ")
                              unit = sz_brk[0] + " x " + sz_brk[2]
                        except:
                           pass
                        file = loc
                        _counter = _counter + 1
                        listdata = htmlhelper.createpsv(_counter, _website, str(_zip), str(today_date), image_link, str(price),
                                                        price_mult, str(eprice), EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                        altindicator, str(pack), str(unit).strip(), str(uom).strip(), "", productid,
                                                        str(upc).replace("1_", ""), asin, category.replace('é', 'e'),
                                                        description.replace('"', '"'), str(note), User_rating, str(valid),
                                                        pageimage, aisle, str(orig_upc).replace("1_", ""), job_number,
                                                        saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                        print(listdata)
                        data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date), image_link, str(price),
                                                                 price_mult, str(eprice), EIndicator, altprice, str(altprice_mult),
                                                                 str(ealtprice), altindicator, str(pack), str(unit).strip(), str(uom).strip(),
                                                                 "", productid, str(upc).replace("1_", ""), asin, category.replace('é', 'e'),
                                                                 description.replace('"', '"'), str(note), User_rating, str(valid),
                                                                 pageimage, aisle, str(orig_upc).replace("1_", ""), job_number,
                                                                 saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                        finaldatalist.append(listdata)
                        data_sql.append(data_tuple)
                else:
                    if 'foodlion' in _website:

                        User_rating = ''
                    file = loc

                    _counter = _counter + 1
                    listdata = htmlhelper.createpsv(_counter, _website, str(_zip), str(today_date), image_link, str(price), price_mult,
                                                    str(eprice), EIndicator, altprice, str(altprice_mult), str(ealtprice), altindicator,
                                                    str(pack), str(unit).strip(), str(uom).strip(), "", productid, str(upc).replace("1_", ""),
                                                    asin, category.replace('é', 'e'), description.replace('"', '"'), str(note),
                                                    User_rating, str(valid), pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                    job_number, saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                    print(listdata)
                    data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date), image_link,
                                                             str(price), price_mult, str(eprice),
                                                             EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                             altindicator,
                                                             str(pack), str(unit).strip(), str(uom).strip(),
                                                             "", productid, str(upc).replace("1_", ""), asin,
                                                             category.replace('é', 'e'),
                                                             description.replace('"', '"'), str(note), User_rating,
                                                             str(valid),
                                                             pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                             job_number,
                                                             saleenddate, '', extra1, extra2, extra3, size, extra5, '', '',
                                                             '', '',file)
                    finaldatalist.append(listdata)
                    data_sql.append(data_tuple)

            if "topsmarkets" in _website and 'Catering > ' not in category:
                if '3' in uom or '1n' in uom or '5' in uom:
                    unit = ""
                    uom = ""
                uom = uom.replace("0z", 'oz')
                if ' o' in unit:
                    uom = 'flo oz'
                    unit = unit.replace(' o', '')
                User_rating = ''
                asin = it["ic_product_id"]
                aisle = ""
                if unit == '' and uom == 'oz':
                    uom = 'fl oz'
                    size_br1 = size.split(' ')
                    unit1 = size_br1[0]
                    unit2 = size_br1[2]
                    unit = str(unit1) + " x " + str(unit2)
                if 'sq' in unit:
                    unit = unit.replace('sq', '')
                    uom = 'sq ft'
                if 'fl' in unit:
                    unit = ''
                if 'oz' in unit:
                    unit = ''
                if 'per' in uom:
                    uom = 'pound'
                if 'sq' in uom:
                    uom = 'sq ft'
                if 'fl' in uom:
                    uom = 'fl oz'
                if uom == '' and unit != '':
                    uom = 'oz'
                if "lb" in price and 'W' in EIndicator:
                    unit = '1'
                    uom = 'lb'
                if unit == "" and uom == 'Net':
                    siz_break1 = size.split(' ')
                    unit = str(siz_break1[0]) + " x " + str(siz_break1[1])
                    uom = 'oz'
                if category != "Tops Pure Cane Granulated Sugar" and category != '':
                    file = loc

                    _counter = _counter + 1
                    listdata = htmlhelper.createpsv(_counter, _website, str(_zip), str(today_date), image_link, str(price),
                                                    price_mult, str(eprice), EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                    altindicator, str(pack), str(unit).strip(), str(uom).strip(),
                                                    "", productid, str(upc).replace("1_", ""), asin,
                                                    category.replace('é', 'e'),
                                                    description.replace('"', '"'), str(note), User_rating, str(valid),
                                                    pageimage, aisle, str(orig_upc).replace("1_", ""), job_number,
                                                    saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                    print(listdata)
                    data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date),
                                                             image_link,
                                                             str(price), price_mult, str(eprice),
                                                             EIndicator, altprice, str(altprice_mult),
                                                             str(ealtprice),
                                                             altindicator,
                                                             str(pack), str(unit).strip(), str(uom).strip(),
                                                             "", productid, str(upc).replace("1_", ""), asin,
                                                             category.replace('é', 'e'),
                                                             description.replace('"', '"'), str(note), User_rating,
                                                             str(valid),
                                                             pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                             job_number,
                                                             saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                    finaldatalist.append(listdata)
                    data_sql.append(data_tuple)
                else:
                    if 'fl' in unit:
                        unit = ''
                    if 'oz' in unit:
                        unit = ''
                    if 'per' in uom:
                        uom = 'pound'
                    if 'sq' in uom:
                        uom = 'sq ft'
                    if 'fl' in uom:
                        uom = 'fl oz'
                    if uom == '' and unit != '':
                        uom = 'oz'
                    if "lb" in price and 'W' in EIndicator:
                        unit = '1'
                        uom = 'lb'
                    if category == "Tops Pure Cane Granulated Sugar" and "/product/39228" in pageimage:
                        file = loc

                        _counter = _counter + 1
                        listdata = htmlhelper.createpsv(_counter, _website, str(_zip), str(today_date), image_link, str(price),
                                                        price_mult, str(eprice), EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                        altindicator, str(pack), str(unit).strip(), str(uom).strip(), "", productid,
                                                        str(upc).replace("1_", ""), asin, category.replace('é', 'e'),
                                                        description.replace('"', '"'), str(note), User_rating, str(valid),
                                                        pageimage, aisle, str(orig_upc).replace("1_", ""), job_number,
                                                        saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                        print(listdata)
                        data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date), image_link, str(price),
                                                                 price_mult, str(eprice), EIndicator, altprice, str(altprice_mult),
                                                                 str(ealtprice), altindicator, str(pack), str(unit).strip(),
                                                                 str(uom).strip(), "", productid, str(upc).replace("1_", ""), asin,
                                                                 category.replace('é', 'e'), description.replace('"', '"'), str(note), User_rating,
                                                                 str(valid), pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                                 job_number, saleenddate, '', extra1, extra2, extra3, size, extra5, '', '', '', '',file)
                        finaldatalist.append(listdata)
                        data_sql.append(data_tuple)
