import asyncio
import csv
import decimal
import datetime
import json
import re
import math
import pyodbc
import os
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date
import datetime
from timeit import default_timer
import GETUnnit_UOM
import ast
import arrow
import numpy as np
import pandas as pd
import requests
import time
from scrapinghelp import htmlhelper
global finaldatalist
finaldatalist = []
import os
import sqlite3
import getpass
from scrapinghelp import htmlhelper

username = getpass. getuser()
global data_sql
data_sql = []

# def downloadingTeam():
#     global dictionary_config
#     file = open('configure', "r")
#     contents = file.read()
#     dictionary_config = ast.literal_eval(contents)
#     file.close()
#     return dictionary_config
#
# main_dictionary = downloadingTeam()
# global job_number
# job_number = main_dictionary["Job"]
# global _zip
# _zip = main_dictionary["zip"]
# global _website
# _website = main_dictionary["website"]
# global _storeid
# _storeid = main_dictionary["store_id"]
# global cookie
# cookie = main_dictionary["cookie"]
# global User_context
# User_context = main_dictionary["user_context"]
#
#
# if 'foodlion' or 'lowesfoods' in _website:
#     global _authorization
#     _authorization = main_dictionary["authorization"]

csv_data = []
url_list = []
page_list = []
list_data = []
location_list = []

global mainlink_list
mainlink_list = []

global Data_log
Data_log = []

global rows
rows = []

global count2
count2 = 0
global product_count
product_count = 0
global dir_path
global GETUnnit_UOM
global address
global upc_dict
upc_dict = {}

global store_info
store_info = []

def address_of_location():
    global store_info
    location_url = _website+'/api/v2/user/profile'
    if 'foodlion' or 'lowesfoods' in _website:
        headers = {"cookie": cookie, "authorization": _authorization}

    else:
        headers = {"cookie": cookie, "user_context": User_context}

    loc_source = requests.get(location_url, headers=headers,verify=False)
    location_source = loc_source.json()
    store_info = location_source['store']['address']

def open_db():
    conn = pyodbc.connect("Driver={SQL SERVER};"
                          "Server=95.217.196.125;"
                          "Database=Webscrape;"
                          "uid=sa;pwd=Dup(e)0@98!")

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

def mainlink():
    global _website
    global mainlink_list
    parent_id_list = []
    url_list=[]

    category_url = _website+"/api/v2/categories?store_id="+_storeid
    if 'foodlion' in _website:
        headers = {"cookie": cookie, "user-context": User_context, "authorization": _authorization}
    elif 'lowesfoods' in _website:
        headers = {"cookie": cookie, "user-context": User_context, "authorization": _authorization}
    else:
        headers = {"cookie": cookie, "user-context": User_context}

    if 'pricechopper' in _website:
        content = requests.get(category_url, headers=headers, verify=False)
    else:
        content = requests.get(category_url, headers=headers)
    source = content.json()
    items = source['items']
    for x in range(len(items)):
        name = items[x]['name'].replace(',', '')
        link = _website+"/shop" + items[x]['href']
        id = items[x]['id']
        parentid = items[x]['parent_id']
        parent_id_list.append(parentid)
        list = [name, link, id, parentid]
        url_list.append(list)

    for x in range(len(url_list)):
        id = url_list[x][2]
        if id not in parent_id_list:
            try:
                Cat_name = url_list[x][0]
                Url = url_list[x][1]
                id = url_list[x][2]
                parent_id = url_list[x][3]
                formatting = _website + "/api/v2/store_products?category_id=" + str(
                    id) + "&limit=100&offset=0&sort=popular"
                mainlink1=[Cat_name, link, id, parentid,formatting]
                mainlink_list.append(mainlink1)

            except Exception as e:
                print(e)

        else:
            pass


    dir_path = "../Sprouts/data_log/" + str(_zip)
    file_url = dir_path + "/" + str(_zip) + "_mainlink.txt"

    with open(file_url, 'w') as outfile:
        json.dump(mainlink_list, outfile)
    # writer.writerows(lst)
    # lst.clear()

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



def product_data(json_data, upc_file,_Website, _Zip, job_number, upc_dict):
    # --------------------------------------------------------------------------
    Location_addrss = os.path.dirname(__file__) + "/SubsetOrWeisLog/" + str(job_number) + "/LocationLog.txt"
    extra1 = ""
    extra2 = ""
    extra3 = ""
    if os.path.exists(Location_addrss):
        with open(Location_addrss) as Location_addrss:
            try:
                Location_addrss = json.load(Location_addrss)
            except Exception as e:
                print(e)
        extra1 = Location_addrss['context']['shopping_mode']
        extra2 = Location_addrss['context']['storeNumber']
        address = Location_addrss['store']['address']['address1']
        postal_code = Location_addrss['store']['address']['postal_code']
        extra3 = str(address) + ", " + str(postal_code)
    # --------------------------------------------------------------------------

    global data_sql
    global GETUnnit_UOM
    global _website
    global rows
    #rows = []
    global product_count
    # global upc_dict
    placement_product = []
    _zip = _Zip
    item = ""

    today = datetime.datetime.now()
    today_date = today.strftime("%m/%d/%Y")

    item = json_data["items"]
    try:
        placements = json_data["placements"]
        for x in range(len(placements)):
            list = placements[x]["product"]
            placement_product.append(list)
        item.extend(placement_product)
    except Exception as e:
        print(e)
        pass

    _website = _Website
    sum = 0
    for it in item:
        altindicator = ''
        asin = ''
        valid = ''
        price_mult = ''
        EIndicator = ''
        price = ''
        ealtprice = ''
        altprice = ''
        altprice_mult = ''
        pack = ''
        unit = ''
        uom = ''
        upc = upc_file
        id = ExceptionHandler(it, "id")
        brand = ExceptionHandler(it, "brand_name")
        if brand == None:
            brand = ""
        if brand != "":
            brand = brand.replace('é', 'e').replace('é', 'e').replace('™', '').replace('à', 'a').replace('’', '').replace('ú', 'u')
            brand = brand.replace('ñ', 'n').replace('í', 'i').replace('ö', 'o').replace('á', 'a').replace('ó', 'o').replace('®', '')
        aisle = ExceptionHandler(it, "aisle")
        size = ExceptionHandler(it, 'size_string')



        if size != None:
            size_break = size.split(' ')
            unit = size_break[0]
            uom = size_break[1]


        if size != None and 'x' in size:
            size_break = size.split(' ')
            unit = size

            if 'oz' in unit:
                unit = unit.replace("oz", "")

            uom = size_break[len(size_break)-1]
            if 'fl' in unit:
                unit = unit.replace("fl", "")
                uom = 'fl ' + str(uom)
            else:
                pass
            if 'fl' in uom:
                uom = uom.replace("fl", "fl oz")
            if 'fl oz oz' in uom:
                uom = uom.replace("oz oz", "oz")

            unit = unit.replace(uom, '')
        if 'pk' in uom.lower() or 'pack' in uom.lower() or 'pkg' in uom.lower() or 'pkt' in uom.lower():
            unit = ''
            uom = ''
        image_link = ExceptionHandler(it, "images,tile,large")
        description = ExceptionHandler(it, "name").replace("|", "-").replace('�', '').replace('®', '').replace('‘N’', 'N').replace('ñ', 'n').replace('é', 'e').replace('è', '').replace('”', '')
        description = description.replace('™', '').replace('’', '').replace('à', 'a').replace('†', '').replace('á', 'a').replace('É', 'E').replace('ó', 'o').replace('ö', 'o').replace(' ', ' ')
        description = description.replace(' – ', ' - ').replace('â', 'a').replace('ú', 'u').replace('ç', 'c').replace(' — ', ' - ').replace('í', 'i').replace('–', '-').replace('¿', '').replace('Â', 'A')

        if 'wegmans' in _website:
            try:
                productid = ExceptionHandler(it, "ext_id")
                orig_upc = upc_file
                upc = orig_upc
                aisle = ExceptionHandler(it, "aisle")
            except Exception as ae:
                orig_upc = ""
        elif 'foodlion' in _website:
            try:
                productid = ExceptionHandler(it, "ext_id")
                orig_upc = upc_dict[productid][0]
                upc = orig_upc
                aisle = ExceptionHandler(it, "aisle")
            except Exception as ae:
                orig_upc = ""

        else:
            orig_upc = ExceptionHandler(it, "ext_id")
            upc = orig_upc[0:-1]

        try:
            values = GETUnnit_UOM.get_Unit_Uom(size)
            pack = values[0]
            if (pack == ""):
                pack = '1'

        except:
            pass
        if (pack == ""):
            pack = '1'
        average_weight = ExceptionHandler(it, "average_weight")
        average_uom = ExceptionHandler(it, "average_uom")
        uomprice = ExceptionHandler(it, "uom_price,price")
        if uomprice!=None:
            uomprice = "{:.2f}".format(float(uomprice))
        uomprice_uom = ExceptionHandler(it, "uom_price,uom")
        promo_tag = ExceptionHandler(it, 'promo_tag')
        display_uom = ExceptionHandler(it, 'display_uom')
        pageimage = _website+"/product/"+id
        base_quantity = ExceptionHandler(it, "base_quantity")

        sale_quantity = ExceptionHandler(it, "sale_quantity")
        if sale_quantity == None:
            sale_quantity = ExceptionHandler(it, "loyalty_quantity")

        base_price = ExceptionHandler(it, 'base_price')
        if base_price != None:
            base_price = "{:.2f}".format(float(base_price))

        if base_quantity != None and base_quantity > 1:
            base_price = str(base_quantity) + " for $" + str(base_price)

        sale_price = ExceptionHandler(it, "sale_price")
        if sale_price == None:
            sale_price = ExceptionHandler(it, "loyalty_price")

        if sale_price != None:
            sale_price = "{:.2f}".format(float(sale_price))

        if sale_price == "0.00":
            sale_price = None

        if sale_quantity != None and sale_quantity > 1:
            sale_price = str(sale_quantity) + " for $" + str(sale_price)

        if average_uom.lower().__contains__("lb") and average_uom != "" and average_uom != None:
            base_price = str(str(uomprice) + "/" + uomprice_uom)
            if promo_tag != None:
                price = base_price
                promo_tag = promo_tag.replace('Save $', '').replace('/lb', '').replace('/per lb', '').replace('bag','').replace('/each','')
                promo_tag = "{:.2f}".format(float(promo_tag))
                altprice = float(uomprice) + float(promo_tag)
                altprice = "{:.2f}".format(float(altprice))
                altprice = str(altprice) + "/lb"


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

        if price == '' and base_price!=None:
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
            eprice = str(price).replace('$', '').replace('/lb.', '').replace('/lb', '').replace('/per lb', '').replace('bag','').replace('/each','')
            price = '$'+str(price)
            price_mult = "1"
        if 'for' not in str(altprice) and altprice != None and altprice != '':
            ealtprice = str(altprice).replace('$', '').replace('/lb.', '').replace('/lb', '').replace('/per lb', '').replace('bag','').replace('/each','')

            altprice = '$'+str(altprice)
            altprice_mult = "1"
        category = ''
        category_arr = ExceptionHandler(it, "categories")
        for x in range(len(category_arr)):
            if x == 0:
                category = category_arr[x]['name'].replace("|", "-")
            else:
                category = category+" > "+category_arr[x]['name'].replace("|", "-")

        saleenddate = ExceptionHandler(it, "sale_end_date")
        if saleenddate == None:
            saleenddate = it["loyalty_end_date"]


        if saleenddate != None:
            saleenddate_break = saleenddate.split('T')
            saleenddate = saleenddate_break[0]

            saleenddate_break = saleenddate.split('-')
            saleenddate = saleenddate_break[1] + "/" + saleenddate_break[2] + "/" + saleenddate_break[0]

            try:
                date_format = "%m/%d/%Y"
                d1 = datetime.datetime.strptime(saleenddate, date_format)
                d2 = datetime.datetime.strptime(today_date, date_format)

                delta = d1 - d2

                if delta.days <= 29:
                    EIndicator = EIndicator + 'A'
                if delta.days > 400:
                    saleenddate = ""
            except Exception as e:
                print(e)


        if 'lb' in price and altprice=='':
            value = 2
        if (price!=None and altprice!=''):
            if 'A' not in EIndicator:
                EIndicator = EIndicator+"*"
        if 'lb' in str(price).lower():
            EIndicator = EIndicator+"W"
        if 'lb' in str(altprice).lower():
            altindicator = "W"


        User_rating = it["product_rating"]["average_rating"]
        User_rating = "{:.1f}".format(float(User_rating))
        if User_rating == "0.0":
            User_rating = ''
        if 'wegmans' in _website:
            if saleenddate != None:
                EIndicator = EIndicator + 'V'

        if 'wegmans' in _website:
            if 'see store associate' in aisle.lower():
                EIndicator = EIndicator + "P"

            # if 'may not be available' in aisle.lower() :

            availability = ExceptionHandler(it, "availability")
            stock_level = availability["stock_level"]

            if stock_level == 3:
                EIndicator = EIndicator + "X"


        if 'foodlion' in _website:
            if saleenddate != None:
                EIndicator = EIndicator + 'V'
        product_count = product_count + 1
        note = brand
        try:
            promo_tag1 = ExceptionHandler(it, "promo_tag")
            if 'each' in promo_tag1.lower() and 'lb' in price.lower():
                altindicator = ''
                altprice = ''
                ealtprice = ''
                altprice_mult = ''
        except Exception as e:
            print(e)
            pass

        if 'wegmans' in _website:
            upc = upc[1:]

        if 'foodlion' in _website:
            orig_upc = productid
            upc = orig_upc
            User_rating = ExceptionHandler(it, "reco_rating")
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

            # updated issues

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

        if 'wegmans' not in _website:
            productid = ''
        else:
            try:
                aisle.strip()
            except Exception as e:
                print(e)

        if "lowesfoods" in _website:
            upc = orig_upc


        if 'pricechopper' in _website:
            upc = orig_upc
            User_rating = ExceptionHandler(it, "reco_rating")
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

            if uom == 'fl':
               uom = 'fl oz'

            if unit == '' and uom == '' and 'x 12' not in size and size != '':
                sz_brek = size.split(' ')
                unit = sz_brek[0]
                uom = sz_brek[1]
            if '1' in uom or '2' in uom:
                unit = ''
                uom = ''
            if User_rating == '0.0':
                User_rating = ''
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
            if ealtprice != "":
               ealtprice = "{:.2f}".format(float(ealtprice))
            if 'lb' not in price and "W" in EIndicator:
                price = str(price) + "/lb"
            if 'lb' not in altprice and "W" in altindicator:
                altprice = str(altprice) + "/lb"
            orig_upc = upc_file
            upc = orig_upc
            asin = it["ic_product_id"]

            if 'wegmans' in _website:
                if 'see store associate' in aisle.lower():
                    EIndicator = EIndicator + "P"

                # if 'may not be available' in aisle.lower() :

                availability = product_data.ExceptionHandler(it, "availability")
                stock_level = availability["stock_level"]

                if stock_level == 3:
                    EIndicator = EIndicator + "X"

            if 'Catering > ' not in category:
                User_rating = ''
                if saleenddate == None:
                    saleenddate = ''
                csv_data = [str(product_count), _website, str(_zip), str(today_date), image_link, str(price),
                            price_mult, str(eprice),
                            EIndicator, altprice, str(altprice_mult), str(ealtprice), altindicator, str(pack),
                            str(unit).strip(), str(uom).strip(),
                            "", productid, str(upc).replace("1_", ""), asin, category.replace('é', 'e'),
                            description.replace('"', '"'), str(note), User_rating, str(valid),
                            pageimage, aisle, str(orig_upc).replace("1_", ""), job_number, saleenddate, '', extra1,
                            extra2, extra3,
                            '', '', '',
                            '', '', '', '']
                data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date), image_link, str(price),
                                                         price_mult, str(eprice),
                                                         EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                         altindicator,
                                                         str(pack), str(unit).strip(), str(uom).strip(), "", productid,
                                                         str(upc).replace("1_", ""), asin,
                                                         category.replace('é', 'e'), description.replace('"', '"'),
                                                         str(note), User_rating,
                                                         str(valid), pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                         job_number,
                                                         saleenddate, '', extra1, extra2, extra3, '', '', '', '', '',
                                                         '', '')
                data_sql.append(data_tuple)
                rows.append(csv_data)
                print(rows)

        # if 'wegmans' in _website:
        #     try:
        #         product = ExceptionHandler(it, "ext_id")
        #         orig_upc = upc_dict[product][0]
        #         upc = orig_upc
        #         aisle = ExceptionHandler(it, "aisle")
        #     except Exception as ae:
        #         orig_upc = ""
        orig_upc = upc_file
        upc = orig_upc
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
        if saleenddate == None:
           saleenddate = ""
        asin = ExceptionHandler(it, "ic_product_id")

        if 'pricechopper' not in _website:
            if 'foodlion' in _website or 'tops' in _website:
                User_rating = ''
            csv_data = [str(product_count), _website, str(_zip), str(today_date), image_link, str(price), price_mult,
                        str(eprice),
                        EIndicator, altprice, str(altprice_mult), str(ealtprice), altindicator, str(pack),
                        str(unit).strip(), str(uom).strip(),
                        "", productid, str(upc).replace("1_", ""), asin, category.replace('é', 'e'),
                        description.replace('"', '"'), str(note), User_rating, str(valid),
                        pageimage, aisle, str(orig_upc).replace("1_", ""), job_number, saleenddate, '', extra1, extra2,
                        extra3, '', '', '',
                        '', '', '', '']
            data_tuple = htmlhelper.create_data_list(_website, str(_zip), str(today_date), image_link, str(price),
                                                     price_mult, str(eprice),
                                                     EIndicator, altprice, str(altprice_mult), str(ealtprice),
                                                     altindicator,
                                                     str(pack), str(unit).strip(), str(uom).strip(), "", productid,
                                                     str(upc).replace("1_", ""), asin,
                                                     category.replace('é', 'e'), description.replace('"', '"'),
                                                     str(note), User_rating,
                                                     str(valid), pageimage, aisle, str(orig_upc).replace("1_", ""),
                                                     job_number,
                                                     saleenddate, '', extra1, extra2, extra3, '', '', '', '', '', '',
                                                     '')

            data_sql.append(data_tuple)
            # print(csv_data)

            rows.append(csv_data)
            print(rows)

def extract_data(total_files, variant):
    global dir_path
    global page_list
    global _website

    for j in range(total_files + 1):
        if j != 0:
            global product_count
            printing = ""
            if variant == 0:
                filename = dir_path + "/" + str(j) + "_excel.txt"

            if variant == 1:
                filename = dir_path + "/" + str(j) + "page_excel.txt"
            try:
                with open(filename) as json_file:
                    try:
                        data = json.load(json_file)
                    except Exception as e:
                        print(e)
                        printing = printing + "," + str(j)
                    json_data = data[2]
                    category = data[0]

                    category_id = data[1]

                    total_count = json_data["item_count"]

                    if total_count <= 100 and variant == 0:
                        product_data(json_data)
                    if variant == 1:
                        product_data(json_data)
                    if total_count >= 100 and variant == 0:
                        formatting = _website+"/api/v2/store_products?category_id=" + str(category_id) + "&limit=" + str(total_count) + "&offset=0&sort=popular"
                        lst = [category, "", category_id, "", formatting]
                        page_list.append(lst)

                    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

            except Exception as ex:
                print(ex)
    dir_path = "../Sprouts/data_log/" + str(_zip)
    file_url = dir_path + "/" + str(_zip) + "page_mainlink.txt"

    with open(file_url, 'w') as outfile:
        json.dump(page_list, outfile)

async def get_data_asynchronous(url_file, variant):
    global store_id
    with open(url_file) as json_file:
        urls_to_fetch = json.load(json_file)

        csvs_to_fetch = []
        if (len(urls_to_fetch)) != 0:
            for i in range(len(urls_to_fetch)):
                csv_list = [urls_to_fetch[i]]
                csvs_to_fetch.append(csv_list)
                # print(csv_list)
                # cat_list.append(urls_to_fetch[i][1])
                # print()
            # print("{0:<30} {1:>20}".format("File", "Completed at"))
            # print(cat_list)
            # print(csvs_to_fetch)

            with ThreadPoolExecutor(max_workers=5) as executor:
                with requests.Session() as session:
                    # Set any session parameters here before calling `fetch`
                    loop = asyncio.get_event_loop()
                    START_TIME = default_timer()
                    # Allows us to pass in multiple arguments to `fetch`

                    tasks = [loop.run_in_executor(executor, fetch, *(session, csv, variant)) for csv in
                             csvs_to_fetch]
                    for response in await asyncio.gather(*tasks):
                        pass

def fetch(session, csv, variant):
    try:
        global store_info
        #print(store_info)
        global cookie
        global Data_log
        global list_data
        global count2
        if 'foodlion' or 'lowesfoods' in _website:
            headers = {"cookie": cookie, "authorization": _authorization}

        else:
            headers = {"cookie": cookie, "user_context": User_context}

        if 'pricechopper' in _website:
           with session.get(csv[0][4], verify=False, headers=headers) as response:
              if response.status_code != 200:
                  pass
              else:
                  jsonresponse = response.json()
                  data = jsonresponse
                  data1 = (csv[0][0], csv[0][2], data, store_info)
                  list_data.append(data1)

                  count2 = count2 + 1
                  print(count2)
                  if variant == 0:
                      filenm = dir_path + "/" + str(count2) + "_excel.txt"
                  if variant == 1:
                      filenm = dir_path + "/" + str(count2) + "page_excel.txt"

                  file_list = [csv[0][0], csv[0][2], filenm]
                  Data_log.append(file_list)

                  with open(filenm, 'w') as outfile:
                      json.dump(data1, outfile)
        else:
            with session.get(csv[0][4], headers=headers) as response:
               if response.status_code != 200:
                 pass
               else:
                  jsonresponse = response.json()
                  data = jsonresponse
                  data1 = (csv[0][0], csv[0][2], data,store_info)
                  list_data.append(data1)
                  count2 = count2 + 1
                  print(count2)
                  if variant == 0:
                      filenm = dir_path + "/" + str(count2) + "_excel.txt"
                  if variant == 1:
                      filenm = dir_path + "/" + str(count2) + "page_excel.txt"

                  file_list = [csv[0][0], csv[0][2], filenm]
                  Data_log.append(file_list)

                  with open(filenm, 'w') as outfile:
                      json.dump(data1, outfile)
    except Exception as ex:
        print(ex)

def data_log():
    global Data_log
    filename = dir_path + "/" + str(
        _zip) + "data_log.txt"
    with open(filename, 'w') as outfile:
        json.dump(Data_log, outfile)

def clean_cookies():
    os.system("taskkill /im chrome.exe /f")  #close chrome
    conn = sqlite3.connect('C:/Users/'+username+'/AppData/Local/Google/Chrome/User Data/Default/Cookies')  # for make connection

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
        for rows in c.execute("SELECT creation_utc from cookies"):   #pick all cookies
            # this is just to check all
            # the urls that are being deleted
            #print(rows)

            # we are first selecting the id
            id = rows[0]

            # append in ids which was initially
            # empty with the id of the selected url
            ids.append((id,))

        # execute many command which is delete
        # from urls (this is the table)
        # where id is ids (list having all the urls)
        c.executemany('DELETE from cookies WHERE creation_utc = ?', ids)  #deleet all cookies
        print("Cookies clean succesfully")
        # commit the changes
        conn.commit()

    # close the connection
    conn.close()

def clean_History():

    os.system("taskkill /im chrome.exe /f")
    # establish the connection with
    # history database file which is
    # located at given location
    # you can search in your system
    # for that location and provide
    # the path here
    conn = sqlite3.connect('C:/Users/'+username+'/AppData/Local/Google/Chrome/User Data/Default/History')

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
        WHERE url LIKE '%"+_website+"%'"):
            # this is just to check all
            # the urls that are being deleted
            #print(rows)

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

def main():
    global _zip
    global dir_path
    global count2
    global rows
    global upc_dict
    upc_dict = {}

    address_of_location()

    dir_path = "../Sprouts/data_log/" + str(_zip)
    try:
        os.mkdir(dir_path)
    except Exception as e:
        pass

    if 'wegmans' in _website:
        upc_dict = open_db()

    mainlink()
    url_file = dir_path + "/" + str(_zip) + "_mainlink.txt"

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(url_file, 0))
    loop.run_until_complete(future)

    extract_data(count2, 0)
    count2 = 0

    page_file = dir_path + "/" + str(_zip) + "page_mainlink.txt"

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(page_file, 1))
    loop.run_until_complete(future)

    extract_data(count2, 1)
    data_log()



    csv_data = "C:\FullPermit\PsvFiles/"+job_number + ".psv"

    header = ["ID", "SITE", "ZIP", "DATE", "IMAGELINK", "PRICE", "PRICE_MULT", "EPRICE", "EINDICATOR",
              "ALTPRICE", "ALTPRICEMULT", "EALTPRICE", "ALTINDICATOR",
              "PACK", "UNIT", "UOM", "SIZE", "PRODUCT_ID", "UPC", "ASIN", "CATEGORY", "DESCRIPTION", "NOTE",
              "RATING", "VALID", "PAGEIMAGE", "AISLE", "ORIG_UPC", "JOB_NUMBER", "SALE_ENDDATE"
              ]

    with open(csv_data, "w", newline='') as data_file:
        data_writer = csv.writer(data_file, delimiter="|")
        data_writer.writerow(i for i in header)
        for i in range(len(rows)):
            try:
                data_writer.writerow(rows[i])
            except:
                pass

    clean_cookies()
    clean_History()


def readfiles(_JobNumber):
    global dir_path

    csv_data = "C:\FullPermit\PsvFiles/" + _JobNumber + ".psv"
    header = ["ID", "SITE", "ZIP", "DATE", "IMAGELINK", "PRICE", "PRICE_MULT", "EPRICE", "EINDICATOR",
              "ALTPRICE", "ALTPRICEMULT", "EALTPRICE", "ALTINDICATOR",
              "PACK", "UNIT", "UOM", "SIZE", "PRODUCT_ID", "UPC", "ASIN", "CATEGORY", "DESCRIPTION", "NOTE",
              "RATING", "VALID", "PAGEIMAGE", "AISLE", "ORIG_UPC", "JOB_NUMBER", "SALE_ENDDATE", "BRAND",
              "EXTRA1", "EXTRA2", "EXTRA3", "EXTRA4", "EXTRA5", "EXTRA6", "EXTRA7", "EXTRA8", "EXTRA9", "LINKID"
              ]

    with open(csv_data, "w", newline='') as data_file:
        data_writer = csv.writer(data_file, delimiter="|")
        data_writer.writerow(i for i in header)
        for i in range(len(rows)):
            try:
                data_writer.writerow(rows[i])
            except:
                pass

    htmlhelper.insert_data('WegmansGroup_data', data_sql)

#main()

#readfiles()
